import json
import hashlib
import os
import asyncio
import base64
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Optional, List, Tuple, Any
import yaml
from app.db import get_connection, check_write_permission, DB_PATH
from app.config import get_telegram_credentials

def compute_hash(raw_text: str, channel_id: int, message_id: int) -> str:
    """Compute SHA256 hash for deduplication."""
    payload = f"{channel_id}:{message_id}:{raw_text}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def ingest_message(channel_id: int, message_id: int, message_date: datetime, raw_text: str, raw_json: dict) -> bool:
    """
    Ingest a raw message into the DB.
    Idempotent based on channel_id + message_id (via DB unique index).
    Also checks for content duplicates.
    """
    check_write_permission()
    
    conn = get_connection()
    cursor = conn.cursor()
    
    content_hash = compute_hash(raw_text, channel_id, message_id)
    
    # Check for content duplicate
    cursor.execute("SELECT id FROM raw_messages WHERE content_hash = ?", (content_hash,))
    existing = cursor.fetchone()
    duplicate_of = existing["id"] if existing else None
    
    raw_json_str = json.dumps(raw_json, ensure_ascii=False, default=_json_default, sort_keys=True)
    
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO raw_messages
            (channel_id, message_id, message_date, raw_text, raw_json, content_hash, duplicate_of)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (channel_id, message_id, message_date, raw_text, raw_json_str, content_hash, duplicate_of))
        conn.commit()
        if cursor.rowcount == 0:
            return False
        print(f"Ingested msg {channel_id}:{message_id}")
        return True
    except Exception as e:
        print(f"Skipped {channel_id}:{message_id} - {e}")
        return False
    finally:
        conn.close()

def _json_default(obj):
    # datetime/date -> ISO8601 string (UTC 'Z' if tz-aware)
    if isinstance(obj, datetime):
        if obj.tzinfo is not None:
            iso = obj.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            iso = obj.strftime("%Y-%m-%dT%H:%M:%S.%f")
        return iso
    if isinstance(obj, date):
        return obj.isoformat()
    # bytes -> base64 string
    if isinstance(obj, (bytes, bytearray)):
        return base64.b64encode(obj).decode("ascii")
    # set/tuple -> list
    if isinstance(obj, (set, tuple)):
        return list(obj)
    # to_dict()
    to_dict = getattr(obj, "to_dict", None)
    if callable(to_dict):
        try:
            return to_dict()
        except Exception:
            pass
    # fallback
    return str(obj)

def _load_config_from_path(config_path: Optional[str]) -> dict:
    if not config_path:
        return {}
    p = Path(config_path)
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _format_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return "None"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _extract_title(msg: Any) -> str:
    # 1) Webpage title
    media = getattr(msg, "media", None)
    webpage = getattr(media, "webpage", None)
    title = getattr(webpage, "title", None)
    if isinstance(title, str) and title.strip():
        return title.strip()[:120]
    # 2) First non-empty line from message
    text = getattr(msg, "message", None) or getattr(msg, "text", None) or ""
    for line in str(text).splitlines():
        s = line.strip()
        if s:
            return s[:120]
    # 3) Fallback
    return "(no title)"

def _extract_url(msg: Any) -> Optional[str]:
    media = getattr(msg, "media", None)
    webpage = getattr(media, "webpage", None)
    url = getattr(webpage, "url", None)
    return url if isinstance(url, str) else None

def _parse_invite_hash(ref: str) -> Optional[str]:
    """Extract invite hash from https://t.me/+HASH or t.me/joinchat/HASH links."""
    import re
    m = re.search(r't\.me/\+([A-Za-z0-9_-]+)', ref)
    if m:
        return m.group(1)
    m = re.search(r't\.me/joinchat/([A-Za-z0-9_-]+)', ref)
    if m:
        return m.group(1)
    if ref.startswith('+') and len(ref) > 1:
        return ref[1:]
    return None

async def _resolve_channel(client, ref: str):
    """Resolve a channel ref — handles @username, invite links, and numeric IDs."""
    from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
    from telethon.errors import UserAlreadyParticipantError, InviteHashExpiredError

    invite_hash = _parse_invite_hash(ref)
    if invite_hash:
        # Try get_entity directly first (already joined)
        try:
            return await client.get_entity(ref)
        except Exception:
            pass
        # Not joined yet — check invite and join
        try:
            await client(CheckChatInviteRequest(hash=invite_hash))
            result = await client(ImportChatInviteRequest(hash=invite_hash))
            print(f"Joined private channel via invite: {ref}")
            return result.chats[0]
        except UserAlreadyParticipantError:
            # Already joined but get_entity failed — iterate dialogs to find it
            async for dialog in client.iter_dialogs():
                ent = dialog.entity
                invite = getattr(ent, 'invite_link', None) or ''
                if invite_hash in invite:
                    return ent
            raise RuntimeError(f"Already joined but could not locate channel: {ref}")
        except InviteHashExpiredError:
            raise RuntimeError(f"Invite link expired: {ref}")
    # Public @username or numeric ID
    return await client.get_entity(ref)

async def _ingest_telethon(channels: List[str], session_path: str, since: Optional[datetime], until: Optional[datetime], progress_every: int = 200) -> Tuple[int, int]:
    try:
        from telethon import TelegramClient
    except Exception as e:
        raise RuntimeError(f"Telethon not available: {e}")
    api_id, api_hash = get_telegram_credentials()
    client = TelegramClient(session_path, api_id, api_hash)
    fetched_total = 0
    inserted_total = 0
    # Normalize bounds to UTC-aware
    since_utc: Optional[datetime] = None
    until_utc: Optional[datetime] = None
    if since:
        since_utc = since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since.astimezone(timezone.utc)
    if until:
        until_utc = until.replace(tzinfo=timezone.utc) if until.tzinfo is None else until.astimezone(timezone.utc)
    async with client:
        for ref in channels:
            try:
                entity = await _resolve_channel(client, ref)
            except Exception as e:
                print(f"Channel resolve failed: {ref} - {e}")
                continue
            # Start near upper bound; newest first
            async for msg in client.iter_messages(entity, offset_date=until_utc, reverse=False):
                d = msg.date  # Telethon returns UTC naive or aware; normalize next
                if d is None:
                    continue
                # 1) Normalize to UTC-aware datetime
                if d.tzinfo is None:
                    d_utc = d.replace(tzinfo=timezone.utc)
                else:
                    d_utc = d.astimezone(timezone.utc)
                # 2) Enforce strict window (until exclusive)
                if until_utc and d_utc >= until_utc:
                    continue
                # Scanning newest->oldest; break once we go below since
                if since_utc and d_utc < since_utc:
                    break
                text = getattr(msg, "message", None) or getattr(msg, "text", None)
                if not text:
                    continue
                fetched_total += 1
                # Debug print (throttled)
                if os.getenv("RC_DEBUG", "0") == "1":
                    if progress_every <= 1 or (fetched_total % progress_every == 0):
                        title = _extract_title(msg)
                        url = _extract_url(msg)
                        d_iso = d_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                        if url:
                            print(f"[DBG] {ref} | id={msg.id} | {d_iso} | {title} | {url}")
                        else:
                            print(f"[DBG] {ref} | id={msg.id} | {d_iso} | {title}")
                try:
                    raw_json = msg.to_dict() if hasattr(msg, "to_dict") else {"id": msg.id}
                except Exception:
                    raw_json = {"id": msg.id}
                chat_id = getattr(msg, "chat_id", None)
                if chat_id is None:
                    try:
                        chat = await msg.get_chat()
                        chat_id = getattr(chat, "id", None)
                    except Exception:
                        chat_id = None
                if chat_id is None:
                    try:
                        chat_id = getattr(entity, "id", None)
                    except Exception:
                        chat_id = 0
                # 4) Store message_date as UTC naive string "YYYY-MM-DD HH:MM:SS"
                msg_dt_str = d_utc.strftime("%Y-%m-%d %H:%M:%S")
                ok = ingest_message(int(chat_id), int(msg.id), msg_dt_str, text, raw_json)
                if ok:
                    inserted_total += 1
    return fetched_total, inserted_total

def run_ingest(config_path: str, since: Optional[datetime], until: Optional[datetime]):
    cfg = _load_config_from_path(config_path)
    tg = (cfg.get("telegram") or {})
    enabled = bool(tg.get("enabled"))
    session_path = tg.get("session_path") or "data/telethon.session"
    raw_channels = tg.get("channels") or []
    # Support both plain strings ('@channel') and dicts ({name, label, tags})
    channels = [
        ch["name"] if isinstance(ch, dict) else ch
        for ch in raw_channels
    ]
    progress_every = int(tg.get("progress_every", 200))
    print(f"DB: {DB_PATH}")
    print(f"telegram.enabled: {enabled}")
    print(f"channels.count: {len(channels)}")
    print(f"channels.head5: {channels[:5]}")
    print(f"since(UTC): {_format_dt(since)}")
    print(f"until(UTC): {_format_dt(until)}")
    if enabled and len(channels) > 0:
        try:
            fetched, inserted = asyncio.run(_ingest_telethon(channels, session_path, since, until, progress_every=progress_every))
            print(f"Fetched: {fetched}, Inserted: {inserted}")
            if fetched == 0:
                raise RuntimeError("0 fetched. Check channels, date range, or credentials.")
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Ingest failed: {e}")
    else:
        print("Fixture mode allowed (telegram.disabled or no channels). Skipping real ingest.")
