"""
Scan raw_messages and tag articles that match keywords defined in configs/keywords.yaml.
Writes results to article_tags (INSERT OR IGNORE).

Matching rules:
  1. URLs (https?://...) are stripped before matching.
  2. Korean keywords (containing non-ASCII) → case-insensitive substring.
  3. English keyword, ≤3 chars → case-sensitive, word-boundary (ASCII) match.
     e.g. OCI, HBM, WTI, LNG, ESS (avoids "social", "single", etc.)
  4. English keyword, ≥4 chars → case-insensitive, word-boundary match.
     e.g. DRAM, NAND, UFLPA, Wacker
  5. channel_sector: if a channel's sector (from config.yaml) matches a keyword
     group name, all messages from that channel are auto-tagged with
     keyword='[channel_sector]'.

Running re-tags for a group (or all groups) replaces any existing tags in that
group so that the output always reflects current keyword rules.

Usage:
  ALLOW_WRITE=1 python telegram_pipeline/scripts/tag_articles.py
  ALLOW_WRITE=1 python telegram_pipeline/scripts/tag_articles.py --group oci
"""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.db import DB_PATH

DEFAULT_DB = str(DB_PATH)
DEFAULT_KEYWORDS = "configs/keywords.yaml"
DEFAULT_CONFIG = "configs/config.yaml"
DEFAULT_SESSION = "data/telethon.session"

URL_RE = re.compile(r"https?://\S+")


def load_keywords(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    out: dict = {}
    for group, kws in data.items():
        if not isinstance(kws, list):
            continue
        out[str(group)] = [str(k).strip() for k in kws if str(k).strip()]
    return out


def _has_non_ascii(s: str) -> bool:
    return any(ord(c) > 127 for c in s)


def compile_keyword(kw: str):
    """
    Return (mode, value) tuple describing how to match this keyword.

    mode='substring'  → value is the lowercased keyword; caller does
                        `value in lowered_text`.
    mode='regex'      → value is a compiled re.Pattern with word-boundary
                        anchors and the right case-sensitivity.
    """
    if _has_non_ascii(kw):
        return ("substring", kw.lower())

    # Pure ASCII — English / digits / spaces.
    compact = kw.replace(" ", "")
    flags = re.ASCII
    if len(compact) > 3:
        flags |= re.IGNORECASE
    pattern = re.compile(r"\b" + re.escape(kw) + r"\b", flags)
    return ("regex", pattern)


def build_channel_sector_map(config_path: str, session_path: str) -> dict[str, str]:
    """Build channel_id → sector mapping by resolving config usernames via Telethon session."""
    config_p = Path(config_path)
    session_p = Path(session_path)
    if not config_p.exists() or not session_p.exists():
        return {}

    with open(config_p, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    channels = cfg.get("telegram", {}).get("channels", [])

    # Read Telethon entity cache: username → numeric id
    sess_conn = sqlite3.connect(session_path)
    entity_rows = sess_conn.execute("SELECT id, username FROM entities WHERE username IS NOT NULL").fetchall()
    sess_conn.close()
    username_to_id = {r[1].lower(): str(r[0]) for r in entity_rows}

    mapping: dict[str, str] = {}
    for ch in channels:
        sector = ch.get("sector", "mixed")
        if sector == "mixed":
            continue
        name = ch.get("name", "")
        username = name.lstrip("@").lower()
        eid = username_to_id.get(username)
        if eid:
            mapping[eid] = sector
    return mapping


def _match(text: str, text_lower: str, mode: str, value) -> bool:
    if mode == "substring":
        return value in text_lower
    return value.search(text) is not None


def tag_articles(db_path: str, keywords_path: str, group_filter: str | None,
                  config_path: str = DEFAULT_CONFIG, session_path: str = DEFAULT_SESSION) -> dict:
    if os.environ.get("ALLOW_WRITE") != "1":
        raise SystemExit("ALLOW_WRITE=1 required for writes.")

    groups = load_keywords(keywords_path)
    if group_filter:
        if group_filter not in groups:
            raise SystemExit(f"Unknown group: {group_filter}. Available: {list(groups)}")
        groups = {group_filter: groups[group_filter]}

    # Precompile matchers: {group: [(kw, mode, value), ...]}
    compiled: dict = {}
    for group, kws in groups.items():
        compiled[group] = [(kw, *compile_keyword(kw)) for kw in kws]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Clear existing tags for groups we are about to re-scan so results reflect
    # the current matching rules and keyword list.
    placeholders = ",".join(["?"] * len(compiled))
    conn.execute(
        f"DELETE FROM article_tags WHERE tag_group IN ({placeholders})",
        list(compiled.keys()),
    )

    # Build channel_id → sector map for auto-tagging by channel sector
    sector_map = build_channel_sector_map(config_path, session_path)

    rows = conn.execute(
        "SELECT channel_id, message_id, message_date, raw_text FROM raw_messages"
    ).fetchall()
    print(f"Scanning {len(rows)} raw_messages across {len(compiled)} groups...")

    counts: dict = {g: 0 for g in compiled}
    for r in rows:
        original = r["raw_text"] or ""
        # Strip URLs before matching (replace with a single space so surrounding
        # words don't get smashed together).
        text = URL_RE.sub(" ", original)
        text_lower = text.lower()
        chan = str(r["channel_id"])
        mid = str(r["message_id"])
        mdate = r["message_date"]

        # Auto-tag by channel sector: if a channel's sector matches a keyword
        # group, tag the message with '[channel_sector]' keyword.
        chan_sector = sector_map.get(chan)
        if chan_sector and chan_sector in compiled:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO article_tags
                    (message_id, channel_id, tag_group, keyword, message_date, raw_text)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (mid, chan, chan_sector, "[channel_sector]", mdate, original),
            )
            if cur.rowcount:
                counts[chan_sector] += 1

        for group, matchers in compiled.items():
            for kw, mode, value in matchers:
                if _match(text, text_lower, mode, value):
                    cur = conn.execute(
                        """
                        INSERT OR IGNORE INTO article_tags
                            (message_id, channel_id, tag_group, keyword, message_date, raw_text)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (mid, chan, group, kw, mdate, original),
                    )
                    if cur.rowcount:
                        counts[group] += 1
    conn.commit()
    conn.close()
    return counts


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--keywords", default=DEFAULT_KEYWORDS)
    p.add_argument("--group", default=None, help="Only tag a single group")
    args = p.parse_args()

    if not Path(args.db).exists():
        raise SystemExit(f"DB not found: {args.db}")
    if not Path(args.keywords).exists():
        raise SystemExit(f"Keywords file not found: {args.keywords}")

    counts = tag_articles(args.db, args.keywords, args.group)
    summary = " / ".join(f"{g}: {n}건" for g, n in counts.items())
    print(summary)


if __name__ == "__main__":
    main()
