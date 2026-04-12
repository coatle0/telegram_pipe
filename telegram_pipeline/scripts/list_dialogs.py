"""
list_dialogs.py — 현재 Telethon 세션에서 구독 중인 채널 목록 출력

실행:
  cd C:\autoai\telegram_pipe
  PYTHONUTF8=1 python telegram_pipeline/scripts/list_dialogs.py

출력:
  채널명 / @username / channel_id  (config.yaml 붙여넣기용 YAML 블록 포함)
"""
import os
import asyncio
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat

API_ID   = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION  = "data/telethon"   # 프로젝트 루트 기준

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.connect()

    rows = []
    async for dialog in client.iter_dialogs():
        ent = dialog.entity
        # 채널/슈퍼그룹만
        if not isinstance(ent, (Channel, Chat)):
            continue
        username = getattr(ent, "username", None)
        name     = dialog.name or ""
        cid      = ent.id
        rows.append((name, username, cid))

    await client.disconnect()

    print(f"\n{'채널명':<30} {'@username / 초대링크':<35} {'channel_id'}")
    print("-" * 80)
    for name, uname, cid in rows:
        uname_str = f"@{uname}" if uname else "(비공개 — 초대링크 필요)"
        print(f"{name:<30} {uname_str:<35} {cid}")

    # config.yaml 붙여넣기용 YAML 블록
    print("\n\n# ── config.yaml 붙여넣기용 ──────────────────")
    print("  channels:")
    for name, uname, cid in rows:
        if uname:
            print(f"    - name: '@{uname}'")
        else:
            print(f"    - name: 'https://t.me/+INVITE_HASH_HERE'  # {name} (비공개채널)")
        print(f"      label: '{name}'")
        print(f"      tags: []")

if __name__ == "__main__":
    asyncio.run(main())
