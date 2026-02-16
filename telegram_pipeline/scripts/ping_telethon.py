import os
import asyncio
from telethon import TelegramClient

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]

SESSION = "data/telethon"   # 확장자 없이
CHANNEL = "@maddingStock"

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    msgs = await client.get_messages(CHANNEL, limit=1)
    if not msgs:
        print("OK connected, but no messages fetched.")
    else:
        m = msgs[0]
        txt = m.message or ""
        print("OK last msg:", "date=", m.date, "text_len=", len(txt))
    await client.disconnect()

asyncio.run(main())
