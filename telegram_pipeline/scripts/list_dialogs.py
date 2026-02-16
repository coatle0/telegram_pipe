import os
from telethon import TelegramClient

api_id = int(os.environ["TELEGRAM_API_ID"])
api_hash = os.environ["TELEGRAM_API_HASH"]
session = "data/telethon"

async def main():
    async for d in client.iter_dialogs():
        ent = d.entity
        username = getattr(ent, "username", None)
        print(f"{d.name}\t id={ent.id}\t username=@{username}" if username else f"{d.name}\t id={ent.id}")

client = TelegramClient(session, api_id, api_hash)
with client:
    client.loop.run_until_complete(main())
