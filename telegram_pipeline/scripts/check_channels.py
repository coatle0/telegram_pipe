import os, yaml, asyncio
from telethon import TelegramClient

def load_channels():
    with open("configs/config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["telegram"]["channels"]

def env_first(*names):
    for n in names:
        v = os.getenv(n)
        if v and str(v).strip():
            return v
    return None

api_id_raw = env_first("TELEGRAM_API_ID", "TG_API_ID")
api_hash = env_first("TELEGRAM_API_HASH", "TG_API_HASH")
if not api_id_raw or not api_hash:
    raise SystemExit("Missing TELEGRAM_API_ID/TELEGRAM_API_HASH (or TG_API_ID/TG_API_HASH)")
api_id = int(api_id_raw)

session_path = "data/telethon"

async def main():
    chans = load_channels()
    client = TelegramClient(session_path, api_id, api_hash)
    await client.start()

    ok, fail = 0, 0
    for ref in chans:
        try:
            # ref가 숫자 문자열이면 int로 시도
            ref_try = int(ref) if isinstance(ref, str) and ref.lstrip("-").isdigit() else ref
            ent = await client.get_entity(ref_try)
            uname = getattr(ent, "username", None)
            title = getattr(ent, "title", None) or getattr(ent, "first_name", None)
            print(f"OK\t{ref}\t->\t{title}\t@{uname}" if uname else f"OK\t{ref}\t->\t{title}\t(id={ent.id})")
            ok += 1
        except Exception as e:
            print(f"FAIL\t{ref}\t->\t{type(e).__name__}: {e}")
            fail += 1

    await client.disconnect()
    print(f"\nSUMMARY: ok={ok}, fail={fail}")

asyncio.run(main())
