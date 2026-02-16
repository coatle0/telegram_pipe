import os, json, hashlib, sqlite3
from datetime import datetime, timezone

DB="data/risk_commander.sqlite"

def sha256(s:str)->str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

if os.environ.get("ALLOW_WRITE") != "1":
    raise SystemExit("DB write blocked: set ALLOW_WRITE=1")

rows = [
  ("fixture", "fixture:1", "2026-02-01T00:00:00Z", "삼전 실적 좋다. 005930.KS"),
  ("fixture", "fixture:2", "2026-02-01T01:00:00Z", "ARM 팔(arm) 아프다. $ARM"),
]

conn = sqlite3.connect(DB)
conn.execute("PRAGMA foreign_keys=ON;")

for channel_id, message_id, message_date, raw_text in rows:
    raw_json = json.dumps({"t":"fixture"}, ensure_ascii=False)
    content_hash = sha256(raw_text)  # simplest stable hash for dedupe

    conn.execute("""
        INSERT OR IGNORE INTO raw_messages
        (channel_id, message_id, message_date, raw_text, raw_json, content_hash, duplicate_of, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
    """, (channel_id, message_id, message_date, raw_text, raw_json, content_hash, utc_now_iso()))

conn.commit()
conn.close()
print("OK seeded into", DB)
