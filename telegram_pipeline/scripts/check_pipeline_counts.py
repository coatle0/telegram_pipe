import sqlite3
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.db import DB_PATH

DB = str(DB_PATH)
c = sqlite3.connect(DB)

def one(q, p=()):
    return c.execute(q, p).fetchone()[0]

print("DB:", DB)
print("raw_messages      =", one("select count(*) from raw_messages"))
print("processed_messages=", one("select count(*) from processed_messages"))
print("extracted_entities=", one("select count(*) from extracted_entities"))
print("extracted_keywords=", one("select count(*) from extracted_keywords"))

# KST day 기준 raw 분포 상위 5일
rows = c.execute("""
select date(datetime(message_date,'+9 hours')) kst_day, count(*)
from raw_messages
group by kst_day
order by kst_day desc
limit 5
""").fetchall()
print("KST raw days(top5) =", rows)

c.close()
