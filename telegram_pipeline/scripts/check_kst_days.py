import sqlite3
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.db import DB_PATH

DB = str(DB_PATH)

c = sqlite3.connect(DB)

rows = c.execute("""
select date(datetime(message_date,'+9 hours')) as kst_day, count(*)
from raw_messages
group by kst_day
order by kst_day desc
limit 14
""").fetchall()

print("DB:", DB)
print("KST day distribution (latest 14 days):")
for day, cnt in rows:
    print(day, cnt)

c.close()
