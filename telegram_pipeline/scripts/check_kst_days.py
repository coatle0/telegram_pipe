import sqlite3

DB = r"data/risk_commander.sqlite"

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
