import sqlite3

DB = r"data/risk_commander.sqlite"
UNTIL = "2026-02-14 00:00:00"

c = sqlite3.connect(DB)
after_until = c.execute(
    "select count(*) from raw_messages where message_date >= ?",
    (UNTIL,)
).fetchone()[0]

mx = c.execute("select max(message_date) from raw_messages").fetchone()[0]
mn = c.execute("select min(message_date) from raw_messages").fetchone()[0]

print("DB:", DB)
print("after_until=", after_until, "UNTIL=", UNTIL)
print("min_date=", mn)
print("max_date=", mx)

c.close()
