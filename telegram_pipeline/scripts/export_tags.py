"""
Export tagged articles from article_tags into tags/{group}_news.md.

Usage:
  python telegram_pipeline/scripts/export_tags.py --group oci
  python telegram_pipeline/scripts/export_tags.py --group oci --days 7
  python telegram_pipeline/scripts/export_tags.py --group oci --from 2026-04-01
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.db import DB_PATH

DEFAULT_DB = str(DB_PATH)
DEFAULT_OUT_DIR = "tags"


def resolve_since(days: int | None, from_date: str | None) -> str | None:
    """Return a UTC naive 'YYYY-MM-DD HH:MM:SS' lower bound, or None."""
    if from_date:
        kst = timezone(timedelta(hours=9))
        d = datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=kst)
        return d.astimezone(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    if days is not None and days > 0:
        now = datetime.now(timezone.utc)
        since = now - timedelta(days=days)
        return since.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    return None


def format_kst(message_date: str | None) -> str:
    if not message_date:
        return ""
    try:
        dt = datetime.strptime(message_date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return message_date
    kst = dt.astimezone(timezone(timedelta(hours=9)))
    return kst.strftime("%Y-%m-%d %H:%M")


def tg_link(channel_id: str, message_id: str) -> str:
    """
    Build a Telegram link. For private/super-group channels (-100XXXXX),
    use https://t.me/c/<internal>/<message_id>.
    """
    try:
        cid_int = int(channel_id)
    except ValueError:
        return f"tg://{channel_id}/{message_id}"
    s = str(cid_int)
    if s.startswith("-100"):
        internal = s[4:]
        return f"https://t.me/c/{internal}/{message_id}"
    if s.startswith("-"):
        return f"https://t.me/c/{s[1:]}/{message_id}"
    return f"https://t.me/c/{s}/{message_id}"


def preview(raw_text: str | None, n: int = 150) -> str:
    if not raw_text:
        return ""
    cleaned = " ".join(raw_text.split())
    if len(cleaned) > n:
        cleaned = cleaned[:n] + "..."
    return cleaned.replace("[", "(").replace("]", ")")


def export(db_path: str, group: str, since: str | None, out_dir: str) -> Path:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    params: list = [group]
    sql = """
        SELECT message_id, channel_id, keyword, message_date, raw_text
        FROM article_tags
        WHERE tag_group = ?
    """
    if since:
        sql += " AND message_date >= ?"
        params.append(since)
    sql += " ORDER BY message_date DESC"

    rows = conn.execute(sql, params).fetchall()
    conn.close()

    # Group by (channel_id, message_id) so one article lists multiple keywords.
    grouped: dict[tuple[str, str], dict] = {}
    for r in rows:
        key = (str(r["channel_id"]), str(r["message_id"]))
        if key not in grouped:
            grouped[key] = {
                "message_date": r["message_date"],
                "raw_text": r["raw_text"],
                "keywords": [],
            }
        grouped[key]["keywords"].append(r["keyword"])

    # Sort by message_date desc (rows are already, but dict iteration order follows insertion).
    articles = sorted(
        grouped.items(), key=lambda kv: kv[1]["message_date"] or "", reverse=True
    )

    lines: list[str] = [f"## {group} 관련 뉴스", ""]
    for (chan, mid), info in articles:
        kst = format_kst(info["message_date"])
        link = tg_link(chan, mid)
        text = preview(info["raw_text"])
        kws = ", ".join(sorted(set(info["keywords"])))
        lines.append(f"- [{kst} | {text}]({link})")
        lines.append(f"  키워드: {kws}")
    lines.append("")

    out_path = Path(out_dir) / f"{group}_news.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {len(articles)} articles -> {out_path}")
    return out_path


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--group", required=True)
    p.add_argument("--days", type=int, default=None, help="Only last N days (UTC now-based)")
    p.add_argument("--from", dest="from_date", default=None, help="KST start date YYYY-MM-DD")
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    args = p.parse_args()

    since = resolve_since(args.days, args.from_date)
    if since:
        print(f"Filter: message_date >= {since} UTC")
    export(args.db, args.group, since, args.out_dir)


if __name__ == "__main__":
    main()
