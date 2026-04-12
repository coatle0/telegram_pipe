"""
Multi-frame LLM analysis for tagged articles.

Each article gets ONE unified API call that returns all 3 investment frames
(momentum, theme, bookie) in a single JSON response.

Model: gpt-4o-mini via OpenAI async API.

Usage:
  set OPENAI_API_KEY=sk-...
  ALLOW_WRITE=1 python telegram_pipeline/scripts/frame_refine.py \
      --day 2026-04-08 --groups macro_energy --limit 5
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
from pathlib import Path

from openai import AsyncOpenAI

MODEL = "gpt-4o-mini"
TEMPERATURE = 0.2
MAX_TOKENS = 900
BATCH_SIZE = 10   # articles per batch
CONCURRENCY = 5   # parallel API calls within a batch
DEFAULT_DB = "data/risk_commander.sqlite"

VALID_SIGNALS = {"bullish", "bearish", "neutral", "mixed"}
FRAME_KEYS = {"momentum", "theme", "bookie"}

SYSTEM = (
    "You are a senior investment analyst at a Korean asset management firm.\n"
    "Analyze the given news article through 3 distinct investment lenses simultaneously.\n"
    "Respond ONLY in valid JSON matching the schema exactly.\n"
    "All text fields must be in Korean.\n"
    "If insufficient information exists for any frame, use null — never hallucinate."
)

USER_TEMPLATE = """다음 텔레그램 뉴스 기사를 3개 프레임으로 동시에 분석하라.

[기사]
{article}

[출력 스키마 - 반드시 준수]
{{
  "message_id": "{message_id}",

  "momentum": {{
    "score": 0~10,
    "signal": "bullish|bearish|neutral|mixed",
    "key_point": "핵심 모멘텀 포인트 1줄",
    "catalyst": "촉매 이벤트명 또는 null"
  }},

  "theme": {{
    "score": 0~10,
    "theme_name": "AI인프라|에너지안보|방산|반도체사이클|중국디커플링|원전SMR|바이오ADC|리쇼어링|기타",
    "direction": "strengthening|weakening|neutral",
    "maturity": "early|growth|mature|declining",
    "key_point": "테마 관점 핵심 1줄"
  }},

  "bookie": {{
    "score": 0~10 또는 null,
    "event_name": "이벤트명 또는 null",
    "decision_date": "예상 결정 시점 또는 null",
    "scenarios": [
      {{
        "name": "시나리오명",
        "probability": 0.0~1.0,
        "market_direction": "bullish|bearish|neutral",
        "key_beneficiary": "수혜 섹터/종목"
      }}
    ] 또는 null,
    "expected_impact": -1.0~1.0 또는 null
  }}
}}

판단 기준:
- 단신/속보 기사: momentum/bookie 위주
- 분석 리포트: theme 위주
- 지정학 뉴스: bookie 위주
- 확정된 사실만 있으면 bookie.score = null"""

# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------


def fetch_articles(db_path: str, day: str, groups: list[str] | None, limit: int | None) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT DISTINCT t.message_id, t.channel_id, t.tag_group, t.message_date, r.raw_text
        FROM article_tags t
        JOIN raw_messages r ON t.message_id = r.message_id AND t.channel_id = r.channel_id
        WHERE t.message_date LIKE ?
    """
    params: list = [f"{day}%"]

    if groups:
        placeholders = ",".join(["?"] * len(groups))
        query += f" AND t.tag_group IN ({placeholders})"
        params.extend(groups)

    query += " ORDER BY t.message_date DESC"
    if limit:
        query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()

    seen: dict[str, dict] = {}
    for r in rows:
        key = f"{r['channel_id']}:{r['message_id']}"
        if key not in seen:
            seen[key] = {
                "message_id": r["message_id"],
                "channel_id": r["channel_id"],
                "date": r["message_date"],
                "group": r["tag_group"],
                "raw_text": r["raw_text"] or "",
            }
    return list(seen.values())


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_result(data: dict) -> bool:
    """Return True if the parsed JSON has all 3 frame keys with valid structure."""
    if not isinstance(data, dict):
        return False
    for key in FRAME_KEYS:
        frame = data.get(key)
        if not isinstance(frame, dict):
            return False
        score = frame.get("score")
        if score is not None:
            if not isinstance(score, (int, float)) or not (0 <= score <= 10):
                return False
        signal = frame.get("signal")
        if signal is not None and signal not in VALID_SIGNALS:
            return False
    return True


# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------


async def call_llm(client: AsyncOpenAI, sem: asyncio.Semaphore, article: dict) -> dict | None:
    mid = article["message_id"]
    text = article["raw_text"]
    user_msg = USER_TEMPLATE.replace("{article}", text).replace("{message_id}", mid)

    async with sem:
        try:
            resp = await client.chat.completions.create(
                model=MODEL,
                temperature=TEMPERATURE,
                max_tokens=MAX_TOKENS,
                messages=[
                    {"role": "system", "content": SYSTEM},
                    {"role": "user", "content": user_msg},
                ],
            )
            raw = resp.choices[0].message.content.strip()
            # Strip markdown code fences
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
                if raw.endswith("```"):
                    raw = raw[:-3]
                raw = raw.strip()
            data = json.loads(raw)

            if not validate_result(data):
                print(f"  SKIP {mid}: validation failed")
                return None

            return {
                "message_id": mid,
                "channel_id": article["channel_id"],
                "date": article["date"],
                "group": article["group"],
                "raw_text": text[:200],
                "frames": {
                    "momentum": data["momentum"],
                    "theme": data["theme"],
                    "bookie": data["bookie"],
                },
            }
        except Exception as e:
            print(f"  SKIP {mid}: {e}")
            return None


async def run(articles: list[dict]) -> list[dict]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("OPENAI_API_KEY 환경변수를 설정해주세요.")

    client = AsyncOpenAI(api_key=api_key)
    sem = asyncio.Semaphore(CONCURRENCY)
    results: list[dict] = []

    for i in range(0, len(articles), BATCH_SIZE):
        batch = articles[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"  Batch {batch_num}: articles {i + 1}-{i + len(batch)}")

        batch_results = await asyncio.gather(
            *[call_llm(client, sem, a) for a in batch]
        )
        results.extend([r for r in batch_results if r is not None])

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    p = argparse.ArgumentParser(description="Multi-frame LLM analysis (unified call)")
    p.add_argument("--day", required=True, help="Date (YYYY-MM-DD)")
    p.add_argument("--groups", default=None, help="Comma-separated groups")
    p.add_argument("--limit", type=int, default=None, help="Max articles")
    p.add_argument("--out", default=None, help="Output path (default: data/frame_scores_DAY.json)")
    p.add_argument("--db", default=DEFAULT_DB)
    args = p.parse_args()

    if not Path(args.db).exists():
        raise SystemExit(f"DB not found: {args.db}")

    groups = [g.strip() for g in args.groups.split(",")] if args.groups else None
    articles = fetch_articles(args.db, args.day, groups, args.limit)
    if not articles:
        raise SystemExit(f"No articles found for day={args.day}, groups={groups}")

    print(f"Found {len(articles)} articles (day={args.day}, groups={groups or 'all'})")
    print(f"Model: {MODEL} | 1 call/article | concurrency={CONCURRENCY}")

    results = asyncio.run(run(articles))

    out_path = Path(args.out) if args.out else Path(f"data/frame_scores_{args.day}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\nDone: {len(results)}/{len(articles)} succeeded -> {out_path}")


if __name__ == "__main__":
    main()
