"""Ingest ic_gate JSON into VSURF Wiki pages via OpenAI API.

Usage:
  OPENAI_API_KEY=sk-... python scripts/wiki_ingest.py --day 2026-04-10
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from openai import AsyncOpenAI

# Python 3.9~3.14 호환
if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except AttributeError:
        pass

DATA_DIR = _REPO_ROOT / "data"
WIKI_DIR = Path(__file__).resolve().parent.parent.parent / "wiki"

MODEL = "gpt-4o-mini"
TEMPERATURE = 0.3
MAX_TOKENS = 800
CONCURRENCY = 3

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

ENTITY_SYSTEM = (
    "You are a financial wiki editor for VSURF Capital.\n"
    "Write a concise entity page in Korean markdown.\n"
    "Follow the format exactly. Include source links.\n"
    "Output ONLY the markdown content, no code fences."
)

ENTITY_PROMPT = """다음 ic_gate 데이터로 종목 wiki 페이지를 작성하라.

[데이터]
{data}

[출력 형식]
---
ticker: {ticker}
sector: (추정)
conviction: {conviction}
action_bias: {action_bias}
last_updated: {date}
hypothesis_tags: []
---

## 현재 thesis
(ic_gate thesis 기반 2~3줄)

## 스폰서 x 밸류체인 x 병목
- 스폰서: {sponsor_name} | {sponsor_direction}
- 밸류체인: {value_chain_layer}
- 병목: {bottleneck_score}/10 — {bottleneck_reason}

## 최근 시그널
- [{date}] {thesis} [[ic_gate_{date_compact}#msg_{message_id}]]

## 리스크 플래그
(risk_flags 기반)

## 관련 테마
- [[themes/{theme_name}]]
"""

THEME_SYSTEM = (
    "You are a financial wiki editor for VSURF Capital.\n"
    "Write a concise theme page in Korean markdown.\n"
    "Follow the format exactly. Output ONLY markdown, no code fences."
)

THEME_PROMPT = """다음 ic_gate 데이터로 테마 wiki 페이지를 작성하라.

[데이터]
{data}

[출력 형식]
## 테마 정의
(테마명 + 1줄 정의)

## 스폰서
(관련 스폰서 목록)

## 밸류체인 레이어
(관련 레이어)

## 현재 병목
(bottleneck 정보)

## 관련 종목
(key_tickers 기반 [[links]])

## 성숙도
(maturity 기반: early|growth|mature|declining)
"""

SPONSOR_SYSTEM = (
    "You are a financial wiki editor for VSURF Capital.\n"
    "Write a concise sponsor page in Korean markdown.\n"
    "Follow the format exactly. Output ONLY markdown, no code fences."
)

SPONSOR_PROMPT = """다음 ic_gate 데이터로 스폰서 wiki 페이지를 작성하라.

[데이터]
{data}

[출력 형식]
## 스폰서 프로필
(스폰서명 + 유형)

## 집행 방향 + 규모
(direction, amount 정보)

## 수혜 밸류체인 레이어
(value_chain_layer 기반)

## 관련 종목/테마
(key_tickers, theme_name 기반 [[links]])
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _slugify(name: str) -> str:
    """Convert a name to a safe filename slug."""
    name = name.strip().lower()
    name = re.sub(r'[/\\:*?"<>|]', '_', name)
    name = re.sub(r'\s+', '_', name)
    return name


def _load_ic_gate(day: str) -> list[dict]:
    path = DATA_DIR / f"ic_gate_{day}.json"
    if not path.exists():
        raise SystemExit(f"IC gate file not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _extract_items(items: list[dict]) -> dict:
    """Extract entities, themes, sponsors from ic_gate items."""
    entities: dict[str, list[dict]] = {}
    themes: dict[str, list[dict]] = {}
    sponsors: dict[str, list[dict]] = {}

    for item in items:
        conviction = item.get("conviction", 0) or 0
        if conviction < 7:
            continue

        # Entities from key_tickers
        tickers = item.get("key_tickers", [])
        if isinstance(tickers, list):
            for ticker in tickers:
                if ticker:
                    entities.setdefault(ticker, []).append(item)

        # Themes from frame_summary or theme data
        theme_name = None
        fs = item.get("frame_summary", {})
        if isinstance(fs, dict):
            theme_name = fs.get("theme_name")
        if not theme_name:
            # Try to get from the group
            theme_name = item.get("group")
        if theme_name:
            themes.setdefault(theme_name, []).append(item)

        # Sponsors
        sponsor_name = item.get("sponsor_name")
        if sponsor_name:
            sponsors.setdefault(sponsor_name, []).append(item)

    return {"entities": entities, "themes": themes, "sponsors": sponsors}


# ---------------------------------------------------------------------------
# LLM calls
# ---------------------------------------------------------------------------


async def _call_llm(client: AsyncOpenAI, sem: asyncio.Semaphore,
                    system: str, prompt: str) -> str | None:
    async with sem:
        try:
            resp = await client.chat.completions.create(
                model=MODEL,
                temperature=TEMPERATURE,
                max_tokens=MAX_TOKENS,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
            )
            raw = resp.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
                if raw.endswith("```"):
                    raw = raw[:-3]
                raw = raw.strip()
            return raw
        except Exception as e:
            print(f"  LLM error: {e}")
            return None


async def _generate_pages(extracted: dict, day: str) -> dict:
    """Generate wiki pages via LLM for all extracted items."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("OPENAI_API_KEY 환경변수를 설정해주세요.")

    client = AsyncOpenAI(api_key=api_key)
    sem = asyncio.Semaphore(CONCURRENCY)
    results = {"entities": {}, "themes": {}, "sponsors": {}}

    tasks = []

    # Entity pages
    for ticker, items in extracted["entities"].items():
        data_json = json.dumps(items, ensure_ascii=False, indent=2)
        item = items[0]
        prompt = ENTITY_PROMPT.format(
            data=data_json,
            ticker=ticker,
            conviction=item.get("conviction", "?"),
            action_bias=item.get("action_bias", "?"),
            date=item.get("date", day)[:10],
            date_compact=day.replace("-", ""),
            message_id=item.get("message_id", "?"),
            sponsor_name=item.get("sponsor_name", "?"),
            sponsor_direction="",
            value_chain_layer=item.get("value_chain_layer", "?"),
            bottleneck_score=item.get("bottleneck_score", "?"),
            bottleneck_reason=item.get("bottleneck_reason", "?"),
            thesis=item.get("thesis", ""),
            theme_name=item.get("group", "?"),
        )
        tasks.append(("entity", ticker, _call_llm(client, sem, ENTITY_SYSTEM, prompt)))

    # Theme pages
    for theme, items in extracted["themes"].items():
        data_json = json.dumps(items, ensure_ascii=False, indent=2)
        prompt = THEME_PROMPT.format(data=data_json)
        tasks.append(("theme", theme, _call_llm(client, sem, THEME_SYSTEM, prompt)))

    # Sponsor pages
    for sponsor, items in extracted["sponsors"].items():
        data_json = json.dumps(items, ensure_ascii=False, indent=2)
        prompt = SPONSOR_PROMPT.format(data=data_json)
        tasks.append(("sponsor", sponsor, _call_llm(client, sem, SPONSOR_SYSTEM, prompt)))

    # Execute all
    coros = [t[2] for t in tasks]
    responses = await asyncio.gather(*coros, return_exceptions=True)

    for (page_type, name, _), response in zip(tasks, responses):
        if isinstance(response, Exception):
            print(f"  SKIP {page_type}/{name}: {response}")
            continue
        if response:
            results[f"{page_type}s" if page_type != "entity" else "entities"][name] = response

    return results


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------


def _write_page(category: str, name: str, content: str, day: str) -> Path:
    """Write or update a wiki page."""
    slug = _slugify(name)
    page_dir = WIKI_DIR / category
    page_dir.mkdir(parents=True, exist_ok=True)
    page_path = page_dir / f"{slug}.md"

    if page_path.exists():
        existing = page_path.read_text(encoding="utf-8")
        # Append new signal to existing page
        date_header = f"## [{day}] 업데이트"
        if day not in existing:
            updated = existing.rstrip() + f"\n\n{date_header}\n{content}\n"
            page_path.write_text(updated, encoding="utf-8")
            print(f"  UPDATE {category}/{slug}.md")
        else:
            print(f"  SKIP {category}/{slug}.md (already has {day})")
    else:
        page_path.write_text(content, encoding="utf-8")
        print(f"  CREATE {category}/{slug}.md")

    return page_path


def _update_index(pages_created: dict[str, list[str]]):
    """Update wiki/index.md with current page counts and listings."""
    index_path = WIKI_DIR / "index.md"

    categories = {
        "entities": "종목별 페이지",
        "themes": "테마별 페이지",
        "sponsors": "스폰서 추적",
        "chronist": "역사 패턴",
        "hypothesis": "가설 상태",
        "answers": "분석 결과",
    }

    lines = [
        f"# VSURF Wiki — Index",
        f"> 갱신: {datetime.now().strftime('%Y-%m-%d %H:%M')} | 자동 관리: wiki_ingest.py",
        "",
        "## 카테고리",
    ]

    for cat, label in categories.items():
        cat_dir = WIKI_DIR / cat
        if cat_dir.exists():
            count = len(list(cat_dir.glob("*.md")))
        else:
            count = 0
        lines.append(f"- {cat}/ — {label} ({count}개)")

    lines.append("")
    lines.append("## 페이지 목록")

    for cat in ["entities", "themes", "sponsors", "chronist", "hypothesis"]:
        cat_dir = WIKI_DIR / cat
        if cat_dir.exists():
            md_files = sorted(cat_dir.glob("*.md"))
            if md_files:
                lines.append(f"\n### {cat}/")
                for f in md_files:
                    lines.append(f"- [[{cat}/{f.stem}]]")

    lines.append("")
    index_path.write_text("\n".join(lines), encoding="utf-8")


def _update_log(day: str, stats: dict):
    """Append ingest record to wiki/log.md."""
    log_path = WIKI_DIR / "log.md"
    existing = log_path.read_text(encoding="utf-8") if log_path.exists() else ""

    entry = (
        f"\n## [{day}] ingest | "
        f"conviction7+: {stats.get('filtered', 0)}건 | "
        f"entities: {stats.get('entities', 0)} | "
        f"themes: {stats.get('themes', 0)} | "
        f"sponsors: {stats.get('sponsors', 0)}\n"
    )
    log_path.write_text(existing + entry, encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _run_async(coro):
    if hasattr(asyncio, "Runner"):
        try:
            with asyncio.Runner() as runner:
                return runner.run(coro)
        except asyncio.CancelledError:
            return {}
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    except asyncio.CancelledError:
        return {}
    finally:
        try:
            pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        finally:
            loop.close()


def _generate_template_page(page_type: str, name: str, items: list[dict], day: str) -> str:
    """Generate a wiki page from template without LLM (fallback)."""
    item = items[0]
    if page_type == "entity":
        return (
            f"---\n"
            f"ticker: {name}\n"
            f"sector: (미정)\n"
            f"conviction: {item.get('conviction', '?')}\n"
            f"action_bias: {item.get('action_bias', '?')}\n"
            f"last_updated: {day}\n"
            f"hypothesis_tags: []\n"
            f"---\n\n"
            f"## 현재 thesis\n"
            f"{item.get('thesis', '(없음)')}\n\n"
            f"## 스폰서 x 밸류체인 x 병목\n"
            f"- 스폰서: {item.get('sponsor_name', '-')}\n"
            f"- 밸류체인: {item.get('value_chain_layer', '-')}\n"
            f"- 병목: {item.get('bottleneck_score', '-')}/10 — {item.get('bottleneck_reason', '-')}\n\n"
            f"## 최근 시그널\n"
            f"- [{day}] {item.get('thesis', '')} "
            f"[[ic_gate_{day.replace('-', '')}#msg_{item.get('message_id', '?')}]]\n\n"
            f"## 리스크 플래그\n"
            + "\n".join(f"- {r}" for r in (item.get("risk_flags") or [])) + "\n\n"
            f"## 관련 테마\n"
            f"- [[themes/{item.get('group', '?')}]]\n"
        )
    elif page_type == "theme":
        tickers = set()
        for i in items:
            for t in (i.get("key_tickers") or []):
                tickers.add(t)
        return (
            f"## 테마 정의\n"
            f"{name}\n\n"
            f"## 스폰서\n"
            + "\n".join(f"- {i.get('sponsor_name', '-')}" for i in items if i.get("sponsor_name")) + "\n\n"
            f"## 밸류체인 레이어\n"
            + "\n".join(f"- {i.get('value_chain_layer', '-')}" for i in items if i.get("value_chain_layer")) + "\n\n"
            f"## 현재 병목\n"
            + "\n".join(f"- {i.get('bottleneck_score', '-')}/10: {i.get('bottleneck_reason', '-')}" for i in items if i.get("bottleneck_score")) + "\n\n"
            f"## 관련 종목\n"
            + "\n".join(f"- [[entities/{t}]]" for t in sorted(tickers)) + "\n\n"
            f"## 성숙도\n"
            f"(미정)\n"
        )
    else:  # sponsor
        return (
            f"## 스폰서 프로필\n"
            f"{name}\n\n"
            f"## 집행 방향 + 규모\n"
            + "\n".join(f"- {i.get('thesis', '')[:80]}" for i in items) + "\n\n"
            f"## 수혜 밸류체인 레이어\n"
            + "\n".join(f"- {i.get('value_chain_layer', '-')}" for i in items if i.get("value_chain_layer")) + "\n\n"
            f"## 관련 종목/테마\n"
            + "\n".join(f"- [[entities/{t}]]" for i in items for t in (i.get("key_tickers") or [])) + "\n"
        )


def ingest(day: str, use_llm: bool = True):
    print(f"Wiki ingest: {day} (LLM={'ON' if use_llm else 'OFF'})")

    # Load and filter
    items = _load_ic_gate(day)
    filtered = [i for i in items if (i.get("conviction") or 0) >= 7]
    print(f"  Total: {len(items)} | conviction 7+: {len(filtered)}")

    if not filtered:
        print("  No items with conviction >= 7. Skipping.")
        return

    # Extract entities/themes/sponsors
    extracted = _extract_items(items)
    print(f"  Extracted: entities={len(extracted['entities'])} "
          f"themes={len(extracted['themes'])} "
          f"sponsors={len(extracted['sponsors'])}")

    if use_llm and os.environ.get("OPENAI_API_KEY"):
        # Generate pages via LLM
        pages = _run_async(_generate_pages(extracted, day))
    else:
        if use_llm:
            print("  OPENAI_API_KEY not set, falling back to template mode")
        # Template-based generation
        pages = {"entities": {}, "themes": {}, "sponsors": {}}
        for ticker, items_list in extracted["entities"].items():
            pages["entities"][ticker] = _generate_template_page("entity", ticker, items_list, day)
        for theme, items_list in extracted["themes"].items():
            pages["themes"][theme] = _generate_template_page("theme", theme, items_list, day)
        for sponsor, items_list in extracted["sponsors"].items():
            pages["sponsors"][sponsor] = _generate_template_page("sponsor", sponsor, items_list, day)

    # Write pages
    pages_written = {"entities": [], "themes": [], "sponsors": []}

    for ticker, content in pages.get("entities", {}).items():
        _write_page("entities", ticker, content, day)
        pages_written["entities"].append(ticker)

    for theme, content in pages.get("themes", {}).items():
        _write_page("themes", theme, content, day)
        pages_written["themes"].append(theme)

    for sponsor, content in pages.get("sponsors", {}).items():
        _write_page("sponsors", sponsor, content, day)
        pages_written["sponsors"].append(sponsor)

    # Update index and log
    _update_index(pages_written)
    _update_log(day, {
        "filtered": len(filtered),
        "entities": len(pages_written["entities"]),
        "themes": len(pages_written["themes"]),
        "sponsors": len(pages_written["sponsors"]),
    })

    print(f"\nDone: entities={len(pages_written['entities'])} "
          f"themes={len(pages_written['themes'])} "
          f"sponsors={len(pages_written['sponsors'])}")


def main():
    p = argparse.ArgumentParser(description="Ingest ic_gate into VSURF Wiki")
    p.add_argument("--day", required=True, help="Date (YYYY-MM-DD)")
    p.add_argument("--no-llm", action="store_true", help="Template mode (no LLM)")
    args = p.parse_args()
    ingest(args.day, use_llm=not args.no_llm)


if __name__ == "__main__":
    main()
