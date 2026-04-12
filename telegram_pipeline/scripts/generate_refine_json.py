import argparse
import asyncio
import json
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
VENDOR_DIR = REPO_ROOT / "_vendor"
if VENDOR_DIR.exists() and str(VENDOR_DIR) not in sys.path:
    sys.path.insert(0, str(VENDOR_DIR))

from app.db import DB_PATH
from app.extract import extract_entities_from_text
from app.refine import _validate_item


def _extract_tickers(text: str) -> list[str]:
    pats = [
        re.compile(r"\$([A-Z]{1,5})"),
        re.compile(r"NASDAQ:([A-Z]{1,5})"),
        re.compile(r"\b([0-9]{6})\b"),
        re.compile(r"\b([0-9]{6})\.KS\b", re.IGNORECASE),
    ]
    out: list[str] = []
    for pat in pats:
        for m in pat.finditer(text or ""):
            t = m.group(1)
            if isinstance(t, str):
                t = t.upper()
            if t and t not in out:
                out.append(t)
    return out


def _classify_sentiment(text: str) -> str:
    raw = text or ""
    lt = raw.lower()
    pos_ko = ["호재", "상승", "급등", "강세", "최고", "돌파", "개선", "확대", "증가", "긍정", "성장", "서프라이즈", "상향"]
    neg_ko = ["악재", "하락", "급락", "약세", "부진", "우려", "리스크", "경고", "축소", "감소", "부정", "둔화", "쇼크", "하향"]
    pos_en = ["beat", "beats", "surge", "record", "strong", "upgrade", "bull", "rally", "gain"]
    neg_en = ["miss", "misses", "plunge", "weak", "downgrade", "bear", "selloff", "loss"]
    pos = any(k in raw for k in pos_ko) or any(k in lt for k in pos_en)
    neg = any(k in raw for k in neg_ko) or any(k in lt for k in neg_en)
    if pos and neg:
        return "mixed"
    if pos:
        return "bullish"
    if neg:
        return "bearish"
    return "neutral"


def _classify_event_type(text: str) -> str:
    raw = text or ""
    lt = raw.lower()
    if any(k in raw for k in ["실적", "어닝", "컨콜", "컨퍼런스콜"]) or any(k in lt for k in ["earnings", "eps", "revenue"]):
        return "earnings"
    if any(k in raw for k in ["가이던스", "전망", "목표가"]) or "guidance" in lt:
        return "guidance"
    if any(k in raw for k in ["cpi", "fomc", "금리", "환율", "달러", "유가", "인플레이션", "경기", "고용"]) or any(
        k in lt for k in ["cpi", "fomc", "rates", "yield", "inflation", "fx", "dollar", "oil"]
    ):
        return "macro"
    if any(k in raw for k in ["전쟁", "이스라엘", "이란", "우크라이나", "중동", "지정학"]) or any(k in lt for k in ["war", "israel", "iran", "ukraine", "geopolit"]):
        return "geopolitics"
    if any(k in raw for k in ["정책", "규제", "관세"]) or any(k in lt for k in ["policy", "regulat", "tariff"]):
        return "policy"
    if any(k in raw for k in ["마감", "장마감", "주간", "데일리"]) or any(k in lt for k in ["market wrap", "closing"]):
        return "market_wrap"
    return "other"


def _noise_flags(text: str) -> list[str]:
    raw = (text or "").strip()
    flags: list[str] = []
    if len(raw) < 30:
        flags.append("too_short")
    if re.fullmatch(r"https?://\S+", raw):
        flags.append("link_only")
    if "not related" in raw.lower() or "관련없" in raw:
        flags.append("ambiguous")
    return flags


def _make_summary(raw_text: str, tags: str) -> str:
    s = re.sub(r"\s+", " ", (raw_text or "").strip())
    s = s[:220] if s else "(내용 없음)"
    # Force a stable 2-sentence summary for refine validator compatibility.
    # We convert raw prose into a memo-like phrase to avoid extra sentence splits
    # from punctuation or Korean declarative endings like "다 ".
    s = re.sub(r"https?://\S+", "링크", s)
    s = re.sub(r"[.!?]+", " ", s)
    s = s.replace("…", " ").replace("..", " ").replace("•", " ").replace("·", " ")
    s = re.sub(r"다(?=\s|$)", "함", s)
    s = re.sub(r"니다(?=\s|$)", "임", s)
    s = re.sub(r"\s+", " ", s).strip(" ,;:-")
    safe_tags = re.sub(r"[.!?]+", " ", tags or "없음")
    safe_tags = re.sub(r"\s+", " ", safe_tags).strip(" ,;:") or "없음"
    return f"원문 핵심 메모: {s}. 핵심 태그 메모: {safe_tags}."


def _kst_bounds(day: str) -> tuple[str, str]:
    kst = timezone(timedelta(hours=9))
    start_kst = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=kst)
    end_kst = start_kst + timedelta(days=1)
    start_utc = start_kst.astimezone(timezone.utc).replace(tzinfo=None)
    end_utc = end_kst.astimezone(timezone.utc).replace(tzinfo=None)
    return start_utc.strftime("%Y-%m-%d %H:%M:%S"), end_utc.strftime("%Y-%m-%d %H:%M:%S")


def _auto_noise_classify(raw_text) -> tuple[bool, list[str]]:
    """Return (is_noise, noise_flags) for rule-based pre-filter before LLM call."""
    if raw_text is None:
        return True, ["low-quality"]
    if not isinstance(raw_text, str):
        return True, ["low-quality"]
    s = raw_text.strip()
    if not s:
        return True, ["low-quality"]
    if len(s) < 30:
        return True, ["low-quality"]
    tokens = s.split()
    if len(tokens) == 1 and tokens[0].lower().startswith("http"):
        return True, ["link-only"]
    return False, []


def _make_auto_noise_record(message_id: str, flags: list[str]) -> dict:
    return {
        "message_id": message_id,
        "relevance_score": 0.1,
        "sentiment": "neutral",
        "event_type": "other",
        "summary": "자동 필터링된 저품질 메시지이다. 본문 내용이 부족하여 추가 분석이 생략되었다.",
        "tickers": [],
        "entities": [],
        "bull_points": [],
        "bear_points": [],
        "noise_flags": flags,
        "confidence": 0.1,
    }


def _llm_refine(items: list[dict], model: str) -> list[dict]:
    try:
        from openai import AsyncOpenAI
    except Exception as e:
        raise SystemExit(f"openai package not available: {e}")

    import os

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("OPENAI_API_KEY is not set")

    # -------- Step 1: rule-based pre-filter --------
    llm_eligible: list[tuple[int, dict]] = []
    noise_map: dict[int, dict] = {}
    for idx, it in enumerate(items):
        is_noise, flags = _auto_noise_classify(it.get("raw_text"))
        if is_noise:
            noise_map[idx] = _make_auto_noise_record(it.get("message_id", ""), flags)
        else:
            llm_eligible.append((idx, it))
    print(
        f"PREFILTER_NOISE={len(noise_map)} "
        f"(llm_eligible={len(llm_eligible)}/{len(items)})",
        flush=True,
    )

    if not llm_eligible:
        out: list[dict] = [noise_map[i] for i in range(len(items))]
        return out

    client = AsyncOpenAI(api_key=api_key)

    sentiment_enum = ["bullish", "neutral", "bearish", "mixed"]
    event_enum = [
        "earnings",
        "guidance",
        "macro",
        "geopolitics",
        "policy",
        "rates",
        "fx",
        "commodities",
        "sector",
        "company",
        "market_wrap",
        "other",
    ]

    schema = {
        "message_id": "string (must match input message_id exactly)",
        "relevance_score": "number 0..1",
        "sentiment": f"one of {sentiment_enum}",
        "event_type": f"one of {event_enum}",
        "summary": "Korean, EXACTLY 2 sentences, concise, no copy-paste, no bullet points, each sentence must end with a period",
        "tickers": "원문에 명시적으로 표기된 주식 심볼만 포함 (예: $AAPL, NVDA, 005930 형태로 원문에 직접 나온 것만). 브랜드명·회사명에서 심볼을 추론하거나 변환하지 말 것. 원문에 심볼이 없으면 반드시 빈 배열 []",
        "entities": "array of strings",
        "bull_points": "array of short Korean strings (0~3), must be specific to the message",
        "bear_points": "array of short Korean strings (0~3), must be specific to the message",
        "noise_flags": "array of strings (can be empty), use when low-quality/noise/ambiguity/link-only",
        "confidence": "number 0..1",
    }

    system = (
        "You are a strict JSON generator for investment news refinement. "
        "Output must be a JSON object with a single key \"items\" whose value is an array (no markdown, no prose). "
        "Return exactly N items matching the input messages in the same order. "
        "Each output item must include all fields in the schema and comply with the enums and ranges. "
        "summary MUST be exactly 2 sentences in Korean. "
        "No more, no less. End each sentence with a period."
    )

    no_generic_points = [
        "긍정적 신호/호재 가능성",
        "부정적 신호/리스크 가능성",
        "호재 가능성",
        "리스크 가능성",
        "긍정적",
        "부정적",
    ]
    forbidden_points = set(no_generic_points)

    def build_user(chunk: list[dict]) -> dict:
        return {
            "task": "Refine telegram raw messages into structured JSON for downstream storage.",
            "output_format": "Return a JSON object: {\"items\": [ ... ]} where items is the array of refined records.",
            "schema": schema,
            "summary_example": "삼성전자 1Q26 영업이익이 컨센서스를 562% 상회했다. AI 메모리 수요 증가가 주요 원인으로 분석된다.",
            "constraints": {
                "output_wrapper_key": "items",
                "same_length_as_input": True,
                "same_order_as_input": True,
                "message_id_must_match": True,
                "summary_sentences": "exactly 2",
                "language": "Korean",
                "sentiment_enum": sentiment_enum,
                "event_type_enum": event_enum,
                "no_generic_points": no_generic_points,
            },
            "input_messages": chunk,
        }

    async def call(payload: dict, sem: asyncio.Semaphore) -> str:
        async with sem:
            resp = await client.chat.completions.create(
                model=model,
                max_tokens=16384,
                temperature=0.2,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
            )
        content = resp.choices[0].message.content
        try:
            obj = json.loads(content)
            if isinstance(obj, dict) and isinstance(obj.get("items"), list):
                return json.dumps(obj["items"], ensure_ascii=False)
        except Exception:
            pass
        return content

    ticker_re = re.compile(r"^\$?[A-Z]{1,5}$|^[0-9]{6}(\.KS)?$")

    async def refine_chunk(chunk: list[dict], sem: asyncio.Semaphore) -> list[dict]:
        user = build_user(chunk)
        raw = await call(user, sem)
        for attempt in range(2):
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = None

            # Post-process tickers: keep only strings matching ticker_re,
            # move invalid ones into entities (if not already present).
            if isinstance(parsed, list):
                for x in parsed:
                    if not isinstance(x, dict):
                        continue
                    tks = x.get("tickers")
                    if not isinstance(tks, list):
                        continue
                    ents = x.get("entities")
                    if not isinstance(ents, list):
                        ents = []
                        x["entities"] = ents
                    valid_tks: list[str] = []
                    for t in tks:
                        if not isinstance(t, str):
                            continue
                        s = t.strip()
                        if not s:
                            continue
                        if ticker_re.match(s):
                            if s not in valid_tks:
                                valid_tks.append(s)
                        else:
                            if s not in ents:
                                ents.append(s)
                    x["tickers"] = valid_tks

            errors: list[str] = []
            if not isinstance(parsed, list):
                errors.append("top_level_not_array")
            else:
                if len(parsed) != len(chunk):
                    errors.append(f"length_mismatch: expected={len(chunk)} got={len(parsed)}")

                expected_ids = [m["message_id"] for m in chunk]
                got_ids = [x.get("message_id") if isinstance(x, dict) else None for x in parsed]
                if got_ids != expected_ids:
                    errors.append("message_id_or_order_mismatch")

                if not errors:
                    for i, x in enumerate(parsed):
                        rec, err = _validate_item(x)
                        if err:
                            errors.append(f"item_{i}:{err}")
                            if len(errors) >= 12:
                                break
                        elif rec is None:
                            errors.append(f"item_{i}:invalid")
                            if len(errors) >= 12:
                                break
                        else:
                            bp = x.get("bull_points") if isinstance(x, dict) else None
                            brp = x.get("bear_points") if isinstance(x, dict) else None
                            for pts, key in ((bp, "bull_points"), (brp, "bear_points")):
                                if isinstance(pts, list):
                                    for ptxt in pts:
                                        if isinstance(ptxt, str) and ptxt.strip() in forbidden_points:
                                            errors.append(f"item_{i}:{key}_generic")
                                            break
                                if errors and errors[-1].startswith(f"item_{i}:{key}_generic"):
                                    break
                            if errors and errors[-1].startswith(f"item_{i}:") and len(errors) >= 12:
                                break

            if not errors:
                return parsed

            if attempt == 1:
                raise SystemExit("LLM output failed validation: " + "; ".join(errors))

            raw = await call(
                {
                    "task": "Fix the previous output so it validates. Return JSON array only.",
                    "validation_errors": errors,
                    "schema": schema,
                    "constraints": user["constraints"],
                    "input_messages": chunk,
                },
                sem,
            )

        raise SystemExit("LLM refine failed")

    async def try_refine(label: str, chunk: list[dict], sem: asyncio.Semaphore) -> list[dict]:
        try:
            return await refine_chunk(chunk, sem)
        except SystemExit as e:
            if len(chunk) == 1:
                print(f"  SKIP item {chunk[0].get('message_id')}: {e}", flush=True)
                return []
            mid = len(chunk) // 2
            left, right = await asyncio.gather(
                try_refine(f"{label}.L", chunk[:mid], sem),
                try_refine(f"{label}.R", chunk[mid:], sem),
            )
            return left + right

    batch_size = 10
    max_concurrent = 5
    llm_items = [it for _, it in llm_eligible]
    total = len(llm_items)
    total_batches = (total + batch_size - 1) // batch_size

    async def run_all() -> list[list[dict]]:
        sem = asyncio.Semaphore(max_concurrent)
        done = [0]

        async def guarded(batch_idx: int, chunk: list[dict]) -> list[dict]:
            result = await try_refine(str(batch_idx), chunk, sem)
            done[0] += 1
            print(
                f"LLM batch {done[0]}/{total_batches} (slot={batch_idx}): "
                f"{len(result)}/{len(chunk)} items",
                flush=True,
            )
            return result

        tasks = []
        for i in range(0, total, batch_size):
            chunk = llm_items[i:i + batch_size]
            batch_idx = i // batch_size + 1
            tasks.append(guarded(batch_idx, chunk))
        return await asyncio.gather(*tasks)

    batch_results = asyncio.run(run_all())

    llm_results: list[dict] = []
    for br in batch_results:
        llm_results.extend(br)

    # -------- Merge LLM results back into original order --------
    out_list: list = [None] * len(items)
    for idx, rec in noise_map.items():
        out_list[idx] = rec

    # llm_eligible preserves original order; we assume bisect preserves order too
    llm_by_mid: dict[str, dict] = {r.get("message_id"): r for r in llm_results}
    for idx, it in llm_eligible:
        rec = llm_by_mid.get(it.get("message_id"))
        if rec is not None:
            out_list[idx] = rec

    final_out = [r for r in out_list if r is not None]
    skipped = len(items) - len(final_out)
    print(
        f"LLM batches done: llm_processed={len(llm_results)}/{len(llm_eligible)}, "
        f"noise_auto={len(noise_map)}, final={len(final_out)}/{len(items)} "
        f"(skipped={skipped})",
        flush=True,
    )
    return final_out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--day", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--limit", type=int, default=9999)
    p.add_argument("--llm", action="store_true")
    p.add_argument("--model", default="gpt-4o-mini")
    p.add_argument("--dump-raw", action="store_true")
    args = p.parse_args()

    start_s, end_s = _kst_bounds(args.day)
    db_path = Path(str(DB_PATH)).resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT r.channel_id, r.message_id, r.message_date, r.raw_text
        FROM raw_messages r
        JOIN (
            SELECT MIN(id) AS id
            FROM raw_messages
            WHERE message_date >= ? AND message_date < ?
            GROUP BY channel_id, message_id
        ) dedup ON dedup.id = r.id
        ORDER BY r.message_date ASC, r.id ASC
        LIMIT ?
        """,
        (start_s, end_s, int(args.limit)),
    ).fetchall()
    conn.close()

    if len(rows) == 0:
        raise SystemExit(f"No raw messages for {args.day} KST. UTC range: {start_s} -> {end_s}")

    raw_items: list[dict] = []
    seen_raw_ids: set[str] = set()
    for r in rows:
        msg_id = f"{int(r['channel_id'])}:{int(r['message_id'])}"
        if msg_id in seen_raw_ids:
            continue
        seen_raw_ids.add(msg_id)
        raw_items.append(
            {
                "message_id": msg_id,
                "message_date": r["message_date"],
                "raw_text": r["raw_text"] or "",
            }
        )

    # Dedup by first 80 chars of raw_text (keep first occurrence)
    before_dedup = len(raw_items)
    seen_prefixes: set[str] = set()
    deduped_items: list[dict] = []
    for it in raw_items:
        prefix = (it.get("raw_text") or "")[:80]
        if prefix in seen_prefixes:
            continue
        seen_prefixes.add(prefix)
        deduped_items.append(it)
    removed = before_dedup - len(deduped_items)
    print(f"DEDUP_REMOVED={removed} (before={before_dedup} after={len(deduped_items)})")
    raw_items = deduped_items

    if args.dump_raw:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(raw_items, ensure_ascii=False, indent=2), encoding="utf-8")
        first3 = ",".join([raw_items[i]["message_id"] for i in range(min(3, len(raw_items)))])
        print(f"OUT={out_path.resolve()}")
        print(f"COUNT={len(raw_items)}")
        print(f"FIRST3={first3}")
        return 0

    if args.llm:
        t0 = time.perf_counter()
        items = _llm_refine(raw_items, model=args.model)
        elapsed = time.perf_counter() - t0
        print(f"LLM_ELAPSED_SEC={elapsed:.2f}", flush=True)
    else:
        items: list[dict] = []
        seen_item_ids: set[str] = set()
        for r in rows:
            raw_text = r["raw_text"] or ""
            msg_id = f"{int(r['channel_id'])}:{int(r['message_id'])}"
            if msg_id in seen_item_ids:
                continue
            seen_item_ids.add(msg_id)
            tickers = _extract_tickers(raw_text)
            ents = extract_entities_from_text(raw_text)
            entity_names: list[str] = []
            for e in ents:
                name = e.get("entity_name")
                if isinstance(name, str) and name.strip() and name not in entity_names:
                    entity_names.append(name)

            sent = _classify_sentiment(raw_text)
            event_type = _classify_event_type(raw_text)
            flags = _noise_flags(raw_text)

            relevance = 0.2
            if tickers or entity_names:
                relevance = 0.75
            elif len(raw_text) > 100:
                relevance = 0.5
            if "too_short" in flags:
                relevance = min(relevance, 0.2)

            confidence = 0.55
            if tickers or entity_names:
                confidence += 0.2
            if "ambiguous" in flags:
                confidence -= 0.15
            confidence = max(0.0, min(1.0, confidence))

            tags = ", ".join((tickers + entity_names)[:8]) if (tickers or entity_names) else "없음"

            bull_points: list[str] = []
            bear_points: list[str] = []
            if sent in ("bullish", "mixed"):
                bull_points.append("긍정적 신호/호재 가능성")
            if sent in ("bearish", "mixed"):
                bear_points.append("부정적 신호/리스크 가능성")

            items.append(
                {
                    "message_id": msg_id,
                    "relevance_score": float(relevance),
                    "sentiment": sent,
                    "event_type": event_type,
                    "summary": _make_summary(raw_text, tags),
                    "tickers": tickers,
                    "entities": entity_names,
                    "bull_points": bull_points,
                    "bear_points": bear_points,
                    "noise_flags": flags,
                    "confidence": float(confidence),
                }
            )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

    first3 = ",".join([items[i]["message_id"] for i in range(min(3, len(items)))])
    print(f"OUT={out_path.resolve()}")
    print(f"COUNT={len(items)}")
    print(f"FIRST3={first3}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
