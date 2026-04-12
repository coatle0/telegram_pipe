import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.db import check_write_permission, get_connection


EVENT_TYPES = {
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
}

SENTIMENTS = {"bullish", "neutral", "bearish", "mixed"}


@dataclass(frozen=True)
class RefinedRecord:
    message_id: str
    relevance_score: float
    sentiment: str
    event_type: str
    summary: str
    tickers: List[str]
    entities: List[str]
    bull_points: List[str]
    bear_points: List[str]
    noise_flags: List[str]
    confidence: float


def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _as_str_list(x: Any) -> Optional[List[str]]:
    if x is None:
        return []
    if not isinstance(x, list):
        return None
    out: List[str] = []
    for it in x:
        if isinstance(it, str):
            s = it.strip()
            if s:
                out.append(s)
        else:
            return None
    return out


def _count_sentences_ko(text: str) -> int:
    t = text.strip()
    if not t:
        return 0
    t = re.sub(r"\s+", " ", t)
    parts = re.split(r"(?<=[\.\!\?])\s+|(?<=다)\s+", t)
    parts = [p.strip() for p in parts if p.strip()]
    return len(parts)


def _validate_item(item: Any) -> Tuple[Optional[RefinedRecord], Optional[str]]:
    if not isinstance(item, dict):
        return None, "item_not_object"

    message_id = item.get("message_id")
    if not isinstance(message_id, str) or not message_id.strip():
        return None, "invalid_message_id"
    message_id = message_id.strip()

    relevance_score = item.get("relevance_score")
    if not _is_number(relevance_score):
        return None, "invalid_relevance_score_type"
    relevance_score = float(relevance_score)
    if not (0.0 <= relevance_score <= 1.0):
        return None, "invalid_relevance_score_range"

    sentiment = item.get("sentiment")
    if not isinstance(sentiment, str):
        return None, "invalid_sentiment_type"
    sentiment = sentiment.strip().lower()
    if sentiment not in SENTIMENTS:
        return None, "invalid_sentiment_enum"

    event_type = item.get("event_type")
    if not isinstance(event_type, str):
        return None, "invalid_event_type"
    event_type = event_type.strip().lower()
    if event_type not in EVENT_TYPES:
        return None, "invalid_event_type_enum"

    summary = item.get("summary")
    if not isinstance(summary, str):
        return None, "invalid_summary_type"
    summary = summary.strip()
    if len(summary) < 10 or len(summary) > 600:
        return None, "invalid_summary_length"
    sent_count = _count_sentences_ko(summary)
    if sent_count < 2 or sent_count > 3:
        return None, "invalid_summary_sentence_count"

    tickers = _as_str_list(item.get("tickers"))
    if tickers is None:
        return None, "invalid_tickers"

    entities = _as_str_list(item.get("entities"))
    if entities is None:
        return None, "invalid_entities"

    bull_points = _as_str_list(item.get("bull_points"))
    if bull_points is None:
        return None, "invalid_bull_points"

    bear_points = _as_str_list(item.get("bear_points"))
    if bear_points is None:
        return None, "invalid_bear_points"

    noise_flags = _as_str_list(item.get("noise_flags"))
    if noise_flags is None:
        return None, "invalid_noise_flags"

    confidence = item.get("confidence")
    if not _is_number(confidence):
        return None, "invalid_confidence_type"
    confidence = float(confidence)
    if not (0.0 <= confidence <= 1.0):
        return None, "invalid_confidence_range"

    return (
        RefinedRecord(
            message_id=message_id,
            relevance_score=relevance_score,
            sentiment=sentiment,
            event_type=event_type,
            summary=summary,
            tickers=tickers,
            entities=entities,
            bull_points=bull_points,
            bear_points=bear_points,
            noise_flags=noise_flags,
            confidence=confidence,
        ),
        None,
    )


def import_refined_json(json_file: str) -> Dict[str, int]:
    check_write_permission()
    p = Path(json_file)
    if not p.exists():
        raise RuntimeError(f"json_file_not_found: {p}")

    data = json.loads(p.read_text(encoding="utf-8-sig"))
    if not isinstance(data, list):
        raise RuntimeError("invalid_json: expected top-level JSON array")

    valid: List[RefinedRecord] = []
    errors = 0
    for item in data:
        rec, err = _validate_item(item)
        if err:
            errors += 1
            continue
        valid.append(rec)

    inserted = 0
    updated = 0
    conn = get_connection(write=True)
    try:
        cur = conn.cursor()
        for rec in valid:
            cur.execute("SELECT 1 FROM llm_refined_news WHERE message_id = ?", (rec.message_id,))
            existed = cur.fetchone() is not None

            cur.execute(
                """
                INSERT INTO llm_refined_news
                    (message_id, relevance_score, sentiment, event_type, summary, tickers, entities, bull_points, bear_points, noise_flags, confidence)
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(message_id) DO UPDATE SET
                    relevance_score = excluded.relevance_score,
                    sentiment = excluded.sentiment,
                    event_type = excluded.event_type,
                    summary = excluded.summary,
                    tickers = excluded.tickers,
                    entities = excluded.entities,
                    bull_points = excluded.bull_points,
                    bear_points = excluded.bear_points,
                    noise_flags = excluded.noise_flags,
                    confidence = excluded.confidence
                """,
                (
                    rec.message_id,
                    rec.relevance_score,
                    rec.sentiment,
                    rec.event_type,
                    rec.summary,
                    json.dumps(rec.tickers, ensure_ascii=False),
                    json.dumps(rec.entities, ensure_ascii=False),
                    json.dumps(rec.bull_points, ensure_ascii=False),
                    json.dumps(rec.bear_points, ensure_ascii=False),
                    json.dumps(rec.noise_flags, ensure_ascii=False),
                    rec.confidence,
                ),
            )
            if existed:
                updated += 1
            else:
                inserted += 1
        conn.commit()
    finally:
        conn.close()

    return {"inserted": inserted, "updated": updated, "errors": errors, "total": len(data)}

