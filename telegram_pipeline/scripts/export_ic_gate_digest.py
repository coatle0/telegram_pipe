"""Export IC Gate digest as markdown for investment review.

Usage:
  python scripts/export_ic_gate_digest.py --day 2026-04-10
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

INPUT_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_DIR = Path(r"C:\DCOS\10_Pillars\20_AutoAI\telepipe")


def load_ic_gate(day: str) -> list[dict]:
    path = INPUT_DIR / f"ic_gate_{day}.json"
    if not path.exists():
        raise SystemExit(f"IC gate file not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def fmt_item(item: dict) -> str:
    group = item.get("group", "?")
    action = item.get("action_bias", "?")
    phase = item.get("market_phase", "?")
    lines = [f"- [{group} | {action} | {phase}]"]
    lines.append(f"  thesis: {item.get('thesis', '')}")
    st = item.get("second_thought")
    if st:
        lines.append(f"  second_thought: {st}")
    sponsor = item.get("sponsor_name")
    layer = item.get("value_chain_layer")
    bn = item.get("bottleneck_score")
    lines.append(f"  스폰서: {sponsor or '-'} → {layer or '-'} → 병목: {bn if bn is not None else '-'}")
    tickers = item.get("key_tickers")
    if tickers:
        lines.append(f"  tickers: {', '.join(tickers)}")
    return "\n".join(lines)


def fmt_bookie(item: dict) -> str:
    group = item.get("group", "?")
    conv = item.get("conviction", "?")
    fs = item.get("frame_summary", {})
    bookie = item.get("frames", {}).get("bookie") if "frames" in item else {}
    if not bookie:
        bookie = {}
    lines = [f"- [{group} | conviction {conv}]"]
    event = bookie.get("event_name") or item.get("bookie_event_name", "?")
    ddate = bookie.get("decision_date") or item.get("bookie_decision_date", "?")
    lines.append(f"  이벤트: {event} | {ddate}")
    scenarios = bookie.get("scenarios")
    if scenarios and isinstance(scenarios, list):
        for s in scenarios:
            name = s.get("name", "?")
            prob = s.get("probability", 0)
            direction = s.get("market_direction", "?")
            lines.append(f"  {name} {prob*100:.0f}% → {direction}")
    return "\n".join(lines)


def export_digest(day: str):
    items = load_ic_gate(day)
    if not items:
        raise SystemExit("No items in IC gate file")

    high = [i for i in items if (i.get("conviction") or 0) >= 8]
    mid = [i for i in items if 6 <= (i.get("conviction") or 0) <= 7]
    # bookie active: frame_summary.bookie_score not null
    bookie_active = []
    for i in items:
        fs = i.get("frame_summary", {})
        bs = fs.get("bookie_score") if isinstance(fs, dict) else None
        if bs is not None:
            bookie_active.append(i)

    all_filtered = [i for i in items if (i.get("conviction") or 0) >= 5]

    lines = [f"# IC Gate — {day} 투자 시그널", ""]

    # conviction 8+
    lines.append("## conviction 8+ 즉각 검토")
    if high:
        for i in high:
            lines.append(fmt_item(i))
            lines.append("")
    else:
        lines.append("- 해당 없음")
        lines.append("")

    # conviction 6~7
    lines.append("## conviction 6~7 모니터링")
    if mid:
        for i in mid:
            lines.append(fmt_item(i))
            lines.append("")
    else:
        lines.append("- 해당 없음")
        lines.append("")

    # bookie active
    lines.append("## bookie 활성 — 미확정 이벤트")
    if bookie_active:
        for i in bookie_active:
            lines.append(fmt_bookie(i))
            lines.append("")
    else:
        lines.append("- 해당 없음")
        lines.append("")

    # summary stats
    total = len(all_filtered)
    n_high = len(high)
    n_buy = len([i for i in all_filtered if i.get("action_bias") in ("buy", "strong_buy")])

    sponsor_counts = Counter(
        i.get("sponsor_name") for i in all_filtered if i.get("sponsor_name")
    )
    layer_counts = Counter(
        i.get("value_chain_layer") for i in all_filtered if i.get("value_chain_layer")
    )

    lines.append("## 요약 통계")
    lines.append(f"- 전체: {total}건 | conviction 8+: {n_high}건 | buy 이상: {n_buy}건")
    top_sponsors = ", ".join(f"{k}({v})" for k, v in sponsor_counts.most_common(3)) or "-"
    lines.append(f"- 주요 스폰서: {top_sponsors}")
    top_layers = ", ".join(f"{k}({v})" for k, v in layer_counts.most_common(3)) or "-"
    lines.append(f"- 주요 병목 레이어: {top_layers}")
    lines.append("")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"ic_gate_digest_{day}.md"
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Digest exported: {out_path}")
    return out_path


def main():
    p = argparse.ArgumentParser(description="Export IC Gate digest markdown")
    p.add_argument("--day", required=True, help="Date (YYYY-MM-DD)")
    args = p.parse_args()
    export_digest(args.day)


if __name__ == "__main__":
    main()
