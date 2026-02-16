# telegram_pipeline/scripts/make_obsidian_note.py
import re
import sys
from pathlib import Path

# ----------------------------
# Utilities
# ----------------------------
def _read_text(path: Path) -> str:
    # report는 utf-8-sig(BOM)로 쓰는 경우가 있으니 안전하게
    return path.read_text(encoding="utf-8-sig", errors="replace")

def _escape_md_link_text(s: str) -> str:
    # Markdown 링크 텍스트에서 대괄호는 깨질 수 있어서 이스케이프
    return s.replace("[", r"\[").replace("]", r"\]")

def _clean_title(title: str) -> str:
    title = title.strip()
    # 흔한 접두어 제거(원하면 더 추가)
    for p in ["공시명:", "기업명:", "회사명:", "종목명:", "티커:", "일시:", "시가총액:", "업종:"]:
        if title.startswith(p):
            title = title[len(p):].strip()
    # 공백 정리
    title = re.sub(r"\s+", " ", title).strip()
    return title

def _normalize_tg_url(url: str) -> str:
    url = url.strip()

    # 1) "tg:tg://..." 같이 잘못 붙은 접두어 제거
    if url.startswith("tg:tg://"):
        url = url[3:]  # drop "tg:"

    # 2) 오타 방어: channnel -> channel
    url = url.replace("channnel=", "channel=")

    # 3) 이미 tg://privatepost면 그대로
    if url.startswith("tg://privatepost"):
        return url

    # 4) t.me/c/<channel>/<post> -> tg://privatepost?channel=<channel>&post=<post>
    m = re.match(r"^https?://t\.me/c/(\d+)/(\d+)", url)
    if m:
        ch, post = m.group(1), m.group(2)
        return f"tg://privatepost?channel={ch}&post={post}"

    # 5) 그 외는 그대로(공개채널 t.me/<username>/<id> 같은 케이스)
    return url

def _extract_section(lines, header: str):
    # header: "## Top Entities" 같은 라인 그대로
    start = None
    for i, ln in enumerate(lines):
        if ln.strip() == header:
            start = i + 1
            break
    if start is None:
        return []

    out = []
    for j in range(start, len(lines)):
        ln = lines[j].rstrip("\n")
        if ln.startswith("## ") and ln.strip() != header:
            break
        out.append(ln)
    return out

def _parse_top_entities(report_text: str):
    """
    report의 '## Top Entities' 섹션을 파싱해서:
    [
      {
        "entity": "클래시스 (214150)",
        "count": 3,
        "evidence": [
           {"date":"2026-02-13","time":"20:00","title":"어닝 ...","url":"tg://privatepost?..."},
           ...
        ]
      },
      ...
    ]
    """
    lines = report_text.splitlines()
    sec = _extract_section(lines, "## Top Entities")

    entity_re = re.compile(r"^\-\s\*\*(.+?)\*\*:\s*(\d+)\s*$")
    ev_re = re.compile(r"^\s*\-\s*\[(\d{4}\-\d{2}\-\d{2})\s+(\d{2}:\d{2})\s+KST\]\s*(.+?)\s*$")

    items = []
    cur = None

    for ln in sec:
        m = entity_re.match(ln.strip())
        if m:
            cur = {"entity": m.group(1).strip(), "count": int(m.group(2)), "evidence": []}
            items.append(cur)
            continue

        m = ev_re.match(ln)
        if m and cur is not None:
            d, t, tail = m.group(1), m.group(2), m.group(3)

            # tail 예:
            # "📊 ... | tg:tg://privatepost?channel=...&post=..."
            # ".... | tg:https://t.me/c/.../..."
            # ".... | tg://privatepost?channel=...&post=..." (혹시 이런 형태도 방어)
            title = tail
            url = ""

            if "| tg:" in tail:
                left, right = tail.split("| tg:", 1)
                title = left.strip()
                url = right.strip()
                # 혹시 뒤에 "| url:..." 붙어있으면 제거
                if "| url:" in url:
                    url = url.split("| url:", 1)[0].strip()
            else:
                # label이 없어도 tail 안에 tg:// or https://t.me 가 있으면 끝부분에서 잡는다
                idx = tail.rfind("tg://privatepost")
                if idx != -1:
                    title = tail[:idx].rstrip(" |").strip()
                    url = tail[idx:].strip()
                else:
                    idx = tail.rfind("https://t.me/")
                    if idx != -1:
                        title = tail[:idx].rstrip(" |").strip()
                        url = tail[idx:].strip()

            title = _clean_title(title)
            url = _normalize_tg_url(url)

            if url:
                cur["evidence"].append({"date": d, "time": t, "title": title, "url": url})

    return items

def _infer_day_from_report_path(path: Path) -> str:
    # report_YYYY-MM-DD.md
    m = re.search(r"report_(\d{4}\-\d{2}\-\d{2})\.md$", path.name)
    if m:
        return m.group(1)
    return "UNKNOWN"

def _default_out_path(report_path: Path, day: str) -> Path:
    # outputs/reports/report_YYYY-MM-DD.md -> outputs/obsidian/daily_YYYY-MM-DD.md
    # report가 outputs/reports 아래에 있다고 가정(아니어도 최대한 안전하게)
    p = report_path.resolve()
    out_dir = None
    if p.parent.name == "reports" and p.parent.parent.name == "outputs":
        out_dir = p.parent.parent / "obsidian"
    else:
        out_dir = p.parent / "obsidian"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"daily_{day}.md"

# ----------------------------
# Main
# ----------------------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python telegram_pipeline/scripts/make_obsidian_note.py <path-to-report.md>")
        sys.exit(2)

    report_path = Path(sys.argv[1])
    if not report_path.exists():
        print(f"ERROR: report not found: {report_path}")
        sys.exit(1)

    day = _infer_day_from_report_path(report_path)
    report_text = _read_text(report_path)

    entities = _parse_top_entities(report_text)

    out_path = _default_out_path(report_path, day)

    lines = []
    lines.append(f"# Telegram Daily Links: {day}")
    lines.append("")
    lines.append("## Top Entities (evidence links)")
    lines.append("")

    if not entities:
        lines.append("(None)")
    else:
        for it in entities:
            entity = it["entity"]
            if not it["evidence"]:
                continue
            lines.append(f"### {entity}")
            for ev in it["evidence"]:
                display = f"{entity} | {ev['date']} {ev['time']} | {ev['title']}"
                display = _escape_md_link_text(display)
                lines.append(f"- [{display}]({ev['url']})")
            lines.append("")

    out_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    print(f"OK: wrote {out_path}")

if __name__ == "__main__":
    main()
