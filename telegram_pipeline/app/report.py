import sqlite3
import re
import json
from datetime import datetime, timezone, timedelta
from collections import Counter
from pathlib import Path
from app.db import get_connection

REPORT_DIR = Path("C:/DCOS/10_Pillars/20_AutoAI/telepipe")
RULES_DIR = Path(__file__).parent / "rules"

# ── Report parameters ─────────────────────────────────────────────
EVIDENCE_PER_ITEM    = 5   # article links shown per entity/keyword
TOP_N_ENTITIES       = 25  # top N entities in report
TOP_N_KEYWORDS       = 20  # top N keywords in report
TOP_N_UNKNOWN        = 30  # top N unknown candidates
# ──────────────────────────────────────────────────────────────────

def load_stopwords():
    try:
        with open(RULES_DIR / "stopwords.json", "r", encoding="utf-8") as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

STOPWORDS = load_stopwords()

def _load_entity_meta():
    """Load sector, related_companies, ticker from entities_seed.json."""
    meta = {}
    try:
        with open(RULES_DIR / "entities_seed.json", "r", encoding="utf-8") as f:
            seeds = json.load(f)
        for rule in seeds:
            entry = {
                "ticker": rule.get("primary_ticker", ""),
                "sector": rule.get("sector", ""),
                "related": rule.get("related_companies", []),
            }
            meta[rule["entity_name"]] = entry
            for alias in rule.get("aliases", []):
                meta[alias] = entry
    except Exception:
        pass
    return meta

ENTITY_META = _load_entity_meta()
KEYWORD_BLACKLIST = {
    "EPS",
    "PER",
    "PBR",
    "ROE",
    "ROA",
    "OPM",
    "NPM",
    "GM",
    "GPM",
    "EBITDA",
    "EV",
    "FCF",
    "CFO",
    "CAPEX",
    "ADR",
    "IPO",
    "URL",
}

REFINED_MIN_RELEVANCE = 0.50
REFINED_MIN_CONFIDENCE = 0.50
WEAK_REFINED_TITLE_PATTERNS = [
    re.compile(r"^\d{4}[-./]\d{2}[-./]\d{2}(?:\s*[월화수목금토일])?$"),
    re.compile(r"^\d{4}[-./]\d{2}[-./]\d{2}\s+[월화수목금토일]$"),
]
WEAK_REFINED_TITLE_CONTAINS = [
    "하루를 돌아보는 뉴스 정리",
    "장 마감 전 뉴스",
    "국내 주식 마감 시황",
    "장 마감 시황",
    "장마감 시황",
    "시황 정리",
    "오늘의 시황",
    "암만 생각해도",
]

def _parse_raw_json(raw_json_str: str):
    try:
        return json.loads(raw_json_str)
    except Exception:
        return {}

def _first_non_empty_line(text: str, max_len: int = 200) -> str:
    if not text:
        return "(no title)"
    # Prefer first non-empty, non-template line
    template_prefixes = [
        r"^일시\s*:",
        r"^기업명\s*:",
        r"^회사명\s*:",
        r"^종목명\s*:",
        r"^티커\s*:",
        r"^시가총액\s*:",
        r"^업종\s*:",
    ]
    template_re = re.compile("|".join(template_prefixes), re.IGNORECASE)
    lines = text.splitlines()
    for line in lines:
        s = line.strip()
        if s and not template_re.search(s):
            return s[:max_len]
    # Fallback to first non-empty line if all were template-like
    for line in lines:
        s = line.strip()
        if s:
            return s[:max_len]
    return "(no title)"

def _extract_raw_body(raw_text: str) -> str:
    """Extract full body text from raw_text, skipping the first (title) line.
    Returns empty string if body is too short to be meaningful."""
    if not raw_text:
        return ""
    non_empty = [l.strip() for l in raw_text.splitlines() if l.strip()]
    if len(non_empty) <= 1:
        return ""  # only title, no body
    body = ' '.join(non_empty[1:])
    body = ' '.join(body.split())
    if len(body) < 60:
        return ""  # too short — not worth showing
    return body

def _extract_title_and_url(raw_json_str: str, raw_text: str):
    """Returns (title, url, description). description is the webpage preview excerpt,
    or raw_text body lines when no webpage description is available."""
    url = None
    description = None
    try:
        payload = _parse_raw_json(raw_json_str)
        media = payload.get("media") or {}
        webpage = media.get("webpage") or {}
        url = webpage.get("url")
        desc = webpage.get("description")
        if desc and isinstance(desc, str) and desc.strip():
            description = desc.strip()
    except Exception:
        pass
    # Prefer raw_text first line (channel's own Korean summary/title)
    # Fall back to webpage title only if raw_text is empty
    text_title = _first_non_empty_line(raw_text, 200) if raw_text else None
    if text_title and text_title != "(no title)":
        # No webpage description → use raw_text body as fallback
        if description is None:
            description = _extract_raw_body(raw_text) or None
        return text_title, url, description
    try:
        payload = _parse_raw_json(raw_json_str)
        media = payload.get("media") or {}
        webpage = media.get("webpage") or {}
        web_title = webpage.get("title")
        if web_title and isinstance(web_title, str) and web_title.strip():
            if description is None:
                description = _extract_raw_body(raw_text) or None
            return web_title.strip()[:200], url, description
    except Exception:
        pass
    return "(no title)", url, description

def _tg_link(channel_id: int, message_id: int) -> str:
    if isinstance(channel_id, str):
        try:
            channel_id = int(channel_id)
        except Exception:
            pass
    try:
        if isinstance(channel_id, int) and channel_id <= -1000000000000:
            internal = abs(channel_id) - 1000000000000
            return f"tg://privatepost?channel={internal}&post={message_id}"
        else:
            return f"tg://privatepost?channel={abs(int(channel_id))}&post={message_id}"
    except Exception:
        return f"tg://privatepost?post={message_id}"

def _md_link_text(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    # Collapse newlines → space (newlines break markdown link text)
    text = text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    # Collapse multiple spaces
    text = ' '.join(text.split())
    # Replace chars that break markdown [text](url) syntax with fullwidth equivalents
    return text.replace("[", "〔").replace("]", "〕").replace("(", "（").replace(")", "）")


def _clean_display_name(name: str, max_len: int = 60) -> str:
    """Strip newlines and truncate entity display names for markdown output."""
    if not isinstance(name, str):
        name = str(name)
    name = name.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    name = ' '.join(name.split())
    if len(name) > max_len:
        name = name[:max_len] + '…'
    return name


def _is_weak_refined_title(title: str) -> bool:
    if not isinstance(title, str):
        return True
    normalized = " ".join(title.split()).strip()
    if not normalized or normalized == "(no title)":
        return True
    for pat in WEAK_REFINED_TITLE_PATTERNS:
        if pat.match(normalized):
            return True
    lower = normalized.lower()
    for phrase in WEAK_REFINED_TITLE_CONTAINS:
        if phrase.lower() in lower:
            return True
    return False

def _kst_str(utc_str: str) -> str:
    # message_date is stored as UTC naive "YYYY-MM-DD HH:MM:SS"
    try:
        dt = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        kst = dt + timedelta(hours=9)
        return kst.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return utc_str

def _kst_window_to_utc(day_str: str):
    # day_str: 'YYYY-MM-DD' in Asia/Seoul (KST)
    start_kst = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone(timedelta(hours=9)))
    start_utc = start_kst.astimezone(timezone.utc)
    end_utc = start_utc + timedelta(days=1)
    return start_utc.strftime("%Y-%m-%d %H:%M:%S"), end_utc.strftime("%Y-%m-%d %H:%M:%S")

def _load_refined_map(cursor, start_utc_str: str = None, end_utc_str: str = None):
    refined = {}
    query = """
        SELECT
            CAST(m.channel_id AS TEXT) || ':' || CAST(m.message_id AS TEXT) AS message_key,
            r.summary,
            r.sentiment,
            r.event_type,
            r.relevance_score,
            r.confidence
        FROM raw_messages m
        JOIN llm_refined_news r
          ON r.message_id = CAST(m.channel_id AS TEXT) || ':' || CAST(m.message_id AS TEXT)
    """
    params = ()
    if start_utc_str and end_utc_str:
        query += " WHERE m.message_date >= ? AND m.message_date < ?"
        params = (start_utc_str, end_utc_str)
    try:
        cursor.execute(query, params)
        for row in cursor.fetchall():
            refined[row["message_key"]] = {
                "summary": row["summary"],
                "sentiment": row["sentiment"],
                "event_type": row["event_type"],
                "relevance_score": row["relevance_score"],
                "confidence": row["confidence"],
            }
    except sqlite3.OperationalError:
        # Table may not exist yet in older DBs.
        return {}
    return refined

def _refined_description(refined_map, channel_id, message_id, fallback_description):
    key = f"{channel_id}:{message_id}"
    refined = refined_map.get(key)
    if not refined:
        return fallback_description
    summary = (refined.get("summary") or "").strip()
    if not summary:
        return fallback_description
    sentiment = (refined.get("sentiment") or "").strip()
    event_type = (refined.get("event_type") or "").strip()
    if sentiment or event_type:
        meta = " / ".join([x for x in [event_type, sentiment] if x])
        return f"[Refined: {meta}] {summary}" if meta else summary
    return summary

def _get_refined_highlights(cursor, start_utc_str: str = None, end_utc_str: str = None, limit: int = 10):
    query = """
        SELECT
            m.channel_id,
            m.message_id,
            m.message_date,
            m.raw_text,
            m.raw_json,
            r.summary,
            r.sentiment,
            r.event_type,
            r.relevance_score,
            r.confidence
        FROM raw_messages m
        JOIN llm_refined_news r
          ON r.message_id = CAST(m.channel_id AS TEXT) || ':' || CAST(m.message_id AS TEXT)
    """
    params = []
    if start_utc_str and end_utc_str:
        query += " WHERE m.message_date >= ? AND m.message_date < ?"
        params.extend([start_utc_str, end_utc_str])
    query += """
        ORDER BY
            r.relevance_score DESC,
            r.confidence DESC,
            m.message_date DESC,
            m.id DESC
        LIMIT ?
    """
    params.append(limit)
    try:
        cursor.execute(query, tuple(params))
        return cursor.fetchall()
    except sqlite3.OperationalError:
        return []

def get_ambiguous_hits(cursor, date_filter):
    cursor.execute(f"""
        SELECT 
            e.entity_name, 
            e.match_text, 
            m.channel_id, 
            m.message_id, 
            m.message_date,
            m.raw_text,
            m.raw_json
        FROM extracted_entities e
        JOIN raw_messages m ON e.raw_id = m.id
        {date_filter} {'AND' if date_filter else 'WHERE'} e.is_ambiguous = 1
        ORDER BY m.message_date DESC, m.id DESC
    """)
    rows = cursor.fetchall()
    
    # Group by entity/match_text
    grouped = {}
    for r in rows:
        key = (r['entity_name'], r['match_text'])
        if key not in grouped:
            grouped[key] = []
        grouped[key].append({
            'channel_id': r['channel_id'],
            'message_id': r['message_id'],
            'message_date': r['message_date'],
            'raw_text': r['raw_text'],
            'raw_json': r['raw_json']
        })
        
    return grouped

def get_unknown_candidates(cursor, date_filter):
    cursor.execute(f"""
        SELECT 
            m.id,
            m.channel_id, 
            m.message_id, 
            m.message_date,
            p.cleaned_text,
            m.raw_text,
            m.raw_json
        FROM processed_messages p
        JOIN raw_messages m ON p.raw_id = m.id
        {date_filter}
        ORDER BY m.message_date, m.id
    """)
    messages = cursor.fetchall()
    
    cursor.execute(f"""
        SELECT raw_id, match_text 
        FROM extracted_entities e
        JOIN raw_messages m ON e.raw_id = m.id
        {date_filter}
    """)
    extracted_map = {}
    for r in cursor.fetchall():
        rid = r['raw_id']
        if rid not in extracted_map:
            extracted_map[rid] = set()
        extracted_map[rid].add(r['match_text'].lower())
        
    cursor.execute(f"""
        SELECT DISTINCT k.keyword
        FROM extracted_keywords k
        JOIN raw_messages m ON k.raw_id = m.id
        {date_filter}
    """)
    known_keywords_upper = {row['keyword'].upper() for row in cursor.fetchall()}
        
    candidates = Counter()
    candidate_refs = {} # token -> list of {channel_id, message_id, message_date, raw_text, raw_json}
    
    patterns = [
        (r'\b[A-Z]{2,6}\b', 'UPPER'),
        (r'\b\d{6}\b', 'KR6'),
        (r'\$[A-Z]{1,6}', 'DOLLAR')
    ]
    
    hard_exclude_upper = KEYWORD_BLACKLIST
    
    for msg in messages:
        rid = msg['id']
        text = msg['cleaned_text']
        known_texts = extracted_map.get(rid, set())
        
        found_tokens = set()
        
        for pat, pat_type in patterns:
            for match in re.finditer(pat, text):
                token = match.group(0)
                
                # Filter: check if token is known (or contained in known entity match?)
                # Simple check: exact match (case-insensitive) against known match_text
                # Also check if token is a substring of any known entity match (e.g. MSFT in $MSFT)
                # This is important because regex \b[A-Z]\b matches inside $TICKER
                is_known = False
                token_lower = token.lower()
                if token_lower in known_texts:
                    is_known = True
                else:
                    for known in known_texts:
                        if token_lower in known:
                            is_known = True
                            break
                
                if is_known:
                    continue
                    
                u = token.upper()
                if u in hard_exclude_upper:
                    continue
                if u in known_keywords_upper:
                    continue
                
                if pat_type == 'UPPER':
                    if token.lower() in STOPWORDS:
                        continue
                    
                if token in found_tokens:
                    continue
                found_tokens.add(token)
                
                candidates[token] += 1
                
                if token not in candidate_refs:
                    candidate_refs[token] = []
                
                if len(candidate_refs[token]) < 5:
                    candidate_refs[token].append({
                        'channel_id': msg['channel_id'],
                        'message_id': msg['message_id'],
                        'message_date': msg['message_date'],
                        'raw_text': msg['raw_text'],
                        'raw_json': msg['raw_json']
                    })

    # Sort candidates: count DESC, token ASC
    sorted_candidates = sorted(candidates.items(), key=lambda x: (-x[1], x[0]))
    
    return sorted_candidates, candidate_refs

def resolve_kr_label(code: str, cursor, start_utc: str, end_utc: str):
    # Find company name patterns from raw_text within the window for this KR code
    try:
        cursor.execute(f"""
            SELECT m.message_date, m.raw_text
            FROM extracted_entities e
            JOIN raw_messages m ON e.raw_id = m.id
            WHERE e.entity_type = 'KR_CODE'
              AND e.entity_name = ?
              AND m.message_date >= ?
              AND m.message_date < ?
            ORDER BY m.message_date DESC, m.id DESC
        """, (code, start_utc, end_utc))
        rows = cursor.fetchall()
    except Exception:
        return None
    name_stats = {}  # name -> {'count': n, 'latest': str, 'length': int}
    # Priority patterns:
    pat_a = re.compile(rf"(?:기업명|회사명|종목명)\s*:\s*(?P<name>[^\(\)\[\]\{{\}}]{{2,40}})\s*\(\s*{re.escape(code)}\s*\)")
    pat_b = re.compile(rf"(?P<name>[^\(\)\[\]\{{\}}]{{2,40}})\s*\(\s*{re.escape(code)}\s*\)")
    def _clean_name(n: str) -> str:
        if not isinstance(n, str):
            return ""
        name = n.strip()
        for pref in ["기업명:", "회사명:", "종목명:", "티커:", "Ticker:", "Company:"]:
            if name.lower().startswith(pref.lower()):
                name = name[len(pref):].strip()
                break
        return name
    for r in rows:
        text = r['raw_text'] or ""
        # a) labeled pattern matches across full body
        found_any = False
        for m in pat_a.finditer(text):
            found_any = True
            name = _clean_name(m.group('name'))
            if len(name) < 2:
                continue
            info = name_stats.get(name)
            if not info:
                name_stats[name] = {'count': 1, 'latest': r['message_date'], 'length': len(name)}
            else:
                info['count'] += 1
                if r['message_date'] > info['latest']:
                    info['latest'] = r['message_date']
                info['length'] = len(name)
        # b) unlabeled pattern fallback only if no a) match in this message
        if not found_any:
            for m in pat_b.finditer(text):
                name = _clean_name(m.group('name'))
                if len(name) < 2:
                    continue
                info = name_stats.get(name)
                if not info:
                    name_stats[name] = {'count': 1, 'latest': r['message_date'], 'length': len(name)}
                else:
                    info['count'] += 1
                    if r['message_date'] > info['latest']:
                        info['latest'] = r['message_date']
                    info['length'] = len(name)
    if not name_stats:
        return None
    # Choose best deterministically: highest count, then most recent, then longer name
    def sort_key(item):
        name, info = item
        return (-info['count'], info['latest'], -info['length'])
    best = sorted(name_stats.items(), key=sort_key)[0][0]
    return best

def generate_report(day: str = None, week: str = None):
    conn = get_connection()
    cursor = conn.cursor()
    
    # Filter clause (KST day -> UTC window)
    date_filter = ""
    start_utc_str = None
    end_utc_str = None
    if day:
        start_utc_str, end_utc_str = _kst_window_to_utc(day)
        date_filter = (
            f"WHERE m.message_date >= '{start_utc_str}' "
            f"AND m.message_date < '{end_utc_str}'"
        )
    # Week logic omitted for brevity, v0.1 supports day only
    refined_map = _load_refined_map(cursor, start_utc_str, end_utc_str)
    
    # Key Companies
    cursor.execute(f"""
        SELECT entity_name, entity_type, COUNT(*) as count 
        FROM extracted_entities e
        JOIN raw_messages m ON e.raw_id = m.id
        {date_filter}
        GROUP BY entity_name, entity_type
        ORDER BY count DESC
        LIMIT {TOP_N_ENTITIES}
    """)
    top_entities = cursor.fetchall()

    # Top Keywords
    cursor.execute(f"""
        SELECT keyword, category, COUNT(*) as count
        FROM extracted_keywords k
        JOIN raw_messages m ON k.raw_id = m.id
        {date_filter}
        GROUP BY keyword, category
        ORDER BY count DESC
        LIMIT {TOP_N_KEYWORDS}
    """)
    top_keywords = cursor.fetchall()
    
    # New Sections
    # Pass date_filter (WHERE clause) directly to helper functions
    # Note: helper functions expect 'date_filter' string starting with WHERE or empty
    unknown_candidates, unknown_refs = get_unknown_candidates(cursor, date_filter)
    ambiguous_hits = get_ambiguous_hits(cursor, date_filter)
    refined_highlights = _get_refined_highlights(cursor, start_utc_str, end_utc_str, limit=10)
    
    # ── 두 버전 동시 빌드 ──────────────────────────────────────────────
    # long  → report_{day}.md    : 본문 포함 (TRAE 분석용)
    # short → digest_{day}.md    : 링크+제목만 (직접 열람용)
    # ──────────────────────────────────────────────────────────────────
    header = f"# Daily Report - {day}\n\n"
    long_content  = header
    short_content = header
    seen_global_titles = set()

    def _both(text):
        nonlocal long_content, short_content
        long_content  += text
        short_content += text

    def _long(text):
        nonlocal long_content
        long_content += text

    # ── Refined Signals ───────────────────────────────────────────────
    _both("## Refined Signals\n")
    if not refined_highlights:
        _both("(None)\n")
    else:
        seen_refined_keys = set()
        for row in refined_highlights:
            refined_key = (row["channel_id"], row["message_id"])
            if refined_key in seen_refined_keys:
                continue
            if row["relevance_score"] < REFINED_MIN_RELEVANCE or row["confidence"] < REFINED_MIN_CONFIDENCE:
                continue
            seen_refined_keys.add(refined_key)
            title, _, _ = _extract_title_and_url(row["raw_json"], row["raw_text"])
            if _is_weak_refined_title(title):
                continue
            tglink = _tg_link(row["channel_id"], row["message_id"])
            kst = _kst_str(row["message_date"])
            meta = f"{row['event_type']} / {row['sentiment']} / rel={row['relevance_score']:.2f}"
            link_text = _md_link_text(f"{kst} | {title}")
            _both(f"- [{link_text}]({tglink})  —  {meta}\n")
            if row["summary"]:
                _long(f"  > {_md_link_text(row['summary'])}\n")

    # ── Key Companies ────────────────────────────────────────────────
    _both("\n## Key Companies\n")
    for row in top_entities:
        display_name = row['entity_name']
        if row['entity_type'] == 'KR_CODE' and re.fullmatch(r"\d{6}", row['entity_name']):
            if start_utc_str and end_utc_str:
                label = resolve_kr_label(row['entity_name'], cursor, start_utc_str, end_utc_str)
                if label:
                    display_name = f"{label} ({row['entity_name']})"
        display_name = _clean_display_name(display_name)
        meta = ENTITY_META.get(row['entity_name'], {})
        ticker = meta.get("ticker", "")
        sector = meta.get("sector", "")
        related = meta.get("related", [])
        meta_parts = []
        if ticker:
            meta_parts.append(f"`{ticker}`")
        if sector:
            meta_parts.append(f"섹터: {sector}")
        meta_str = " | ".join(meta_parts)
        entity_header = f"- **{display_name}**: {row['count']}건"
        if meta_str:
            entity_header += f"  —  {meta_str}"
        entity_header += "\n"
        _both(entity_header)
        if related:
            _both(f"  - 📌 아이디어 확장 검토: {', '.join(related)}\n")
        cursor.execute(f"""
            SELECT m.channel_id, m.message_id, m.message_date, m.raw_text, m.raw_json
            FROM extracted_entities e
            JOIN raw_messages m ON e.raw_id = m.id
            {date_filter} {'AND' if date_filter else 'WHERE'} e.entity_name = ?
            ORDER BY m.message_date DESC, m.id DESC
            LIMIT {EVIDENCE_PER_ITEM * 10}
        """, (row['entity_name'],))
        evid = cursor.fetchall()
        seen_ev = set()
        seen_ev_titles = set()
        shown_ev = 0
        for ev in evid:
            if shown_ev >= EVIDENCE_PER_ITEM:
                break
            key = (ev['channel_id'], ev['message_id'])
            if key in seen_ev:
                continue
            title, _, description = _extract_title_and_url(ev['raw_json'], ev['raw_text'])
            title_key = ' '.join(title.lower().split())[:80]
            if title_key and title_key != "(no title)" and title_key in seen_global_titles:
                continue
            seen_ev.add(key)
            if title_key and title_key != "(no title)":
                seen_global_titles.add(title_key)
            shown_ev += 1
            tglink = _tg_link(ev['channel_id'], ev['message_id'])
            kst = _kst_str(ev['message_date'])
            link_text = _md_link_text(f"{display_name} | {kst} | {title}")
            _both(f"  - [{link_text}]({tglink})\n")
            description = _refined_description(refined_map, ev['channel_id'], ev['message_id'], description)
            if description:
                _long(f"    > {_md_link_text(description)}\n")

    # ── Top Keywords ──────────────────────────────────────────────────
    seen_kw_links = set()
    _both("\n## Top Keywords\n")
    for row in top_keywords:
        kw_upper = row['keyword'].upper()
        if kw_upper in KEYWORD_BLACKLIST:
            continue
        _both(f"- **{row['keyword']}** ({row['category']}): {row['count']}\n")
        cursor.execute(f"""
            SELECT m.channel_id, m.message_id, m.message_date, m.raw_text, m.raw_json
            FROM extracted_keywords k
            JOIN raw_messages m ON k.raw_id = m.id
            {date_filter} {'AND' if date_filter else 'WHERE'} k.keyword = ? AND k.category = ?
            ORDER BY m.message_date DESC, m.id DESC
            LIMIT {EVIDENCE_PER_ITEM * 10}
        """, (row['keyword'], row['category']))
        evid = cursor.fetchall()
        shown = 0
        for ev in evid:
            if shown >= EVIDENCE_PER_ITEM:
                break
            key = (ev['channel_id'], ev['message_id'])
            if key in seen_kw_links:
                continue
            title, _, description = _extract_title_and_url(ev['raw_json'], ev['raw_text'])
            title_key = ' '.join(title.lower().split())[:80]
            if title_key and title_key != "(no title)" and title_key in seen_global_titles:
                continue
            seen_kw_links.add(key)
            if title_key and title_key != "(no title)":
                seen_global_titles.add(title_key)
            shown += 1
            tglink = _tg_link(ev['channel_id'], ev['message_id'])
            kst = _kst_str(ev['message_date'])
            link_text = _md_link_text(f"{row['keyword']} | {kst} | {title}")
            _both(f"  - [{link_text}]({tglink})\n")
            description = _refined_description(refined_map, ev['channel_id'], ev['message_id'], description)
            if description:
                _long(f"    > {_md_link_text(description)}\n")

    # ── Unknown Candidates ────────────────────────────────────────────
    _both("\n## Unknown Candidates\n")
    if not unknown_candidates:
        _both("(None)\n")
    else:
        seen_unk_links = set()
        for token, count in unknown_candidates[:TOP_N_UNKNOWN]:
            _both(f"- **{token}**: {count}\n")
            refs = sorted(unknown_refs[token], key=lambda x: (x['message_date'], x['message_id']), reverse=True)
            shown = 0
            for ref in refs:
                if shown >= EVIDENCE_PER_ITEM:
                    break
                key = (ref['channel_id'], ref['message_id'])
                if key in seen_unk_links:
                    continue
                title, _, description = _extract_title_and_url(ref['raw_json'], ref['raw_text'])
                title_key = ' '.join(title.lower().split())[:80]
                if title_key and title_key != "(no title)" and title_key in seen_global_titles:
                    continue
                seen_unk_links.add(key)
                if title_key and title_key != "(no title)":
                    seen_global_titles.add(title_key)
                shown += 1
                tglink = _tg_link(ref['channel_id'], ref['message_id'])
                kst = _kst_str(ref['message_date'])
                link_text = _md_link_text(f"{token} | {kst} | {title}")
                _both(f"  - [{link_text}]({tglink})\n")
                description = _refined_description(refined_map, ref['channel_id'], ref['message_id'], description)
                if description:
                    _long(f"    > {_md_link_text(description)}\n")

    # ── Ambiguous Hits ────────────────────────────────────────────────
    _both("\n## Ambiguous Hits\n")
    if not ambiguous_hits:
        _both("(None)\n")
    else:
        sorted_keys = sorted(ambiguous_hits.keys(), key=lambda x: (x[0], x[1]))
        for entity, match_text in sorted_keys:
            refs = ambiguous_hits[(entity, match_text)]
            _both(f"- **{entity}** (matched: '{match_text}'): {len(refs)}\n")
            refs_sorted = sorted(refs, key=lambda x: (x['message_date'], x['message_id']), reverse=True)[:EVIDENCE_PER_ITEM]
            for ref in refs_sorted:
                title, _, description = _extract_title_and_url(ref['raw_json'], ref['raw_text'])
                title_key = ' '.join(title.lower().split())[:80]
                if title_key and title_key != "(no title)" and title_key in seen_global_titles:
                    continue
                if title_key and title_key != "(no title)":
                    seen_global_titles.add(title_key)
                tglink = _tg_link(ref['channel_id'], ref['message_id'])
                kst = _kst_str(ref['message_date'])
                base = f"{entity} | {match_text}"
                link_text = _md_link_text(f"{base} | {kst} | {title}")
                _both(f"  - [{link_text}]({tglink})\n")
                description = _refined_description(refined_map, ref['channel_id'], ref['message_id'], description)
                if description:
                    _long(f"    > {_md_link_text(description)}\n")

    # ── Save both files ───────────────────────────────────────────────
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    suffix = day if day else "latest"

    long_path  = REPORT_DIR / f"report_{suffix}.md"
    short_path = REPORT_DIR / f"digest_{suffix}.md"

    with open(long_path,  "w", encoding="utf-8-sig") as f:
        f.write(long_content)
    with open(short_path, "w", encoding="utf-8-sig") as f:
        f.write(short_content)

    print(f"[TRAE용]  {long_path}")
    print(f"[열람용]  {short_path}")
    conn.close()
