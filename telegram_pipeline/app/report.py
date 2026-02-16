import sqlite3
import re
import json
from datetime import datetime, timezone, timedelta
from collections import Counter
from pathlib import Path
from app.db import get_connection

REPORT_DIR = Path("outputs/reports")
RULES_DIR = Path(__file__).parent / "rules"
EVIDENCE_PER_ITEM = 3

def load_stopwords():
    try:
        with open(RULES_DIR / "stopwords.json", "r", encoding="utf-8") as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

STOPWORDS = load_stopwords()
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

def _extract_title_and_url(raw_json_str: str, raw_text: str):
    payload = _parse_raw_json(raw_json_str)
    title = None
    url = None
    try:
        media = payload.get("media") or {}
        webpage = media.get("webpage") or {}
        title = webpage.get("title")
        url = webpage.get("url")
    except Exception:
        pass
    if title and isinstance(title, str) and title.strip():
        return title.strip()[:200], url
    # fallback to first non-empty line in raw_text
    return _first_non_empty_line(raw_text, 200), url

def _tg_link(channel_id: int, message_id: int) -> str:
    # Telegram deep link using tg://privatepost scheme
    # If internal (-100...) id, convert to internal short id
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
    return text.replace("]", ")")

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
    
    # Top Entities
    cursor.execute(f"""
        SELECT entity_name, entity_type, COUNT(*) as count 
        FROM extracted_entities e
        JOIN raw_messages m ON e.raw_id = m.id
        {date_filter}
        GROUP BY entity_name, entity_type
        ORDER BY count DESC 
        LIMIT 10
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
        LIMIT 10
    """)
    top_keywords = cursor.fetchall()
    
    # New Sections
    # Pass date_filter (WHERE clause) directly to helper functions
    # Note: helper functions expect 'date_filter' string starting with WHERE or empty
    unknown_candidates, unknown_refs = get_unknown_candidates(cursor, date_filter)
    ambiguous_hits = get_ambiguous_hits(cursor, date_filter)
    
    report_content = f"# Daily Report - {day}\n\n"
    
    report_content += "## Top Entities\n"
    for row in top_entities:
        display_name = row['entity_name']
        # KR code labeling for 6-digit codes
        if row['entity_type'] == 'KR_CODE' and re.fullmatch(r"\d{6}", row['entity_name']):
            if start_utc_str and end_utc_str:
                label = resolve_kr_label(row['entity_name'], cursor, start_utc_str, end_utc_str)
                if label:
                    display_name = f"{label} ({row['entity_name']})"
        report_content += f"- **{display_name}**: {row['count']}\n"
        cursor.execute(f"""
            SELECT m.channel_id, m.message_id, m.message_date, m.raw_text, m.raw_json
            FROM extracted_entities e
            JOIN raw_messages m ON e.raw_id = m.id
            {date_filter} {'AND' if date_filter else 'WHERE'} e.entity_name = ?
            ORDER BY m.message_date DESC, m.id DESC
            LIMIT {EVIDENCE_PER_ITEM}
        """, (row['entity_name'],))
        evid = cursor.fetchall()
        for ev in evid:
            title, _ = _extract_title_and_url(ev['raw_json'], ev['raw_text'])
            tglink = _tg_link(ev['channel_id'], ev['message_id'])
            kst = _kst_str(ev['message_date'])
            link_text = _md_link_text(f"{display_name} | {kst} | {title}")
            report_content += f"  - [{link_text}]({tglink})\n"
        
    report_content += "\n## Top Keywords\n"
    for row in top_keywords:
        kw_upper = row['keyword'].upper()
        if kw_upper in KEYWORD_BLACKLIST:
            continue
        report_content += f"- **{row['keyword']}** ({row['category']}): {row['count']}\n"
        cursor.execute(f"""
            SELECT m.channel_id, m.message_id, m.message_date, m.raw_text, m.raw_json
            FROM extracted_keywords k
            JOIN raw_messages m ON k.raw_id = m.id
            {date_filter} {'AND' if date_filter else 'WHERE'} k.keyword = ? AND k.category = ?
            ORDER BY m.message_date DESC, m.id DESC
            LIMIT {EVIDENCE_PER_ITEM}
        """, (row['keyword'], row['category']))
        evid = cursor.fetchall()
        for ev in evid:
            title, _ = _extract_title_and_url(ev['raw_json'], ev['raw_text'])
            tglink = _tg_link(ev['channel_id'], ev['message_id'])
            kst = _kst_str(ev['message_date'])
            link_text = _md_link_text(f"{row['keyword']} | {kst} | {title}")
            report_content += f"  - [{link_text}]({tglink})\n"

    report_content += "\n## Unknown Candidates\n"
    if not unknown_candidates:
        report_content += "(None)\n"
    else:
        for token, count in unknown_candidates[:20]:
            report_content += f"- **{token}**: {count}\n"
            refs = sorted(unknown_refs[token], key=lambda x: (x['message_date'], x['message_id']), reverse=True)[:EVIDENCE_PER_ITEM]
            for ref in refs:
                title, _ = _extract_title_and_url(ref['raw_json'], ref['raw_text'])
                tglink = _tg_link(ref['channel_id'], ref['message_id'])
                kst = _kst_str(ref['message_date'])
                link_text = _md_link_text(f"{token} | {kst} | {title}")
                report_content += f"  - [{link_text}]({tglink})\n"
    
    report_content += "\n## Ambiguous Hits\n"
    if not ambiguous_hits:
        report_content += "(None)\n"
    else:
        sorted_keys = sorted(ambiguous_hits.keys(), key=lambda x: (x[0], x[1]))
        for entity, match_text in sorted_keys:
            refs = ambiguous_hits[(entity, match_text)]
            report_content += f"- **{entity}** (matched: '{match_text}'): {len(refs)}\n"
            refs_sorted = sorted(refs, key=lambda x: (x['message_date'], x['message_id']), reverse=True)[:EVIDENCE_PER_ITEM]
            for ref in refs_sorted:
                title, _ = _extract_title_and_url(ref['raw_json'], ref['raw_text'])
                tglink = _tg_link(ref['channel_id'], ref['message_id'])
                kst = _kst_str(ref['message_date'])
                base = f"{entity} | {match_text}"
                link_text = _md_link_text(f"{base} | {kst} | {title}")
                report_content += f"  - [{link_text}]({tglink})\n"
        
    # Save
    filename = f"report_{day}.md" if day else "report_latest.md"
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_DIR / filename, "w", encoding="utf-8-sig") as f:
        f.write(report_content)
        
    print(f"Report generated: {REPORT_DIR / filename}")
    conn.close()
