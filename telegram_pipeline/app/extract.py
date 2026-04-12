import json
import re
from pathlib import Path
from app.db import get_connection, check_write_permission

# Load Rules
RULES_DIR = Path(__file__).parent / "rules"

def load_json_rule(filename):
    with open(RULES_DIR / filename, "r", encoding="utf-8") as f:
        return json.load(f)

ENTITIES_SEED = load_json_rule("entities_seed.json")
KEYWORDS_SEED = load_json_rule("keywords_seed.json")
AMBIGUITY_RULES = load_json_rule("ambiguity.json")

# Build map for canonicalization (Alias/EntityName -> PrimaryTicker)
CANONICAL_MAP = {}
for rule in ENTITIES_SEED:
    primary = rule.get("primary_ticker")
    if primary:
        CANONICAL_MAP[rule["entity_name"]] = primary
        for alias in rule.get("aliases", []):
            CANONICAL_MAP[alias] = primary

def extract_entities_from_text(text: str):
    raw_results = []
    
    # 1. Explicit Ticker Patterns
    # $AAPL, NASDAQ:TSLA, 6-digit KR (005930), .KS suffix
    ticker_patterns = [
        (r'\$([A-Z]{1,5})', 'US_TICKER'),
        (r'NASDAQ:([A-Z]{1,5})', 'US_TICKER'),
        (r'\b([0-9]{6})\b', 'KR_CODE'),
        (r'\b([0-9]{6})\.KS\b', 'KR_CODE')
    ]
    
    for pat, type_label in ticker_patterns:
        matches = re.finditer(pat, text)
        for m in matches:
            raw_results.append({
                "entity_name": m.group(1), # Placeholder name using code
                "entity_type": type_label,
                "match_text": m.group(0),
                "confidence": 1.0,
                "is_ambiguous": False
            })

    # 2. Alias Dictionary Match
    for rule in ENTITIES_SEED:
        aliases = rule.get("aliases", [])
        aliases.append(rule["entity_name"])
        
        for alias in aliases:
            # Simple word boundary check
            # Escaping regex characters in alias is important
            pattern = r'\b' + re.escape(alias) + r'\b'
            if re.search(pattern, text, re.IGNORECASE):
                # 3. Ambiguity Check
                is_ambiguous = False
                for neg in AMBIGUITY_RULES["negative_contexts"]:
                    if neg in text: # Naive context check, can be improved to proximity
                        is_ambiguous = True
                        break
                
                raw_results.append({
                    "entity_name": rule["entity_name"],
                    "entity_type": rule["entity_type"],
                    "match_text": alias,
                    "confidence": 0.8 if is_ambiguous else 1.0,
                    "is_ambiguous": is_ambiguous,
                    "entity_rule_id": None # Would lookup DB id in real app
                })
                break # Match once per entity rule per message

    # Deduplication and Canonicalization
    # Group by canonical key
    groups = {}
    for res in raw_results:
        # Determine canonical key
        name = res["entity_name"]
        if res["entity_type"] in ['US_TICKER', 'KR_CODE']:
            # For tickers, the name itself (the code) is the canonical key
            canon_key = name
        else:
            # For aliases, map to primary ticker if available, else use name
            canon_key = CANONICAL_MAP.get(name, name)
            
        if canon_key not in groups:
            groups[canon_key] = []
        groups[canon_key].append(res)
        
    final_results = []
    for key, candidates in groups.items():
        # Select best candidate
        # Priority:
        # 1) Ticker with suffix (.KS)
        # 2) Higher confidence
        # 3) Longer match text
        
        def priority_key(c):
            # 1. Suffix check (only relevant for tickers really, but safe to check text)
            has_suffix = ".KS" in c["match_text"] or "." in c["match_text"] # Naive suffix check
            # 2. Confidence
            conf = c["confidence"]
            # 3. Length
            length = len(c["match_text"])
            return (has_suffix, conf, length)
            
        best = max(candidates, key=priority_key)
        final_results.append(best)
        
    return final_results

def extract_keywords_from_text(text: str, taxonomy=None):
    if taxonomy is None:
        taxonomy = KEYWORDS_SEED
        
    raw_results = []
    
    for rule in taxonomy:
        kw = rule["keyword"]
        match_type = rule.get("match_type", "EXACT")
        category = rule["category"]
        
        matches = []
        
        if match_type == "EXACT":
            # Find all occurrences
            # Escaping kw for regex finditer is safer even for EXACT to get spans
            pattern = re.escape(kw)
            for m in re.finditer(pattern, text, re.IGNORECASE):
                matches.append(m)
                
        elif match_type == "REGEX":
            for m in re.finditer(kw, text, re.IGNORECASE):
                matches.append(m)
                
        for m in matches:
            raw_results.append({
                "keyword": kw, # Canonical keyword from rule
                "category": category,
                "match_text": m.group(0),
                "match_start": m.start(),
                "match_len": m.end() - m.start()
            })
            
    # Deduplicate: same (category, canonical_keyword) should appear once per message.
    # We prioritize the first occurrence (lowest match_start).
    # If multiple matches start at same position (e.g. overlapping regex), take longest match_len.
    
    # First, sort by start asc, then len desc
    raw_results.sort(key=lambda x: (x["match_start"], -x["match_len"]))
    
    unique_map = {} # (category, keyword) -> match
    
    for res in raw_results:
        key = (res["category"], res["keyword"])
        if key not in unique_map:
            unique_map[key] = res
            
    # Deterministic output sorting:
    # sort by (category, canonical_keyword, match_start, match_len)
    final_results = list(unique_map.values())
    final_results.sort(key=lambda x: (x["category"], x["keyword"], x["match_start"], x["match_len"]))
    
    return final_results

def run_extract(since=None, until=None):
    """
    Extract entities and keywords from processed_messages.
    """
    check_write_permission()
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT p.raw_id, p.cleaned_text
        FROM processed_messages p
        JOIN raw_messages r ON r.id = p.raw_id
    """
    params = []
    conditions = []

    if since is not None:
        conditions.append("r.message_date >= ?")
        params.append(since)
    if until is not None:
        conditions.append("r.message_date < ?")
        params.append(until)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    for row in rows:
        raw_id = row['raw_id']
        text = row['cleaned_text']

        # Recompute extraction idempotently for the selected scope to avoid
        # duplicates or partial state from interrupted runs.
        cursor.execute("DELETE FROM extracted_entities WHERE raw_id = ?", (raw_id,))
        cursor.execute("DELETE FROM extracted_keywords WHERE raw_id = ?", (raw_id,))
            
        entities = extract_entities_from_text(text)
        for e in entities:
            cursor.execute("""
                INSERT INTO extracted_entities 
                (raw_id, entity_name, entity_type, confidence, match_text, is_ambiguous)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (raw_id, e['entity_name'], e['entity_type'], e['confidence'], e['match_text'], e['is_ambiguous']))
            
        keywords = extract_keywords_from_text(text)
        for k in keywords:
            cursor.execute("""
                INSERT INTO extracted_keywords
                (raw_id, keyword, category, match_text)
                VALUES (?, ?, ?, ?)
            """, (raw_id, k['keyword'], k['category'], k['match_text']))
            
    conn.commit()
    conn.close()
    print("Extraction complete.")
