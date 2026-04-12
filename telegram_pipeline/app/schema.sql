-- Raw Log Storage (Immutable)
CREATE TABLE IF NOT EXISTS raw_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    message_date DATETIME NOT NULL, -- UTC
    raw_text TEXT NOT NULL,
    raw_json TEXT NOT NULL, -- Full JSON payload
    content_hash TEXT NOT NULL, -- SHA256(raw_text + stable_fields)
    duplicate_of INTEGER, -- FK to raw_messages.id if duplicate
    ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(duplicate_of) REFERENCES raw_messages(id)
);

CREATE INDEX IF NOT EXISTS idx_raw_content_hash ON raw_messages(content_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_msg_unique ON raw_messages(channel_id, message_id);

-- Immutable Triggers
CREATE TRIGGER IF NOT EXISTS prevent_raw_update
BEFORE UPDATE ON raw_messages
BEGIN
    SELECT RAISE(ABORT, 'Update not allowed on raw_messages');
END;

CREATE TRIGGER IF NOT EXISTS prevent_raw_delete
BEFORE DELETE ON raw_messages
BEGIN
    SELECT RAISE(ABORT, 'Delete not allowed on raw_messages');
END;

-- Processed Text (Idempotent)
CREATE TABLE IF NOT EXISTS processed_messages (
    raw_id INTEGER PRIMARY KEY,
    cleaned_text TEXT NOT NULL,
    is_repost BOOLEAN DEFAULT 0,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(raw_id) REFERENCES raw_messages(id)
);

-- Entity Dictionary (Rule-based)
CREATE TABLE IF NOT EXISTS entity_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_name TEXT NOT NULL,
    entity_type TEXT NOT NULL, -- 'STOCK_KR', 'STOCK_US', 'CRYPTO', etc.
    primary_ticker TEXT,
    aliases TEXT, -- JSON list of alias strings
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Keyword Taxonomy (Rule-based)
CREATE TABLE IF NOT EXISTS keyword_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword TEXT NOT NULL,
    category TEXT NOT NULL, -- 'Theme', 'Event', 'ValueChain', 'Macro'
    match_type TEXT DEFAULT 'EXACT', -- 'EXACT', 'REGEX'
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Extraction Results
CREATE TABLE IF NOT EXISTS extracted_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id INTEGER NOT NULL,
    entity_rule_id INTEGER, -- Link back to rule if matched
    entity_name TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    confidence REAL DEFAULT 1.0,
    match_text TEXT NOT NULL,
    is_ambiguous BOOLEAN DEFAULT 0,
    extracted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(raw_id) REFERENCES raw_messages(id),
    FOREIGN KEY(entity_rule_id) REFERENCES entity_rules(id)
);

CREATE TABLE IF NOT EXISTS extracted_keywords (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id INTEGER NOT NULL,
    keyword_rule_id INTEGER,
    keyword TEXT NOT NULL,
    category TEXT NOT NULL,
    match_text TEXT NOT NULL,
    extracted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(raw_id) REFERENCES raw_messages(id),
    FOREIGN KEY(keyword_rule_id) REFERENCES keyword_rules(id)
);

-- Defects / Quality Issues
CREATE TABLE IF NOT EXISTS defects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id INTEGER,
    defect_type TEXT NOT NULL, -- 'PARSE_ERROR', 'AMBIGUOUS', 'UNKNOWN_FORMAT'
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(raw_id) REFERENCES raw_messages(id)
);

CREATE TABLE IF NOT EXISTS article_tags (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id   TEXT NOT NULL,
    channel_id   TEXT NOT NULL,
    tag_group    TEXT NOT NULL,
    keyword      TEXT NOT NULL,
    message_date TEXT,
    raw_text     TEXT,
    UNIQUE(message_id, channel_id, keyword)
);

CREATE INDEX IF NOT EXISTS idx_article_tags_group ON article_tags(tag_group);
CREATE INDEX IF NOT EXISTS idx_article_tags_date ON article_tags(message_date);

CREATE TABLE IF NOT EXISTS llm_refined_news (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL UNIQUE,
    relevance_score REAL NOT NULL,
    sentiment TEXT NOT NULL,
    event_type TEXT NOT NULL,
    summary TEXT NOT NULL,
    tickers TEXT NOT NULL,
    entities TEXT NOT NULL,
    bull_points TEXT NOT NULL,
    bear_points TEXT NOT NULL,
    noise_flags TEXT NOT NULL,
    confidence REAL NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
