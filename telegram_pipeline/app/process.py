import re
from app.db import get_connection, check_write_permission

def clean_text(text: str) -> str:
    """
    Normalize text:
    - Replace URLs with <URL>
    - Remove zero-width chars
    - Normalize whitespace
    - Strip forward headers (basic regex)
    """
    if not text:
        return ""
    
    # Replace URLs
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '<URL>', text)
    
    # Zero-width chars
    text = text.replace('\u200b', '').replace('\u200c', '').replace('\u200d', '')
    
    # Forward headers (Korean/English common patterns)
    text = re.sub(r'Forwarded from:.*', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\[Forwarded from .*\]', '', text, flags=re.IGNORECASE)
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def process_messages(since=None, until=None):
    """
    Read raw_messages, clean text, insert into processed_messages.
    Idempotent: uses INSERT OR IGNORE or checks existence.
    """
    check_write_permission()
    conn = get_connection()
    cursor = conn.cursor()
    
    # Select unprocessed messages
    # In v0.1, we can just select all that aren't in processed_messages
    cursor.execute("""
        SELECT r.id, r.raw_text, r.duplicate_of 
        FROM raw_messages r
        LEFT JOIN processed_messages p ON r.id = p.raw_id
        WHERE p.raw_id IS NULL
    """)
    
    rows = cursor.fetchall()
    count = 0
    
    for row in rows:
        # If it's a content duplicate, we might still process it but mark it? 
        # Requirement says "duplicate_of set if same content_hash exists".
        # Let's process everything but maybe flag is_repost if duplicate_of is not null
        
        cleaned = clean_text(row['raw_text'])
        is_repost = 1 if row['duplicate_of'] else 0
        
        # Additional repost detection logic could go here (e.g. "RT @...")
        if "RT @" in cleaned or "repost" in cleaned.lower():
            is_repost = 1
            
        cursor.execute("""
            INSERT INTO processed_messages (raw_id, cleaned_text, is_repost)
            VALUES (?, ?, ?)
        """, (row['id'], cleaned, is_repost))
        count += 1
        
    conn.commit()
    conn.close()
    print(f"Processed {count} messages.")
