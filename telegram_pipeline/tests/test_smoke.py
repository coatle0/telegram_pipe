import os
import sqlite3
import pytest
from datetime import datetime
from app.db import init_db, get_connection
from app.ingest import ingest_message
from app.process import process_messages
from app.extract import run_extract

# Set env for testing
os.environ["ALLOW_WRITE"] = "1"

def test_pipeline_smoke():
    # 1. Init DB
    if os.path.exists("data/risk_commander.sqlite"):
        os.remove("data/risk_commander.sqlite")
    init_db()
    
    # 2. Ingest Sample Messages
    # Msg 1: Explicit Ticker + Keyword
    ingest_message(
        channel_id=1001,
        message_id=1,
        message_date=datetime.utcnow(),
        raw_text="Samsung Electronics (005930) reports strong AI earnings (실적). HBM demand rising.",
        raw_json={"dummy": True}
    )
    
    # Msg 2: Alias + Ambiguity
    ingest_message(
        channel_id=1001,
        message_id=2,
        message_date=datetime.utcnow(),
        raw_text="This is not related to Apple but similar to Tesla.",
        raw_json={"dummy": True}
    )
    
    # 3. Process
    process_messages()
    
    # 4. Extract
    run_extract()
    
    # 5. Verification
    conn = get_connection()
    cursor = conn.cursor()
    
    # Check Processed
    cursor.execute("SELECT * FROM processed_messages")
    processed = cursor.fetchall()
    assert len(processed) == 2
    
    # Check Entities (Msg 1)
    cursor.execute("SELECT * FROM extracted_entities WHERE raw_id = (SELECT id FROM raw_messages WHERE message_id=1)")
    ents1 = cursor.fetchall()
    # Should match Samsung (Alias) and 005930 (Ticker) -> Deduplication should reduce to 1
    assert len(ents1) == 1 
    names1 = [r['entity_name'] for r in ents1]
    # Either Samsung Electronics or 005930 should be present
    assert "Samsung Electronics" in names1 or "005930" in names1
    
    # Check Keywords (Msg 1)
    cursor.execute("SELECT * FROM extracted_keywords WHERE raw_id = (SELECT id FROM raw_messages WHERE message_id=1)")
    kws1 = cursor.fetchall()
    kw_texts = [r['keyword'] for r in kws1]
    
    # Must find all 3 keywords: AI, HBM, 실적
    assert "AI" in kw_texts
    assert "HBM" in kw_texts
    assert "실적" in kw_texts
    assert len(kws1) >= 3
    
    # Check Ambiguity (Msg 2)
    cursor.execute("SELECT * FROM extracted_entities WHERE raw_id = (SELECT id FROM raw_messages WHERE message_id=2)")
    ents2 = cursor.fetchall()
    # Apple should be ambiguous because of "not related to"
    for ent in ents2:
        if ent['entity_name'] == "Apple":
            assert ent['is_ambiguous'] == 1
            
    # 6. Immutability Test
    with pytest.raises(sqlite3.IntegrityError):
        cursor.execute("UPDATE raw_messages SET raw_text='Hacked' WHERE message_id=1")
        
    conn.close()
    print("Smoke test passed!")

if __name__ == "__main__":
    test_pipeline_smoke()
