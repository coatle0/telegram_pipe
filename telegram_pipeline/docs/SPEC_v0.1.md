# Specification v0.1

## Core Principles
1. **Immutability**: Raw logs are never altered.
2. **Determinism**: Extraction logic produces the same output for the same input and rules.
3. **Safety**: Write operations require explicit `ALLOW_WRITE=1` flag.

## Data Flow
1. **Ingest**: Telethon -> `raw_messages` (Raw Text + JSON)
2. **Process**: `raw_messages` -> `processed_messages` (Cleaned Text)
3. **Extract**: `processed_messages` -> `extracted_entities` / `extracted_keywords`

## Schema
See `app/schema.sql` for details.
