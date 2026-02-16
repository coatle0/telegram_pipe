# Telegram Channel News → Entity/Keyword Pipeline v0.1

A robust, rule-first extraction pipeline for Telegram financial news.

## Features
- **Immutable Log Storage**: Raw messages are stored in SQLite with triggers preventing modification.
- **Deterministic Extraction**: Rule-based approach using regex, aliases, and ambiguity contexts.
- **Reproducible**: Separation of raw ingestion, text processing, and extraction steps.

## Directory Structure
```
repo/
  app/
    config.py       # Configuration loader
    db.py           # Database connection & safety checks
    schema.sql      # SQLite schema with triggers
    ingest.py       # Telethon ingestion logic (Mocked in v0.1)
    process.py      # Text cleaning & normalization
    extract.py      # Entity & Keyword extraction
    report.py       # Markdown report generation
    rules/          # JSON-based rules (Seeds)
  cli.py            # Typer CLI entry point
  tests/            # Smoke tests
  data/             # SQLite DB location
  outputs/          # Generated reports
```

## Quick Start

1. **Install Dependencies**
   ```bash
   pip install typer sqlite3 pytest pyyaml
   ```

2. **Initialize Database**
   ```bash
   python cli.py init
   ```

3. **Run Pipeline (Manual Simulation)**
   ```bash
   # Set Safety Flag
   export ALLOW_WRITE=1  # (PowerShell: $env:ALLOW_WRITE="1")

   # Ingest (Mock)
   python cli.py ingest

   # Process
   python cli.py process

   # Extract
   python cli.py extract

   # Report
   python cli.py report --day 2024-01-01
   ```

4. **Run Tests**
   ```bash
   python tests/test_smoke.py
   ```

## Constraints
- **ALLOW_WRITE=1** environment variable is required for any operation that writes to the DB.
- Raw messages cannot be updated or deleted.
