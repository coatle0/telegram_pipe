# Boot Sequence

1. **Environment Setup**
   - Python 3.9+
   - SQLite3

2. **Installation**
   ```bash
   pip install -r requirements.txt
   ```

3. **Database Initialization**
   ```bash
   python cli.py init
   ```
   This creates `data/risk_commander.sqlite` and applies `app/schema.sql`.

4. **Running the Pipeline**
   - Ingest: `python cli.py ingest`
   - Process: `python cli.py process`
   - Extract: `python cli.py extract`
   - Report: `python cli.py report`
