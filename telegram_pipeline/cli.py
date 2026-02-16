import typer
from typing import Optional
from datetime import datetime
from app.db import init_db
from app.ingest import run_ingest
from app.process import process_messages
from app.extract import run_extract
from app.report import generate_report

app = typer.Typer()

@app.command()
def init():
    """Initialize DB and seed rules."""
    init_db()
    print("Database initialized.")

@app.command()
def ingest(config: str = "configs/config.yaml", since: Optional[datetime] = None, until: Optional[datetime] = None):
    run_ingest(config, since, until)

@app.command()
def process(since: Optional[datetime] = None, until: Optional[datetime] = None):
    process_messages(since, until)

@app.command()
def extract(since: Optional[datetime] = None, until: Optional[datetime] = None):
    run_extract(since, until)

@app.command()
def report(day: Optional[str] = None, week: Optional[str] = None):
    generate_report(day, week)

@app.command()
def reprocess(since: Optional[datetime] = None, until: Optional[datetime] = None):
    """
    Full re-run of process + extract. 
    Does NOT delete raw_messages (immutable).
    """
    # In a real app, we might clear processed/extracted tables for the range first
    # For v0.1, we assume append-only or simple logic
    print("Reprocessing...")
    process_messages(since, until)
    run_extract(since, until)

if __name__ == "__main__":
    app()
