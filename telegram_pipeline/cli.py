import typer
from typing import Optional
from datetime import datetime, timedelta, timezone
from app.db import init_db
from app.ingest import run_ingest
from app.process import process_messages
from app.extract import run_extract
from app.report import generate_report
from app.analyze import analyze_report
from app.refine import import_refined_json

app = typer.Typer()


def _resolve_day_range(day: Optional[str]):
    if not day:
        return None, None

    try:
        kst = timezone(timedelta(hours=9))
        day_kst = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=kst)
        since = day_kst.astimezone(timezone.utc).replace(tzinfo=None)
        until = (day_kst + timedelta(days=1)).astimezone(timezone.utc).replace(tzinfo=None)
        return since, until
    except ValueError as exc:
        raise typer.BadParameter("Invalid format for --day. Use YYYY-MM-DD.") from exc

@app.command()
def init():
    """Initialize DB and seed rules."""
    init_db()
    print("Database initialized.")

@app.command()
def ingest(
    config: str = "configs/config.yaml",
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    day: Optional[str] = None  # Format: YYYY-MM-DD
):
    """
    Ingest messages from Telegram channels.
    
    If --day is provided (YYYY-MM-DD), it sets --since to that day 00:00:00 and --until to next day 00:00:00.
    """
    if day:
        since, until = _resolve_day_range(day)
        print(f"Ingesting for day: {day} KST (UTC range: {since} -> {until})")

    run_ingest(config, since, until)

@app.command()
def process(
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    day: Optional[str] = None
):
    if day:
        since, until = _resolve_day_range(day)
    process_messages(since, until)

@app.command()
def extract(
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    day: Optional[str] = None
):
    if day:
        since, until = _resolve_day_range(day)
    run_extract(since, until)

@app.command()
def report(day: Optional[str] = None, week: Optional[str] = None):
    generate_report(day, week)

@app.command()
def analyze(day: Optional[str] = None):
    """Generate AI investment commentary from the daily report via Claude API."""
    if not day:
        from datetime import date
        day = date.today().strftime("%Y-%m-%d")
    analyze_report(day)

@app.command()
def refine_import(
    json_file: Optional[str] = typer.Argument(None, help="Path to TRAE/LLM refined JSON array file."),
    json_file_option: Optional[str] = typer.Option(
        None,
        "--json-file",
        help="Path to TRAE/LLM refined JSON array file.",
    ),
):
    if json_file and json_file_option:
        raise typer.BadParameter("Use either the positional JSON_FILE or --json-file, not both.")

    json_path = json_file_option or json_file
    if not json_path:
        raise typer.BadParameter("Provide a JSON file path via JSON_FILE or --json-file.")

    result = import_refined_json(json_path)
    print(
        f"refine_import done: total={result['total']} inserted={result['inserted']} updated={result['updated']} errors={result['errors']}"
    )

@app.command()
def reprocess(
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    day: Optional[str] = None
):
    """
    Full re-run of process + extract. 
    Does NOT delete raw_messages (immutable).
    """
    if day:
        since, until = _resolve_day_range(day)
    print("Reprocessing...")
    process_messages(since, until)
    run_extract(since, until)


@app.command("run-day")
def run_day(day: str, config: str = "configs/config.yaml"):
    """
    Run ingest -> process -> extract -> report for one KST day.
    """
    since, until = _resolve_day_range(day)
    print(f"Running full daily pipeline for {day} KST (UTC range: {since} -> {until})")
    run_ingest(config, since, until)
    process_messages(since, until)
    run_extract(since, until)
    generate_report(day, None)
    print(f"Daily pipeline complete for {day}.")

@app.command()
def synthesize(day: str = typer.Option(..., help="Date (YYYY-MM-DD)")):
    """Synthesize frame scores into IC gate decisions (Howard Marks persona)."""
    import json
    from pathlib import Path
    from app.analyze import synthesize_frames

    in_path = Path(f"data/frame_scores_{day}.json")
    if not in_path.exists():
        raise typer.Exit(f"Input not found: {in_path}")

    with open(in_path, "r", encoding="utf-8") as f:
        frame_scores = json.load(f)

    results = synthesize_frames(frame_scores, day)

    out_path = Path(f"data/ic_gate_{day}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Saved: {out_path} ({len(results)} articles)")


if __name__ == "__main__":
    app()
