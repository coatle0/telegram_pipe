"""VSURF Daily Pipeline — 10-step automated ingestion.

Usage:
  python scripts/daily_pipeline.py            # Run full pipeline for yesterday
  python scripts/daily_pipeline.py --dry-run  # Check steps without executing
  python scripts/daily_pipeline.py --day 2026-04-10  # Specific day
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = _REPO_ROOT / "scripts"
CLI_PATH = _REPO_ROOT / "cli.py"
LOG_DIR = Path(r"C:\DCOS\10_Pillars\20_AutoAI\telepipe\logs")

GROUPS = "macro_energy,defense,bio,ai_tech,macro_semicon"


def _get_yesterday() -> str:
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    yesterday = (now_kst - timedelta(days=1)).strftime("%Y-%m-%d")
    return yesterday


def _setup_logging(day: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"pipeline_{day}.log"

    logger = logging.getLogger("daily_pipeline")
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(str(log_file), encoding="utf-8")
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def _run_step(logger: logging.Logger, step_num: int, desc: str,
              cmd: list[str], critical: bool = False) -> bool:
    """Run a pipeline step. Returns True on success."""
    logger.info(f"Step {step_num}: {desc}")
    logger.info(f"  CMD: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            cwd=str(_REPO_ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=600,
        )
        if result.stdout:
            for line in result.stdout.strip().split("\n")[-5:]:
                logger.info(f"  OUT: {line}")
        if result.returncode != 0:
            logger.error(f"  FAIL (exit {result.returncode})")
            if result.stderr:
                for line in result.stderr.strip().split("\n")[-3:]:
                    logger.error(f"  ERR: {line}")
            if critical:
                logger.error("  CRITICAL step failed — aborting pipeline")
                return False
            logger.warning("  Non-critical — continuing")
            return True  # continue despite failure
        logger.info(f"  OK")
        return True
    except subprocess.TimeoutExpired:
        logger.error(f"  TIMEOUT (600s)")
        if critical:
            return False
        return True
    except Exception as e:
        logger.error(f"  EXCEPTION: {e}")
        if critical:
            return False
        return True


def run_pipeline(day: str, dry_run: bool = False):
    logger = _setup_logging(day)
    logger.info(f"{'DRY RUN — ' if dry_run else ''}Daily pipeline for {day}")
    logger.info(f"Repo: {_REPO_ROOT}")

    py = sys.executable
    data_dir = _REPO_ROOT / "data"

    steps = [
        # (step_num, description, command, critical)
        (1, f"run-day {day}",
         [py, str(CLI_PATH), "run-day", day], True),

        (2, f"generate_refine_json --day {day}",
         [py, str(SCRIPTS_DIR / "generate_refine_json.py"),
          "--day", day, "--limit", "9999", "--llm"], False),

        (3, f"refine-import",
         [py, str(CLI_PATH), "refine-import",
          "--json-file", str(data_dir / f"trae_refined_{day}.json")], False),

        (4, f"report --day {day}",
         [py, str(CLI_PATH), "report", "--day", day], False),

        (5, "tag_articles",
         [py, str(SCRIPTS_DIR / "tag_articles.py")], False),

        (6, "export_tags",
         [py, str(SCRIPTS_DIR / "export_tags.py")], False),

        (7, f"frame_refine --day {day}",
         [py, str(SCRIPTS_DIR / "frame_refine.py"),
          "--day", day, "--groups", GROUPS], False),

        (8, f"synthesize --day {day}",
         [py, str(CLI_PATH), "synthesize", "--day", day], False),

        (9, f"export_ic_gate_digest --day {day}",
         [py, str(SCRIPTS_DIR / "export_ic_gate_digest.py"),
          "--day", day], False),

        (10, f"wiki_ingest --day {day}",
         [py, str(SCRIPTS_DIR / "wiki_ingest.py"),
          "--day", day], False),
    ]

    if dry_run:
        print(f"\n=== DRY RUN: Daily Pipeline for {day} ===\n")
        for num, desc, cmd, critical in steps:
            crit_tag = " [CRITICAL]" if critical else ""
            print(f"  Step {num:2d}{crit_tag}: {desc}")
            print(f"          {' '.join(cmd)}")
        print(f"\n  Total: {len(steps)} steps")
        print(f"  Log: {LOG_DIR / f'pipeline_{day}.log'}")
        return

    logger.info(f"Starting {len(steps)} steps")

    for num, desc, cmd, critical in steps:
        ok = _run_step(logger, num, desc, cmd, critical)
        if not ok:
            logger.error(f"Pipeline aborted at Step {num}")
            return

    logger.info("Pipeline complete")


def main():
    p = argparse.ArgumentParser(description="VSURF Daily Pipeline")
    p.add_argument("--day", default=None, help="Date (YYYY-MM-DD), default=yesterday")
    p.add_argument("--dry-run", action="store_true", help="Check steps only")
    args = p.parse_args()

    day = args.day or _get_yesterday()
    run_pipeline(day, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
