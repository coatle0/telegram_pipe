"""`app.db.get_checkpoint`/`set_checkpoint` 단위 테스트.

주의: 이 모듈은 절대 `app.db.DB_PATH`(라이브 운영 DB)를 사용하지 않는다.
격리된 임시 sqlite 연결만 사용해 실제 데이터를 건드리지 않는다.
"""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import get_checkpoint, set_checkpoint  # noqa: E402

CHECKPOINT_DDL = """
CREATE TABLE ingest_checkpoints (
    channel_id INTEGER PRIMARY KEY,
    last_message_id INTEGER NOT NULL,
    last_message_date DATETIME,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""


def _isolated_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(CHECKPOINT_DDL)
    return conn


def test_get_checkpoint_returns_none_when_absent():
    conn = _isolated_conn()
    assert get_checkpoint(conn, 12345) is None


def test_set_then_get_roundtrip():
    conn = _isolated_conn()
    set_checkpoint(conn, -1001185561205, 102430, "2026-09-23 01:39:31")
    assert get_checkpoint(conn, -1001185561205) == 102430


def test_set_checkpoint_never_regresses():
    conn = _isolated_conn()
    set_checkpoint(conn, -1001185561205, 102430, "2026-09-23 01:39:31")
    set_checkpoint(conn, -1001185561205, 100000, "2026-08-01 00:00:00")  # older/smaller id
    assert get_checkpoint(conn, -1001185561205) == 102430  # unchanged, did not regress


def test_set_checkpoint_advances_forward():
    conn = _isolated_conn()
    set_checkpoint(conn, -1001185561205, 102430, "2026-09-23 01:39:31")
    set_checkpoint(conn, -1001185561205, 102999, "2026-09-25 00:00:00")
    assert get_checkpoint(conn, -1001185561205) == 102999


def test_checkpoints_are_isolated_per_channel():
    conn = _isolated_conn()
    set_checkpoint(conn, 111, 5, "2026-01-01 00:00:00")
    set_checkpoint(conn, 222, 999, "2026-01-01 00:00:00")
    assert get_checkpoint(conn, 111) == 5
    assert get_checkpoint(conn, 222) == 999


if __name__ == "__main__":  # pragma: no cover
    import traceback

    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception:
                failures += 1
                print(f"FAIL {name}")
                traceback.print_exc()
    print(f"\n{'ALL PASS' if not failures else f'{failures} FAILED'}")
    raise SystemExit(1 if failures else 0)
