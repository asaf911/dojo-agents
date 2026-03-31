#!/usr/bin/env python3
"""Apply growth layer SQL (50–53) to appsflyer SQLite. Order matters."""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

_APPSFLYER_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = _APPSFLYER_DIR / "sql"

VIEW_SQL_FILES = [
    "50_growth_fact_daily.sql",
    "51_growth_daily_totals_la.sql",
    "52_growth_breakdowns_experimental.sql",
    "53_growth_weekly_totals_la.sql",
]


def apply_views(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        for name in VIEW_SQL_FILES:
            path = SQL_DIR / name
            if not path.is_file():
                raise FileNotFoundError(f"Missing SQL file: {path}")
            conn.executescript(path.read_text())
        conn.commit()
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Apply growth views 50–53 to SQLite.")
    p.add_argument(
        "--db",
        default=str(_APPSFLYER_DIR / "data" / "appsflyer.db"),
        help="Path to appsflyer.db",
    )
    args = p.parse_args(argv)
    db_path = Path(args.db).expanduser().resolve()
    apply_views(db_path)
    print(f"Applied views {VIEW_SQL_FILES} to {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
