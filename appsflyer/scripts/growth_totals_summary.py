#!/usr/bin/env python3
"""
Read-only aggregates from growth_daily_totals_la for agents and cron helpers.
Prints one JSON object to stdout (suitable for OpenClaw / Slack bots).
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
import common  # noqa: E402

LA = common.business_zoneinfo()
DEFAULT_DB = _ROOT / "data" / "appsflyer.db"


def main() -> int:
    p = argparse.ArgumentParser(description="Summarize growth_daily_totals_la for a date window.")
    p.add_argument(
        "--db",
        default=os.environ.get("APPSFLYER_SQLITE_PATH", str(DEFAULT_DB)),
        help="Path to appsflyer.db",
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument(
        "--days",
        type=int,
        default=30,
        help="Last N calendar days in America/Los_Angeles ending today LA (inclusive).",
    )
    g.add_argument(
        "--from-date",
        dest="from_date",
        metavar="YYYY-MM-DD",
        help="Start fact_date (inclusive). Use with --to-date.",
    )
    p.add_argument(
        "--to-date",
        dest="to_date",
        metavar="YYYY-MM-DD",
        help="End fact_date (inclusive). Default: today LA.",
    )
    args = p.parse_args()
    db_path = Path(args.db).expanduser().resolve()

    if args.from_date:
        start = args.from_date
        if args.to_date:
            end = args.to_date
        else:
            end = datetime.now(LA).date().isoformat()
    else:
        n = max(1, args.days)
        end_d = datetime.now(LA).date()
        start_d = end_d - timedelta(days=n - 1)
        start = start_d.isoformat()
        end = end_d.isoformat()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              COUNT(*) AS day_count,
              COALESCE(SUM(installs), 0) AS installs,
              COALESCE(SUM(spend), 0) AS spend,
              COALESCE(SUM(af_start_trial), 0) AS af_start_trial,
              COALESCE(SUM(af_subscribe), 0) AS af_subscribe,
              COALESCE(SUM(revenue), 0) AS revenue
            FROM growth_daily_totals_la
            WHERE fact_date >= ? AND fact_date <= ?
            """,
            (start, end),
        )
        row = cur.fetchone()
        cur.execute(
            "SELECT MAX(fact_date) FROM growth_daily_totals_la WHERE fact_date <= ?",
            (end,),
        )
        max_in_range = cur.fetchone()[0]
    finally:
        conn.close()

    day_count, installs, spend, trials, subs, revenue = row
    out = {
        "view": "growth_daily_totals_la",
        "business_timezone": common.business_timezone_name(),
        "date_from": start,
        "date_to": end,
        "day_count": day_count,
        "totals": {
            "installs": installs,
            "spend": spend,
            "af_start_trial": trials,
            "af_subscribe": subs,
            "revenue": revenue,
        },
        "derived": {
            "cost_per_trial": (spend / trials) if trials else None,
            "cac_subscribe": (spend / subs) if subs else None,
        },
        "max_fact_date_in_db_on_or_before_end": max_in_range,
        "db_path": str(db_path),
    }
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
