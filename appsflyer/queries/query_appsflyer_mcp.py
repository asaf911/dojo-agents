#!/usr/bin/env python3
"""Query KPI data fetched through AppsFlyer MCP (mcp_kpi_performance table)."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "appsflyer.db"


def connect(db_path: Path) -> sqlite3.Connection:
    if not db_path.is_file():
        raise FileNotFoundError(f"SQLite file not found: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def top_ads(conn: sqlite3.Connection, date_from: str, date_to: str, kpi_name: str, limit: int) -> list[sqlite3.Row]:
    sql = """
    SELECT
        COALESCE(ad, '(unknown)') AS ad,
        COALESCE(adset, '(unknown)') AS adset,
        COALESCE(campaign, '(unknown)') AS campaign,
        COALESCE(media_source, '(unknown)') AS media_source,
        SUM(COALESCE(metric_value, 0)) AS kpi_value,
        SUM(COALESCE(installs, 0)) AS installs,
        CASE WHEN SUM(COALESCE(metric_value, 0)) > 0
             THEN SUM(COALESCE(installs, 0)) * 1.0 / SUM(COALESCE(metric_value, 0))
             ELSE NULL END AS installs_per_kpi
    FROM mcp_kpi_performance
    WHERE date_from = ? AND date_to = ? AND kpi_name = ?
    GROUP BY 1, 2, 3, 4
    ORDER BY kpi_value DESC
    LIMIT ?
    """
    return list(conn.execute(sql, (date_from, date_to, kpi_name, limit)))


def print_rows(rows: list[sqlite3.Row]) -> None:
    if not rows:
        print("(no rows)")
        return
    keys = rows[0].keys()
    print("\t".join(keys))
    for r in rows:
        print("\t".join("" if r[k] is None else str(r[k]) for k in keys))


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Query AppsFlyer MCP KPI data")
    p.add_argument("command", choices=["top-ads"])
    p.add_argument("--from", dest="date_from", required=True)
    p.add_argument("--to", dest="date_to", required=True)
    p.add_argument(
        "--kpi",
        dest="kpi_name",
        default="af_start_trial_unique_users",
        help="KPI key (default: af_start_trial_unique_users)",
    )
    p.add_argument("--limit", type=int, default=10)
    p.add_argument("--db", default=str(DEFAULT_DB_PATH))
    args = p.parse_args(argv)

    conn = connect(Path(args.db).expanduser().resolve())
    try:
        if args.command == "top-ads":
            print_rows(top_ads(conn, args.date_from, args.date_to, args.kpi_name, args.limit))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
