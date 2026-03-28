#!/usr/bin/env python3
"""
Query the local AppsFlyer SQLite cache (daily_performance).

Requires data ingested by fetcher/fetch_appsflyer.py. Media-source and campaign
breakdowns only populate when the ingested report includes those dimensions
(choose the appropriate AppsFlyer aggregate export / segment when fetching).
"""

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


def summary(conn: sqlite3.Connection, date_from: str, date_to: str) -> list[sqlite3.Row]:
    sql = """
    SELECT
        COUNT(*) AS row_count,
        SUM(COALESCE(impressions, 0)) AS impressions,
        SUM(COALESCE(clicks, 0)) AS clicks,
        SUM(COALESCE(installs, 0)) AS installs,
        SUM(COALESCE(sessions, 0)) AS sessions,
        SUM(COALESCE(revenue, 0)) AS revenue,
        SUM(COALESCE(cost, 0)) AS cost
    FROM daily_performance
    WHERE report_date >= ? AND report_date <= ?
    """
    return list(conn.execute(sql, (date_from, date_to)))


def media_sources(conn: sqlite3.Connection, date_from: str, date_to: str) -> list[sqlite3.Row]:
    sql = """
    SELECT
        COALESCE(media_source, '(unknown)') AS media_source,
        SUM(COALESCE(impressions, 0)) AS impressions,
        SUM(COALESCE(clicks, 0)) AS clicks,
        SUM(COALESCE(installs, 0)) AS installs,
        SUM(COALESCE(sessions, 0)) AS sessions,
        SUM(COALESCE(revenue, 0)) AS revenue,
        SUM(COALESCE(cost, 0)) AS cost
    FROM daily_performance
    WHERE report_date >= ? AND report_date <= ?
    GROUP BY 1
    ORDER BY COALESCE(installs, 0) DESC, COALESCE(revenue, 0) DESC
    """
    return list(conn.execute(sql, (date_from, date_to)))


def campaigns(conn: sqlite3.Connection, date_from: str, date_to: str) -> list[sqlite3.Row]:
    sql = """
    SELECT
        COALESCE(campaign, '(unknown)') AS campaign,
        COALESCE(media_source, '(unknown)') AS media_source,
        SUM(COALESCE(impressions, 0)) AS impressions,
        SUM(COALESCE(clicks, 0)) AS clicks,
        SUM(COALESCE(installs, 0)) AS installs,
        SUM(COALESCE(sessions, 0)) AS sessions,
        SUM(COALESCE(revenue, 0)) AS revenue,
        SUM(COALESCE(cost, 0)) AS cost
    FROM daily_performance
    WHERE report_date >= ? AND report_date <= ?
    GROUP BY 1, 2
    ORDER BY COALESCE(installs, 0) DESC, COALESCE(revenue, 0) DESC
    """
    return list(conn.execute(sql, (date_from, date_to)))


def print_rows(rows: list[sqlite3.Row]) -> None:
    if not rows:
        print("(no rows)")
        return
    keys = rows[0].keys()
    print("\t".join(keys))
    for r in rows:
        print("\t".join("" if r[k] is None else str(r[k]) for k in keys))


def _add_range_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--from", dest="date_from", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", required=True, help="End date YYYY-MM-DD (inclusive)")
    p.add_argument(
        "--db",
        default=str(DEFAULT_DB_PATH),
        help="SQLite database path",
    )


def main(argv: list[str] | None = None) -> None:
    root = argparse.ArgumentParser(description="Query cached AppsFlyer daily_performance data.")
    sub = root.add_subparsers(dest="command", required=True)

    p_sum = sub.add_parser("summary", help="Totals across the date range")
    _add_range_args(p_sum)

    p_ms = sub.add_parser("media-sources", help="Breakdown by media_source")
    _add_range_args(p_ms)

    p_c = sub.add_parser("campaigns", help="Breakdown by campaign (and media_source)")
    _add_range_args(p_c)

    args = root.parse_args(argv)
    db_path = Path(args.db).expanduser().resolve()
    conn = connect(db_path)
    try:
        if args.command == "summary":
            print_rows(summary(conn, args.date_from, args.date_to))
        elif args.command == "media-sources":
            print_rows(media_sources(conn, args.date_from, args.date_to))
        elif args.command == "campaigns":
            print_rows(campaigns(conn, args.date_from, args.date_to))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
