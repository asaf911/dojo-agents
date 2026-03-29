#!/usr/bin/env python3
"""Query normalized AppsFlyer marketing facts for CAC and trend analysis."""

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


def print_rows(rows: list[sqlite3.Row]) -> None:
    if not rows:
        print("(no rows)")
        return
    keys = rows[0].keys()
    print("\t".join(keys))
    for r in rows:
        print("\t".join("" if r[k] is None else str(r[k]) for k in keys))


def top_entities(conn: sqlite3.Connection, date_from: str, date_to: str, entity: str, limit: int) -> list[sqlite3.Row]:
    if entity not in {"media_source", "campaign", "adset", "ad"}:
        raise ValueError("entity must be one of media_source, campaign, adset, ad")
    sql = f"""
    SELECT
        COALESCE({entity}, '(unknown)') AS entity,
        SUM(COALESCE(spend, 0)) AS spend,
        SUM(COALESCE(af_subscribe, 0)) AS af_subscribe,
        SUM(COALESCE(af_start_trial, 0)) AS af_start_trial,
        SUM(COALESCE(installs, 0)) AS installs,
        CASE WHEN SUM(COALESCE(af_subscribe, 0)) > 0
             THEN SUM(COALESCE(spend, 0)) * 1.0 / SUM(COALESCE(af_subscribe, 0))
             ELSE NULL END AS cac
    FROM marketing_fact_daily
    WHERE fact_date BETWEEN ? AND ?
    GROUP BY 1
    ORDER BY af_subscribe DESC, spend DESC
    LIMIT ?
    """
    return list(conn.execute(sql, (date_from, date_to, limit)))


def weekly_cac(conn: sqlite3.Connection, date_from: str, date_to: str) -> list[sqlite3.Row]:
    sql = """
    SELECT
        strftime('%Y-W%W', fact_date) AS year_week,
        MIN(fact_date) AS week_start,
        MAX(fact_date) AS week_end,
        SUM(COALESCE(spend, 0)) AS spend,
        SUM(COALESCE(af_subscribe, 0)) AS af_subscribe,
        SUM(COALESCE(af_start_trial, 0)) AS af_start_trial,
        SUM(COALESCE(installs, 0)) AS installs,
        CASE WHEN SUM(COALESCE(af_subscribe, 0)) > 0
             THEN SUM(COALESCE(spend, 0)) * 1.0 / SUM(COALESCE(af_subscribe, 0))
             ELSE NULL END AS cac
    FROM marketing_fact_daily
    WHERE fact_date BETWEEN ? AND ?
    GROUP BY 1
    ORDER BY week_start
    """
    return list(conn.execute(sql, (date_from, date_to)))


def daily_cac(conn: sqlite3.Connection, date_from: str, date_to: str) -> list[sqlite3.Row]:
    sql = """
    SELECT
        fact_date,
        SUM(COALESCE(spend, 0)) AS spend,
        SUM(COALESCE(af_subscribe, 0)) AS af_subscribe,
        SUM(COALESCE(af_start_trial, 0)) AS af_start_trial,
        SUM(COALESCE(installs, 0)) AS installs,
        CASE WHEN SUM(COALESCE(af_subscribe, 0)) > 0
             THEN SUM(COALESCE(spend, 0)) * 1.0 / SUM(COALESCE(af_subscribe, 0))
             ELSE NULL END AS cac
    FROM marketing_fact_daily
    WHERE fact_date BETWEEN ? AND ?
    GROUP BY fact_date
    ORDER BY fact_date
    """
    return list(conn.execute(sql, (date_from, date_to)))


def summary(conn: sqlite3.Connection, date_from: str, date_to: str) -> list[sqlite3.Row]:
    sql = """
    SELECT
        ? AS date_from,
        ? AS date_to,
        SUM(COALESCE(spend, 0)) AS spend,
        SUM(COALESCE(af_subscribe, 0)) AS af_subscribe,
        SUM(COALESCE(af_start_trial, 0)) AS af_start_trial,
        SUM(COALESCE(installs, 0)) AS installs,
        CASE WHEN SUM(COALESCE(af_subscribe, 0)) > 0
             THEN SUM(COALESCE(spend, 0)) * 1.0 / SUM(COALESCE(af_subscribe, 0))
             ELSE NULL END AS cac
    FROM marketing_fact_daily
    WHERE fact_date BETWEEN ? AND ?
    """
    return list(conn.execute(sql, (date_from, date_to, date_from, date_to)))


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Query normalized AppsFlyer marketing facts")
    p.add_argument("command", choices=["summary", "daily-cac", "weekly-cac", "top-media", "top-campaigns", "top-adsets", "top-ads"])
    p.add_argument("--from", dest="date_from", required=True)
    p.add_argument("--to", dest="date_to", required=True)
    p.add_argument("--limit", type=int, default=10)
    p.add_argument("--db", default=str(DEFAULT_DB_PATH))
    args = p.parse_args(argv)

    conn = connect(Path(args.db).expanduser().resolve())
    try:
        if args.command == "summary":
            print_rows(summary(conn, args.date_from, args.date_to))
        elif args.command == "daily-cac":
            print_rows(daily_cac(conn, args.date_from, args.date_to))
        elif args.command == "weekly-cac":
            print_rows(weekly_cac(conn, args.date_from, args.date_to))
        elif args.command == "top-media":
            print_rows(top_entities(conn, args.date_from, args.date_to, "media_source", args.limit))
        elif args.command == "top-campaigns":
            print_rows(top_entities(conn, args.date_from, args.date_to, "campaign", args.limit))
        elif args.command == "top-adsets":
            print_rows(top_entities(conn, args.date_from, args.date_to, "adset", args.limit))
        elif args.command == "top-ads":
            print_rows(top_entities(conn, args.date_from, args.date_to, "ad", args.limit))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
