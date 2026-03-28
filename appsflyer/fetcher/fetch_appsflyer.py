#!/usr/bin/env python3
"""
Fetch AppsFlyer aggregate report data and persist rows into SQLite.

This module targets the Pull API style aggregate export (CSV). The exact URL
path segment and query parameters differ by report type and AppsFlyer product
version — adjust REPORT_PATH_TEMPLATE and build_report_url() to match your
contract / docs (e.g. daily vs partners-by-date vs campaign-enriched exports).

Auth: many accounts use ``Authorization: Bearer <API_V2_TOKEN>``; some older
flows use ``?api_token=`` on the query string. If requests fail with 401/403,
verify the method described in your AppsFlyer token settings.

Docs (verify against your dashboard entitlements):
https://dev.appsflyer.com/hc/hc/reference/get_app-id-daily-report-v5-1
https://support.appsflyer.com/hc/en-us/articles/207034346-Pull-API-aggregate-data
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote

import requests
from dotenv import load_dotenv

# Default DB location (override with APPSFLYER_SQLITE_PATH).
DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "appsflyer.db"

# -----------------------------------------------------------------------------
# Endpoint construction — ADJUST to match the report you license (path + params).
# Example host used in many Pull API docs; some tenants use hq1 vs hq2, etc.
# -----------------------------------------------------------------------------
APPSFLYER_AGG_BASE = os.environ.get(
    "APPSFLYER_AGG_BASE",
    "https://hq1.appsflyer.com/api/agg-data/export/app",
)
# Path template: {base}/{app_id}/{report_segment}/v5 — report_segment is the
# fragile part (e.g. "daily_report", "partners_by_date_report", "partners_report").
REPORT_PATH_TEMPLATE = "{base}/{app_id}/{report_segment}/v5"


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_config() -> tuple[str, str]:
    """Read secrets from environment (populated via appsflyer/.env when using dotenv)."""
    load_dotenv(_project_root() / ".env")
    app_id = os.environ.get("APPSFLYER_APP_ID", "").strip()
    token = os.environ.get("APPSFLYER_API_TOKEN", "").strip()
    if not app_id or not token:
        raise RuntimeError(
            "Missing APPSFLYER_APP_ID or APPSFLYER_API_TOKEN. "
            "Set them in the environment or in appsflyer/.env (not committed)."
        )
    return app_id, token


def build_report_url(
    app_id: str,
    date_from: str,
    date_to: str,
    *,
    report_segment: str,
    extra_params: dict[str, str] | None = None,
) -> str:
    """
    Build the full report URL for a date range.

    ``report_segment`` must match AppsFlyer's path for your chosen aggregate
    report (daily totals, partners-by-date for media-source + date, etc.).
    ``extra_params`` can hold timezone, reattr, additional_fields, etc. per docs.
    """
    path = REPORT_PATH_TEMPLATE.format(
        base=APPSFLYER_AGG_BASE.rstrip("/"),
        app_id=app_id,
        report_segment=report_segment.strip("/"),
    )
    params: list[tuple[str, str]] = [
        ("from", date_from),
        ("to", date_to),
    ]
    if extra_params:
        params.extend(sorted(extra_params.items()))
    q = "&".join(f"{k}={quote(v, safe='')}" for k, v in params)
    return f"{path}?{q}"


def fetch_report_csv(
    url: str,
    token: str,
    *,
    timeout_sec: float = 120.0,
) -> str:
    """
    Perform the HTTP GET and return response body as text (CSV).

    ADJUST headers or query string if your token must be passed differently.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "text/csv",
    }
    resp = requests.get(url, headers=headers, timeout=timeout_sec)
    resp.raise_for_status()
    return resp.text


# -----------------------------------------------------------------------------
# CSV → row dict: header names vary by report; map synonyms into canonical keys.
# ADJUST FIELD_ALIASES when your export uses different column labels.
# -----------------------------------------------------------------------------
FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "report_date": ("date", "install date", "event date", "event_time", "day"),
    "media_source": ("media source", "partner", "pid", "media_source", "af_prt"),
    "campaign": ("campaign", "campaign name", "campaign_name", "af_c_id", "c"),
    "impressions": ("impressions", "impr."),
    "clicks": ("clicks", "click", "clicks (total)"),
    "installs": ("installs", "install", "conversions", "installs (total)"),
    "sessions": ("sessions", "sessions (total)"),
    "revenue": ("revenue", "revenue (usd)", "revenue usd", "total revenue"),
    "cost": ("cost", "cost (usd)", "spend", "ecpi"),
    "currency": ("currency", "curr"),
}


def _normalize_header(h: str) -> str:
    return h.strip().lower()


def _pick(row: dict[str, str], canonical: str) -> str | None:
    aliases = FIELD_ALIASES.get(canonical, ())
    for key, val in row.items():
        nk = _normalize_header(key)
        if nk == _normalize_header(canonical):
            return val.strip() if val else None
        for alias in aliases:
            if nk == _normalize_header(alias):
                return val.strip() if val else None
    return None


def _to_int(raw: str | None) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        return int(float(raw.replace(",", "")))
    except ValueError:
        return None


def _to_float(raw: str | None) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        return float(raw.replace(",", ""))
    except ValueError:
        return None


def parse_csv_rows(csv_text: str) -> list[dict[str, Any]]:
    """Parse CSV text into normalized rows ready for SQLite insertion."""
    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        return []

    fetched_at = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    for raw in reader:
        # Preserve original row for debugging / future column adds (optional).
        raw_blob = json.dumps(raw, ensure_ascii=False)

        report_date = _pick(raw, "report_date")
        if not report_date:
            # Skip rows we cannot attribute to a day — widen FIELD_ALIASES if this fires often.
            continue

        out.append(
            {
                "report_date": report_date[:10],
                "media_source": _pick(raw, "media_source"),
                "campaign": _pick(raw, "campaign"),
                "currency": _pick(raw, "currency"),
                "impressions": _to_int(_pick(raw, "impressions")),
                "clicks": _to_int(_pick(raw, "clicks")),
                "installs": _to_int(_pick(raw, "installs")),
                "sessions": _to_int(_pick(raw, "sessions")),
                "revenue": _to_float(_pick(raw, "revenue")),
                "cost": _to_float(_pick(raw, "cost")),
                "source_payload": raw_blob,
                "fetched_at": fetched_at,
            }
        )
    return out


DDL_DAILY_PERFORMANCE = """
CREATE TABLE IF NOT EXISTS daily_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date TEXT NOT NULL,
    media_source TEXT,
    campaign TEXT,
    currency TEXT,
    impressions INTEGER,
    clicks INTEGER,
    installs INTEGER,
    sessions INTEGER,
    revenue REAL,
    cost REAL,
    source_payload TEXT,
    fetched_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_daily_performance_date
    ON daily_performance (report_date);
CREATE INDEX IF NOT EXISTS idx_daily_performance_media
    ON daily_performance (media_source);
CREATE INDEX IF NOT EXISTS idx_daily_performance_campaign
    ON daily_performance (campaign);
"""


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL_DAILY_PERFORMANCE)
    conn.commit()


def delete_range(conn: sqlite3.Connection, date_from: str, date_to: str) -> None:
    """Remove cached rows in the inclusive date range before reloading."""
    conn.execute(
        "DELETE FROM daily_performance WHERE report_date >= ? AND report_date <= ?",
        (date_from, date_to),
    )


def insert_rows(conn: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> int:
    sql = """
    INSERT INTO daily_performance (
        report_date, media_source, campaign, currency,
        impressions, clicks, installs, sessions, revenue, cost,
        source_payload, fetched_at
    ) VALUES (
        :report_date, :media_source, :campaign, :currency,
        :impressions, :clicks, :installs, :sessions, :revenue, :cost,
        :source_payload, :fetched_at
    )
    """
    cur = conn.cursor()
    n = 0
    for row in rows:
        cur.execute(sql, row)
        n += 1
    conn.commit()
    return n


def run_fetch(
    date_from: str,
    date_to: str,
    *,
    db_path: Path,
    report_segment: str,
    dry_run: bool = False,
) -> int:
    app_id, token = load_config()
    url = build_report_url(app_id, date_from, date_to, report_segment=report_segment)
    if dry_run:
        print(f"[dry-run] Would GET:\n{url}")
        conn = connect_db(db_path)
        try:
            init_schema(conn)
        finally:
            conn.close()
        return 0

    csv_text = fetch_report_csv(url, token)
    rows = parse_csv_rows(csv_text)
    conn = connect_db(db_path)
    try:
        init_schema(conn)
        delete_range(conn, date_from, date_to)
        inserted = insert_rows(conn, rows)
    finally:
        conn.close()
    return inserted


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch AppsFlyer aggregate CSV into SQLite.")
    p.add_argument("--from", dest="date_from", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", required=True, help="End date YYYY-MM-DD (inclusive)")
    p.add_argument(
        "--report-segment",
        default=os.environ.get("APPSFLYER_REPORT_SEGMENT", "partners_by_date_report"),
        help=(
            "URL path segment for the aggregate report (AppsFlyer-specific). "
            "Examples: daily_report, partners_by_date_report — confirm in docs."
        ),
    )
    p.add_argument(
        "--db",
        default=os.environ.get("APPSFLYER_SQLITE_PATH", str(DEFAULT_DB_PATH)),
        help="SQLite database path",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate env + schema; print URL; do not call the network or write rows.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    db_path = Path(args.db).expanduser().resolve()
    n = run_fetch(
        args.date_from,
        args.date_to,
        db_path=db_path,
        report_segment=args.report_segment,
        dry_run=args.dry_run,
    )
    if not args.dry_run:
        print(f"Inserted {n} rows into {db_path}")


if __name__ == "__main__":
    main()
