#!/usr/bin/env python3
"""
Fetch AppsFlyer Pull API aggregate data as the LA-aligned daily truth / validation layer.

Purpose:
- provide a canonical daily source aligned with the AppsFlyer dashboard timezone
- validate / complement the richer but UTC-based MCP source

This fetcher intentionally focuses on stable daily totals and basic breakdowns.
It is not the rich dimensions layer; MCP continues to serve that role.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv

_APPSFLYER_DIR = Path(__file__).resolve().parent.parent
if str(_APPSFLYER_DIR) not in sys.path:
    sys.path.insert(0, str(_APPSFLYER_DIR))
import common

DEFAULT_DB_PATH = common.project_root() / 'data' / 'appsflyer.db'
DEFAULT_AGG_BASE = os.environ.get('APPSFLYER_AGG_BASE', 'https://hq1.appsflyer.com/api/agg-data/export/app')
DEFAULT_REPORT_SEGMENT = os.environ.get('APPSFLYER_REPORT_SEGMENT', 'partners_by_date_report')
BUSINESS_TIMEZONE = os.environ.get('APPSFLYER_BUSINESS_TIMEZONE', 'America/Los_Angeles')

DDL = """
CREATE TABLE IF NOT EXISTS appsflyer_pull_daily_truth (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fact_date TEXT NOT NULL,
    media_source TEXT,
    campaign TEXT,
    adset TEXT,
    ad TEXT,
    installs REAL,
    clicks REAL,
    impressions REAL,
    cost REAL,
    revenue REAL,
    af_start_trial REAL,
    af_subscribe REAL,
    rc_trial_converted_event REAL,
    af_tutorial_completion REAL,
    timezone TEXT NOT NULL,
    currency TEXT,
    report_segment TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    source_payload TEXT,
    UNIQUE(fact_date, media_source, campaign, adset, ad, report_segment, timezone)
);
CREATE INDEX IF NOT EXISTS idx_pull_truth_date ON appsflyer_pull_daily_truth(fact_date);
CREATE INDEX IF NOT EXISTS idx_pull_truth_media ON appsflyer_pull_daily_truth(media_source);
CREATE INDEX IF NOT EXISTS idx_pull_truth_campaign ON appsflyer_pull_daily_truth(campaign);
"""

UPSERT = """
INSERT INTO appsflyer_pull_daily_truth (
    fact_date, media_source, campaign, adset, ad,
    installs, clicks, impressions, cost, revenue,
    af_start_trial, af_subscribe, rc_trial_converted_event, af_tutorial_completion,
    timezone, currency, report_segment, fetched_at, source_payload
) VALUES (
    :fact_date, :media_source, :campaign, :adset, :ad,
    :installs, :clicks, :impressions, :cost, :revenue,
    :af_start_trial, :af_subscribe, :rc_trial_converted_event, :af_tutorial_completion,
    :timezone, :currency, :report_segment, :fetched_at, :source_payload
)
ON CONFLICT(fact_date, media_source, campaign, adset, ad, report_segment, timezone)
DO UPDATE SET
    installs = excluded.installs,
    clicks = excluded.clicks,
    impressions = excluded.impressions,
    cost = excluded.cost,
    revenue = excluded.revenue,
    af_start_trial = excluded.af_start_trial,
    af_subscribe = excluded.af_subscribe,
    rc_trial_converted_event = excluded.rc_trial_converted_event,
    af_tutorial_completion = excluded.af_tutorial_completion,
    currency = excluded.currency,
    fetched_at = excluded.fetched_at,
    source_payload = excluded.source_payload
"""

FIELD_ALIASES = {
    'date': 'fact_date',
    'media source': 'media_source',
    'campaign': 'campaign',
    'adset': 'adset',
    'ad': 'ad',
    'installs': 'installs',
    'clicks': 'clicks',
    'impressions': 'impressions',
    'total cost': 'cost',
    'cost': 'cost',
    'total revenue': 'revenue',
    'revenue': 'revenue',
    'af_start_trial (unique users)': 'af_start_trial',
    'af_subscribe (unique users)': 'af_subscribe',
    'rc_trial_converted_event (unique users)': 'rc_trial_converted_event',
    'af_tutorial_completion (unique users)': 'af_tutorial_completion',
}


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


def _parse_float(v: str | None):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def build_pull_url(app_id: str, date_from: str, date_to: str, report_segment: str) -> str:
    base = DEFAULT_AGG_BASE.rstrip('/')
    path = f"{base}/{app_id}/{report_segment}/v5"
    qp = {
        'from': date_from,
        'to': date_to,
        'timezone': BUSINESS_TIMEZONE,
    }
    ordered = sorted(qp.items())
    q = '&'.join(f"{k}={quote(v, safe='')}" for k, v in ordered)
    return f"{path}?{q}"


def fetch_csv(date_from: str, date_to: str, report_segment: str) -> tuple[str, str | None]:
    load_dotenv(common.project_root() / '.env')
    app_id = os.environ.get('APPSFLYER_APP_ID', '').strip()
    token = os.environ.get('APPSFLYER_API_TOKEN', '').strip()
    if not app_id or not token:
        raise RuntimeError('Missing APPSFLYER_APP_ID or APPSFLYER_API_TOKEN in appsflyer/.env')
    url = build_pull_url(app_id, date_from, date_to, report_segment)
    return common.get_with_retries(url, token, accept='text/csv, text/plain, */*', timeout_sec=180.0)


def rows_from_csv(text: str):
    return list(csv.DictReader(io.StringIO(text))) if text.strip() else []


def normalize_row(raw: dict[str, str], report_segment: str):
    out = {
        'fact_date': None,
        'media_source': None,
        'campaign': None,
        'adset': None,
        'ad': None,
        'installs': None,
        'clicks': None,
        'impressions': None,
        'cost': None,
        'revenue': None,
        'af_start_trial': None,
        'af_subscribe': None,
        'rc_trial_converted_event': None,
        'af_tutorial_completion': None,
        'timezone': BUSINESS_TIMEZONE,
        'currency': None,
        'report_segment': report_segment,
        'fetched_at': datetime.now(timezone.utc).isoformat(),
        'source_payload': str(raw),
    }
    for k, v in raw.items():
        nk = k.strip().lower()
        field = FIELD_ALIASES.get(nk)
        if not field:
            continue
        if field in {'fact_date', 'media_source', 'campaign', 'adset', 'ad'}:
            out[field] = v.strip() if v is not None else None
        else:
            out[field] = _parse_float(v)
    return out


def run(date_from: str, date_to: str, *, db_path: Path, report_segment: str, dry_run: bool = False) -> int:
    if dry_run:
        text, _ = fetch_csv(date_from, date_to, report_segment)
        print(text[:2000])
        return 0

    text, _ = fetch_csv(date_from, date_to, report_segment)
    rows = rows_from_csv(text)
    conn = connect_db(db_path)
    try:
        init_schema(conn)
        cur = conn.cursor()
        n = 0
        for raw in rows:
            row = normalize_row(raw, report_segment)
            if not row['fact_date']:
                continue
            cur.execute(UPSERT, row)
            n += 1
        conn.commit()
        return n
    finally:
        conn.close()


def main(argv=None):
    p = argparse.ArgumentParser(description='Fetch AppsFlyer Pull API daily truth into SQLite')
    p.add_argument('--from', dest='date_from', required=True, type=lambda s: common.parse_iso_date_arg('--from', s))
    p.add_argument('--to', dest='date_to', required=True, type=lambda s: common.parse_iso_date_arg('--to', s))
    p.add_argument('--db', default=os.environ.get('APPSFLYER_SQLITE_PATH', str(DEFAULT_DB_PATH)))
    p.add_argument('--report-segment', default=DEFAULT_REPORT_SEGMENT)
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args(argv)
    db_path = Path(args.db).expanduser().resolve()
    n = run(args.date_from, args.date_to, db_path=db_path, report_segment=args.report_segment, dry_run=args.dry_run)
    if not args.dry_run:
        print(f'Upserted {n} pull-truth rows into {db_path}')


if __name__ == '__main__':
    main()
