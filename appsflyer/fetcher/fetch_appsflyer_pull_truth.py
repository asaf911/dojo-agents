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

# No UNIQUE on dimensions alone: AppsFlyer can return multiple rows with empty
# dimensions (different metric slices). A dim-only UNIQUE + UPSERT overwrote siblings.
# Each fetch run DELETEs its date window then INSERTs fresh CSV rows; dedupe removes
# identical duplicate lines (e.g. tripled NULL-key rows from old SQLite behavior).
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
    source_payload TEXT
);
CREATE INDEX IF NOT EXISTS idx_pull_truth_date ON appsflyer_pull_daily_truth(fact_date);
CREATE INDEX IF NOT EXISTS idx_pull_truth_media ON appsflyer_pull_daily_truth(media_source);
CREATE INDEX IF NOT EXISTS idx_pull_truth_campaign ON appsflyer_pull_daily_truth(campaign);
"""

INSERT_ROW = """
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
"""

FIELD_ALIASES = {
    'date': 'fact_date',
    'media source': 'media_source',
    'campaign': 'campaign',
    'adset': 'adset',
    'ad': 'ad',
    'installs': 'installs',
    'conversions': 'installs',
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


def _field_for_csv_header(header: str) -> str | None:
    """
    Map AppsFlyer agg CSV column names to our fields.
    Export headers often look like 'Media Source (pid)', 'Campaign (c)' — strip the parenthetical.
    """
    if not header or not str(header).strip():
        return None
    nk = str(header).strip().lower()
    if nk in FIELD_ALIASES:
        return FIELD_ALIASES[nk]
    base = nk.split("(")[0].strip()
    if base in FIELD_ALIASES:
        return FIELD_ALIASES[base]
    return None


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _migrate_legacy_unique_constraint(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='appsflyer_pull_daily_truth'"
    ).fetchone()
    if not row or not row[0] or "UNIQUE" not in row[0].upper():
        return
    conn.execute("ALTER TABLE appsflyer_pull_daily_truth RENAME TO appsflyer_pull_daily_truth_legacy")
    conn.executescript(DDL)
    conn.execute(
        """
        INSERT INTO appsflyer_pull_daily_truth (
            fact_date, media_source, campaign, adset, ad, installs, clicks, impressions, cost, revenue,
            af_start_trial, af_subscribe, rc_trial_converted_event, af_tutorial_completion,
            timezone, currency, report_segment, fetched_at, source_payload
        )
        SELECT fact_date,
               COALESCE(media_source, ''), COALESCE(campaign, ''), COALESCE(adset, ''), COALESCE(ad, ''),
               installs, clicks, impressions, cost, revenue,
               af_start_trial, af_subscribe, rc_trial_converted_event, af_tutorial_completion,
               timezone, currency, report_segment, fetched_at, source_payload
        FROM appsflyer_pull_daily_truth_legacy
        """
    )
    conn.execute("DROP TABLE appsflyer_pull_daily_truth_legacy")
    conn.commit()


def init_schema(conn: sqlite3.Connection) -> None:
    _migrate_legacy_unique_constraint(conn)
    conn.executescript(DDL)
    conn.commit()


def _parse_float(v: str | None):
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.upper() in {"N/A", "NA", "—", "-"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def build_pull_url(app_id: str, date_from: str, date_to: str, report_segment: str,
                    *, reattr: bool = False) -> str:
    base = DEFAULT_AGG_BASE.rstrip('/')
    path = f"{base}/{app_id}/{report_segment}/v5"
    qp = {
        'from': date_from,
        'to': date_to,
        'timezone': common.business_timezone_name(),
    }
    if reattr:
        qp['reattr'] = 'true'
    ordered = sorted(qp.items())
    q = '&'.join(f"{k}={quote(v, safe='')}" for k, v in ordered)
    return f"{path}?{q}"


def fetch_csv(date_from: str, date_to: str, report_segment: str,
              *, reattr: bool = False) -> tuple[str, str | None]:
    load_dotenv(common.project_root() / '.env')
    app_id = os.environ.get('APPSFLYER_APP_ID', '').strip()
    token = os.environ.get('APPSFLYER_API_TOKEN', '').strip()
    if not app_id or not token:
        raise RuntimeError('Missing APPSFLYER_APP_ID or APPSFLYER_API_TOKEN in appsflyer/.env')
    url = build_pull_url(app_id, date_from, date_to, report_segment, reattr=reattr)
    return common.get_with_retries(url, token, accept='text/csv, text/plain, */*', timeout_sec=180.0)


def rows_from_csv(text: str):
    return list(csv.DictReader(io.StringIO(text))) if text.strip() else []


def _dim_key(v: str | None) -> str:
    """Normalize dimension strings; AppsFlyer CSV often uses 'None' / 'N/A' for empty."""
    if v is None:
        return ""
    s = str(v).strip()
    if not s or s.lower() in ("none", "n/a", "na", "null", "(not set)"):
        return ""
    return s


def normalize_row(raw: dict[str, str], report_segment: str):
    out = {
        'fact_date': None,
        'media_source': "",
        'campaign': "",
        'adset': "",
        'ad': "",
        'installs': None,
        'clicks': None,
        'impressions': None,
        'cost': None,
        'revenue': None,
        'af_start_trial': None,
        'af_subscribe': None,
        'rc_trial_converted_event': None,
        'af_tutorial_completion': None,
        'timezone': common.business_timezone_name(),
        'currency': None,
        'report_segment': report_segment,
        'fetched_at': datetime.now(timezone.utc).isoformat(),
        'source_payload': str(raw),
    }
    for k, v in raw.items():
        field = _field_for_csv_header(k)
        if not field:
            continue
        if field in {'fact_date', 'media_source', 'campaign', 'adset', 'ad'}:
            if field == 'fact_date':
                out[field] = v.strip() if v is not None else None
            else:
                out[field] = _dim_key(v)
        else:
            out[field] = _parse_float(v)
    # Ensure dimensions are never NULL (matches UNIQUE key and ON CONFLICT behavior)
    for f in ('media_source', 'campaign', 'adset', 'ad'):
        out[f] = _dim_key(out.get(f))
    return out


def dedupe_pull_daily_truth(conn: sqlite3.Connection) -> int:
    """
    Remove duplicate rows that are true duplicates: same date, dimensions, segment, tz, and metrics.
    Dimensions use COALESCE so NULL vs '' does not create extra rows on re-ingest.
    Rows with the same empty dimensions but *different* metrics (e.g. two unnamed slices) stay separate.
    Keeps the row with latest fetched_at per duplicate group.
    Returns number of rows deleted.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM appsflyer_pull_daily_truth")
    before = cur.fetchone()[0]
    # Sentinel -1e30 for NULL metrics in PARTITION only (not stored).
    cur.execute(
        """
        DELETE FROM appsflyer_pull_daily_truth WHERE id IN (
            SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY fact_date,
                                        COALESCE(media_source, ''),
                                        COALESCE(campaign, ''),
                                        COALESCE(adset, ''),
                                        COALESCE(ad, ''),
                                        report_segment,
                                        timezone,
                                        COALESCE(installs, -1e30),
                                        COALESCE(clicks, -1e30),
                                        COALESCE(impressions, -1e30),
                                        COALESCE(cost, -1e30),
                                        COALESCE(revenue, -1e30),
                                        COALESCE(af_start_trial, -1e30),
                                        COALESCE(af_subscribe, -1e30),
                                        COALESCE(rc_trial_converted_event, -1e30),
                                        COALESCE(af_tutorial_completion, -1e30)
                           ORDER BY fetched_at DESC, id DESC
                       ) AS rn
                FROM appsflyer_pull_daily_truth
            ) WHERE rn > 1
        )
        """
    )
    cur.execute(
        """
        UPDATE appsflyer_pull_daily_truth SET
            media_source = COALESCE(media_source, ''),
            campaign = COALESCE(campaign, ''),
            adset = COALESCE(adset, ''),
            ad = COALESCE(ad, '')
        """
    )
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM appsflyer_pull_daily_truth")
    after = cur.fetchone()[0]
    return max(0, before - after)


def _dates_with_cost(rows: list[dict]) -> set[str]:
    """Return the set of fact_dates where at least one row has cost > 0."""
    cost_dates: set[str] = set()
    for r in rows:
        c = r.get('cost')
        if c is not None and c > 0 and r.get('fact_date'):
            cost_dates.add(r['fact_date'])
    return cost_dates


def _existing_dates_with_cost(conn: sqlite3.Connection, date_from: str, date_to: str,
                               report_segment: str, tz: str) -> set[str]:
    """Return dates in the DB that already have cost > 0."""
    cur = conn.execute(
        """
        SELECT DISTINCT fact_date FROM appsflyer_pull_daily_truth
        WHERE fact_date >= ? AND fact_date <= ?
          AND report_segment = ? AND timezone = ?
          AND cost IS NOT NULL AND cost > 0
        """,
        (date_from, date_to, report_segment, tz),
    )
    return {r[0] for r in cur.fetchall()}


REATTR_SEGMENT_SUFFIX = '_reattr'


def _fetch_retargeting(date_from: str, date_to: str, report_segment: str,
                       conn: sqlite3.Connection, tz: str) -> int:
    """Fetch retargeting data (re-attributions + re-engagements) and add to the truth table.

    The dashboard "Non-organic" count includes re-attributions and re-engagements.
    The standard partners_by_date_report omits them.  Adding reattr=true captures
    these as "Conversions" which we map to the installs column.
    """
    reattr_segment = report_segment + REATTR_SEGMENT_SUFFIX
    try:
        text, _ = fetch_csv(date_from, date_to, report_segment, reattr=True)
    except RuntimeError as exc:
        print(f'Retargeting fetch skipped (non-fatal): {exc}', file=sys.stderr)
        return 0

    raw_rows = rows_from_csv(text)
    normalized = []
    for raw in raw_rows:
        row = normalize_row(raw, reattr_segment)
        if not row['fact_date']:
            continue
        for col in ('cost', 'revenue', 'clicks', 'impressions',
                    'af_start_trial', 'af_subscribe',
                    'rc_trial_converted_event', 'af_tutorial_completion'):
            row[col] = None
        conv_type = ''
        for k, v in raw.items():
            if k.strip().lower() == 'conversion type':
                conv_type = _dim_key(v)
                break
        row['ad'] = conv_type or 'retargeting'
        normalized.append(row)

    if not normalized:
        return 0

    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM appsflyer_pull_daily_truth
        WHERE fact_date >= ? AND fact_date <= ?
          AND report_segment = ? AND timezone = ?
        """,
        (date_from, date_to, reattr_segment, tz),
    )
    n = 0
    for row in normalized:
        cur.execute(INSERT_ROW, row)
        n += 1
    conn.commit()
    if n:
        print(f'Loaded {n} retargeting row(s) (re-attributions + re-engagements)', file=sys.stderr)
    return n


def run(date_from: str, date_to: str, *, db_path: Path, report_segment: str, dry_run: bool = False) -> int:
    if dry_run:
        text, _ = fetch_csv(date_from, date_to, report_segment)
        print(text[:2000])
        return 0

    text, _ = fetch_csv(date_from, date_to, report_segment)
    raw_rows = rows_from_csv(text)
    normalized = [normalize_row(r, report_segment) for r in raw_rows]
    normalized = [r for r in normalized if r['fact_date']]

    conn = connect_db(db_path)
    try:
        init_schema(conn)
        tz = common.business_timezone_name()

        new_cost_dates = _dates_with_cost(normalized)
        old_cost_dates = _existing_dates_with_cost(conn, date_from, date_to, report_segment, tz)

        all_dates: set[str] = {r['fact_date'] for r in normalized}
        skip_dates = old_cost_dates - new_cost_dates
        replace_dates = all_dates - skip_dates

        if skip_dates:
            print(
                f'Preserving {len(skip_dates)} date(s) where Pull cost data expired '
                f'but DB has good cost: {sorted(skip_dates)}',
                file=sys.stderr,
            )

        cur = conn.cursor()
        for d in sorted(replace_dates):
            cur.execute(
                """
                DELETE FROM appsflyer_pull_daily_truth
                WHERE fact_date = ?
                  AND report_segment = ?
                  AND timezone = ?
                """,
                (d, report_segment, tz),
            )

        n = 0
        for row in normalized:
            if row['fact_date'] in skip_dates:
                continue
            cur.execute(INSERT_ROW, row)
            n += 1
        conn.commit()

        n_reattr = _fetch_retargeting(date_from, date_to, report_segment, conn, tz)

        removed = dedupe_pull_daily_truth(conn)
        if removed:
            print(f'Deduped {removed} duplicate pull-truth row(s) (NULL/empty dimension keys)', file=sys.stderr)
        return n + n_reattr
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
        print(f'Loaded {n} pull-truth row(s) into {db_path} (window replaced + deduped)')


if __name__ == '__main__':
    main()
