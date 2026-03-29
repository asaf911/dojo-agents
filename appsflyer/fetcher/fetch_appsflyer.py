#!/usr/bin/env python3
"""
Fetch AppsFlyer aggregate report data and persist rows into SQLite.

This module expects a CSV-shaped HTTP response from an AppsFlyer aggregate export.
The exact host, path, query parameters, and auth mechanism must match what your
AppsFlyer account and documentation specify — confirm in the official docs and
dashboard before relying on defaults here (do not treat comments or placeholders
as guaranteed endpoints).

Useful starting points (verify against your contract; URLs and paths change):
https://dev.appsflyer.com/hc/hc/reference/get_app-id-daily-report-v5-1
https://support.appsflyer.com/hc/en-us/articles/207034346-Pull-API-aggregate-data

Ad and ad set fields appear only when the chosen aggregate export and ad network
provide them; map additional CSV headers via FIELD_ALIASES as needed.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote

import requests
from dotenv import load_dotenv

# Default DB location (override with APPSFLYER_SQLITE_PATH).
DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "appsflyer.db"

# -----------------------------------------------------------------------------
# URL construction — values below are placeholders until confirmed in AppsFlyer
# documentation for your app / token / report type. Adjust APPSFLYER_AGG_BASE,
# REPORT_PATH_TEMPLATE, query params, and auth in fetch_report_csv() accordingly.
# -----------------------------------------------------------------------------
APPSFLYER_AGG_BASE = os.environ.get(
    "APPSFLYER_AGG_BASE",
    "https://hq1.appsflyer.com/api/agg-data/export/app",
)
REPORT_PATH_TEMPLATE = "{base}/{app_id}/{report_segment}/v5"

# Retry: transient network / server errors only (not 4xx except 429).
_MAX_ATTEMPTS = 5
_BACKOFF_START_SEC = 0.5


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def parse_iso_date_arg(label: str, value: str) -> str:
    """Strict YYYY-MM-DD for CLI arguments."""
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{label} must be a calendar date in YYYY-MM-DD format (got {value!r})"
        ) from exc
    return value


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
    Build the request URL for a date range.

    Path segments and query keys are not standardized here — match the Pull /
    aggregate export you are entitled to (confirm in AppsFlyer docs).
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


def _http_error_message(status_code: int, url: str, body_preview: str) -> str:
    preview = (body_preview or "").strip().replace("\n", " ")[:200]
    if status_code == 401:
        return (
            "HTTP 401 Unauthorized: invalid or missing token, or wrong auth scheme. "
            "Confirm token type and whether AppsFlyer expects Bearer vs query api_token. "
            f"URL (truncated): {url[:120]}… Preview: {preview!r}"
        )
    if status_code == 403:
        return (
            "HTTP 403 Forbidden: token may lack permission for this report or app. "
            f"URL (truncated): {url[:120]}… Preview: {preview!r}"
        )
    if status_code == 404:
        return (
            "HTTP 404 Not Found: wrong host/path/app id/report segment, or export not "
            f"available. URL (truncated): {url[:120]}… Preview: {preview!r}"
        )
    return f"HTTP {status_code} from AppsFlyer. URL (truncated): {url[:120]}… Preview: {preview!r}"


def _response_looks_like_csv(content_type: str | None, text: str) -> bool:
    ct = (content_type or "").lower()
    if "csv" in ct:
        return True
    if "text/plain" in ct:
        # Many CSV exports use text/plain; require at least one comma in the first line.
        first = text.lstrip().splitlines()[:1]
        return bool(first) and "," in first[0]
    if "application/octet-stream" in ct and text.strip():
        first = text.lstrip().splitlines()[:1]
        return bool(first) and "," in first[0]
    return False


def _raise_if_not_csv(url: str, response: requests.Response, text: str) -> None:
    if _response_looks_like_csv(response.headers.get("Content-Type"), text):
        return
    snippet = text.strip().replace("\n", " ")[:300]
    ct = response.headers.get("Content-Type", "(missing Content-Type)")
    raise RuntimeError(
        "Expected CSV from AppsFlyer export, but the response does not look like CSV "
        f"(Content-Type={ct!r}). First ~300 chars: {snippet!r}. "
        "Confirm the URL, report segment, and Accept header against AppsFlyer docs."
    )


def fetch_report_csv(
    url: str,
    token: str,
    *,
    timeout_sec: float = 120.0,
) -> str:
    """
    GET the report body as text.

    Auth: confirm in AppsFlyer whether Bearer, query token, or other — adjust
    headers and URL to match your dashboard token instructions.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "text/csv, text/plain, */*",
    }
    for attempt in range(_MAX_ATTEMPTS):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout_sec)
            text = resp.text

            if resp.status_code in (429, 502, 503, 504):
                if attempt < _MAX_ATTEMPTS - 1:
                    delay = _BACKOFF_START_SEC * (2**attempt)
                    time.sleep(delay)
                    continue
                raise RuntimeError(
                    f"AppsFlyer returned HTTP {resp.status_code} after {_MAX_ATTEMPTS} attempts. "
                    f"URL (truncated): {url[:120]}…"
                )

            if resp.status_code == 401:
                raise RuntimeError(_http_error_message(401, url, text))
            if resp.status_code == 403:
                raise RuntimeError(_http_error_message(403, url, text))
            if resp.status_code == 404:
                raise RuntimeError(_http_error_message(404, url, text))

            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:
                raise RuntimeError(
                    _http_error_message(resp.status_code, url, text)
                ) from exc

            _raise_if_not_csv(url, resp, text)
            return text

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt < _MAX_ATTEMPTS - 1:
                delay = _BACKOFF_START_SEC * (2**attempt)
                time.sleep(delay)
                continue
            raise RuntimeError(
                f"AppsFlyer request failed after {_MAX_ATTEMPTS} attempts: {exc}"
            ) from exc

    raise RuntimeError("AppsFlyer request failed: exhausted retries without response")


# -----------------------------------------------------------------------------
# CSV → row dict: header names vary by report; map synonyms into canonical keys.
# ADJUST FIELD_ALIASES when your export uses different column labels.
# -----------------------------------------------------------------------------
FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "report_date": ("date", "install date", "event date", "event_time", "day"),
    "media_source": ("media source", "partner", "pid", "media_source", "af_prt"),
    "campaign": ("campaign", "campaign name", "campaign_name", "af_c_id", "c"),
    # Ad set / ad labels vary by export and ad network; extend aliases from your CSV headers.
    "adset": (
        "adset",
        "ad set",
        "ad_set",
        "adset name",
        "ad set name",
        "af_adset",
        "af_adset_id",
        "adset id",
    ),
    "ad": (
        "ad",
        "ad name",
        "ad id",
        "ad_id",
        "af_ad",
        "af_ad_id",
        "creative",
        "creative name",
        "creative id",
    ),
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


def print_detected_csv_headers(csv_text: str) -> None:
    """Print header row field names as seen by the CSV parser (for debugging)."""
    reader = csv.DictReader(io.StringIO(csv_text))
    names = list(reader.fieldnames or [])
    if not names:
        print("[verbose] CSV: no header row detected")
        return
    print("[verbose] CSV headers (%d): %s" % (len(names), ", ".join(names)))


def parse_csv_rows(
    csv_text: str,
    report_segment: str,
    *,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """Parse CSV text into normalized rows ready for SQLite upsert."""
    if verbose:
        print_detected_csv_headers(csv_text)

    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        return []

    fetched_at = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    for raw in reader:
        raw_blob = json.dumps(raw, ensure_ascii=False)

        report_date = _pick(raw, "report_date")
        if not report_date:
            continue

        out.append(
            {
                "report_date": report_date[:10],
                "media_source": _pick(raw, "media_source"),
                "campaign": _pick(raw, "campaign"),
                "adset": _pick(raw, "adset"),
                "ad": _pick(raw, "ad"),
                "report_segment": report_segment,
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


DDL_DAILY_PERFORMANCE_TABLE = """
CREATE TABLE IF NOT EXISTS daily_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date TEXT NOT NULL,
    media_source TEXT,
    campaign TEXT,
    adset TEXT,
    ad TEXT,
    report_segment TEXT NOT NULL,
    currency TEXT,
    impressions INTEGER,
    clicks INTEGER,
    installs INTEGER,
    sessions INTEGER,
    revenue REAL,
    cost REAL,
    source_payload TEXT,
    fetched_at TEXT NOT NULL,
    UNIQUE (report_date, media_source, campaign, adset, ad)
);
"""

# Indexes on adset/ad must run only after those columns exist (see init_schema).
DDL_DAILY_PERFORMANCE_INDEXES_CORE = """
CREATE INDEX IF NOT EXISTS idx_daily_performance_date
    ON daily_performance (report_date);
CREATE INDEX IF NOT EXISTS idx_daily_performance_media
    ON daily_performance (media_source);
CREATE INDEX IF NOT EXISTS idx_daily_performance_campaign
    ON daily_performance (campaign);
CREATE INDEX IF NOT EXISTS idx_daily_performance_segment
    ON daily_performance (report_segment);
"""

DDL_DAILY_PERFORMANCE_INDEXES_AD = """
CREATE INDEX IF NOT EXISTS idx_daily_performance_adset
    ON daily_performance (adset);
CREATE INDEX IF NOT EXISTS idx_daily_performance_ad
    ON daily_performance (ad);
"""


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def _legacy_table_missing_uniqueness(conn: sqlite3.Connection) -> bool:
    """True if daily_performance exists but was created without a UNIQUE clause (pre-migration)."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='daily_performance'"
    ).fetchone()
    if not row or not row[0]:
        return False
    return "UNIQUE" not in row[0].upper()


def _migrate_widen_unique_for_ad_adset(conn: sqlite3.Connection) -> None:
    """
    Rebuild daily_performance: SQLite cannot change UNIQUE columns in place.
    Existing rows get NULL adset/ad (same logical key as before ad-level exports).
    """
    conn.execute("ALTER TABLE daily_performance RENAME TO daily_performance_old")
    conn.executescript(DDL_DAILY_PERFORMANCE_TABLE)
    conn.executescript(DDL_DAILY_PERFORMANCE_INDEXES_CORE)
    conn.executescript(DDL_DAILY_PERFORMANCE_INDEXES_AD)
    conn.execute(
        """
        INSERT INTO daily_performance (
            report_date, media_source, campaign, adset, ad, report_segment, currency,
            impressions, clicks, installs, sessions, revenue, cost,
            source_payload, fetched_at
        )
        SELECT
            report_date, media_source, campaign, NULL, NULL, report_segment, currency,
            impressions, clicks, installs, sessions, revenue, cost,
            source_payload, fetched_at
        FROM daily_performance_old
        """
    )
    conn.execute("DROP TABLE daily_performance_old")


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL_DAILY_PERFORMANCE_TABLE)
    conn.executescript(DDL_DAILY_PERFORMANCE_INDEXES_CORE)
    cols = _table_columns(conn, "daily_performance")
    if cols and "report_segment" not in cols:
        conn.execute(
            "ALTER TABLE daily_performance ADD COLUMN report_segment TEXT NOT NULL DEFAULT 'legacy'"
        )
        cols = _table_columns(conn, "daily_performance")
    if cols and "ad" not in cols:
        _migrate_widen_unique_for_ad_adset(conn)
        cols = _table_columns(conn, "daily_performance")
    elif cols and "ad" in cols:
        conn.executescript(DDL_DAILY_PERFORMANCE_INDEXES_AD)
    if _legacy_table_missing_uniqueness(conn):
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_daily_performance_rpt_src_camp "
            "ON daily_performance (report_date, media_source, campaign, adset, ad)"
        )
    conn.commit()


UPSERT_SQL = """
INSERT INTO daily_performance (
    report_date, media_source, campaign, adset, ad, report_segment, currency,
    impressions, clicks, installs, sessions, revenue, cost,
    source_payload, fetched_at
) VALUES (
    :report_date, :media_source, :campaign, :adset, :ad, :report_segment, :currency,
    :impressions, :clicks, :installs, :sessions, :revenue, :cost,
    :source_payload, :fetched_at
)
ON CONFLICT (report_date, media_source, campaign, adset, ad) DO UPDATE SET
    report_segment = excluded.report_segment,
    currency = excluded.currency,
    impressions = excluded.impressions,
    clicks = excluded.clicks,
    installs = excluded.installs,
    sessions = excluded.sessions,
    revenue = excluded.revenue,
    cost = excluded.cost,
    source_payload = excluded.source_payload,
    fetched_at = excluded.fetched_at
"""


def upsert_rows(conn: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> int:
    cur = conn.cursor()
    n = 0
    for row in rows:
        cur.execute(UPSERT_SQL, row)
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
    verbose: bool = False,
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
    rows = parse_csv_rows(csv_text, report_segment, verbose=verbose)
    conn = connect_db(db_path)
    try:
        init_schema(conn)
        n = upsert_rows(conn, rows)
    finally:
        conn.close()
    return n


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch AppsFlyer aggregate CSV into SQLite.")
    p.add_argument(
        "--from",
        dest="date_from",
        required=True,
        type=lambda s: parse_iso_date_arg("--from", s),
        help="Start date YYYY-MM-DD",
    )
    p.add_argument(
        "--to",
        dest="date_to",
        required=True,
        type=lambda s: parse_iso_date_arg("--to", s),
        help="End date YYYY-MM-DD (inclusive)",
    )
    p.add_argument(
        "--report-segment",
        default=os.environ.get("APPSFLYER_REPORT_SEGMENT", "partners_by_date_report"),
        help=(
            "URL path segment for the aggregate report — must match AppsFlyer docs "
            "for your export (confirm manually; do not rely on this default). "
            "Ad/adset columns require an export that includes those dimensions "
            "(availability depends on the ad network and report type)."
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
    p.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print detected CSV header names before parsing rows.",
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
        verbose=args.verbose,
    )
    if not args.dry_run:
        print(f"Upserted {n} rows into {db_path}")


if __name__ == "__main__":
    main()
