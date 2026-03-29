#!/usr/bin/env python3
"""
Fetch AppsFlyer KPIs through AppsFlyer MCP and persist into two layers:

1) Source-of-truth layer (AppsFlyer-shaped):
   - appsflyer_mcp_source_rows
   - one row per fetch result row / KPI / fetch window

2) Normalized analytics layer (business-shaped daily facts):
   - marketing_fact_daily
   - one row per date x media_source x campaign x adset x ad
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

_APPSFLYER_DIR = Path(__file__).resolve().parent.parent
if str(_APPSFLYER_DIR) not in sys.path:
    sys.path.insert(0, str(_APPSFLYER_DIR))
import common

MCP_URL = os.environ.get("APPSFLYER_MCP_URL", "https://mcp.appsflyer.com/auth/mcp")
DEFAULT_DB_PATH = common.project_root() / "data" / "appsflyer.db"


@dataclass
class KpiSpec:
    name: str
    metric_name: str
    period: str = ""
    in_app_event: str | None = None


KPI_SPECS: list[KpiSpec] = [
    KpiSpec("af_start_trial_unique_users", "Unique users", in_app_event="af_start_trial"),
    KpiSpec("af_subscribe_unique_users", "Unique users", in_app_event="af_subscribe"),
    KpiSpec("af_tutorial_completion_unique_users", "Unique users", in_app_event="af_tutorial_completion"),
    KpiSpec("rc_trial_converted_event_unique_users", "Unique users", in_app_event="rc_trial_converted_event"),
    KpiSpec("arpu_ltv", "ARPU", period="ltv"),
]

DIMENSION_ALIASES = {
    "Date": "fact_date",
    "Media source": "media_source",
    "Campaign": "campaign",
    "Adset": "adset",
    "Ad": "ad",
}

DDL = """
CREATE TABLE IF NOT EXISTS appsflyer_mcp_source_rows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fetch_window_from TEXT NOT NULL,
    fetch_window_to TEXT NOT NULL,
    fact_date TEXT,
    media_source TEXT,
    campaign TEXT,
    adset TEXT,
    ad TEXT,
    kpi_name TEXT NOT NULL,
    metric_column TEXT,
    metric_value REAL,
    installs REAL,
    cost REAL,
    in_app_event TEXT,
    period TEXT,
    timezone TEXT,
    currency TEXT,
    fetched_at TEXT NOT NULL,
    raw_row_json TEXT NOT NULL,
    raw_metadata_json TEXT,
    UNIQUE(fetch_window_from, fetch_window_to, fact_date, media_source, campaign, adset, ad, kpi_name)
);
CREATE INDEX IF NOT EXISTS idx_af_src_fact_date ON appsflyer_mcp_source_rows(fact_date);
CREATE INDEX IF NOT EXISTS idx_af_src_kpi ON appsflyer_mcp_source_rows(kpi_name);
CREATE INDEX IF NOT EXISTS idx_af_src_media ON appsflyer_mcp_source_rows(media_source);
CREATE INDEX IF NOT EXISTS idx_af_src_campaign ON appsflyer_mcp_source_rows(campaign);

CREATE TABLE IF NOT EXISTS marketing_fact_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fact_date TEXT NOT NULL,
    source_system TEXT NOT NULL DEFAULT 'appsflyer_mcp',
    media_source TEXT,
    campaign TEXT,
    adset TEXT,
    ad TEXT,
    installs REAL,
    spend REAL,
    af_start_trial REAL,
    af_subscribe REAL,
    af_tutorial_completion REAL,
    rc_trial_converted_event REAL,
    arpu_ltv REAL,
    currency TEXT,
    timezone TEXT,
    fetched_at TEXT NOT NULL,
    UNIQUE(fact_date, source_system, media_source, campaign, adset, ad)
);
CREATE INDEX IF NOT EXISTS idx_mkt_daily_date ON marketing_fact_daily(fact_date);
CREATE INDEX IF NOT EXISTS idx_mkt_daily_media ON marketing_fact_daily(media_source);
CREATE INDEX IF NOT EXISTS idx_mkt_daily_campaign ON marketing_fact_daily(campaign);
CREATE INDEX IF NOT EXISTS idx_mkt_daily_adset ON marketing_fact_daily(adset);
CREATE INDEX IF NOT EXISTS idx_mkt_daily_ad ON marketing_fact_daily(ad);
"""

UPSERT_SOURCE = """
INSERT INTO appsflyer_mcp_source_rows (
    fetch_window_from, fetch_window_to, fact_date, media_source, campaign, adset, ad,
    kpi_name, metric_column, metric_value, installs, cost, in_app_event, period,
    timezone, currency, fetched_at, raw_row_json, raw_metadata_json
) VALUES (
    :fetch_window_from, :fetch_window_to, :fact_date, :media_source, :campaign, :adset, :ad,
    :kpi_name, :metric_column, :metric_value, :installs, :cost, :in_app_event, :period,
    :timezone, :currency, :fetched_at, :raw_row_json, :raw_metadata_json
)
ON CONFLICT(fetch_window_from, fetch_window_to, fact_date, media_source, campaign, adset, ad, kpi_name)
DO UPDATE SET
    metric_column = excluded.metric_column,
    metric_value = excluded.metric_value,
    installs = excluded.installs,
    cost = excluded.cost,
    in_app_event = excluded.in_app_event,
    period = excluded.period,
    timezone = excluded.timezone,
    currency = excluded.currency,
    fetched_at = excluded.fetched_at,
    raw_row_json = excluded.raw_row_json,
    raw_metadata_json = excluded.raw_metadata_json
"""

UPSERT_MARKETING_DAILY = """
INSERT INTO marketing_fact_daily (
    fact_date, source_system, media_source, campaign, adset, ad,
    installs, spend, af_start_trial, af_subscribe, af_tutorial_completion,
    rc_trial_converted_event, arpu_ltv, currency, timezone, fetched_at
) VALUES (
    :fact_date, 'appsflyer_mcp', :media_source, :campaign, :adset, :ad,
    :installs, :spend, :af_start_trial, :af_subscribe, :af_tutorial_completion,
    :rc_trial_converted_event, :arpu_ltv, :currency, :timezone, :fetched_at
)
ON CONFLICT(fact_date, source_system, media_source, campaign, adset, ad)
DO UPDATE SET
    installs = COALESCE(excluded.installs, marketing_fact_daily.installs),
    spend = COALESCE(excluded.spend, marketing_fact_daily.spend),
    af_start_trial = COALESCE(excluded.af_start_trial, marketing_fact_daily.af_start_trial),
    af_subscribe = COALESCE(excluded.af_subscribe, marketing_fact_daily.af_subscribe),
    af_tutorial_completion = COALESCE(excluded.af_tutorial_completion, marketing_fact_daily.af_tutorial_completion),
    rc_trial_converted_event = COALESCE(excluded.rc_trial_converted_event, marketing_fact_daily.rc_trial_converted_event),
    arpu_ltv = COALESCE(excluded.arpu_ltv, marketing_fact_daily.arpu_ltv),
    currency = COALESCE(excluded.currency, marketing_fact_daily.currency),
    timezone = COALESCE(excluded.timezone, marketing_fact_daily.timezone),
    fetched_at = excluded.fetched_at
"""


def _mcp_post(token: str, payload: dict[str, Any], timeout_sec: float = 120.0, *, expect_data: bool = True) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
    }
    resp = requests.post(MCP_URL, headers=headers, data=json.dumps(payload), timeout=timeout_sec)
    resp.raise_for_status()
    data_lines = [ln[6:] for ln in resp.text.splitlines() if ln.startswith("data: ")]
    if not data_lines:
        if expect_data:
            raise RuntimeError("MCP response did not include data payload")
        return {}
    return json.loads(data_lines[-1])


def _extract_text_result(obj: dict[str, Any]) -> str:
    result = obj.get("result", {})
    content = result.get("content", [])
    if not content:
        return ""
    for item in content:
        if item.get("type") == "text":
            return item.get("text", "")
    return ""


def _split_data_and_metadata(text_result: str) -> tuple[str, dict[str, Any]]:
    if "## Data:" not in text_result:
        return "", {}
    payload = text_result.split("## Data:", 1)[1]
    if "; ## Metadata:" in payload:
        data_part, md_part = payload.split("; ## Metadata:", 1)
        data_csv = data_part.strip()
        md_part = md_part.strip()
        try:
            metadata = json.loads(md_part)
        except json.JSONDecodeError:
            metadata = {"raw": md_part}
        return data_csv, metadata
    return payload.strip(), {}


def _rows_from_csv(csv_text: str) -> list[dict[str, str]]:
    if not csv_text.strip():
        return []
    reader = csv.DictReader(io.StringIO(csv_text))
    return list(reader)


def _parse_float(v: str | None) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _normalize_fact_date(v: str | None, fallback_from: str, fallback_to: str) -> str | None:
    if v and str(v).strip():
        return str(v).strip()
    if fallback_from == fallback_to:
        return fallback_from
    return None


def _identify_columns(row: dict[str, str]) -> tuple[str, float | None, float | None, float | None]:
    metric_column = ""
    metric_value = None
    installs = None
    cost = None
    for k, v in row.items():
        parsed = _parse_float(v)
        if parsed is None:
            continue
        kl = k.lower()
        if 'install' in kl:
            installs = parsed
        elif kl == 'cost' or 'cost' in kl:
            cost = parsed
        elif k not in DIMENSION_ALIASES:
            metric_column = k
            metric_value = parsed
    return metric_column, metric_value, installs, cost


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


def _initialize_session(token: str) -> None:
    _mcp_post(
        token,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "dojo-appsflyer-fetcher", "version": "0.2.1"},
            },
        },
    )
    _mcp_post(token, {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}, expect_data=False)


def fetch_kpi_rows(token: str, app_id: str, date_from: str, date_to: str, spec: KpiSpec, *, row_count: int) -> list[dict[str, Any]]:
    _initialize_session(token)

    metrics: list[dict[str, str]] = [
        {"metric_name": spec.metric_name, **({"period": spec.period} if spec.period else {})},
        {"metric_name": "Cost"},
        {"metric_name": "Installs"},
    ]

    candidate_groupings = [
        ["Date", "Media source", "Campaign", "Adset"],
        ["Date", "Media source", "Campaign"],
        ["Date", "Media source"],
        ["Media source", "Campaign", "Adset", "Ad"],
    ]

    for groupings in candidate_groupings:
        query: dict[str, Any] = {
            "start_date": date_from,
            "end_date": date_to,
            "app_ids": [app_id],
            "metrics": metrics,
            "groupings": groupings,
            "row_count": row_count,
            "sort_by_metrics": [{"metric_name": spec.metric_name, "order": "desc"}],
        }
        if spec.in_app_event:
            query["in_app_event"] = [spec.in_app_event]

        response = _mcp_post(
            token,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "fetch_aggregated_data", "arguments": {"query": query}},
            },
        )

        text_result = _extract_text_result(response)
        if not text_result:
            continue
        if "validation errors" in text_result.lower():
            continue

        csv_text, metadata = _split_data_and_metadata(text_result)
        parsed_rows = _rows_from_csv(csv_text)
        if not parsed_rows:
            continue

        fetched_at = datetime.now(timezone.utc).isoformat()
        timezone_name = metadata.get("timezone")
        currency = metadata.get("currency")

        out: list[dict[str, Any]] = []
        for row in parsed_rows:
            metric_column, metric_value, installs, cost = _identify_columns(row)
            mapped = {dst: row.get(src) for src, dst in DIMENSION_ALIASES.items()}
            fact_date = _normalize_fact_date(mapped.get("fact_date"), date_from, date_to)
            out.append(
                {
                    "fetch_window_from": date_from,
                    "fetch_window_to": date_to,
                    "fact_date": fact_date,
                    "media_source": mapped.get("media_source"),
                    "campaign": mapped.get("campaign"),
                    "adset": mapped.get("adset"),
                    "ad": mapped.get("ad"),
                    "kpi_name": spec.name,
                    "metric_column": metric_column,
                    "metric_value": metric_value,
                    "installs": installs,
                    "cost": cost,
                    "in_app_event": spec.in_app_event,
                    "period": spec.period,
                    "timezone": timezone_name,
                    "currency": currency,
                    "fetched_at": fetched_at,
                    "raw_row_json": json.dumps(row, ensure_ascii=False),
                    "raw_metadata_json": json.dumps(metadata, ensure_ascii=False),
                }
            )
        if out:
            return out
    return []


def rebuild_marketing_fact_daily(conn: sqlite3.Connection, date_from: str, date_to: str) -> int:
    # De-duplicate overlapping source windows before building daily facts.
    # Canonical pick rule per (fact_date, media_source, campaign, adset, ad, kpi_name):
    # 1) narrowest fetch window wins
    # 2) if tied, latest fetched_at wins
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                fact_date,
                media_source,
                campaign,
                adset,
                ad,
                kpi_name,
                metric_value,
                installs,
                cost,
                currency,
                timezone,
                fetched_at,
                fetch_window_from,
                fetch_window_to,
                (julianday(fetch_window_to) - julianday(fetch_window_from)) AS window_days,
                ROW_NUMBER() OVER (
                    PARTITION BY fact_date, media_source, campaign, adset, ad, kpi_name
                    ORDER BY (julianday(fetch_window_to) - julianday(fetch_window_from)) ASC,
                             fetched_at DESC
                ) AS rn
            FROM appsflyer_mcp_source_rows
            WHERE fact_date BETWEEN ? AND ?
        ),
        canonical AS (
            SELECT *
            FROM ranked
            WHERE rn = 1
        )
        SELECT
            fact_date,
            media_source,
            campaign,
            adset,
            ad,
            MAX(currency) AS currency,
            MAX(timezone) AS timezone,
            MAX(fetched_at) AS fetched_at,
            MAX(installs) AS installs,
            MAX(cost) AS spend,
            SUM(CASE WHEN kpi_name = 'af_start_trial_unique_users' THEN COALESCE(metric_value,0) ELSE 0 END) AS af_start_trial,
            SUM(CASE WHEN kpi_name = 'af_subscribe_unique_users' THEN COALESCE(metric_value,0) ELSE 0 END) AS af_subscribe,
            SUM(CASE WHEN kpi_name = 'af_tutorial_completion_unique_users' THEN COALESCE(metric_value,0) ELSE 0 END) AS af_tutorial_completion,
            SUM(CASE WHEN kpi_name = 'rc_trial_converted_event_unique_users' THEN COALESCE(metric_value,0) ELSE 0 END) AS rc_trial_converted_event,
            MAX(CASE WHEN kpi_name = 'arpu_ltv' THEN metric_value END) AS arpu_ltv
        FROM canonical
        GROUP BY fact_date, media_source, campaign, adset, ad
        """,
        (date_from, date_to),
    ).fetchall()

    cur = conn.cursor()
    # Clear the rebuilt date range first so stale duplicated rows don't survive.
    cur.execute(
        "DELETE FROM marketing_fact_daily WHERE fact_date BETWEEN ? AND ? AND source_system = 'appsflyer_mcp'",
        (date_from, date_to),
    )
    n = 0
    for row in rows:
        cur.execute(
            UPSERT_MARKETING_DAILY,
            {
                "fact_date": row["fact_date"],
                "media_source": row["media_source"],
                "campaign": row["campaign"],
                "adset": row["adset"],
                "ad": row["ad"],
                "installs": row["installs"],
                "spend": row["spend"],
                "af_start_trial": row["af_start_trial"],
                "af_subscribe": row["af_subscribe"],
                "af_tutorial_completion": row["af_tutorial_completion"],
                "rc_trial_converted_event": row["rc_trial_converted_event"],
                "arpu_ltv": row["arpu_ltv"],
                "currency": row["currency"],
                "timezone": row["timezone"],
                "fetched_at": row["fetched_at"] or datetime.now(timezone.utc).isoformat(),
            },
        )
        n += 1
    conn.commit()
    return n


def run(date_from: str, date_to: str, *, db_path: Path, row_count: int, dry_run: bool = False) -> tuple[int, int]:
    load_dotenv(common.project_root() / ".env")
    app_id = os.environ.get("APPSFLYER_APP_ID", "").strip()
    mcp_token = os.environ.get("APPSFLYER_MCP_TOKEN", "").strip()
    if not app_id or not mcp_token:
        raise RuntimeError("Missing APPSFLYER_APP_ID or APPSFLYER_MCP_TOKEN in appsflyer/.env")

    if dry_run:
        print(f"[dry-run] MCP URL={MCP_URL} app_id={app_id} from={date_from} to={date_to} kpis={len(KPI_SPECS)} row_count={row_count}")
        return (0, 0)

    conn = connect_db(db_path)
    try:
        init_schema(conn)
        all_rows: list[dict[str, Any]] = []
        for spec in KPI_SPECS:
            all_rows.extend(fetch_kpi_rows(mcp_token, app_id, date_from, date_to, spec, row_count=row_count))

        cur = conn.cursor()
        source_n = 0
        for row in all_rows:
            cur.execute(UPSERT_SOURCE, row)
            source_n += 1
        conn.commit()

        fact_n = rebuild_marketing_fact_daily(conn, date_from, date_to)
        return (source_n, fact_n)
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Fetch AppsFlyer KPIs via MCP into source + normalized SQLite tables.")
    p.add_argument("--from", dest="date_from", required=True, type=lambda s: common.parse_iso_date_arg("--from", s))
    p.add_argument("--to", dest="date_to", required=True, type=lambda s: common.parse_iso_date_arg("--to", s))
    p.add_argument("--row-count", type=int, default=300, help="Top rows returned by MCP per KPI (max 300)")
    p.add_argument("--db", default=os.environ.get("APPSFLYER_SQLITE_PATH", str(DEFAULT_DB_PATH)), help="SQLite database path")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    if args.row_count < 1 or args.row_count > 300:
        raise SystemExit("--row-count must be between 1 and 300")

    db_path = Path(args.db).expanduser().resolve()
    source_n, fact_n = run(args.date_from, args.date_to, db_path=db_path, row_count=args.row_count, dry_run=args.dry_run)
    if not args.dry_run:
        print(f"Upserted {source_n} source rows and rebuilt {fact_n} marketing daily fact rows in {db_path}")


if __name__ == "__main__":
    main()
