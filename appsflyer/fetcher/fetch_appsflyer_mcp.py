#!/usr/bin/env python3
"""
Fetch AppsFlyer KPIs through AppsFlyer MCP (fetch_aggregated_data) and persist to SQLite.

Phase-1 scope:
- Primary KPI: af_start_trial (Unique users)
- Supporting KPIs: af_subscribe, af_tutorial_completion, rc_trial_converted_event (Unique users)
- LTV proxy from AppsFlyer MCP: ARPU period=ltv
- Cost + installs are requested in each call for CPE calculations

Notes:
- MCP limits: 20 calls/min, 200 calls/day per user.
- This script intentionally makes a small fixed number of calls (5) per run.
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
    KpiSpec(
        "af_tutorial_completion_unique_users",
        "Unique users",
        in_app_event="af_tutorial_completion",
    ),
    KpiSpec(
        "rc_trial_converted_event_unique_users",
        "Unique users",
        in_app_event="rc_trial_converted_event",
    ),
    KpiSpec("arpu_ltv", "ARPU", period="ltv"),
]


def _mcp_post(
    token: str,
    payload: dict[str, Any],
    timeout_sec: float = 120.0,
    *,
    expect_data: bool = True,
) -> dict[str, Any]:
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


def _first_numeric_metric_column(row: dict[str, str]) -> tuple[str, float | None]:
    # Grouping columns are well-known; metric columns are everything else.
    grouping_cols = {"Media source", "Campaign", "Adset", "Ad"}
    for k, v in row.items():
        if k in grouping_cols or k == "Installs appsflyer":
            continue
        parsed = _parse_float(v)
        if parsed is not None:
            return k, parsed
    return "", None


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


DDL = """
CREATE TABLE IF NOT EXISTS mcp_kpi_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_from TEXT NOT NULL,
    date_to TEXT NOT NULL,
    media_source TEXT,
    campaign TEXT,
    adset TEXT,
    ad TEXT,
    kpi_name TEXT NOT NULL,
    metric_column TEXT,
    metric_value REAL,
    installs REAL,
    in_app_event TEXT,
    period TEXT,
    timezone TEXT,
    currency TEXT,
    fetched_at TEXT NOT NULL,
    raw_metadata TEXT,
    UNIQUE(date_from, date_to, media_source, campaign, adset, ad, kpi_name)
);
CREATE INDEX IF NOT EXISTS idx_mcp_kpi_range ON mcp_kpi_performance(date_from, date_to);
CREATE INDEX IF NOT EXISTS idx_mcp_kpi_ad ON mcp_kpi_performance(ad);
CREATE INDEX IF NOT EXISTS idx_mcp_kpi_adset ON mcp_kpi_performance(adset);
CREATE INDEX IF NOT EXISTS idx_mcp_kpi_kpi ON mcp_kpi_performance(kpi_name);
"""

UPSERT = """
INSERT INTO mcp_kpi_performance (
    date_from, date_to, media_source, campaign, adset, ad,
    kpi_name, metric_column, metric_value, installs, in_app_event, period,
    timezone, currency, fetched_at, raw_metadata
) VALUES (
    :date_from, :date_to, :media_source, :campaign, :adset, :ad,
    :kpi_name, :metric_column, :metric_value, :installs, :in_app_event, :period,
    :timezone, :currency, :fetched_at, :raw_metadata
)
ON CONFLICT(date_from, date_to, media_source, campaign, adset, ad, kpi_name)
DO UPDATE SET
    metric_column = excluded.metric_column,
    metric_value = excluded.metric_value,
    installs = excluded.installs,
    in_app_event = excluded.in_app_event,
    period = excluded.period,
    timezone = excluded.timezone,
    currency = excluded.currency,
    fetched_at = excluded.fetched_at,
    raw_metadata = excluded.raw_metadata
"""


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


def fetch_kpi_rows(
    token: str,
    app_id: str,
    date_from: str,
    date_to: str,
    spec: KpiSpec,
    *,
    row_count: int,
) -> list[dict[str, Any]]:
    # Minimal initialize sequence per call keeps state handling simple.
    _mcp_post(
        token,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "dojo-appsflyer-fetcher", "version": "0.1.0"},
            },
        },
    )
    _mcp_post(
        token,
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        expect_data=False,
    )

    metric_obj: dict[str, str] = {"metric_name": spec.metric_name}
    if spec.period:
        metric_obj["period"] = spec.period

    query: dict[str, Any] = {
        "start_date": date_from,
        "end_date": date_to,
        "app_ids": [app_id],
        "metrics": [metric_obj],
        "groupings": ["Media source", "Campaign", "Adset", "Ad"],
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
            "params": {
                "name": "fetch_aggregated_data",
                "arguments": {"query": query},
            },
        },
    )

    text_result = _extract_text_result(response)
    if not text_result:
        return []
    if "validation errors" in text_result.lower():
        raise RuntimeError(text_result)

    csv_text, metadata = _split_data_and_metadata(text_result)
    parsed_rows = _rows_from_csv(csv_text)

    fetched_at = datetime.now(timezone.utc).isoformat()
    timezone_name = metadata.get("timezone")
    currency = metadata.get("currency")

    out: list[dict[str, Any]] = []
    for row in parsed_rows:
        metric_column, metric_value = _first_numeric_metric_column(row)
        out.append(
            {
                "date_from": date_from,
                "date_to": date_to,
                "media_source": row.get("Media source"),
                "campaign": row.get("Campaign"),
                "adset": row.get("Adset"),
                "ad": row.get("Ad"),
                "kpi_name": spec.name,
                "metric_column": metric_column,
                "metric_value": metric_value,
                "installs": _parse_float(row.get("Installs appsflyer")),
                "in_app_event": spec.in_app_event,
                "period": spec.period,
                "timezone": timezone_name,
                "currency": currency,
                "fetched_at": fetched_at,
                "raw_metadata": json.dumps(metadata, ensure_ascii=False),
            }
        )
    return out


def run(date_from: str, date_to: str, *, db_path: Path, row_count: int, dry_run: bool = False) -> int:
    load_dotenv(common.project_root() / ".env")
    app_id = os.environ.get("APPSFLYER_APP_ID", "").strip()
    mcp_token = os.environ.get("APPSFLYER_MCP_TOKEN", "").strip()
    if not app_id or not mcp_token:
        raise RuntimeError(
            "Missing APPSFLYER_APP_ID or APPSFLYER_MCP_TOKEN in appsflyer/.env"
        )

    if dry_run:
        print(
            f"[dry-run] MCP URL={MCP_URL} app_id={app_id} from={date_from} to={date_to} "
            f"kpis={len(KPI_SPECS)}"
        )
        return 0

    conn = connect_db(db_path)
    try:
        init_schema(conn)
        all_rows: list[dict[str, Any]] = []
        for spec in KPI_SPECS:
            all_rows.extend(
                fetch_kpi_rows(
                    mcp_token,
                    app_id,
                    date_from,
                    date_to,
                    spec,
                    row_count=row_count,
                )
            )

        cur = conn.cursor()
        n = 0
        for row in all_rows:
            cur.execute(UPSERT, row)
            n += 1
        conn.commit()
        return n
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Fetch AppsFlyer KPIs via MCP into SQLite.")
    p.add_argument("--from", dest="date_from", required=True, type=lambda s: common.parse_iso_date_arg("--from", s))
    p.add_argument("--to", dest="date_to", required=True, type=lambda s: common.parse_iso_date_arg("--to", s))
    p.add_argument("--row-count", type=int, default=300, help="Top rows returned by MCP per KPI (max 300)")
    p.add_argument("--db", default=os.environ.get("APPSFLYER_SQLITE_PATH", str(DEFAULT_DB_PATH)), help="SQLite database path")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    if args.row_count < 1 or args.row_count > 300:
        raise SystemExit("--row-count must be between 1 and 300")

    db_path = Path(args.db).expanduser().resolve()
    n = run(args.date_from, args.date_to, db_path=db_path, row_count=args.row_count, dry_run=args.dry_run)
    if not args.dry_run:
        print(f"Upserted {n} rows into {db_path}")


if __name__ == "__main__":
    main()
