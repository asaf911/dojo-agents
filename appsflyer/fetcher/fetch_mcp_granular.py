#!/usr/bin/env python3
"""
Ad-level MCP fetcher: one day at a time, ad-grain only.

The existing fetch_appsflyer_mcp.py handles adset-level data.  This fetcher
adds ad-level granularity by fetching [Date, Media source, Campaign, Ad].
Single-day windows keep row counts well under the 300-row MCP cap.

Bronze:  appsflyer_mcp_source_rows (shared; ad-level rows use kpi_name
         prefixed with 'ad_' to avoid collisions with adset-grain rows)
Silver:  marketing_fact_ad (one row per date × media_source × campaign × ad)
Map:     mcp_adset_ad_map  (media_source × campaign × ad → adset)
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

_APPSFLYER_DIR = Path(__file__).resolve().parent.parent
if str(_APPSFLYER_DIR) not in sys.path:
    sys.path.insert(0, str(_APPSFLYER_DIR))

import common
from fetcher.fetch_appsflyer_mcp import (
    DEFAULT_DB_PATH,
    DIMENSION_ALIASES,
    UPSERT_SOURCE,
    _extract_text_result,
    _identify_columns,
    _mcp_post,
    _normalize_fact_date,
    _rows_from_csv,
    _split_data_and_metadata,
    connect_db,
    init_schema,
)

AD_GROUPINGS = ["Date", "Media source", "Campaign", "Ad"]
MAPPING_GROUPINGS = ["Media source", "Campaign", "Adset", "Ad"]

EVENT_NAMES = [
    "af_start_trial",
    "af_subscribe",
    "rc_trial_converted_event",
    "af_tutorial_completion",
]

DDL_AD = """
CREATE TABLE IF NOT EXISTS marketing_fact_ad (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fact_date TEXT NOT NULL,
    source_system TEXT NOT NULL DEFAULT 'appsflyer_mcp_ad',
    media_source TEXT,
    campaign TEXT,
    adset TEXT,
    ad TEXT,
    attribution_model TEXT NOT NULL DEFAULT 'ltv',
    installs REAL,
    spend REAL,
    af_start_trial REAL,
    af_subscribe REAL,
    af_tutorial_completion REAL,
    rc_trial_converted_event REAL,
    currency TEXT,
    timezone TEXT,
    fetched_at TEXT NOT NULL,
    UNIQUE(fact_date, media_source, campaign, ad, attribution_model)
);
CREATE INDEX IF NOT EXISTS idx_mfa_date ON marketing_fact_ad(fact_date);
CREATE INDEX IF NOT EXISTS idx_mfa_media ON marketing_fact_ad(media_source);
CREATE INDEX IF NOT EXISTS idx_mfa_campaign ON marketing_fact_ad(campaign);
CREATE INDEX IF NOT EXISTS idx_mfa_ad ON marketing_fact_ad(ad);
CREATE INDEX IF NOT EXISTS idx_mfa_attr ON marketing_fact_ad(attribution_model);

CREATE TABLE IF NOT EXISTS mcp_adset_ad_map (
    media_source TEXT NOT NULL,
    campaign TEXT NOT NULL,
    adset TEXT NOT NULL,
    ad TEXT NOT NULL,
    installs REAL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (media_source, campaign, ad)
);
"""


def init_granular_schema(conn: sqlite3.Connection) -> None:
    init_schema(conn)

    _needs_mfa_migration = False
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(marketing_fact_ad)").fetchall()}
        if cols and "attribution_model" not in cols:
            _needs_mfa_migration = True
    except sqlite3.OperationalError:
        pass

    if _needs_mfa_migration:
        print("[migrate] Adding attribution_model to marketing_fact_ad", file=sys.stderr)
        conn.executescript(
            "CREATE TABLE IF NOT EXISTS _mfa_bak AS SELECT * FROM marketing_fact_ad;"
            "DROP TABLE IF EXISTS marketing_fact_ad;"
        )

    conn.executescript(DDL_AD)
    conn.commit()

    if _needs_mfa_migration:
        conn.execute("""
            INSERT OR IGNORE INTO marketing_fact_ad (
                fact_date, source_system, media_source, campaign, adset, ad,
                attribution_model, installs, spend, af_start_trial, af_subscribe,
                af_tutorial_completion, rc_trial_converted_event,
                currency, timezone, fetched_at
            )
            SELECT
                fact_date, source_system, media_source, campaign, adset, ad,
                'ltv', installs, spend, af_start_trial, af_subscribe,
                af_tutorial_completion, rc_trial_converted_event,
                currency, timezone, fetched_at
            FROM _mfa_bak
        """)
        conn.execute("DROP TABLE IF EXISTS _mfa_bak")
        conn.commit()
        print("[migrate] marketing_fact_ad migration complete", file=sys.stderr)


# ── MCP helpers ──────────────────────────────────────────────────────────

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
                "clientInfo": {"name": "dojo-appsflyer-granular", "version": "0.3.0"},
            },
        },
    )
    _mcp_post(
        token,
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        expect_data=False,
    )


def _fetch_one(
    token: str,
    app_id: str,
    date_from: str,
    date_to: str,
    *,
    groupings: list[str],
    metrics: list[dict[str, str]],
    in_app_event: str | None = None,
    sort_metric: str = "Cost",
    row_count: int = 300,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    query: dict[str, Any] = {
        "start_date": date_from,
        "end_date": date_to,
        "app_ids": [app_id],
        "metrics": metrics,
        "groupings": groupings,
        "row_count": row_count,
        "sort_by_metrics": [{"metric_name": sort_metric, "order": "desc"}],
    }
    if in_app_event:
        query["in_app_event"] = [in_app_event]

    resp = _mcp_post(
        token,
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "fetch_aggregated_data", "arguments": {"query": query}},
        },
    )
    text = _extract_text_result(resp)
    if not text or "validation errors" in text.lower():
        return [], {}
    csv_text, metadata = _split_data_and_metadata(text)
    return _rows_from_csv(csv_text), metadata


def _rows_to_source_dicts(
    rows: list[dict[str, str]],
    metadata: dict[str, Any],
    *,
    date_from: str,
    date_to: str,
    kpi_name: str,
    in_app_event: str | None,
    period: str = "",
) -> list[dict[str, Any]]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    tz_name = metadata.get("timezone")
    currency = metadata.get("currency")
    out: list[dict[str, Any]] = []
    for row in rows:
        metric_col, metric_val, installs, cost = _identify_columns(row)
        mapped = {dst: row.get(src) for src, dst in DIMENSION_ALIASES.items()}
        fact_date = _normalize_fact_date(mapped.get("fact_date"), date_from, date_to)
        out.append({
            "fetch_window_from": date_from,
            "fetch_window_to": date_to,
            "fact_date": fact_date,
            "media_source": mapped.get("media_source"),
            "campaign": mapped.get("campaign"),
            "adset": mapped.get("adset"),
            "ad": mapped.get("ad"),
            "kpi_name": kpi_name,
            "metric_column": metric_col,
            "metric_value": metric_val,
            "installs": installs,
            "cost": cost,
            "in_app_event": in_app_event,
            "period": period,
            "timezone": tz_name,
            "currency": currency,
            "fetched_at": fetched_at,
            "raw_row_json": json.dumps(row, ensure_ascii=False),
            "raw_metadata_json": json.dumps(metadata, ensure_ascii=False),
        })
    return out


# ── Fetch one UTC calendar day at ad grain ───────────────────────────────

def fetch_day_ad_level(
    token: str,
    app_id: str,
    la_day: str,
    *,
    row_count: int = 300,
) -> list[dict[str, Any]]:
    """
    9 MCP calls for a single LA day at [Date, Media source, Campaign, Ad]:
      1.   Performance (Cost + Installs)
      2-5. Per-event Unique users (LTV, default period)
      6-9. Per-event Unique users (Activity, period='activity')
    """
    _initialize_session(token)
    all_src: list[dict[str, Any]] = []

    perf_metrics = [{"metric_name": "Cost"}, {"metric_name": "Installs"}]
    ltv_event_metrics = [
        {"metric_name": "Unique users"},
        {"metric_name": "Cost"},
        {"metric_name": "Installs"},
    ]
    activity_event_metrics = [
        {"metric_name": "Unique users", "period": "activity"},
        {"metric_name": "Cost"},
        {"metric_name": "Installs"},
    ]

    rows, md = _fetch_one(
        token, app_id, la_day, la_day,
        groupings=AD_GROUPINGS, metrics=perf_metrics,
        sort_metric="Cost", row_count=row_count,
    )
    all_src.extend(_rows_to_source_dicts(
        rows, md, date_from=la_day, date_to=la_day,
        kpi_name="ad_performance", in_app_event=None,
    ))

    for event in EVENT_NAMES:
        rows, md = _fetch_one(
            token, app_id, la_day, la_day,
            groupings=AD_GROUPINGS, metrics=ltv_event_metrics,
            in_app_event=event, sort_metric="Unique users",
            row_count=row_count,
        )
        all_src.extend(_rows_to_source_dicts(
            rows, md, date_from=la_day, date_to=la_day,
            kpi_name=f"ad_{event}_unique_users", in_app_event=event,
        ))

    for event in EVENT_NAMES:
        rows, md = _fetch_one(
            token, app_id, la_day, la_day,
            groupings=AD_GROUPINGS, metrics=activity_event_metrics,
            in_app_event=event, sort_metric="Unique users",
            row_count=row_count,
        )
        all_src.extend(_rows_to_source_dicts(
            rows, md, date_from=la_day, date_to=la_day,
            kpi_name=f"ad_{event}_unique_users_activity", in_app_event=event,
            period="activity",
        ))

    return all_src


def fetch_adset_ad_map(
    token: str,
    app_id: str,
    date_from: str,
    date_to: str,
    *,
    row_count: int = 300,
) -> list[dict[str, Any]]:
    """Fetch adset↔ad hierarchy (uses all 4 groupings: MS, Campaign, Adset, Ad)."""
    _initialize_session(token)
    rows, md = _fetch_one(
        token, app_id, date_from, date_to,
        groupings=MAPPING_GROUPINGS,
        metrics=[{"metric_name": "Installs"}],
        sort_metric="Installs", row_count=row_count,
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    for row in rows:
        mapped = {dst: row.get(src) for src, dst in DIMENSION_ALIASES.items()}
        _, _, installs, _ = _identify_columns(row)
        out.append({
            "media_source": mapped.get("media_source") or "",
            "campaign": mapped.get("campaign") or "",
            "adset": mapped.get("adset") or "",
            "ad": mapped.get("ad") or "",
            "installs": installs,
            "fetched_at": fetched_at,
        })
    return out


# ── Rebuild silver: marketing_fact_ad ────────────────────────────────────

def rebuild_marketing_fact_ad(conn: sqlite3.Connection, date_from: str, date_to: str) -> int:
    """Rebuild silver marketing_fact_ad from bronze source rows.

    Performance (installs/cost) is shared across both attribution models.
    Event kpi_names ending in '_activity' map to attribution_model='activity';
    all others map to 'ltv'.
    """
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                fact_date, media_source, campaign, ad,
                kpi_name, metric_value, installs, cost,
                currency, timezone, fetched_at,
                ROW_NUMBER() OVER (
                    PARTITION BY fact_date, media_source, campaign, ad, kpi_name
                    ORDER BY (julianday(fetch_window_to) - julianday(fetch_window_from)) ASC,
                             fetched_at DESC
                ) AS rn
            FROM appsflyer_mcp_source_rows
            WHERE fact_date BETWEEN ? AND ?
              AND kpi_name LIKE 'ad\\_%' ESCAPE '\\'
        )
        SELECT * FROM ranked WHERE rn = 1
        """,
        (date_from, date_to),
    ).fetchall()

    EVENT_MAP = {
        "ad_af_start_trial_unique_users": "af_start_trial",
        "ad_af_subscribe_unique_users": "af_subscribe",
        "ad_af_tutorial_completion_unique_users": "af_tutorial_completion",
        "ad_rc_trial_converted_event_unique_users": "rc_trial_converted_event",
    }

    perf: dict[tuple, dict[str, Any]] = {}
    merged: dict[tuple, dict[str, Any]] = defaultdict(lambda: {
        "installs": 0.0, "spend": 0.0,
        "af_start_trial": 0.0, "af_subscribe": 0.0,
        "af_tutorial_completion": 0.0, "rc_trial_converted_event": 0.0,
        "currency": None, "timezone": None, "fetched_at": None,
    })

    for row in rows:
        fd = row["fact_date"]
        if fd < date_from or fd > date_to:
            continue
        kpi = row["kpi_name"]

        if kpi == "ad_performance":
            pk = (fd, row["media_source"], row["campaign"], row["ad"])
            perf[pk] = {
                "installs": float(row["installs"] or 0),
                "spend": float(row["cost"] or 0),
                "currency": row["currency"],
            }
            continue

        if kpi.endswith("_activity"):
            attr_model = "activity"
            base_kpi = kpi[:-9]
        else:
            attr_model = "ltv"
            base_kpi = kpi

        key = (fd, row["media_source"], row["campaign"], row["ad"], attr_model)
        acc = merged[key]
        if base_kpi in EVENT_MAP:
            acc[EVENT_MAP[base_kpi]] += float(row["metric_value"] or 0)
        acc["timezone"] = common.business_timezone_name()
        fa = row["fetched_at"]
        if fa and (acc["fetched_at"] is None or fa > acc["fetched_at"]):
            acc["fetched_at"] = fa

    for (fd, ms, camp, ad, _attr), acc in merged.items():
        p = perf.get((fd, ms, camp, ad), {})
        acc["installs"] = p.get("installs", acc["installs"])
        acc["spend"] = p.get("spend", acc["spend"])
        if not acc["currency"]:
            acc["currency"] = p.get("currency")

    adset_map: dict[tuple, str] = {}
    try:
        for r in conn.execute("SELECT media_source, campaign, ad, adset FROM mcp_adset_ad_map"):
            adset_map[(r["media_source"], r["campaign"], r["ad"])] = r["adset"]
    except sqlite3.OperationalError:
        pass

    cur = conn.cursor()
    cur.execute(
        "DELETE FROM marketing_fact_ad WHERE fact_date BETWEEN ? AND ?",
        (date_from, date_to),
    )
    n = 0
    for (fd, ms, camp, ad, attr_model), acc in merged.items():
        adset = adset_map.get((ms, camp, ad), "")
        cur.execute(
            """INSERT INTO marketing_fact_ad
               (fact_date, source_system, media_source, campaign, adset, ad,
                attribution_model, installs, spend, af_start_trial, af_subscribe,
                af_tutorial_completion, rc_trial_converted_event,
                currency, timezone, fetched_at)
               VALUES (?, 'appsflyer_mcp_ad', ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(fact_date, media_source, campaign, ad, attribution_model) DO UPDATE SET
                 adset=excluded.adset, installs=excluded.installs, spend=excluded.spend,
                 af_start_trial=excluded.af_start_trial, af_subscribe=excluded.af_subscribe,
                 af_tutorial_completion=excluded.af_tutorial_completion,
                 rc_trial_converted_event=excluded.rc_trial_converted_event,
                 currency=excluded.currency, timezone=excluded.timezone,
                 fetched_at=excluded.fetched_at""",
            (fd, ms, camp, adset, ad, attr_model,
             acc["installs"], acc["spend"],
             acc["af_start_trial"], acc["af_subscribe"],
             acc["af_tutorial_completion"], acc["rc_trial_converted_event"],
             acc["currency"], acc["timezone"],
             acc["fetched_at"] or datetime.now(timezone.utc).isoformat()),
        )
        n += 1
    conn.commit()
    return n


# ── Entry point ──────────────────────────────────────────────────────────

def run(
    date_from: str,
    date_to: str,
    *,
    db_path: Path,
    row_count: int = 300,
    dry_run: bool = False,
) -> dict[str, int]:
    load_dotenv(common.project_root() / ".env")
    app_id = os.environ.get("APPSFLYER_APP_ID", "").strip()
    mcp_token = os.environ.get("APPSFLYER_MCP_TOKEN", "").strip()
    if not app_id or not mcp_token:
        raise RuntimeError("Missing APPSFLYER_APP_ID or APPSFLYER_MCP_TOKEN")

    d_from = datetime.strptime(date_from, "%Y-%m-%d").date()
    d_to = datetime.strptime(date_to, "%Y-%m-%d").date()

    if dry_run:
        days = (d_to - d_from).days + 1
        calls = days * 5 + 1
        print(f"[dry-run] {days} day(s), ~{calls} MCP calls (ad-level), row_count={row_count}")
        return {"source_rows": 0, "ad_facts": 0}

    conn = connect_db(db_path)
    try:
        init_granular_schema(conn)
        cur = conn.cursor()
        total_src = 0

        day = d_from
        while day <= d_to:
            api_day = day.isoformat()
            src_rows = fetch_day_ad_level(mcp_token, app_id, api_day, row_count=row_count)
            for row in src_rows:
                cur.execute(UPSERT_SOURCE, row)
                total_src += 1
            conn.commit()
            print(f"  LA {day.isoformat()}: {len(src_rows)} ad source rows", file=sys.stderr)
            day += timedelta(days=1)

        map_rows = fetch_adset_ad_map(
            mcp_token, app_id, date_from,
            date_to,
            row_count=row_count,
        )
        for mr in map_rows:
            cur.execute(
                """INSERT INTO mcp_adset_ad_map
                   (media_source, campaign, adset, ad, installs, fetched_at)
                   VALUES (:media_source, :campaign, :adset, :ad, :installs, :fetched_at)
                   ON CONFLICT(media_source, campaign, ad) DO UPDATE SET
                     adset=excluded.adset, installs=excluded.installs,
                     fetched_at=excluded.fetched_at""",
                mr,
            )
        conn.commit()
        print(f"  Adset↔Ad map: {len(map_rows)} rows", file=sys.stderr)

        ad_facts = rebuild_marketing_fact_ad(conn, date_from, date_to)
        return {"source_rows": total_src, "ad_facts": ad_facts}
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Ad-level MCP fetch, one day at a time.")
    p.add_argument("--from", dest="date_from", required=True,
                   type=lambda s: common.parse_iso_date_arg("--from", s))
    p.add_argument("--to", dest="date_to", required=True,
                   type=lambda s: common.parse_iso_date_arg("--to", s))
    p.add_argument("--row-count", type=int, default=300)
    p.add_argument("--db", default=os.environ.get("APPSFLYER_SQLITE_PATH", str(DEFAULT_DB_PATH)))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)
    db_path = Path(args.db).expanduser().resolve()
    result = run(args.date_from, args.date_to, db_path=db_path,
                 row_count=args.row_count, dry_run=args.dry_run)
    if not args.dry_run:
        print(f"Ad-level: {result['source_rows']} source rows, {result['ad_facts']} ad fact rows")


if __name__ == "__main__":
    main()
