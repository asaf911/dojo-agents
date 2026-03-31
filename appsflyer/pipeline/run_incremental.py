#!/usr/bin/env python3
"""
Incremental AppsFlyer pipeline: Pull API + MCP fetch, refresh SQLite views, write manifest.
Date window defaults to the last N calendar days in America/Los_Angeles (inclusive).
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_APPSFLYER_DIR = Path(__file__).resolve().parent.parent

from dotenv import load_dotenv

from pipeline.apply_sqlite_views import apply_views

LA = ZoneInfo("America/Los_Angeles")
DEFAULT_DB = _APPSFLYER_DIR / "data" / "appsflyer.db"
ARTIFACTS_DIR = _APPSFLYER_DIR / "data" / "artifacts"
MANIFEST_PATH = ARTIFACTS_DIR / "run_manifest.json"


def la_date_window(lookback_days: int) -> tuple[str, str]:
    if lookback_days < 1:
        raise ValueError("lookback_days must be >= 1")
    today_la = datetime.now(LA).date()
    start = today_la - timedelta(days=lookback_days - 1)
    return start.isoformat(), today_la.isoformat()


def git_short_sha(cwd: Path) -> str | None:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if r.returncode == 0:
            return r.stdout.strip() or None
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


def run_step(
    label: str,
    cmd: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
) -> dict:
    started = datetime.now(tz=LA).isoformat()
    proc = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
    out = (proc.stdout or "") + (proc.stderr or "")
    tail = out[-8000:] if len(out) > 8000 else out
    return {
        "step": label,
        "cmd": cmd,
        "exit_code": proc.returncode,
        "started_at": started,
        "log_tail": tail,
    }


def _pull_looks_rate_limited(log_tail: str) -> bool:
    t = (log_tail or "").lower()
    return (
        "limit reached" in t
        or "403 forbidden" in t
        or "429" in t
        or "too many requests" in t
    )


def run_pull_with_retry(
    cmd: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
) -> dict:
    """Retry Pull API on AppsFlyer rate / quota errors (common for partners_by_date_report)."""
    max_attempts = max(1, int(os.environ.get("APPSFLYER_PULL_MAX_RETRIES", "3")))
    sleep_sec = max(0, int(os.environ.get("APPSFLYER_PULL_RETRY_SLEEP_SEC", "90")))
    last: dict = {}
    for attempt in range(1, max_attempts + 1):
        last = run_step("fetch_appsflyer_pull_truth", cmd, cwd=cwd, env=env)
        last["attempt"] = attempt
        last["max_attempts"] = max_attempts
        if last["exit_code"] == 0:
            return last
        if attempt < max_attempts and _pull_looks_rate_limited(last.get("log_tail", "")):
            time.sleep(sleep_sec)
            continue
        return last
    return last


def query_freshness(db_path: Path) -> dict:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(fact_date) FROM growth_daily_totals_la")
        max_date = cur.fetchone()[0]
        yesterday = (datetime.now(LA).date() - timedelta(days=1)).isoformat()
        cur.execute(
            "SELECT COUNT(*) FROM growth_daily_totals_la WHERE fact_date = ?",
            (yesterday,),
        )
        yesterday_rows = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM growth_daily_totals_la")
        total_rows = cur.fetchone()[0]
        return {
            "max_fact_date_growth_daily_totals_la": max_date,
            "yesterday_la": yesterday,
            "yesterday_row_count": yesterday_rows,
            "growth_daily_totals_la_row_count": total_rows,
            "yesterday_present": yesterday_rows > 0,
        }
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run incremental AppsFlyer SQLite pipeline.")
    p.add_argument(
        "--lookback-days",
        type=int,
        default=int(os.environ.get("APPSFLYER_SYNC_LOOKBACK_DAYS", "3")),
        help="LA calendar days to sync (inclusive, ending today LA)",
    )
    p.add_argument(
        "--db",
        default=os.environ.get("APPSFLYER_SQLITE_PATH", str(DEFAULT_DB)),
    )
    p.add_argument(
        "--row-count",
        type=int,
        default=300,
        help="MCP --row-count (max 300)",
    )
    p.add_argument(
        "--skip-mcp",
        action="store_true",
        help="Only pull + views (no MCP fetch)",
    )
    p.add_argument(
        "--skip-pull",
        action="store_true",
        help="Only MCP + views (no Pull API)",
    )
    p.add_argument(
        "--validate",
        action="store_true",
        help="Run scripts/validate_export_parity.py after pipeline",
    )
    args = p.parse_args(argv)

    load_dotenv(_APPSFLYER_DIR / ".env")
    db_path = Path(args.db).expanduser().resolve()
    date_from, date_to = la_date_window(args.lookback_days)

    env = {**os.environ, "APPSFLYER_SQLITE_PATH": str(db_path)}
    py = sys.executable

    steps: list[dict] = []
    overall_ok = True

    if not args.skip_pull:
        cmd = [
            py,
            str(_APPSFLYER_DIR / "fetcher" / "fetch_appsflyer_pull_truth.py"),
            "--from",
            date_from,
            "--to",
            date_to,
            "--db",
            str(db_path),
        ]
        info = run_pull_with_retry(cmd, cwd=_APPSFLYER_DIR, env=env)
        steps.append(info)
        if info["exit_code"] != 0:
            overall_ok = False

    if not args.skip_mcp:
        if args.row_count < 1 or args.row_count > 300:
            print("--row-count must be 1..300", file=sys.stderr)
            return 2
        cmd = [
            py,
            str(_APPSFLYER_DIR / "fetcher" / "fetch_appsflyer_mcp.py"),
            "--from",
            date_from,
            "--to",
            date_to,
            "--db",
            str(db_path),
            "--row-count",
            str(args.row_count),
        ]
        info = run_step("fetch_appsflyer_mcp", cmd, cwd=_APPSFLYER_DIR, env=env)
        steps.append(info)
        if info["exit_code"] != 0:
            overall_ok = False

    fetch_steps = [s for s in steps if str(s.get("step", "")).startswith("fetch_")]
    fetch_all_ok = all(s.get("exit_code") == 0 for s in fetch_steps)
    any_fetch_ok = any(s.get("exit_code") == 0 for s in fetch_steps)
    views_applied = False
    if fetch_steps and any_fetch_ok:
        try:
            apply_views(db_path)
            views_applied = True
            steps.append(
                {
                    "step": "apply_sqlite_views",
                    "exit_code": 0,
                    "files": [
                        "50_growth_fact_daily.sql",
                        "51_growth_daily_totals_la.sql",
                        "52_growth_breakdowns_experimental.sql",
                        "53_growth_weekly_totals_la.sql",
                    ],
                }
            )
        except Exception as exc:
            steps.append(
                {
                    "step": "apply_sqlite_views",
                    "exit_code": 1,
                    "error": str(exc),
                }
            )
            overall_ok = False
    elif not fetch_steps:
        steps.append(
            {
                "step": "apply_sqlite_views",
                "skipped": True,
                "reason": "both --skip-pull and --skip-mcp",
            }
        )
    else:
        steps.append(
            {
                "step": "apply_sqlite_views",
                "skipped": True,
                "reason": "all fetch steps failed",
            }
        )
        overall_ok = False

    if fetch_steps and not fetch_all_ok:
        overall_ok = False

    freshness: dict = {}
    try:
        freshness = query_freshness(db_path)
    except Exception as exc:
        freshness = {"error": str(exc)}

    if not fetch_steps:
        overall_ok = False

    validate_result: dict | None = None
    if args.validate and overall_ok:
        vcmd = [py, str(_APPSFLYER_DIR / "scripts" / "validate_export_parity.py")]
        validate_result = run_step("validate_export_parity", vcmd, cwd=_APPSFLYER_DIR, env=env)
        if validate_result["exit_code"] != 0:
            overall_ok = False

    manifest = {
        "version": 1,
        "finished_at": datetime.now(tz=LA).isoformat(),
        "business_timezone": "America/Los_Angeles",
        "date_from": date_from,
        "date_to": date_to,
        "lookback_days": args.lookback_days,
        "db_path": str(db_path),
        "overall_ok": overall_ok,
        "fetch_all_ok": fetch_all_ok if fetch_steps else None,
        "views_applied": views_applied,
        "git_sha": git_short_sha(_APPSFLYER_DIR.parent),
        "steps": steps,
        "freshness": freshness,
    }
    if validate_result is not None:
        manifest["validate_export_parity"] = validate_result

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
