#!/usr/bin/env python3
"""
Fetch AppsFlyer Master API reports as raw JSON or CSV for inspection.

Endpoint family (confirm path/query names in official docs — they can change):
https://dev.appsflyer.com/hc/reference/master_api_get
https://dev.appsflyer.com/hc/reference/overview-9

Master API is pivot-style: you pass groupings (dimensions) and KPIs (metrics).
Ad, ad set, and other breakdowns are only present when AppsFlyer and the ad
network support them for your app — not every grouping is available everywhere.

This script does not normalize into SQLite; use the Pull fetcher or add a mapper
after you inspect a sample response.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

_APPSFLYER_DIR = Path(__file__).resolve().parent.parent
if str(_APPSFLYER_DIR) not in sys.path:
    sys.path.insert(0, str(_APPSFLYER_DIR))
import common

# Default from Get Master Report reference; override with APPSFLYER_MASTER_BASE if needed.
DEFAULT_MASTER_BASE = os.environ.get(
    "APPSFLYER_MASTER_BASE",
    "https://hq1.appsflyer.com/api/master-agg-data/v4/app",
)

DEFAULT_RAW_DIR = common.project_root() / "data" / "master_raw"

_fmt_env = os.environ.get("APPSFLYER_MASTER_FORMAT", "json").lower()
_DEFAULT_MASTER_FORMAT = _fmt_env if _fmt_env in ("json", "csv") else "json"


def _parse_param(spec: str) -> tuple[str, str]:
    if "=" not in spec:
        raise argparse.ArgumentTypeError(f"Expected KEY=value, got {spec!r}")
    key, _, val = spec.partition("=")
    key, val = key.strip(), val.strip()
    if not key:
        raise argparse.ArgumentTypeError(f"Expected KEY=value, got {spec!r}")
    return key, val


def build_master_url(
    app_id: str,
    date_from: str,
    date_to: str,
    *,
    groupings: str | None,
    kpis: str | None,
    format_: str | None,
    extra_params: dict[str, str],
) -> str:
    """
    Build GET URL for Master Report v4.

    Query keys `groupings`, `kpis`, and `format` match common AppsFlyer examples;
    verify exact spelling and required params against the developer hub for your version.
    Later keys in ``extra_params`` override earlier ones with the same name.
    """
    base = DEFAULT_MASTER_BASE.rstrip("/")
    path = f"{base}/{app_id}"
    qp: dict[str, str] = {"from": date_from, "to": date_to}
    if groupings:
        qp["groupings"] = groupings
    if kpis:
        qp["kpis"] = kpis
    if format_:
        qp["format"] = format_
    qp.update(extra_params)
    ordered = sorted(qp.items())
    q = "&".join(f"{k}={quote(v, safe='')}" for k, v in ordered)
    return f"{path}?{q}"


def _pick_extension(format_arg: str | None, content_type: str | None) -> str:
    if format_arg:
        low = format_arg.lower()
        if low in ("json", "csv"):
            return low
    ct = (content_type or "").lower()
    if "json" in ct:
        return "json"
    if "csv" in ct or "text/plain" in ct:
        return "csv"
    return "txt"


def _maybe_pretty_json(body: str, *, enabled: bool) -> str:
    if not enabled:
        return body
    try:
        parsed = json.loads(body)
        return json.dumps(parsed, indent=2, ensure_ascii=False) + "\n"
    except json.JSONDecodeError:
        return body


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description="Download AppsFlyer Master API report (raw body) for inspection."
    )
    p.add_argument(
        "--from",
        dest="date_from",
        required=True,
        type=lambda s: common.parse_iso_date_arg("--from", s),
        help="Start date YYYY-MM-DD",
    )
    p.add_argument(
        "--to",
        dest="date_to",
        required=True,
        type=lambda s: common.parse_iso_date_arg("--to", s),
        help="End date YYYY-MM-DD (inclusive)",
    )
    p.add_argument(
        "--groupings",
        default=os.environ.get("APPSFLYER_MASTER_GROUPINGS", ""),
        help=(
            "Comma-separated dimension groupings (e.g. date,pid,c,af_adset,af_ad). "
            "Names must match AppsFlyer Master API docs; ad/adset availability is "
            "network-dependent. Leave empty only if you pass equivalent --param entries."
        ),
    )
    p.add_argument(
        "--kpis",
        default=os.environ.get("APPSFLYER_MASTER_KPIS", ""),
        help="Comma-separated KPIs (e.g. impressions,clicks,installs,cost,revenue).",
    )
    p.add_argument(
        "--format",
        dest="format_",
        choices=("json", "csv"),
        default=_DEFAULT_MASTER_FORMAT,
        help="Response format query param when supported (default: json or APPSFLYER_MASTER_FORMAT).",
    )
    p.add_argument(
        "--param",
        action="append",
        default=[],
        type=_parse_param,
        metavar="KEY=value",
        help="Extra query parameter (repeatable). Overrides or supplements built-in params.",
    )
    p.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Output file path (default: data/master_raw/master_<from>_<to>_<utc>.<ext>)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print URL only; do not call the API.",
    )
    p.add_argument(
        "--pretty",
        action="store_true",
        help="If body parses as JSON, re-write indented JSON before saving.",
    )
    args = p.parse_args(argv)

    app_id, token = common.load_config()
    extra = dict(args.param)
    groupings = args.groupings.strip() or None
    kpis = args.kpis.strip() or None
    format_ = args.format_ if args.format_ else None

    url = build_master_url(
        app_id,
        args.date_from,
        args.date_to,
        groupings=groupings,
        kpis=kpis,
        format_=format_,
        extra_params=extra,
    )

    if args.dry_run:
        print(url)
        return

    body, content_type = common.get_with_retries(
        url,
        token,
        accept="application/json, text/csv, text/plain, */*",
        timeout_sec=180.0,
        after_success=None,
    )
    body = _maybe_pretty_json(body, enabled=args.pretty)

    ext = _pick_extension(format_, content_type)
    out_dir = DEFAULT_RAW_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    default_name = f"master_{args.date_from}_{args.date_to}_{ts}.{ext}"
    out_path = args.output.expanduser().resolve() if args.output else (out_dir / default_name)
    if args.output:
        out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(body, encoding="utf-8")
    print(f"Wrote {len(body)} bytes to {out_path}")


if __name__ == "__main__":
    main()
