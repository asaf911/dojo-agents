"""
Shared AppsFlyer helpers: env loading, strict dates, Bearer GET with retries.

Used by Pull aggregate (`fetcher/fetch_appsflyer.py`) and Master API
(`fetcher/fetch_appsflyer_master.py`).
"""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Callable
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

MAX_ATTEMPTS = 5
BACKOFF_START_SEC = 0.5

DEFAULT_BUSINESS_TIMEZONE = "America/Los_Angeles"
_UTC_ZONE = ZoneInfo("UTC")


def business_timezone_name() -> str:
    """IANA timezone for reporting (Pull API, gold `fact_date` alignment). Override via APPSFLYER_BUSINESS_TIMEZONE."""
    load_dotenv(project_root() / ".env")
    raw = os.environ.get("APPSFLYER_BUSINESS_TIMEZONE", DEFAULT_BUSINESS_TIMEZONE).strip()
    return raw or DEFAULT_BUSINESS_TIMEZONE


@lru_cache(maxsize=16)
def _zoneinfo_named(name: str) -> ZoneInfo:
    return ZoneInfo(name)


def business_zoneinfo() -> ZoneInfo:
    return _zoneinfo_named(business_timezone_name())


def utc_calendar_date_to_business_date(iso_date: str) -> str:
    """
    Map a vendor UTC calendar day (YYYY-MM-DD at 00:00:00 UTC) to the business calendar date.

    Used when AppsFlyer MCP returns UTC-dated rows: the instant midnight UTC falls on the prior
    calendar day in Los Angeles for part of the year (e.g. 2026-03-31Z -> 2026-03-30 in LA PDT).
    DST is handled by ZoneInfo (astimezone).
    """
    dt = datetime.strptime(iso_date, "%Y-%m-%d").replace(tzinfo=_UTC_ZONE)
    return dt.astimezone(business_zoneinfo()).date().isoformat()


def is_utc_like_report_tz(tz: str | None) -> bool:
    """True if MCP (or similar) metadata indicates calendar dates are UTC, not business TZ."""
    if tz is None or not str(tz).strip():
        return True
    t = str(tz).strip().upper().replace(" ", "_")
    return t in ("UTC", "GMT", "ETC/UTC", "ETC/GMT", "Z")


def project_root() -> Path:
    """Directory containing `appsflyer/.env` (parent of `fetcher/`)."""
    return Path(__file__).resolve().parent


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
    """Read APPSFLYER_APP_ID and APPSFLYER_API_TOKEN from env / appsflyer/.env."""
    load_dotenv(project_root() / ".env")
    app_id = os.environ.get("APPSFLYER_APP_ID", "").strip()
    token = os.environ.get("APPSFLYER_API_TOKEN", "").strip()
    if not app_id or not token:
        raise RuntimeError(
            "Missing APPSFLYER_APP_ID or APPSFLYER_API_TOKEN. "
            "Set them in the environment or in appsflyer/.env (not committed)."
        )
    return app_id, token


def http_error_message(status_code: int, url: str, body_preview: str) -> str:
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
            "HTTP 404 Not Found: wrong host/path/app id, or report not available. "
            f"URL (truncated): {url[:120]}… Preview: {preview!r}"
        )
    return f"HTTP {status_code} from AppsFlyer. URL (truncated): {url[:120]}… Preview: {preview!r}"


def get_with_retries(
    url: str,
    token: str,
    *,
    accept: str,
    timeout_sec: float = 120.0,
    after_success: Callable[[requests.Response, str], None] | None = None,
) -> tuple[str, str | None]:
    """
    GET with Bearer token, retrying transient failures.
    Returns (response body text, Content-Type header value).
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": accept,
    }
    for attempt in range(MAX_ATTEMPTS):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout_sec)
            text = resp.text
            ct = resp.headers.get("Content-Type")

            if resp.status_code in (429, 502, 503, 504):
                if attempt < MAX_ATTEMPTS - 1:
                    delay = BACKOFF_START_SEC * (2**attempt)
                    time.sleep(delay)
                    continue
                raise RuntimeError(
                    f"AppsFlyer returned HTTP {resp.status_code} after {MAX_ATTEMPTS} attempts. "
                    f"URL (truncated): {url[:120]}…"
                )

            if resp.status_code == 401:
                raise RuntimeError(http_error_message(401, url, text))
            if resp.status_code == 403:
                raise RuntimeError(http_error_message(403, url, text))
            if resp.status_code == 404:
                raise RuntimeError(http_error_message(404, url, text))

            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:
                raise RuntimeError(http_error_message(resp.status_code, url, text)) from exc

            if after_success is not None:
                after_success(resp, text)
            return text, ct

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt < MAX_ATTEMPTS - 1:
                delay = BACKOFF_START_SEC * (2**attempt)
                time.sleep(delay)
                continue
            raise RuntimeError(
                f"AppsFlyer request failed after {MAX_ATTEMPTS} attempts: {exc}"
            ) from exc

    raise RuntimeError("AppsFlyer request failed: exhausted retries without response")
