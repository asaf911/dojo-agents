"""Tests for business calendar mapping (UTC report day -> business TZ date)."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


class TestUtcCalendarDateToBusinessDate(unittest.TestCase):
    """Uses America/Los_Angeles explicitly; clears ZoneInfo cache when env is patched."""

    def setUp(self) -> None:
        import common

        common._zoneinfo_named.cache_clear()

    def test_march31_utc_to_march30_la_pdt(self) -> None:
        import common

        with patch.dict(os.environ, {"APPSFLYER_BUSINESS_TIMEZONE": "America/Los_Angeles"}):
            common._zoneinfo_named.cache_clear()
            self.assertEqual(
                common.utc_calendar_date_to_business_date("2026-03-31"),
                "2026-03-30",
            )

    def test_winter_and_summer_utc_midnight_prior_la_day(self) -> None:
        """PST vs PDT: midnight UTC lands on previous calendar day in Los Angeles."""
        import common

        with patch.dict(os.environ, {"APPSFLYER_BUSINESS_TIMEZONE": "America/Los_Angeles"}):
            common._zoneinfo_named.cache_clear()
            self.assertEqual(
                common.utc_calendar_date_to_business_date("2026-01-15"),
                "2026-01-14",
            )
            self.assertEqual(
                common.utc_calendar_date_to_business_date("2026-07-15"),
                "2026-07-14",
            )

    def test_dst_spring_utc_day_maps_consistently(self) -> None:
        """Week of US spring forward (2026-03-08): mapping remains defined (no crash)."""
        import common

        with patch.dict(os.environ, {"APPSFLYER_BUSINESS_TIMEZONE": "America/Los_Angeles"}):
            common._zoneinfo_named.cache_clear()
            out = common.utc_calendar_date_to_business_date("2026-03-09")
            self.assertRegex(out, r"^\d{4}-\d{2}-\d{2}$")
            self.assertEqual(out, "2026-03-08")


class TestIsUtcLikeReportTz(unittest.TestCase):
    def test_empty_and_utc(self) -> None:
        import common

        self.assertTrue(common.is_utc_like_report_tz(None))
        self.assertTrue(common.is_utc_like_report_tz(""))
        self.assertTrue(common.is_utc_like_report_tz("UTC"))
        self.assertTrue(common.is_utc_like_report_tz("Etc/UTC"))
        self.assertFalse(common.is_utc_like_report_tz("America/Los_Angeles"))


if __name__ == "__main__":
    unittest.main()
