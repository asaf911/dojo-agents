#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

_APPSFLYER_ROOT = Path(__file__).resolve().parents[1]
SQLITE_PATH = _APPSFLYER_ROOT / 'data' / 'appsflyer.db'
RAW_ROOT = _APPSFLYER_ROOT / 'data' / 'raw'
WAREHOUSE_ROOT = _APPSFLYER_ROOT / 'data' / 'warehouse_exports'

SOURCE_QUERY = '''
select
  fetch_window_from,
  fetch_window_to,
  fact_date,
  media_source,
  campaign,
  adset,
  ad,
  kpi_name,
  metric_column,
  metric_value,
  installs,
  cost,
  in_app_event,
  period,
  timezone,
  currency,
  fetched_at,
  raw_row_json,
  raw_metadata_json
from appsflyer_mcp_source_rows
order by fact_date, media_source, campaign, adset, ad, kpi_name
'''

FACT_QUERY = '''
select
  fact_date,
  source_system,
  media_source,
  campaign,
  adset,
  ad,
  installs,
  spend,
  af_start_trial,
  af_subscribe,
  af_tutorial_completion,
  rc_trial_converted_event,
  arpu_ltv,
  currency,
  timezone,
  fetched_at
from marketing_fact_daily
order by fact_date, media_source, campaign, adset, ad
'''


def norm(v):
    return None if v in (None, '', 'None') else v


def main() -> None:
    RAW_ROOT.mkdir(parents=True, exist_ok=True)
    WAREHOUSE_ROOT.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    raw_by_report_and_date: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in cur.execute(SOURCE_QUERY):
        d = dict(row)
        report_name = 'cohort_revenue' if d.get('period') == 'ltv' else 'in_app_events'
        fact_date = d.get('fact_date') or d.get('fetch_window_from')
        payload = json.loads(d['raw_row_json']) if d.get('raw_row_json') else {}
        wrapped = {
            '_source_system': 'appsflyer_mcp_sqlite_export',
            '_report_name': report_name,
            '_exported_at': datetime.now(timezone.utc).isoformat(),
            '_request_context': {
                'fetch_window_from': d.get('fetch_window_from'),
                'fetch_window_to': d.get('fetch_window_to'),
                'kpi_name': d.get('kpi_name'),
                'metric_column': d.get('metric_column'),
                'in_app_event': d.get('in_app_event'),
                'period': d.get('period'),
                'currency': d.get('currency'),
                'timezone': d.get('timezone'),
                'fetched_at': d.get('fetched_at'),
            },
            'row': payload,
        }
        raw_by_report_and_date[(report_name, fact_date)].append(wrapped)

    written_raw = 0
    for (report_name, fact_date), rows in raw_by_report_and_date.items():
        out_dir = RAW_ROOT / report_name / f'dt={fact_date}'
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / 'sqlite_export.json'
        out_file.write_text(json.dumps({'rows': rows}, ensure_ascii=False, indent=2))
        written_raw += 1

    facts = []
    for row in cur.execute(FACT_QUERY):
        d = {k: norm(v) if k in ('media_source', 'campaign', 'adset', 'ad', 'currency', 'timezone') else v for k, v in dict(row).items()}
        facts.append(d)

    fact_out = WAREHOUSE_ROOT / 'marketing_fact_daily.json'
    fact_out.write_text(json.dumps({'rows': facts}, ensure_ascii=False, indent=2))

    summary = {
        'exported_at': datetime.now(timezone.utc).isoformat(),
        'sqlite_path': str(SQLITE_PATH),
        'raw_snapshot_files_written': written_raw,
        'normalized_rows_written': len(facts),
        'raw_root': str(RAW_ROOT),
        'normalized_export': str(fact_out),
    }
    (WAREHOUSE_ROOT / 'export_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
