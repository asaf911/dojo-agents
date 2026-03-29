#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SQLITE_PATH = ROOT / 'appsflyer' / 'data' / 'appsflyer.db'
EXPORT_PATH = ROOT / 'appsflyer' / 'data' / 'warehouse_exports' / 'marketing_fact_daily.json'
SUMMARY_PATH = ROOT / 'appsflyer' / 'data' / 'warehouse_exports' / 'parity_summary.json'

KEYS = ['fact_date', 'source_system', 'media_source', 'campaign', 'adset', 'ad']
METRICS = ['installs', 'spend', 'af_start_trial', 'af_subscribe', 'af_tutorial_completion', 'rc_trial_converted_event', 'arpu_ltv']


def norm(v):
    return None if v in ('None', '') else v


def key_of(row: dict):
    return tuple(norm(row.get(k)) for k in KEYS)


def metric_tuple(row: dict):
    return tuple(row.get(m) for m in METRICS + ['currency', 'timezone'])


def main() -> None:
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    sqlite_rows = [dict(r) for r in conn.execute('select fact_date, source_system, media_source, campaign, adset, ad, installs, spend, af_start_trial, af_subscribe, af_tutorial_completion, rc_trial_converted_event, arpu_ltv, currency, timezone from marketing_fact_daily')]
    conn.close()

    exported = json.loads(EXPORT_PATH.read_text())['rows']

    sqlite_map = {key_of(r): metric_tuple(r) for r in sqlite_rows}
    export_map = {key_of(r): metric_tuple(r) for r in exported}

    sqlite_keys = set(sqlite_map)
    export_keys = set(export_map)
    sort_key = lambda t: tuple('' if v is None else str(v) for v in t)
    only_sqlite = sorted(sqlite_keys - export_keys, key=sort_key)
    only_export = sorted(export_keys - sqlite_keys, key=sort_key)

    mismatches = []
    for k in sorted(sqlite_keys & export_keys, key=sort_key):
        if sqlite_map[k] != export_map[k]:
            mismatches.append({
                'key': k,
                'sqlite': sqlite_map[k],
                'export': export_map[k],
            })
            if len(mismatches) >= 20:
                break

    summary = {
        'sqlite_row_count': len(sqlite_rows),
        'export_row_count': len(exported),
        'sqlite_unique_keys': len(sqlite_keys),
        'export_unique_keys': len(export_keys),
        'keys_only_in_sqlite': len(only_sqlite),
        'keys_only_in_export': len(only_export),
        'metric_mismatch_sample_count': len(mismatches),
        'parity_ok': len(only_sqlite) == 0 and len(only_export) == 0 and len(mismatches) == 0,
        'sample_only_in_sqlite': only_sqlite[:10],
        'sample_only_in_export': only_export[:10],
        'sample_metric_mismatches': mismatches,
    }
    SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
