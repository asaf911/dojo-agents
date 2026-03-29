from __future__ import annotations
from pathlib import Path
from datetime import datetime
import hashlib
import json
import sys
import duckdb

ROOT = Path(__file__).resolve().parents[3]
DB_PATH = ROOT / 'data' / 'warehouse' / 'dojo_marketing.duckdb'
RAW_ROOT = ROOT / 'data' / 'appsflyer' / 'raw'

REPORT_TABLES = {
    'aggregated_performance': 'raw_appsflyer.aggregated_performance',
    'in_app_events': 'raw_appsflyer.in_app_events',
    'cohort_revenue': 'raw_appsflyer.cohort_revenue',
}

def stable_hash(payload: dict, report_name: str, extract_date: str) -> str:
    raw = json.dumps({'report_name': report_name, 'extract_date': extract_date, 'payload': payload}, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(raw.encode()).hexdigest()


def load_file(path: Path) -> int:
    report_name = path.parents[1].name
    if report_name not in REPORT_TABLES:
        raise ValueError(f'Unknown report path: {path}')
    dt_part = path.parent.name
    if not dt_part.startswith('dt='):
        raise ValueError(f'Expected dt=YYYY-MM-DD partition, got: {dt_part}')
    extract_date = dt_part.split('=', 1)[1]
    batch_id = hashlib.sha256(str(path).encode()).hexdigest()[:16]
    payload = json.loads(path.read_text())
    rows = payload if isinstance(payload, list) else payload.get('rows', [])
    conn = duckdb.connect(str(DB_PATH))
    inserted = 0
    for row in rows:
        conn.execute(
            f"INSERT INTO {REPORT_TABLES[report_name]} VALUES (?, CURRENT_TIMESTAMP, ?, 'appsflyer', ?, ?, ?, ?)",
            [batch_id, extract_date, report_name, json.dumps({'file_path': str(path)}), stable_hash(row, report_name, extract_date), json.dumps(row)],
        )
        inserted += 1
    conn.execute(
        "INSERT OR REPLACE INTO raw_appsflyer.extract_batches(batch_id, report_name, extract_date, extract_started_at, ingested_at, source_system, request_params_json, file_path, row_count, status) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 'appsflyer', ?, ?, ?, 'loaded')",
        [batch_id, report_name, extract_date, datetime.utcnow(), json.dumps({'file_path': str(path)}), str(path), inserted],
    )
    return inserted


def main() -> None:
    paths = [Path(p) for p in sys.argv[1:]]
    if not paths:
        paths = list(RAW_ROOT.glob('*/*/*.json'))
    total = 0
    for path in paths:
        total += load_file(path)
        print(f'Loaded {path}')
    print(f'Total rows loaded: {total}')


if __name__ == '__main__':
    main()
