from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
paths = [
    ROOT / 'data' / 'appsflyer' / 'raw' / 'aggregated_performance',
    ROOT / 'data' / 'appsflyer' / 'raw' / 'in_app_events',
    ROOT / 'data' / 'appsflyer' / 'raw' / 'cohort_revenue',
    ROOT / 'data' / 'warehouse',
]
for p in paths:
    p.mkdir(parents=True, exist_ok=True)
    print(f'Ensured {p}')
