from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[3]
DB_PATH = ROOT / 'data' / 'warehouse' / 'dojo_marketing.duckdb'
SQL_DIR = ROOT / 'dojo-agents' / 'appsflyer' / 'sql'

DB_PATH.parent.mkdir(parents=True, exist_ok=True)

conn = duckdb.connect(str(DB_PATH))
for name in [
    '00_init_schemas.sql',
    '10_raw_tables.sql',
    '20_staging_views.sql',
    '30_intermediate_models.sql',
    '40_marts.sql',
]:
    conn.execute((SQL_DIR / name).read_text())

print(f'Initialized DuckDB warehouse at {DB_PATH}')
