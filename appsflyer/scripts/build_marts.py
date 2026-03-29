from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[3]
DB_PATH = ROOT / 'data' / 'warehouse' / 'dojo_marketing.duckdb'
SQL_DIR = ROOT / 'dojo-agents' / 'appsflyer' / 'sql'

conn = duckdb.connect(str(DB_PATH))
for name in [
    '20_staging_views.sql',
    '30_intermediate_models.sql',
    '40_marts.sql',
]:
    conn.execute((SQL_DIR / name).read_text())

print('Built staging, intermediate models, and marts.')
