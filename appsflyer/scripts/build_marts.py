from pathlib import Path
import duckdb

# Experimental DuckDB warehouse (not the SQLite SoT used by agents). See docs/DATA_LAYERS.md.
_APPSFLYER_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = _APPSFLYER_ROOT / 'data' / 'warehouse' / 'dojo_marketing.duckdb'
SQL_DIR = _APPSFLYER_ROOT / 'sql'

conn = duckdb.connect(str(DB_PATH))
for name in [
    '20_staging_views.sql',
    '30_intermediate_models.sql',
    '40_marts.sql',
]:
    conn.execute((SQL_DIR / name).read_text())

print('Built staging, intermediate models, and marts.')
