# notebooks/otter_churn_starter.py
import duckdb, pandas as pd
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score

BASE = Path(__file__).resolve().parents[1]
DATA_RAW = BASE / "data_raw"
DATA_WORK = BASE / "data_work"
SQL = BASE / "sql" / "otter_churn_duckdb.sql"

required = [
    DATA_RAW / "oa_churn_requests_sample.csv",
    DATA_RAW / "oa_account_dimensions_sample.csv",
    DATA_RAW / "oa_product_activity_sample.csv",
]
missing = [str(p) for p in required if not p.exists()]
if missing:
    print("Missing files:", missing)

con = duckdb.connect()
con.execute(f"SET VARIABLE v_churn='{required[0]}';")
con.execute(f"SET VARIABLE v_accts='{required[1]}';")
con.execute(f"SET VARIABLE v_act='{required[2]}';")
con.execute(open(SQL,'r').read())

print("Exports in data_work/:")
for p in sorted(DATA_WORK.glob('*.csv')):
    print('-', p.name)

df = con.execute("""
SELECT
  1 AS churned,  -- this table contains churn rows; model demo may be limited
  COALESCE(events_30d,0) AS events_30d,
  COALESCE(active_days_30d,0) AS active_days_30d,
  COALESCE(max_categories_30d,0) AS max_categories_30d,
  COALESCE(days_since_last_use, 999) AS days_since_last_use,
  COALESCE(tenure_days,0) AS tenure_days,
  COALESCE(segment,'UNK') AS segment,
  COALESCE(market,'UNK') AS market,
  COALESCE(onboarding_type,'UNK') AS onboarding_type
FROM churn_enriched
""").df()

if df.shape[0] == 0:
    print("No rows in churn_enriched; check your column names in the SQL views.")
else:
    print("Sample of churn_enriched features:")
    print(df.head(5))
