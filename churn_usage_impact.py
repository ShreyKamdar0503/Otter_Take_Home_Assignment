
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import timedelta

BASE = Path(__file__).resolve().parents[0].parent
OUT  = BASE / "churn_usage_project" / "data_work"
OUT.mkdir(parents=True, exist_ok=True)

CHURN_CSV    = BASE / "oa_churn_requests_sample.csv"
ACCOUNTS_CSV = BASE / "oa_account_dimensions_sample.csv"
ACTIVITY_CSV = BASE / "oa_product_activity_sample.csv"

churn = pd.read_csv(CHURN_CSV, parse_dates=["churn_date","created_date","saved_date","winback_date"], dayfirst=False)
acts  = pd.read_csv(ACTIVITY_CSV, parse_dates=["date"], dayfirst=False)
try:
    accts = pd.read_csv(ACCOUNTS_CSV, parse_dates=["created_date"], dayfirst=False)
except Exception:
    accts = pd.DataFrame()

if "account_id" not in churn.columns:
    raise ValueError("churn CSV missing account_id")
if "churn_date" not in churn.columns:
    if "request_date" in churn.columns:
        churn["churn_date"] = pd.to_datetime(churn["request_date"])
    else:
        raise ValueError("churn CSV missing churn_date/request_date")
for col in ("account_id","date"):
    if col not in acts.columns:
        raise ValueError(f"activity CSV missing required column: {col}")

usage_cols = [c for c in acts.columns if c.endswith("_usage")]
access_cols = [c for c in acts.columns if c.endswith("_access")]
for c in usage_cols + access_cols:
    if c in acts.columns:
        acts[c] = pd.to_numeric(acts[c], errors="coerce").fillna(0).astype(int)

grp = acts.groupby(["account_id","date"], as_index=False)
daily = grp[usage_cols].sum() if usage_cols else acts[["account_id","date"]].copy()
if usage_cols:
    daily["events_all"] = daily[usage_cols].sum(axis=1)
    daily["categories_used"] = (daily[usage_cols] > 0).sum(axis=1)
else:
    daily["events_all"] = 0
    daily["categories_used"] = 0

def window_agg(df, ref_date, days):
    start = ref_date - timedelta(days=days)
    w = df.loc[(df["date"] >= start) & (df["date"] < ref_date)]
    return pd.Series({
        f"events_{days}d": w["events_all"].sum(),
        f"active_days_{days}d": w["date"].nunique(),
        f"breadth_max_{days}d": w["categories_used"].max() if not w.empty else 0
    })

def build_churn_features(churn_row):
    aid = churn_row["account_id"]
    ref = churn_row["churn_date"]
    d = daily.loc[daily["account_id"] == aid]
    last_dt = d.loc[d["date"] < ref, "date"].max()
    out = {"account_id": aid, "churn_dt": ref, "last_active_dt": last_dt}
    for win in (7,30,60):
        out.update(window_agg(d, ref, win))
    out["days_since_last_use"] = (ref - last_dt).days if pd.notnull(last_dt) else np.nan
    return pd.Series(out)

churn_feats = churn.apply(build_churn_features, axis=1)

all_accounts = set(acts["account_id"].unique().tolist())
churned_accounts = set(churn["account_id"].unique().tolist())
retained_accounts = sorted(list(all_accounts - churned_accounts))
global_max_date = acts["date"].max()
ret = daily[daily["account_id"].isin(retained_accounts)]

def build_retained_features(group):
    aid = group.name
    ref = global_max_date + pd.Timedelta(days=1)
    last_dt = group["date"].max()
    out = {"account_id": aid, "churn_dt": global_max_date, "last_active_dt": last_dt}
    for win in (7,30,60):
        out.update(window_agg(group, ref, win))
    out["days_since_last_use"] = (ref - last_dt).days if pd.notnull(last_dt) else np.nan
    return pd.Series(out)

retained_feats = ret.groupby("account_id").apply(build_retained_features).reset_index(drop=True)
churn_feats["churned"] = 1
retained_feats["churned"] = 0
df_all = pd.concat([churn_feats, retained_feats], ignore_index=True, sort=False)

if not accts.empty:
    accts = accts.rename(columns={
        "customer_segment":"segment",
        "channel_category":"market",
        "csm_status":"onboarding_type"
    })
    keep_cols = ["account_id","segment","market","onboarding_type","created_date"]
    keep_cols = [c for c in keep_cols if c in accts.columns]
    accts_small = accts[keep_cols].drop_duplicates("account_id", keep="last")
    df_all = df_all.merge(accts_small, on="account_id", how="left")

feature_cols = [c for c in df_all.columns if any(x in c for x in ["events_","active_days_","breadth_max_","days_since_last_use"])]
mean_cmp = df_all.groupby("churned")[feature_cols].mean().T
mean_cmp.columns = ["retained_mean" if c==0 else "churned_mean" for c in mean_cmp.columns]
mean_cmp["delta_churned_minus_retained"] = mean_cmp["churned_mean"] - mean_cmp["retained_mean"]
mean_cmp = mean_cmp.sort_values("delta_churned_minus_retained")
mean_cmp.to_csv(OUT / "feature_means_churn_vs_retained.csv")

tmp = df_all.copy()
tmp["breadth_30d_bucket"] = pd.cut(tmp["breadth_max_30d"], bins=[-0.1,0,1,2,3,10], labels=["0","1","2","3","4+"])
tmp.groupby("breadth_30d_bucket")["churned"].mean().reset_index().rename(columns={"churned":"churn_rate"}).to_csv(OUT / "churn_rate_by_breadth_30d.csv", index=False)

tmp["no_use_14d"] = (tmp["days_since_last_use"] >= 14).astype(int)
tmp.groupby("no_use_14d")["churned"].mean().reset_index().rename(columns={"churned":"churn_rate"}).to_csv(OUT / "churn_rate_by_no_use_14d.csv", index=False)

if "segment" in df_all.columns:
    df_all.groupby("segment")["churned"].mean().reset_index().rename(columns={"churned":"churn_rate"}).to_csv(OUT / "churn_rate_by_segment.csv", index=False)
if "market" in df_all.columns:
    df_all.groupby("market")["churned"].mean().reset_index().rename(columns={"churned":"churn_rate"}).to_csv(OUT / "churn_rate_by_market.csv", index=False)
if "onboarding_type" in df_all.columns:
    df_all.groupby("onboarding_type")["churned"].mean().reset_index().rename(columns={"churned":"churn_rate"}).to_csv(OUT / "churn_rate_by_onboarding_type.csv", index=False)

model_cols = ["account_id","churned","churn_dt","last_active_dt"] + feature_cols
for c in ["segment","market","onboarding_type"]:
    if c in df_all.columns: model_cols.append(c)
model_df = df_all[model_cols]
model_df.to_csv(OUT / "modeling_dataset.csv", index=False)

print("WROTE:")
for p in sorted(OUT.glob("*.csv")):
    print("-", p.name)
print("\\nHEAD(modeling_dataset):")
print(model_df.head(10))
