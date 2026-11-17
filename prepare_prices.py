"""One-time: turn the wide Kaggle file (one column per ticker) into a long file
(one row per ticker per day). Run from this folder:  python prepare_prices.py"""
import os
import pandas as pd

SRC = "SnP_daily_update.csv"
DST = "prices_daily.csv"
FIELDS = ["Open", "High", "Low", "Close", "Volume"]

here = os.path.dirname(os.path.abspath(__file__))
src, dst = os.path.join(here, SRC), os.path.join(here, DST)

df = pd.read_csv(src, header=[0, 1], index_col=0, low_memory=False)
df = df[df.index != "Date"]                      # the third header line
df.index = pd.to_datetime(df.index).strftime("%Y-%m-%d")
df.index.name = "date"

parts = []
for f in FIELDS:
    s = df[f].apply(pd.to_numeric, errors="coerce")
    s.columns.name = "ticker"
    parts.append(s.melt(ignore_index=False, value_name=f.lower()).reset_index()
                  .set_index(["date", "ticker"]))
out = pd.concat(parts, axis=1)
out = out.dropna(subset=["close"]).reset_index().sort_values(["date", "ticker"])
out["volume"] = out["volume"].fillna(0).astype("int64")
out.to_csv(dst, index=False)

print("rows:", len(out))
print("dates:", out["date"].min(), "->", out["date"].max(), "| days:", out["date"].nunique())
print("tickers:", out["ticker"].nunique())
print("rows on last day:", (out["date"] == out["date"].max()).sum())
print(out.head(3).to_string(index=False))
