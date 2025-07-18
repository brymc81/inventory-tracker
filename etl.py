#!/usr/bin/env python3
"""
Pull all Market Monitor CSV URLs from csv_catalog.json,
normalise to a daily wide table, and write docs/inventory.json
ready for Chart.js.
"""
import json, io, sys, csv, datetime as dt
from pathlib import Path

import pandas as pd
import requests

CATALOG = Path(__file__).with_name("csv_catalog.json")
OUTPUT  = Path(__file__).with_suffix("") / "docs" / "inventory.json"

def load_catalog():
    with open(CATALOG) as fh:
        return json.load(fh)["datasets"]

def fetch_csv(url):
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    return pd.read_csv(
        io.StringIO(r.text),
        engine="python",      # tolerant tokenizer
        usecols=[0, 1],       # keep PeriodBegin + first numeric value
        thousands=",",
        on_bad_lines="skip",  # ← silently drop rows that don't match header width
    )

def tidy(df, short_name):
    df = df.copy()

    # 1️⃣ Date column = whatever sits in column 0
    df["date"] = pd.to_datetime(df.iloc[:, 0], format="%Y-%m-%d", errors="coerce")

    # 2️⃣ Force column 1 to numeric; bad cells → NaN
    val_col = df.columns[1]
    df[val_col] = pd.to_numeric(df[val_col].str.replace(",", ""), errors="coerce")

    # 3️⃣ Drop rows missing either field
    df = df.dropna(subset=["date", val_col])

    # 4️⃣ Return with date as the index
    return (
        df.set_index("date")[[val_col]]
          .rename(columns={val_col: short_name})
          .sort_index()
    )

def main():
    frames = []
    for ds in load_catalog():
        print("→", ds["short_name"])
        csv_df = fetch_csv(ds["csv_url"])
        frames.append(tidy(csv_df, ds["short_name"]))

    wide = (
    pd.concat(frames, axis=1)   # rows align on the date index
      .asfreq("D")              # daily grid
      .ffill()                  # forward-fill gaps
    )

    # rolling 7-day New Listings
    nl_cols = [c for c in wide.columns if c.startswith("newlistings_")]
    wide[[f"{c}_7d" for c in nl_cols]] = wide[nl_cols].rolling(7).sum()

    OUTPUT.parent.mkdir(exist_ok=True)
    wide.reset_index().to_json(OUTPUT, orient="records", date_format="iso")
    print("Wrote", OUTPUT)

if __name__ == "__main__":
    sys.exit(main())