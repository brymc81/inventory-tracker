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
    return pd.read_csv(io.StringIO(r.text))

def tidy(df, short_name):
    # 10K CSVs use PeriodBegin for the month’s first day
    df["date"] = pd.to_datetime(df["PeriodBegin"])
    # first numeric column is always the value
    val_col = df.select_dtypes("number").columns[0]
    return df[["date", val_col]].rename(columns={val_col: short_name})

def main():
    frames = []
    for ds in load_catalog():
        print("→", ds["short_name"])
        csv_df = fetch_csv(ds["csv_url"])
        frames.append(tidy(csv_df, ds["short_name"]))

    wide = (
        pd.concat(frames, axis=1)
          .groupby("date", as_index=False).first()
          .set_index("date")
          .asfreq("D")            # daily index
          .ffill()
    )

    # rolling 7-day New Listings
    nl_cols = [c for c in wide.columns if c.startswith("newlistings_")]
    wide[[f"{c}_7d" for c in nl_cols]] = wide[nl_cols].rolling(7).sum()

    OUTPUT.parent.mkdir(exist_ok=True)
    wide.reset_index().to_json(OUTPUT, orient="records", date_format="iso")
    print("Wrote", OUTPUT)

if __name__ == "__main__":
    sys.exit(main())