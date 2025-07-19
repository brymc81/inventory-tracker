#!/usr/bin/env python3
"""
Pull all Market Monitor CSV URLs from csv_catalog.json,
normalise to a daily wide table, and write docs/inventory.json
ready for Chart.js.
"""

import io
import json
import sys
from pathlib import Path

import pandas as pd
import requests

# --------------------------------------------------------------------------- #
#  Paths
# --------------------------------------------------------------------------- #

ROOT    = Path(__file__).resolve().parent        # project root
CATALOG = ROOT / "csv_catalog.json"              # input mapping
OUTPUT  = ROOT / "docs" / "inventory.json"       # build target

# --------------------------------------------------------------------------- #
#  Helpers
# --------------------------------------------------------------------------- #

def load_catalog() -> list[dict]:
    with open(CATALOG, encoding="utf-8") as fh:
        return json.load(fh)["datasets"]

def fetch_csv(url: str) -> pd.DataFrame:
    """Download a Market Monitor CSV and return its first two columns."""
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    return pd.read_csv(
        io.StringIO(r.text),
        engine="python",    # tolerant tokenizer
        usecols=[0, 1],     # date col + first numeric col
        thousands=",",      # strip thousands separators
        on_bad_lines="skip" # silently drop malformed rows
    )

def tidy(df: pd.DataFrame, short_name: str) -> pd.DataFrame:
    """Coerce, clean, and return a single-series frame indexed by date."""
    df = df.copy()

    # 1️⃣ parse whatever sits in column 0 as the date
    df["date"] = pd.to_datetime(df.iloc[:, 0],
                                format="%Y-%m-%d",
                                errors="coerce")

    # 2️⃣ force column 1 numeric; bad cells → NaN
    val_col = df.columns[1]
    df[val_col] = pd.to_numeric(
        df[val_col].astype(str).str.replace(",", ""),
        errors="coerce"
    )

    # 3️⃣ drop rows missing either field
    df = df.dropna(subset=["date", val_col])

    # 4️⃣ return one column, indexed by date
    return (
        df.set_index("date")[[val_col]]
          .rename(columns={val_col: short_name})
          .sort_index()
    )

# --------------------------------------------------------------------------- #
#  Main ETL
# --------------------------------------------------------------------------- #

def main() -> None:
    frames = []
    for ds in load_catalog():
        print("→", ds["short_name"])
        csv_df = fetch_csv(ds["csv_url"])

        # DEBUG: peek at the first three raw date strings
        if csv_df.iloc[0, 0][:10].isalpha():      # first char isn’t a digit
            print("DEBUG", ds["short_name"], "dates look like",
                  csv_df.iloc[:3, 0].tolist())

        frames.append(tidy(csv_df, ds["short_name"]))

    # combine on the shared date index
    wide = (
        pd.concat(frames, axis=1)
          .asfreq("D")   # daily grid
          .ffill()       # forward-fill gaps
    )

    # rolling 7-day totals for New Listings
    nl_cols = [c for c in wide.columns if c.startswith("newlistings_")]
    wide[[f"{c}_7d" for c in nl_cols]] = wide[nl_cols].rolling(7).sum()

    print("Wide shape →", wide.shape)   # debug line

    # ensure docs/ exists and write the JSON
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    wide.reset_index().to_json(OUTPUT, orient="records", date_format="iso")
    print("Wrote", OUTPUT)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("ETL failed →", exc, file=sys.stderr)
        sys.exit(1)