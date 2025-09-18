#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Daily CEO Articles normalizer.

Writes (at minimum) an empty-but-valid CSV so the dashboard can load:
  data_ceos/articles/YYYY-MM-DD-articles.csv

If you have a real source, add a loader in `load_source_articles(date_str)`:
- Pull from S3, a scraper output, GDELT, etc.
- Return a list of dict rows with: ceo, company, title, url, source, sentiment
"""

from __future__ import annotations
import os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

BASE = Path(__file__).parent
OUT_DIR = BASE / "data_ceos" / "articles"
OUT_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED_COLS = ["ceo", "company", "title", "url", "source", "sentiment"]

def get_target_date() -> str:
    # Use YYYY-MM-DD from env or today UTC
    d = os.environ.get("ARTICLES_DATE", "").strip()
    if d:
        try:
            datetime.strptime(d, "%Y-%m-%d")
            return d
        except ValueError:
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def load_source_articles(date_str: str) -> list[dict]:
    """
    TODO: Implement this for your real source.
    For now, returns an empty list so we at least create the file with headers.
    Examples to add later:
      - Read from S3: tk-public-data/.../{date}-ceo-articles.csv
      - Read from a local CSV in data/articles/{date}.csv
    """
    rows: list[dict] = []
    # Example (uncomment and adapt if you have a local file):
    # src = BASE / "data" / "articles" / f"{date_str}.csv"
    # if src.exists():
    #     df = pd.read_csv(src)
    #     for _, r in df.iterrows():
    #         rows.append({
    #             "ceo": r.get("ceo",""),
    #             "company": r.get("company",""),
    #             "title": r.get("title",""),
    #             "url": r.get("url",""),
    #             "source": r.get("source",""),
    #             "sentiment": str(r.get("sentiment","")).lower()
    #         })
    return rows

def normalize_rows(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=REQUIRED_COLS)
    df = pd.DataFrame(rows)
    for col in REQUIRED_COLS:
        if col not in df.columns:
            df[col] = ""
    # Basic cleanup
    for c in ["ceo", "company", "title", "url", "source", "sentiment"]:
        df[c] = df[c].astype(str).fillna("").str.strip()
    # Normalize sentiment labels
    df["sentiment"] = df["sentiment"].str.lower().map(
        lambda s: "positive" if s.startswith("pos") else ("negative" if s.startswith("neg") else "neutral")
    )
    # Keep only expected columns/order
    return df[REQUIRED_COLS]

def main():
    date_str = get_target_date()
    out_path = OUT_DIR / f"{date_str}-articles.csv"

    rows = load_source_articles(date_str)
    df = normalize_rows(rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"✔ Wrote {len(df):,} rows → {out_path}")

if __name__ == "__main__":
    main()
