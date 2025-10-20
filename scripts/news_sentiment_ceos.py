#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Daily CEO News Sentiment — counts normalizer

NOW WITH GOOGLE SHEETS INTEGRATION - writes to both CSV and Google Sheets!
"""

from __future__ import annotations
import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pandas as pd

# NEW: Import Google Sheets helper
try:
    from sheets_helper import write_ceo_articles_to_sheets
    SHEETS_HELPER_AVAILABLE = True
except ImportError:
    SHEETS_HELPER_AVAILABLE = False
    print("[INFO] sheets_helper not available - will only write CSVs")

# -------- Defaults -------- #
DEFAULT_ROSTER = "rosters/main-roster.csv"
DEFAULT_ARTICLES_DIR = "data/processed_articles"
DEFAULT_DAILY_DIR = "data/processed_articles"
DEFAULT_OUT = "data/daily_counts/ceo-articles-daily-counts-chart.csv"

# NEW: Enable/disable Google Sheets writing
WRITE_TO_SHEETS = os.environ.get('WRITE_TO_SHEETS', 'true').lower() == 'true'

# ---------------------- Helpers ---------------------------- #

def iso_today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def load_roster(path: Path) -> pd.DataFrame:
    """Load and normalize roster data."""
    if not path.exists():
        raise FileNotFoundError(f"Roster file not found: {path}")

    df = pd.read_csv(path, encoding="utf-8-sig")
    cols = {c.strip().lower(): c for c in df.columns}

    def col(*names: str) -> str:
        for name in names:
            for k, v in cols.items():
                if k == name.lower():
                    return v
        raise KeyError(f"Expected one of {names} columns in {path}")

    alias_col = col("ceo alias", "alias")
    ceo_col = col("ceo")
    company_col = col("company")

    out = df[[alias_col, ceo_col, company_col]].copy()
    out.columns = ["alias", "ceo", "company"]
    
    for c in ["alias", "ceo", "company"]:
        out[c] = out[c].astype(str).fillna("").str.strip()
    
    out = out[(out["alias"] != "") & (out["ceo"] != "") & (out["alias"] != "nan")]
    out = out.drop_duplicates(subset=["ceo"]).reset_index(drop=True)
    
    if out.empty:
        raise ValueError("No valid CEO rows after normalization.")
    return out

def load_articles(articles_dir: Path, date_str: str) -> pd.DataFrame:
    """Load articles for a specific date."""
    f = articles_dir / f"{date_str}-ceo-articles-modal.csv"
    cols = ["ceo", "company", "title", "url", "source", "sentiment"]
    if not f.exists():
        return pd.DataFrame(columns=cols)

    df = pd.read_csv(f)
    if df.empty:
        return pd.DataFrame(columns=cols)

    df = df.rename(columns={c: c.lower() for c in df.columns})
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    for c in ["ceo", "company", "title", "url", "source", "sentiment"]:
        df[c] = df[c].astype(str).fillna("").str.strip()
    df["sentiment"] = df["sentiment"].str.lower()
    return df[cols]

def aggregate_counts(roster: pd.DataFrame, articles: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """Aggregate sentiment counts per CEO."""
    base = roster.copy()

    if not articles.empty:
        grp = (
            articles.assign(sentiment=articles["sentiment"].str.lower())
            .groupby("ceo", dropna=False)["sentiment"]
            .value_counts()
            .unstack(fill_value=0)
            .rename(columns={"positive": "positive", "neutral": "neutral", "negative": "negative"})
        )
        for col in ["positive", "neutral", "negative"]:
            if col not in grp.columns:
                grp[col] = 0
        grp = grp[["positive", "neutral", "negative"]]
        base = base.merge(grp, how="left", left_on="ceo", right_index=True)
    else:
        base["positive"] = 0
        base["neutral"] = 0
        base["negative"] = 0

    base[["positive", "neutral", "negative"]] = base[["positive", "neutral", "negative"]].fillna(0).astype(int)
    base["total"] = base["positive"] + base["neutral"] + base["negative"]
    base["neg_pct"] = base.apply(
        lambda r: round(100.0 * (r["negative"] / r["total"]), 1) if r["total"] > 0 else 0.0, axis=1
    )

    base["theme"] = ""

    out = base[["ceo", "company", "positive", "neutral", "negative", "total", "neg_pct", "theme", "alias"]].copy()
    out.insert(0, "date", date_str)
    return out

def write_daily_file(daily_dir: Path, date_str: str, daily_rows: pd.DataFrame) -> Path:
    """Write per-day CSV file."""
    daily_dir.mkdir(parents=True, exist_ok=True)
    path = daily_dir / f"{date_str}-ceo-articles-table.csv"
    daily_rows.to_csv(path, index=False)
    return path

def upsert_master_index(out_path: Path, date_str: str, daily_rows: pd.DataFrame) -> pd.DataFrame:
    """Update rolling index CSV and return complete DataFrame."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    if out_path.exists():
        master = pd.read_csv(out_path)
        master = master.rename(columns={c: c.lower() for c in master.columns})
        expected = ["date", "ceo", "company", "positive", "neutral", "negative", "total", "neg_pct", "theme", "alias"]
        for col in expected:
            if col not in master.columns:
                master[col] = [] if col in ["theme", "alias"] else 0
        master = master[expected]
        master = master[master["date"].astype(str) != date_str]
        master = pd.concat([master, daily_rows], ignore_index=True)
    else:
        master = daily_rows.copy()

    master["date"] = master["date"].astype(str)
    master = master.sort_values(["date", "ceo"]).reset_index(drop=True)
    master.to_csv(out_path, index=False)
    
    return master

# ----------------------- CLI / Main ------------------------ #

def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build daily CEO sentiment counts.")
    p.add_argument("--date", default=iso_today_utc(), help="Target date (YYYY-MM-DD).")
    p.add_argument("--roster", default=DEFAULT_ROSTER, help="Path to main-roster.csv")
    p.add_argument("--articles-dir", default=DEFAULT_ARTICLES_DIR, help="Folder with daily articles CSVs")
    p.add_argument("--daily-dir", default=DEFAULT_DAILY_DIR, help="Folder to write per-day CSVs")
    p.add_argument("--out", default=DEFAULT_OUT, help="Path to write/append master index")
    # NEW: Add flag to skip sheets writing
    p.add_argument("--skip-sheets", action="store_true", 
                   help="Skip writing to Google Sheets")
    return p.parse_args(argv)

def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)

    # Validate date
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        raise SystemExit(f"Invalid --date '{args.date}'. Expected YYYY-MM-DD.")

    roster = load_roster(Path(args.roster))
    articles = load_articles(Path(args.articles_dir), args.date)
    daily_rows = aggregate_counts(roster, articles, args.date)

    daily_path = write_daily_file(Path(args.daily_dir), args.date, daily_rows)
    master_df = upsert_master_index(Path(args.out), args.date, daily_rows)

    print(f"✔ Wrote per-day file:  {daily_path}")
    print(f"✔ Updated master index: {args.out} (rows: {len(master_df):,})")
    
    # ===================================================================
    # NEW: Write to Google Sheets
    # ===================================================================
    if WRITE_TO_SHEETS and not args.skip_sheets and SHEETS_HELPER_AVAILABLE:
        try:
            print(f"\n[INFO] Writing CEO article data to Google Sheets...")
            articles_for_modal = load_articles(Path(args.articles_dir), args.date)
            success = write_ceo_articles_to_sheets(
                rows_df=articles_for_modal,        # Individual articles for modal
                daily_df=daily_rows,               # Daily aggregates for table
                rollup_df=master_df,               # Rolling index for chart
                target_date=args.date
            )
            if success:
                print(f"[OK] Successfully wrote CEO article data to Google Sheets!")
            else:
                print(f"[WARN] Some Google Sheets writes may have failed")
        except Exception as e:
            print(f"[WARN] Could not write to Google Sheets: {e}")
            print(f"[INFO] CSV files were still created successfully")
    elif not SHEETS_HELPER_AVAILABLE:
        print(f"\n[INFO] Google Sheets packages not installed - data saved to CSV only")
    elif args.skip_sheets:
        print(f"\n[INFO] Skipped Google Sheets writing (--skip-sheets flag)")
    elif not WRITE_TO_SHEETS:
        print(f"\n[INFO] Google Sheets writing disabled (WRITE_TO_SHEETS=false)")
    # ===================================================================
    
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
