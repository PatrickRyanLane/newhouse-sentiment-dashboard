#!/usr/bin/env python3
"""
Daily Brand News Sentiment - Aggregator

NOW WITH GOOGLE SHEETS INTEGRATION - writes to both CSV and Google Sheets!
"""

import argparse
import csv
import os
import sys
from pathlib import Path
from datetime import date, timedelta

import pandas as pd

# NEW: Import Google Sheets helper
try:
    from sheets_helper import write_brand_articles_to_sheets
    SHEETS_HELPER_AVAILABLE = True
except ImportError:
    SHEETS_HELPER_AVAILABLE = False
    print("[INFO] sheets_helper not available - will only write CSVs")

# Updated to use new directory and naming convention
ARTICLES_DIR = Path("data/processed_articles")
OUT_DIR      = Path("data/processed_articles")
OUT_DIR.mkdir(parents=True, exist_ok=True)
DAILY_INDEX  = Path("data/daily_counts") / "brand-articles-daily-counts-chart.csv"

# NEW: Enable/disable Google Sheets writing
WRITE_TO_SHEETS = os.environ.get('WRITE_TO_SHEETS', 'true').lower() == 'true'

# Columns we will ALWAYS write for the daily index - STANDARDIZED NAMING
INDEX_FIELDS = ["date","company","positive_articles","neutral_articles","negative_articles","total","neg_pct"]

def iter_dates(from_str: str, to_str: str):
    d0 = date.fromisoformat(from_str)
    d1 = date.fromisoformat(to_str)
    if d1 < d0:
        raise SystemExit(f"--to ({to_str}) is before --from ({from_str})")
    d = d0
    one = timedelta(days=1)
    while d <= d1:
        yield d.isoformat()
        d += one

def read_articles(dstr: str):
    """Read articles CSV for a date and return list of dictionaries.
    
    This reads the INDIVIDUAL ARTICLES (modal data) which contain student edits.
    We need this data to pass to sheets_helper so it can preserve sentiment edits.
    """
    f = ARTICLES_DIR / f"{dstr}-brand-articles-modal.csv"
    if not f.exists():
        print(f"[INFO] No headline file for {dstr} at {f}; nothing to aggregate.", flush=True)
        return []

    rows = []
    with f.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append({
                "company": (row.get("company") or "").strip(),
                "sentiment": (row.get("sentiment") or "").strip().lower(),
            })
    return rows

def aggregate(rows):
    agg = {}
    for r in rows:
        company = r["company"]
        if not company:
            continue
        s = r["sentiment"]
        bucket = agg.setdefault(company, {"positive":0,"neutral":0,"negative":0,"total":0})
        if s not in ("positive","neutral","negative"):
            s = "neutral"
        bucket[s] += 1
        bucket["total"] += 1
    return agg

def write_daily(dstr: str, agg: dict):
    """Write per-day file and return DataFrame for Sheets."""
    out = OUT_DIR / f"{dstr}-brand-articles-table.csv"
    
    # Build DataFrame with standardized column names
    rows = []
    for company, c in sorted(agg.items()):
        total = c["total"]
        neg_pct = (c["negative"] / total) if total else 0.0
        rows.append({
            "date": dstr,
            "company": company,
            "positive_articles": c["positive"],
            "neutral_articles": c["neutral"],
            "negative_articles": c["negative"],
            "total": total,
            "neg_pct": f"{neg_pct:.6f}"
        })
    
    df = pd.DataFrame(rows)
    df.to_csv(out, index=False)
    print(f"[OK] Wrote {out}")
    
    return df

def upsert_daily_index(dstr: str, agg: dict):
    """Update rolling index and return complete DataFrame for Sheets."""
    DAILY_INDEX.parent.mkdir(parents=True, exist_ok=True)
    
    rows = []
    if DAILY_INDEX.exists():
        with DAILY_INDEX.open(newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))

    # Drop existing rows for this date
    rows = [r for r in rows if r.get("date") != dstr]
    
    # Add new rows with standardized column names
    for company, c in agg.items():
        total = c["total"]
        neg_pct = (c["negative"] / total) if total else 0.0
        rows.append({
            "date": dstr,
            "company": company,
            "positive_articles": str(c["positive"]),
            "neutral_articles": str(c["neutral"]),
            "negative_articles": str(c["negative"]),
            "total":    str(total),
            "neg_pct":  f"{neg_pct:.6f}",
        })

    # Sort by date, then company
    rows.sort(key=lambda r: (r["date"], r["company"]))

    with DAILY_INDEX.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=INDEX_FIELDS)
        w.writeheader()
        cleaned = [{k: r.get(k, "") for k in INDEX_FIELDS} for r in rows]
        w.writerows(cleaned)
    print(f"[OK] Updated {DAILY_INDEX}")
    
    # Return as DataFrame
    return pd.DataFrame(cleaned)

def process_one(dstr: str, skip_sheets=False):
    """Process articles for a single date.
    
    This function:
    1. Reads articles and aggregates sentiment counts
    2. Writes summary CSVs
    3. Sends data to Google Sheets WITH EDIT PRESERVATION
    """
    print(f"Processing {dstr}...")
    rows = read_articles(dstr)
    if not rows:
        return
    
    agg = aggregate(rows)
    
    # Write CSVs and get DataFrames
    daily_df = write_daily(dstr, agg)
    rollup_df = upsert_daily_index(dstr, agg)
    
    # ✅ FIXED: Convert rows to DataFrame and pass it to sheets writer
    # This DataFrame contains the individual articles with their sentiments,
    # which allows merge_preserving_edits() to match by URL and preserve student edits
    rows_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=['company', 'sentiment', 'url'])
    
    # If we have full article data with URL, use that; otherwise fall back to simplified
    # The sheets_helper will use the URL to match against previous day's data
    f = ARTICLES_DIR / f"{dstr}-brand-articles-modal.csv"
    if f.exists():
        try:
            rows_df = pd.read_csv(f)  # Use full data with all columns including URL
        except Exception as e:
            print(f"[WARN] Could not read full article data: {e}")
            # Fall back to aggregated rows
            rows_df = pd.DataFrame(rows)
    
    # ===================================================================
    # NEW: Write to Google Sheets
    # ===================================================================
    if WRITE_TO_SHEETS and not skip_sheets and SHEETS_HELPER_AVAILABLE:
        try:
            print(f"\n[INFO] Writing brand article data to Google Sheets...")
            print(f"[INFO] Preserving student sentiment edits from {dstr}...")
            success = write_brand_articles_to_sheets(
                rows_df=rows_df,             # ✅ NOW PASSING: Individual articles (modal data with student edits!)
                daily_df=daily_df,           # Summary counts by company and sentiment
                rollup_df=rollup_df,         # Rolling historical data
                target_date=dstr
            )
            if success:
                print(f"[OK] Successfully wrote brand article data to Google Sheets!")
            else:
                print(f"[WARN] Some Google Sheets writes may have failed")
        except Exception as e:
            print(f"[WARN] Could not write to Google Sheets: {e}")
            print(f"[INFO] CSV files were still created successfully")
    elif not SHEETS_HELPER_AVAILABLE:
        print(f"\n[INFO] Google Sheets packages not installed - data saved to CSV only")
    elif skip_sheets:
        print(f"\n[INFO] Skipped Google Sheets writing (--skip-sheets flag)")
    elif not WRITE_TO_SHEETS:
        print(f"\n[INFO] Google Sheets writing disabled (WRITE_TO_SHEETS=false)")
    # ===================================================================

def main():
    ap = argparse.ArgumentParser(description="Aggregate brand news sentiment by day/company.")
    ap.add_argument("--date", help="single YYYY-MM-DD to process")
    ap.add_argument("--from", dest="from_date", help="start YYYY-MM-DD (inclusive)")
    ap.add_argument("--to",   dest="to_date",   help="end YYYY-MM-DD (inclusive)")
    # NEW: Add flag to skip sheets writing
    ap.add_argument("--skip-sheets", action="store_true", 
                    help="Skip writing to Google Sheets")
    args = ap.parse_args()

    if args.date:
        dates = [args.date]
    elif args.from_date and args.to_date:
        dates = list(iter_dates(args.from_date, args.to_date))
    else:
        dates = [date.today().isoformat()]

    for dstr in dates:
        process_one(dstr, skip_sheets=args.skip_sheets)

if __name__ == "__main__":
    main()
