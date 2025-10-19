#!/usr/bin/env python3
"""
Fetch brand news articles and analyze sentiment

NOW WITH GOOGLE SHEETS INTEGRATION - writes individual headlines to Sheets!
This is the file students will edit to correct brand article sentiment.
"""

import argparse
import csv
import os
import re
import sys
import time
import urllib.parse
import requests
from datetime import datetime, timezone
from pathlib import Path
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd

# NEW: Import Google Sheets helper
try:
    from sheets_helper import write_to_sheet
    SHEETS_HELPER_AVAILABLE = True
except ImportError:
    SHEETS_HELPER_AVAILABLE = False
    print("[INFO] sheets_helper not available - will only write CSVs")

# Updated paths
BASE = Path(__file__).parent.parent
MAIN_ROSTER = BASE / "rosters" / "main-roster.csv"
OUT_DIR = BASE / "data" / "processed_articles"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# NEW: Enable/disable Google Sheets writing
WRITE_TO_SHEETS = os.environ.get('WRITE_TO_SHEETS', 'true').lower() == 'true'

# Tunables (env overrides)
MAX_PER_ALIAS = int(os.getenv("ARTICLES_MAX_PER_ALIAS", "50"))

def google_news_rss(q):
    qs = urllib.parse.quote(q)
    return f"https://news.google.com/rss/search?q={qs}&hl=en-US&gl=US&ceid=US:en"

def classify(headline, analyzer):
    s = analyzer.polarity_scores(headline or "")
    c = s["compound"]
    if c >= 0.25:  return "positive"
    if c <= -0.05: return "negative"
    return "neutral"

def fetch_one(brand, analyzer, date, pause=1.2):
    url = google_news_rss(f'"{brand}"')
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "xml")
    out = []
    for item in soup.find_all("item"):
        title = (item.title.text or "").strip()
        link  = (item.link.text  or "").strip()
        try:
            if "url=" in link:
                link = urllib.parse.parse_qs(urllib.parse.urlparse(link).query).get("url", [link])[0]
        except Exception:
            pass
        source = (item.source.text or "").strip() if item.source else ""
        sent   = classify(title, analyzer)
        out.append({
            "company": brand,
            "title": title,
            "url": link,
            "source": source,
            "date": date,
            "sentiment": sent
        })
    time.sleep(pause)  # be respectful
    return out[:MAX_PER_ALIAS]  # cap results

def load_companies_from_roster():
    """Load unique company names from rosters/main-roster.csv"""
    if not MAIN_ROSTER.exists():
        raise FileNotFoundError(f"Main roster not found: {MAIN_ROSTER}")
    
    companies = set()
    with MAIN_ROSTER.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = {h.strip().lower(): h for h in (reader.fieldnames or [])}
        
        company_col = None
        for key in ["company"]:
            if key in headers:
                company_col = headers[key]
                break
        
        if not company_col:
            raise ValueError("No 'Company' column found in main-roster.csv")
        
        for row in reader:
            company = (row.get(company_col) or "").strip()
            if company:
                companies.add(company)
    
    return sorted(companies)

def main():
    parser = argparse.ArgumentParser(description="Fetch brand news articles and analyze sentiment")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to use for the data file (YYYY-MM-DD). Defaults to today."
    )
    # NEW: Add flag to skip sheets writing
    parser.add_argument("--skip-sheets", action="store_true", 
                       help="Skip writing to Google Sheets")
    args = parser.parse_args()
    
    # Use provided date or default to today
    if args.date:
        date = args.date
    else:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Set output file path
    out_file = OUT_DIR / f"{date}-brand-articles-modal.csv"
    
    if not MAIN_ROSTER.exists():
        print(f"ERROR: {MAIN_ROSTER} not found", file=sys.stderr)
        sys.exit(1)
    
    brands = load_companies_from_roster()
    print(f"Loaded {len(brands)} companies from {MAIN_ROSTER}")
    print(f"Processing articles for date: {date}")
    
    analyzer = SentimentIntensityAnalyzer()

    rows = []
    for b in brands:
        try:
            rows.extend(fetch_one(b, analyzer, date))
        except Exception as e:
            print(f"[WARN] {b}: {e}", file=sys.stderr)

    # Write to CSV
    with out_file.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["company","title","url","source","date","sentiment"])
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {out_file} ({len(rows)} rows)")
    
    # ===================================================================
    # NEW: Calculate aggregations and write to Google Sheets
    # ===================================================================
    if WRITE_TO_SHEETS and not args.skip_sheets and SHEETS_HELPER_AVAILABLE:
        try:
            # Convert rows to DataFrame (modal sheet data)
            modal_df = pd.DataFrame(rows)
            if modal_df.empty:
                modal_df = pd.DataFrame(columns=["company","title","url","source","date","sentiment"])
            
            # Calculate daily aggregation by company
            if not modal_df.empty:
                daily_df = (
                    modal_df.groupby("company", as_index=False)
                    .agg(
                        total=("company", "size"),
                        negative_articles=("sentiment", lambda s: (s == "negative").sum()),
                        neutral_articles=("sentiment", lambda s: (s == "neutral").sum()),
                        positive_articles=("sentiment", lambda s: (s == "positive").sum()),
                    )
                )
                daily_df.insert(0, "date", date)
            else:
                daily_df = pd.DataFrame(columns=["date", "company", "total", "negative_articles", "neutral_articles", "positive_articles"])
            
            # Create rolling index (read existing, merge, sort)
            rollup_path = OUT_DIR / "brand-articles-daily-counts-chart.csv"
            if rollup_path.exists():
                rollup_df = pd.read_csv(rollup_path)
                # Remove any rows for this date
                rollup_df = rollup_df[rollup_df["date"] != date]
                # Add new data
                rollup_df = pd.concat([rollup_df, daily_df], ignore_index=True)
            else:
                rollup_df = daily_df.copy()
            
            # Sort by date and company
            rollup_df = rollup_df.sort_values(["date", "company"]).reset_index(drop=True)
            
            # Save rollup to CSV
            rollup_path.parent.mkdir(parents=True, exist_ok=True)
            rollup_df.to_csv(rollup_path, index=False)
            print(f"[OK] Updated rolling index → {rollup_path}")
            
            # Write all three sheets to Google Sheets
            from sheets_helper import write_brand_articles_to_sheets
            
            success = write_brand_articles_to_sheets(
                rows_df=modal_df,
                daily_df=daily_df,
                rollup_df=rollup_df,
                target_date=date
            )
            if success:
                print(f"[OK] Successfully wrote brand articles to Google Sheets!") 
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

if __name__ == "__main__":
    main()
