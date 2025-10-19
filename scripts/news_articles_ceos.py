#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Builds daily CEO articles from Google News RSS

NOW WITH GOOGLE SHEETS INTEGRATION - writes individual headlines to Sheets!
This is the file students will edit to correct CEO article sentiment.
"""

from __future__ import annotations
import argparse
import os
import time
import html
import sys
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote_plus, urlparse

import pandas as pd
import requests
import feedparser
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

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

USER_AGENT = "Mozilla/5.0 (compatible; CEO-NewsBot/1.0; +https://example.com/bot)"
RSS_TMPL = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

# Tunables (env overrides)
MAX_PER_ALIAS = int(os.getenv("ARTICLES_MAX_PER_ALIAS", "25"))
SLEEP_SEC = float(os.getenv("ARTICLES_SLEEP_SEC", "0.35"))
TARGET_DATE = os.getenv("ARTICLES_DATE", "").strip()

def target_date() -> str:
    if TARGET_DATE:
        try:
            datetime.strptime(TARGET_DATE, "%Y-%m-%d")
            return TARGET_DATE
        except ValueError:
            print(f"WARNING: invalid ARTICLES_DATE={TARGET_DATE!r}; falling back to today.")
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def read_roster(path: Path) -> pd.DataFrame:
    """Read CEO roster from main-roster.csv"""
    if not path.exists():
        raise FileNotFoundError(f"Missing roster file: {path}")
    
    df = pd.read_csv(path, encoding="utf-8-sig")
    cols = {c.strip().lower(): c for c in df.columns}

    def col(name: str) -> str:
        for k, v in cols.items():
            if k == name.lower():
                return v
        raise KeyError(f"Expected column '{name}' in {path.name}")

    ceo_col = col("ceo")
    company_col = col("company")
    alias_col = col("ceo alias")

    out = df[[alias_col, ceo_col, company_col]].copy()
    out.columns = ["alias", "ceo", "company"]
    out["alias"] = out["alias"].astype(str).str.strip()
    out["ceo"] = out["ceo"].astype(str).str.strip()
    out["company"] = out["company"].astype(str).str.strip()
    
    out = out[(out["alias"] != "") & (out["ceo"] != "") & (out["alias"] != "nan")]
    if out.empty:
        raise ValueError("No valid CEO rows after normalization.")
    return out.drop_duplicates()

def fetch_rss(query: str) -> feedparser.FeedParserDict:
    url = RSS_TMPL.format(query=quote_plus(query))
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    return feedparser.parse(resp.content)

def label_sentiment(analyzer: SentimentIntensityAnalyzer, text: str) -> str:
    s = analyzer.polarity_scores(text or "")
    c = s.get("compound", 0.0)
    if c >= 0.25:
        return "positive"
    if c <= -0.05:
        return "negative"
    return "neutral"

def extract_source(entry) -> str:
    try:
        src = entry.get("source", {}).get("title", "") or ""
    except Exception:
        src = ""
    if src:
        return str(src).strip()
    link = entry.get("link") or entry.get("id") or ""
    try:
        host = urlparse(link).hostname or ""
        return host.replace("www.", "")
    except Exception:
        return ""

def build_articles_for_alias(alias: str, ceo: str, company: str, analyzer) -> list[dict]:
    try:
        feed = fetch_rss(alias)
    except Exception as e:
        print(f"ERROR fetching RSS for {alias!r}: {e}")
        return []

    rows = []
    for entry in (feed.entries or [])[:MAX_PER_ALIAS]:
        title = html.unescape(entry.get("title", "")).strip()
        link = (entry.get("link") or entry.get("id") or "").strip()
        source = extract_source(entry)
        if not title:
            continue
        sent = label_sentiment(analyzer, title)
        rows.append({
            "ceo": ceo,
            "company": company,
            "title": title,
            "url": link,
            "source": source,
            "sentiment": sent,
        })
    return rows

def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch CEO news articles and analyze sentiment")
    # NEW: Add skip-sheets flag
    parser.add_argument("--skip-sheets", action="store_true", 
                       help="Skip writing to Google Sheets")
    args = parser.parse_args()
    
    out_date = target_date()
    out_path = OUT_DIR / f"{out_date}-ceo-articles-modal.csv"
    print(f"Building articles for {out_date} → {out_path}")

    try:
        roster = read_roster(MAIN_ROSTER)
    except Exception as e:
        print(f"FATAL: {e}")
        # Still write an empty file
        pd.DataFrame(columns=["ceo","company","title","url","source","sentiment"]).to_csv(out_path, index=False)
        return 1

    analyzer = SentimentIntensityAnalyzer()
    all_rows: list[dict] = []

    for i, row in roster.iterrows():
        alias = row["alias"]
        ceo = row["ceo"]
        company = row["company"]
        if not alias:
            continue
        print(f"[{i+1}/{len(roster)}] {alias}")
        rows = build_articles_for_alias(alias, ceo, company, analyzer)
        all_rows.extend(rows)
        time.sleep(SLEEP_SEC)

    # De-duplicate
    if all_rows:
        df = pd.DataFrame(all_rows)
        df = df.drop_duplicates(subset=["ceo","title","url"]).reset_index(drop=True)
    else:
        df = pd.DataFrame(columns=["ceo","company","title","url","source","sentiment"])

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"✔ Wrote {len(df):,} rows → {out_path}")
    
    # ===================================================================
    # NEW: Calculate aggregations and write to Google Sheets
    # ===================================================================
    if WRITE_TO_SHEETS and not args.skip_sheets and SHEETS_HELPER_AVAILABLE:
        try:
            # Use the DataFrame we already have (modal sheet data)
            modal_df = df.copy()
            
            # Calculate daily aggregation by CEO
            if not modal_df.empty:
                daily_df = (
                    modal_df.groupby("ceo", as_index=False)
                    .agg(
                        total=("ceo", "size"),
                        negative_articles=("sentiment", lambda s: (s == "negative").sum()),
                        neutral_articles=("sentiment", lambda s: (s == "neutral").sum()),
                        positive_articles=("sentiment", lambda s: (s == "positive").sum()),
                        company=("company", lambda s: s.mode()[0] if not s.mode().empty else ""),
                    )
                )
                daily_df.insert(0, "date", out_date)
            else:
                daily_df = pd.DataFrame(columns=["date", "ceo", "total", "negative_articles", "neutral_articles", "positive_articles", "company"])
            
            # Create rolling index (read existing, merge, sort)
            rollup_path = OUT_DIR / "ceo-articles-daily-counts-chart.csv"
            if rollup_path.exists():
                rollup_df = pd.read_csv(rollup_path)
                # Remove any rows for this date
                rollup_df = rollup_df[rollup_df["date"] != out_date]
                # Add new data
                rollup_df = pd.concat([rollup_df, daily_df], ignore_index=True)
            else:
                rollup_df = daily_df.copy()
            
            # Sort by date and CEO
            rollup_df = rollup_df.sort_values(["date", "ceo"]).reset_index(drop=True)
            
            # Save rollup to CSV
            rollup_path.parent.mkdir(parents=True, exist_ok=True)
            rollup_df.to_csv(rollup_path, index=False)
            print(f"[OK] Updated rolling index → {rollup_path}")
            
            # Write all three sheets to Google Sheets
            from sheets_helper import write_ceo_articles_to_sheets
            
            success = write_ceo_articles_to_sheets(
                rows_df=modal_df,
                daily_df=daily_df,
                rollup_df=rollup_df,
                target_date=out_date
            )
            if success:
                print(f"[OK] Successfully wrote CEO articles to Google Sheets!")
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
    sys.exit(main())
