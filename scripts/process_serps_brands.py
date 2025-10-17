#!/usr/bin/env python3
"""
Process daily BRAND SERP data WITH GOOGLE SHEETS INTEGRATION

Enhancements:
- Now writes to both CSV (backup) and Google Sheets (live editing)
- Uses sheets_helper.py for Google Sheets operations
- Gracefully handles missing Google Sheets packages
"""

from __future__ import annotations

import argparse
import csv
import io
import os
from datetime import datetime
from typing import Dict, Tuple, Set
from urllib.parse import urlparse

import pandas as pd
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# NEW: Import Google Sheets helper (gracefully fails if packages not installed)
try:
    from sheets_helper import write_serps_to_sheets
    SHEETS_HELPER_AVAILABLE = True
except ImportError:
    SHEETS_HELPER_AVAILABLE = False
    print("[INFO] sheets_helper not available - will only write CSVs")

# -----------------------
# Config / constants
# -----------------------
S3_URL_TEMPLATE = (
    "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files_plane/{date}-brand-serps.csv"
)

MAIN_ROSTER_PATH = "rosters/main-roster.csv"

OUT_ROWS_DIR = "data/processed_serps"
OUT_DAILY_DIR = "data/processed_serps"
OUT_ROLLUP = "data/daily_counts/brand-serps-daily-counts-chart.csv"

FORCE_POSITIVE_IF_CONTROLLED = True

ALWAYS_CONTROLLED_DOMAINS: Set[str] = {
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "play.google.com",
    "apps.apple.com",
}

# NEW: Enable/disable Google Sheets writing
WRITE_TO_SHEETS = os.environ.get('WRITE_TO_SHEETS', 'true').lower() == 'true'

# -----------------------
# Argument parsing / dates
# -----------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Process daily brand SERPs.")
    ap.add_argument("--date", help="YYYY-MM-DD (defaults to today)", default=None)
    # NEW: Add flag to skip sheets writing
    ap.add_argument("--skip-sheets", action="store_true", 
                    help="Skip writing to Google Sheets")
    return ap.parse_args()

def get_target_date(arg_date: str | None) -> str:
    if arg_date:
        try:
            datetime.strptime(arg_date, "%Y-%m-%d")
            return arg_date
        except ValueError:
            pass
    return datetime.utcnow().strftime("%Y-%m-%d")

# -----------------------
# Files / I/O helpers
# -----------------------
def ensure_dirs() -> None:
    os.makedirs(OUT_ROWS_DIR, exist_ok=True)
    os.makedirs(OUT_DAILY_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(OUT_ROLLUP), exist_ok=True)

def fetch_csv_from_s3(url: str) -> pd.DataFrame | None:
    try:
        resp = requests.get(url, timeout=45)
        resp.raise_for_status()
        return pd.read_csv(io.StringIO(resp.text))
    except Exception as e:
        print(f"[WARN] Could not fetch {url} — {e}")
        return None

# -----------------------
# Domain normalization
# -----------------------
def _hostname(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
        return host.replace("www.", "")
    except Exception:
        return ""

def _norm_token(s: str) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())

def _norm_domain_for_name_match(host: str) -> str:
    return "".join(ch for ch in (host or "") if ch.isalnum())

# -----------------------
# Roster loading
# -----------------------
def load_roster_domains(path: str = MAIN_ROSTER_PATH) -> Set[str]:
    domains: Set[str] = set()

    if not os.path.exists(path):
        print(f"[WARN] roster not found at {path}; proceeding with empty controlled set")
        return domains

    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
        cols = {c.strip().lower(): c for c in df.columns}
        
        website_col = None
        for key in ["website", "domain", "url", "site", "homepage"]:
            if key in cols:
                website_col = cols[key]
                break
        
        if not website_col:
            print(f"[WARN] No website/domain column found in {path}")
            return domains
        
        for val in df[website_col].dropna().astype(str):
            val = val.strip()
            if val and val != "nan":
                if not val.startswith(("http://", "https://")):
                    val = f"http://{val}"
                host = _hostname(val)
                if host and "." in host:
                    domains.add(host)
                    
    except Exception as e:
        print(f"[WARN] failed reading roster at {path}: {e}")

    return domains

# -----------------------
# Control classification
# -----------------------
def classify_control(company: str, url: str, roster_domains: Set[str]) -> bool:
    host = _hostname(url)
    if not host:
        return False

    for good in ALWAYS_CONTROLLED_DOMAINS:
        if host == good or host.endswith("." + good):
            return True

    for rd in roster_domains:
        if host == rd or host.endswith("." + rd):
            return True

    brand_token = _norm_token(company)
    if brand_token:
        host_token = _norm_domain_for_name_match(host)
        if brand_token in host_token:
            return True

    return False

# -----------------------
# Sentiment
# -----------------------
def vader_label_on_title(analyzer: SentimentIntensityAnalyzer, title: str) -> Tuple[float, str]:
    s = analyzer.polarity_scores(title or "")
    c = s.get("compound", 0.0)
    if c >= 0.2:
        lab = "positive"
    elif c <= -0.1:
        lab = "negative"
    else:
        lab = "neutral"
    return c, lab

# -----------------------
# Main processing
# -----------------------
def process_for_date(target_date: str, skip_sheets: bool = False) -> None:
    print(f"[INFO] Processing brand SERPs for {target_date} …")
    ensure_dirs()

    roster_domains = load_roster_domains()

    url = S3_URL_TEMPLATE.format(date=target_date)
    raw = fetch_csv_from_s3(url)
    if raw is None or raw.empty:
        print(f"[WARN] No raw brand SERP data available for {target_date}. Nothing to write.")
        return

    expected = ["company", "position", "title", "link", "snippet"]
    for col in expected:
        if col not in raw.columns:
            raw[col] = ""

    analyzer = SentimentIntensityAnalyzer()

    processed_rows = []
    for _, row in raw.iterrows():
        company = str(row.get("company", "") or "").strip()
        if not company:
            continue

        title = str(row.get("title", "") or "").strip()
        url = str(row.get("link", "") or "").strip()
        snippet = str(row.get("snippet", "") or "").strip()

        pos_val = row.get("position", 0)
        try:
            position = int(float(pos_val) if pos_val not in (None, "") else 0)
        except Exception:
            position = 0

        controlled = classify_control(company, url, roster_domains)

        _, label = vader_label_on_title(analyzer, title)
        if FORCE_POSITIVE_IF_CONTROLLED and controlled:
            label = "positive"

        processed_rows.append({
            "date": target_date,
            "company": company,
            "title": title,
            "url": url,
            "position": position,
            "snippet": snippet,
            "sentiment": label,
            "controlled": controlled,
        })

    if not processed_rows:
        print(f"[WARN] No processed rows for {target_date}.")
        return

    rows_df = pd.DataFrame(processed_rows)
    row_out_path = os.path.join(OUT_ROWS_DIR, f"{target_date}-brand-serps-modal.csv")
    rows_df.to_csv(row_out_path, index=False)
    print(f"[OK] Wrote row-level SERPs → {row_out_path}")

    agg = (
        rows_df.groupby("company", as_index=False)
        .agg(
            total=("company", "size"),
            controlled=("controlled", "sum"),
            negative_serp=("sentiment", lambda s: (s == "negative").sum()),
            neutral_serp=("sentiment", lambda s: (s == "neutral").sum()),
            positive_serp=("sentiment", lambda s: (s == "positive").sum()),
        )
    )
    agg.insert(0, "date", target_date)

    daily_out_path = os.path.join(OUT_DAILY_DIR, f"{target_date}-brand-serps-table.csv")
    agg.to_csv(daily_out_path, index=False)
    print(f"[OK] Wrote daily aggregate → {daily_out_path}")

    if os.path.exists(OUT_ROLLUP):
        roll = pd.read_csv(OUT_ROLLUP)
        roll = roll[roll["date"] != target_date]
        roll = pd.concat([roll, agg], ignore_index=True)
    else:
        roll = agg.copy()

    cols = [
        "date",
        "company",
        "total",
        "controlled",
        "negative_serp",
        "neutral_serp",
        "positive_serp",
    ]
    roll = roll[cols].sort_values(["date", "company"]).reset_index(drop=True)
    roll.to_csv(OUT_ROLLUP, index=False)
    print(f"[OK] Updated rolling index → {OUT_ROLLUP}")

    # ===================================================================
    # NEW: Write to Google Sheets
    # ===================================================================
    if WRITE_TO_SHEETS and not skip_sheets and SHEETS_HELPER_AVAILABLE:
        try:
            print(f"\n[INFO] Writing data to Google Sheets...")
            success = write_serps_to_sheets(
                rows_df=rows_df,
                daily_df=agg,
                rollup_df=roll,
                target_date=target_date
            )
            if success:
                print(f"[OK] Successfully wrote all data to Google Sheets!")
            else:
                print(f"[WARN] Some Google Sheets writes may have failed (check logs above)")
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

def main() -> None:
    args = parse_args()
    date_str = get_target_date(args.date)
    process_for_date(date_str, skip_sheets=args.skip_sheets)

if __name__ == "__main__":
    main()
