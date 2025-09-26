#!/usr/bin/env python3
"""
Process daily BRAND SERP data:

- Fetch raw SERPs from S3: https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-brand-serps.csv
- Classify sentiment (VADER) using ONLY the page TITLE
- Classify CONTROL using three rules:
    (1) Always-controlled platforms (and their subdomains):
        facebook.com, instagram.com, twitter.com, x.com, linkedin.com, play.google.com, apps.apple.com
    (2) Any domain (or its subdomains) present in data/roster.csv
    (3) Domain contains the normalized brand token (e.g., "capitalone" matches capitalone.com, capitalonetravel.com, ir.capitalone.com)

- If CONTROLLED and FORCE_POSITIVE_IF_CONTROLLED = True -> sentiment is forced to "positive"

Outputs:
  1) Row-level processed SERPs:       data/serp_rows/{date}-brand-serps-rows.csv
  2) Per-company daily aggregate:     data/processed_serps/{date}-brand-serps-processed.csv
  3) Rolling daily index (append/replace date): data/serps/brand_serps_daily.csv

Raw input headings expected (brand SERPs):
    prompt, company, position, title, link, displayed_link, snippet, thumbnail, favicon, redirect_link, rich_snippet, error_status

Row-level output columns:
    date, company, title, url, position, snippet, sentiment, controlled

Per-company aggregate columns:
    date, company, total, controlled, negative_serp, neutral_serp, positive_serp
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

# -----------------------
# Config / constants
# -----------------------
S3_URL_TEMPLATE = (
    "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-brand-serps.csv"
)

ROSTER_PATH = "data/roster.csv"   # single authoritative roster

OUT_ROWS_DIR = "data/serp_rows"
OUT_DAILY_DIR = "data/processed_serps"
OUT_ROLLUP = "data/serps/brand_serps_daily.csv"

# If controlled, force sentiment to positive
FORCE_POSITIVE_IF_CONTROLLED = True

# Domains explicitly CONTROLLED (social + app stores)
ALWAYS_CONTROLLED_DOMAINS: Set[str] = {
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "play.google.com",
    "apps.apple.com",
}

# -----------------------
# Argument parsing / dates
# -----------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Process daily brand SERPs.")
    ap.add_argument("--date", help="YYYY-MM-DD (defaults to today)", default=None)
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
    # alphanumeric only, lowercase (for brand name matching in host)
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())

def _norm_domain_for_name_match(host: str) -> str:
    # strip dots, dashes, etc. (simple heuristic)
    return "".join(ch for ch in (host or "") if ch.isalnum())

# -----------------------
# Roster loading
# -----------------------
def load_roster_domains(path: str = ROSTER_PATH) -> Set[str]:
    """
    Read domains from data/roster.csv and return a set of hostnames treated as controlled.
    Accept case-insensitive columns: domain, website, url, site, homepage.
    Accepts either plain domains (e.g., capitalone.com) or full URLs.
    """
    wanted_cols = {"domain", "website", "url", "site", "homepage"}
    domains: Set[str] = set()

    if not os.path.exists(path):
        print(f"[WARN] roster not found at {path}; proceeding with empty controlled set")
        return domains

    try:
        with open(path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                if not row:
                    continue
                for k, v in row.items():
                    if not k:
                        continue
                    if k.strip().lower() in wanted_cols and v:
                        val = str(v).strip()
                        url = val if val.startswith(("http://", "https://")) else "http://" + val
                        host = _hostname(url)
                        if host:
                            domains.add(host)
    except Exception as e:
        print(f"[WARN] failed reading roster at {path}: {e}")

    return domains

# -----------------------
# Control classification
# -----------------------
def classify_control(company: str, url: str, roster_domains: Set[str]) -> bool:
    """
    Controlled if:
      (1) Host equals / endswith an ALWAYS_CONTROLLED_DOMAIN
      (2) Host equals / endswith a roster domain
      (3) Host token contains normalized brand token
    """
    host = _hostname(url)
    if not host:
        return False

    # (1) Always-controlled platforms
    for good in ALWAYS_CONTROLLED_DOMAINS:
        if host == good or host.endswith("." + good):
            return True

    # (2) Controlled via roster (allow subdomains)
    for rd in roster_domains:
        if host == rd or host.endswith("." + rd):
            return True

    # (3) Brand token appears anywhere in the host token
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
    """
    Return (compound, label) where label in {'positive','neutral','negative'}.
    Uses VADER on the TITLE ONLY.
    """
    s = analyzer.polarity_scores(title or "")
    c = s.get("compound", 0.0)
    if c >= 0.05:
        lab = "positive"
    elif c <= -0.05:
        lab = "negative"
    else:
        lab = "neutral"
    return c, lab

# -----------------------
# Main processing
# -----------------------
def process_for_date(target_date: str) -> None:
    print(f"[INFO] Processing brand SERPs for {target_date} …")
    ensure_dirs()

    roster_domains = load_roster_domains()

    # Fetch raw SERPs from S3
    url = S3_URL_TEMPLATE.format(date=target_date)
    raw = fetch_csv_from_s3(url)
    if raw is None or raw.empty:
        print(f"[WARN] No raw brand SERP data available for {target_date}. Nothing to write.")
        return

    # Normalize columns that we rely on
    expected = ["company", "position", "title", "link", "snippet"]
    for col in expected:
        if col not in raw.columns:
            raw[col] = ""

    # Sentiment analyzer
    analyzer = SentimentIntensityAnalyzer()

    # Row-level processing
    processed_rows = []
    for _, row in raw.iterrows():
        company = str(row.get("company", "") or "").strip()
        if not company:
            continue

        title = str(row.get("title", "") or "").strip()
        url = str(row.get("link", "") or "").strip()
        snippet = str(row.get("snippet", "") or "").strip()

        # position may be float in the raw
        pos_val = row.get("position", 0)
        try:
            position = int(float(pos_val) if pos_val not in (None, "") else 0)
        except Exception:
            position = 0

        controlled = classify_control(company, url, roster_domains)

        # Sentiment: TITLE ONLY
        _, label = vader_label_on_title(analyzer, title)
        if FORCE_POSITIVE_IF_CONTROLLED and controlled:
            label = "positive"

        processed_rows.append(
            {
                "date": target_date,
                "company": company,
                "title": title,
                "url": url,
                "position": position,
                "snippet": snippet,      # kept for completeness; not used for sentiment
                "sentiment": label,
                "controlled": controlled,
            }
        )

    if not processed_rows:
        print(f"[WARN] No processed rows for {target_date}.")
        return

    # Save row-level
    rows_df = pd.DataFrame(processed_rows)
    row_out_path = os.path.join(OUT_ROWS_DIR, f"{target_date}-brand-serps-rows.csv")
    rows_df.to_csv(row_out_path, index=False)
    print(f"[OK] Wrote row-level SERPs → {row_out_path}")

    # Aggregate per company
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

    # Save daily aggregate
    daily_out_path = os.path.join(OUT_DAILY_DIR, f"{target_date}-brand-serps-processed.csv")
    agg.to_csv(daily_out_path, index=False)
    print(f"[OK] Wrote daily aggregate → {daily_out_path}")

    # Update rolling index (replace rows for this date, then append new)
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

def main() -> None:
    args = parse_args()
    date_str = get_target_date(args.date)
    process_for_date(date_str)

if __name__ == "__main__":
    main()
