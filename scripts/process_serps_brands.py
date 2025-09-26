#!/usr/bin/env python3
"""
Process daily BRAND SERP data:
- Fetch raw SERPs from S3: https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-brand-serps.csv
- Classify sentiment (VADER) and control (using data/roster.csv for canonical domains).
- Apply rules:
    * Any result matching the brand's canonical domain from roster is CONTROLLED.
    * If CONTROLLED -> sentiment defaults to POSITIVE (overrides VADER).
- Write outputs:
    1) Row-level processed SERPs:       data/serp_rows/{date}-brand-serps-rows.csv
    2) Per-company daily aggregate:     data/processed_serps/{date}-brand-serps-processed.csv
    3) Rolling daily index (append):    data/serps/brand_serps_daily.csv

Raw input headings expected (brand SERPs):
    prompt, company, position, title, link, displayed_link, snippet, thumbnail, favicon, redirect_link, rich_snippet, error_status

Output — row-level columns:
    date, company, title, url, position, snippet, sentiment, controlled

Output — per-company aggregate columns (counts):
    date, company, total, controlled, negative_serp, neutral_serp, positive_serp
"""

import argparse
import csv
import io
import os
from datetime import datetime
from typing import Dict, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# -----------------------
# Config / constants
# -----------------------
S3_URL_TEMPLATE = "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-brand-serps.csv"

ROSTER_CSV = "data/roster.csv"

OUT_ROWS_DIR = "data/serp_rows"
OUT_DAILY_DIR = "data/processed_serps"
OUT_ROLLUP = "data/serps/brand_serps_daily.csv"

# If controlled, force sentiment to positive (matches your CEO rule change)
FORCE_POSITIVE_IF_CONTROLLED = True

# Domains explicitly CONTROLLED (social/app stores)
ALWAYS_CONTROLLED_DOMAINS = {
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "play.google.com",
    "apps.apple.com",
}

# -----------------------
# Helpers
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
    return datetime.now().strftime("%Y-%m-%d")

def ensure_dirs():
    os.makedirs(OUT_ROWS_DIR, exist_ok=True)
    os.makedirs(OUT_DAILY_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(OUT_ROLLUP), exist_ok=True)

def fetch_csv_from_s3(url: str) -> pd.DataFrame | None:
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return pd.read_csv(io.StringIO(resp.text))
    except Exception as e:
        print(f"[WARN] Could not fetch {url} — {e}")
        return None

def load_company_domains(path: str = ROSTER_CSV) -> Dict[str, str]:
    """
    Build a mapping: lower(company) -> base domain from Website column in roster.csv
    """
    mapping: Dict[str, str] = {}
    if not os.path.exists(path):
        print(f"[WARN] roster not found at {path}. Control classification will be limited.")
        return mapping

    try:
        with open(path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                company = (row.get("Company") or "").strip()
                website = (row.get("Website") or "").strip()
                if not company or not website:
                    continue
                try:
                    host = urlparse(website).hostname or ""
                    host = host.replace("www.", "")
                    if host:
                        mapping[company.lower()] = host
                except Exception:
                    continue
    except Exception as e:
        print(f"[WARN] Failed reading roster at {path}: {e}")

    return mapping

def extract_domain(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        return host.replace("www.", "")
    except Exception:
        return ""

def classify_control(company: str, url: str, roster_domains: set[str]) -> bool:
    """
    Controlled if:
      1) Host is in ALWAYS_CONTROLLED_DOMAINS (or is a subdomain of those),
      2) Host matches (is or endswith) any roster domain,
      3) Host 'core' contains normalized brand name (e.g., capitalone in capitalonetravel.com).

    Uncontrolled if:
      - Host in UNCONTROLLED_DOMAINS (YouTube/TikTok).
    """
    host = _hostname(url)
    if not host:
        return False

    # Always uncontrolled
    for bad in UNCONTROLLED_DOMAINS:
        if host == bad or host.endswith("." + bad):
            return False

    # Always controlled
    for good in ALWAYS_CONTROLLED_DOMAINS:
        if host == good or host.endswith("." + good):
            return True

    # Roster domains → controlled (allow subdomains)
    for rd in roster_domains:
        if host == rd or host.endswith("." + rd):
            return True

    # Brand-name heuristic → controlled
    brand_token = _norm_token(company)
    if brand_token:
        host_token = _norm_domain_for_name_match(host)
        if brand_token in host_token:
            return True

    return False


def vader_label(analyzer: SentimentIntensityAnalyzer, text: str) -> Tuple[float, str]:
    """
    Return (compound, label) where label in {'positive','neutral','negative'}.
    """
    s = analyzer.polarity_scores(text or "")
    c = s.get("compound", 0.0)
    if c >= 0.05:
        lab = "positive"
    elif c <= -0.05:
        lab = "negative"
    else:
        lab = "neutral"
    return c, lab

def _hostname(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        return host.lower().replace("www.", "")
    except Exception:
        return ""

def _norm_token(s: str) -> str:
    # lowercase and keep only [a-z0-9]; collapse whitespace/punct
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())

def _norm_domain_for_name_match(host: str) -> str:
    # strip dots and hyphens before TLD awareness (simple heuristic)
    return "".join(ch for ch in host if ch.isalnum())

def load_controlled_domains_from_roster() -> set[str]:
    """
    Read domains from roster files and return hostnames treated as controlled.
    Accept columns like: domain, website, url, site, homepage (any case).
    """
    candidates = ROSTER_CANDIDATES
    domains: set[str] = set()
    wanted_cols = {"domain", "website", "url", "site", "homepage"}

    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path, newline="", encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                # Map header names to lowercase once
                lower_headers = [h.lower() for h in rdr.fieldnames or []]
                col_ix = [i for i, h in enumerate(lower_headers) if h in wanted_cols]

                for row in rdr:
                    # try each eligible column for a URL/domain
                    values = []
                    for k, v in row.items():
                        if k and k.lower() in wanted_cols and v:
                            values.append(str(v).strip())
                    for v in values:
                        host = _hostname(v if v.startswith(("http://", "https://")) else "http://" + v)
                        if host:
                            domains.add(host)
        except Exception as e:
            print(f"[WARN] Failed reading roster at {path}: {e}")
            continue

    return domains


# -----------------------
# Main processing
# -----------------------
def process_for_date(target_date: str):
    print(f"[INFO] Processing brand SERPs for {target_date} …")
    ensure_dirs()

    # Load canonical domains from roster
    company_domains = load_company_domains()

    # Fetch raw SERPs from S3
    url = S3_URL_TEMPLATE.format(date=target_date)
    raw = fetch_csv_from_s3(url)
    if raw is None or raw.empty:
        print(f"[WARN] No raw brand SERP data available for {target_date}. Nothing to write.")
        return

    # Normalize column names we expect
    # Expected raw columns:
    # prompt, company, position, title, link, displayed_link, snippet, thumbnail, favicon, redirect_link, rich_snippet, error_status
    for col in ["company", "position", "title", "link", "snippet"]:
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
        try:
            position = int(row.get("position", 0) or 0)
        except Exception:
            position = 0

        # Control classification
        controlled = classify_control(company, url, company_domains)

        # Sentiment: headline + snippet, unless controlled (then force positive if configured)
        joined = " ".join([title, snippet]).strip()
        _, label = vader_label(analyzer, joined)
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

    # Save row-level
    rows_df = pd.DataFrame(processed_rows)
    row_out_path = os.path.join(OUT_ROWS_DIR, f"{target_date}-brand-serps-rows.csv")
    rows_df.to_csv(row_out_path, index=False)
    print(f"[OK] Wrote row-level SERPs → {row_out_path}")

    # Aggregate per company
    agg = (
        rows_df
        .groupby("company", as_index=False)
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

    # Update rolling index (append/replace that date)
    if os.path.exists(OUT_ROLLUP):
        roll = pd.read_csv(OUT_ROLLUP)
        # Remove existing rows for this date
        roll = roll[roll["date"] != target_date]
        roll = pd.concat([roll, agg], ignore_index=True)
    else:
        roll = agg.copy()

    # Keep a stable column order
    cols = ["date", "company", "total", "controlled", "negative_serp", "neutral_serp", "positive_serp"]
    roll = roll[cols].sort_values(["date", "company"]).reset_index(drop=True)
    roll.to_csv(OUT_ROLLUP, index=False)
    print(f"[OK] Updated rolling index → {OUT_ROLLUP}")

def main():
    args = parse_args()
    date_str = get_target_date(args.date)
    process_for_date(date_str)

if __name__ == "__main__":
    main()
