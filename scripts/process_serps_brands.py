#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Process BRAND SERPs for a given date:
  1) Download raw rows from S3
  2) Classify CONTROL vs UNCONTROLLED (using roster + known hosts)
  3) Normalize sentiment (fallbacks for messy data)
  4) Write:
       - data/serp_rows/{date}-brand-serps-rows.csv
       - data/processed_serps/{date}-brand-serps-processed.csv
       - data/serps/brand_serps_daily.csv (upsert for date)
Usage:
    python scripts/process_serps_brands.py --date 2025-09-24
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.parse import urlparse

import requests

# ----------------------------
# Paths & constants
# ----------------------------
ROSTER_CSV = "data/roster.csv"

OUT_ROWS_DIR = "data/serp_rows"
OUT_DAILY_DIR = "data/processed_serps"
OUT_ROLLUP = "data/serps/brand_serps_daily.csv"

# Raw CSV location (produced by the SERP crawler)
S3_URL_TEMPLATE = "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-brand-serps.csv"

# If controlled, force sentiment to positive (matches CEO rule)
FORCE_POSITIVE_IF_CONTROLLED = True

# Hosts that are always CONTROLLED for brands (exact host or any subdomain)
CONTROLLED_HOSTS = {
    "play.google.com",     # Google Play
    "apps.apple.com",      # Apple App Store
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
}

# ----------------------------
# Paths
# ----------------------------
def rows_path_for_date(date_str: str) -> Path:
    return Path(OUT_ROWS_DIR) / f"{date_str}-brand-serps-rows.csv"

def processed_path_for_date(date_str: str) -> Path:
    return Path(OUT_DAILY_DIR) / f"{date_str}-brand-serps-processed.csv"

# ----------------------------
# Helpers
# ----------------------------
def ensure_dirs():
    Path(OUT_ROWS_DIR).mkdir(parents=True, exist_ok=True)
    Path(OUT_DAILY_DIR).mkdir(parents=True, exist_ok=True)
    Path(Path(OUT_ROLLUP).parent).mkdir(parents=True, exist_ok=True)

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="Date in YYYY-MM-DD (UTC) to process")
    return ap.parse_args()

def extract_domain(url: str) -> str:
    """Return normalized host for a URL (lowercased, strip www and port)."""
    try:
        u = urlparse(url or "")
        host = (u.netloc or "").lower()
        if ":" in host:
            host = host.split(":", 1)[0]
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""

def _slugify_name(name: str) -> str:
    """Lowercase, remove non-alnum, drop common suffixes for fuzzy contains."""
    s = (name or "").lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    for token in ("inc", "corp", "corporation", "company", "co", "ltd", "plc", "the"):
        s = s.replace(token, "")
    return s

def load_company_domains_from_roster(path: str = ROSTER_CSV) -> Dict[str, str]:
    """
    Roster needs columns:
      - company (display name)
      - domain  (canonical domain) OR website
    Returns dict keyed by lowercase company -> bare domain (no scheme, no www).
    """
    out: Dict[str, str] = {}
    p = Path(path)
    if not p.exists():
        print(f"[WARN] Roster not found at {path}; continuing without roster domain matches.", file=sys.stderr)
        return out

    with p.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = (row.get("company") or row.get("Company") or "").strip()
            website = (row.get("domain") or row.get("Domain") or row.get("website") or row.get("Website") or "").strip()
            if not company or not website:
                continue
            if website.startswith("http://") or website.startswith("https://"):
                host = extract_domain(website)
            else:
                host = website.lower()
                if host.startswith("www."):
                    host = host[4:]
            if host:
                out[company.lower()] = host
    return out

def classify_control(company: str, url: str, company_domains: Dict[str, str]) -> bool:
    """
    CONTROL rules:
      1) CONTROLLED_HOSTS (incl. subdomains) -> controlled
      2) Roster domain exact or suffix match -> controlled
      3) Company slug contained in domain (dots removed) -> controlled
      Else -> uncontrolled
    """
    host = extract_domain(url)
    if not host:
        return False

    # 1) Explicit hosts
    for good in CONTROLLED_HOSTS:
        if host == good or host.endswith("." + good):
            return True

    # 2) Canonical domain match
    bd = company_domains.get((company or "").lower())
    if bd:
        bd = bd.lower()
        if host == bd or host.endswith("." + bd):
            return True

    # 3) Brand slug containment
    slug = _slugify_name(company)
    if slug:
        if slug in host.replace(".", ""):
            return True

    return False

@dataclass
class RowResult:
    company: str
    url: str
    sentiment: str   # 'positive' | 'neutral' | 'negative'
    controlled: bool

@dataclass
class BrandDayAgg:
    date: str
    company: str
    total: int
    controlled: int
    negative_serp: int
    neutral_serp: int
    positive_serp: int

# ----------------------------
# S3 fetch + robust CSV parsing
# ----------------------------
def fetch_csv_from_s3(url: str) -> List[dict] | None:
    """Download CSV from S3 and parse into a list[dict]. Returns None if missing."""
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200 or not r.text.strip():
            print(f"[WARN] Could not fetch {url} — HTTP {r.status_code}", file=sys.stderr)
            return None
        # Parse with csv.DictReader to be tolerant of SerpAPI glitches
        buf = io.StringIO(r.text)
        reader = csv.DictReader(buf)
        rows = [row for row in reader]
        if not rows:
            print(f"[WARN] Empty CSV at {url}", file=sys.stderr)
            return None
        return rows
    except Exception as e:
        print(f"[WARN] Could not fetch {url} — {e}", file=sys.stderr)
        return None

# ----------------------------
# Core transforms
# ----------------------------
def normalize_sentiment(row: dict) -> str:
    """
    Use string label if present; otherwise fall back to numeric polarity; default neutral.
    """
    sent = (row.get("sentiment") or row.get("label") or row.get("sent") or "").strip().lower()
    if sent in ("positive", "neutral", "negative"):
        return sent
    # Try numeric polarity (-1..1 or -1/0/1)
    for key in ("polarity", "score", "sentiment_score"):
        val = row.get(key)
        if val is None or str(val).strip() == "":
            continue
        try:
            s = float(str(val).strip())
            return "positive" if s > 0 else ("negative" if s < 0 else "neutral")
        except Exception:
            pass
    return "neutral"

def make_row_results(raw_rows: List[dict], company_domains: Dict[str, str]) -> List[RowResult]:
    out: List[RowResult] = []
    for r in raw_rows:
        company = (r.get("company") or r.get("brand") or r.get("Company") or "").strip()
        if not company:
            # Skip rows with no brand/company
            continue
        url = (r.get("url") or r.get("link") or "").strip()
        sent = normalize_sentiment(r)
        is_ctrl = classify_control(company, url, company_domains)
        if is_ctrl and FORCE_POSITIVE_IF_CONTROLLED:
            sent = "positive"
        out.append(RowResult(company=company, url=url, sentiment=sent, controlled=is_ctrl))
    return out

def aggregate_by_company(date_str: str, rows: Iterable[RowResult]) -> List[BrandDayAgg]:
    counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {
        "total": 0, "controlled": 0, "negative": 0, "neutral": 0, "positive": 0
    })
    for r in rows:
        key = r.company.strip()
        if not key:
            continue
        c = counts[key]
        c["total"] += 1
        if r.controlled:
            c["controlled"] += 1
        if r.sentiment in ("positive", "neutral", "negative"):
            c[r.sentiment] += 1
        else:
            c["neutral"] += 1

    out: List[BrandDayAgg] = []
    for comp, c in counts.items():
        out.append(BrandDayAgg(
            date=date_str, company=comp, total=c["total"], controlled=c["controlled"],
            negative_serp=c["negative"], neutral_serp=c["neutral"], positive_serp=c["positive"]
        ))
    out.sort(key=lambda a: a.company.lower())
    return out

# ----------------------------
# Writers
# ----------------------------
def write_rows_csv(date_str: str, rows: List[RowResult]) -> Path:
    out_path = rows_path_for_date(date_str)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "company", "url", "sentiment", "controlled"])
        for r in rows:
            w.writerow([date_str, r.company, r.url, r.sentiment, "true" if r.controlled else "false"])
    return out_path

def write_processed_for_date(date_str: str, rows: List[BrandDayAgg]) -> Path:
    out_path = processed_path_for_date(date_str)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "company", "total", "controlled", "negative_serp", "neutral_serp", "positive_serp"])
        for r in rows:
            w.writerow([r.date, r.company, r.total, r.controlled, r.negative_serp, r.neutral_serp, r.positive_serp])
    return out_path

def update_rollup(date_str: str, day_rows: List[BrandDayAgg]) -> Path:
    """
    Upsert this date's rows into OUT_ROLLUP, replacing any existing rows for date_str.
    """
    roll = Path(OUT_ROLLUP)
    header = ["date", "company", "total", "controlled", "negative_serp", "neutral_serp", "positive_serp"]
    existing: List[List[str]] = []

    if roll.exists():
        with roll.open(newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            try:
                first = next(reader)
            except StopIteration:
                first = header
            if [h.strip().lower() for h in first] == [h.strip().lower() for h in header]:
                existing_rows = list(reader)
            else:
                # header mismatch: keep all rows, treat first line as data
                existing_rows = [first] + list(reader)

        for row in existing_rows:
            if row and row[0] != date_str:
                existing.append(row)

    for r in day_rows:
        existing.append([
            r.date, r.company, str(r.total), str(r.controlled),
            str(r.negative_serp), str(r.neutral_serp), str(r.positive_serp)
        ])

    existing.sort(key=lambda row: (row[0], row[1].lower()))

    roll.parent.mkdir(parents=True, exist_ok=True)
    with roll.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(existing)
    return roll

# ----------------------------
# Pipeline (S3 -> rows -> processed -> rollup)
# ----------------------------
def process_for_date(target_date: str) -> int:
    ensure_dirs()
    # 1) Fetch raw CSV from S3
    url = S3_URL_TEMPLATE.format(date=target_date)
    raw = fetch_csv_from_s3(url)
    if raw is None:
        print(f"[WARN] No raw brand SERP data available for {target_date}. Nothing to write.", file=sys.stderr)
        return 0  # non-fatal so workflows can continue/skip

    # 2) Classify + normalize
    company_domains = load_company_domains_from_roster()
    rows = make_row_results(raw, company_domains)

    # 3) Write row-level output
    out_rows = write_rows_csv(target_date, rows)
    print(f"[OK] wrote rows {out_rows}")

    # 4) Aggregate + write processed
    aggs = aggregate_by_company(target_date, rows)
    out_daily = write_processed_for_date(target_date, aggs)
    print(f"[OK] wrote processed {out_daily}")

    # 5) Update rollup
    out_roll = update_rollup(target_date, aggs)
    print(f"[OK] updated rollup {out_roll}")

    return 0

# ----------------------------
# Main
# ----------------------------
def main() -> int:
    args = parse_args()
    date_str = args.date.strip()
    # Validate date early
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print(f"ERROR: --date must be YYYY-MM-DD, got {date_str}", file=sys.stderr)
        return 2
    return process_for_date(date_str)

if __name__ == "__main__":
    sys.exit(main())
