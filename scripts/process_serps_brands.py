#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Process BRAND SERP rows for a given date into per-brand daily aggregates,
and update the rolling daily CSV used by the dashboards.

Usage:
    python scripts/process_serps_brands.py --date 2025-09-23
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.parse import urlparse
import re

# ----------------------------
# Paths & constants
# ----------------------------
ROSTER_CSV = "data/roster.csv"

OUT_ROWS_DIR = "data/serp_rows"
OUT_DAILY_DIR = "data/processed_serps"
OUT_ROLLUP = "data/serps/brand_serps_daily.csv"

# If controlled, force sentiment to positive (matches your CEO rule change)
FORCE_POSITIVE_IF_CONTROLLED = True

# Hosts that are always considered CONTROLLED for brands (matches on exact host or any subdomain)
CONTROLLED_HOSTS = {
    "play.google.com",     # Google Play
    "apps.apple.com",      # Apple App Store
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
}

# Input rows file pattern (produced earlier in your pipeline)
# Example: data/serp_rows/2025-09-23-brand-serps-rows.csv
def rows_path_for_date(date_str: str) -> Path:
    return Path(OUT_ROWS_DIR) / f"{date_str}-brand-serps-rows.csv"

# Output processed (per-day) file pattern
# Example: data/processed_serps/2025-09-23-brand-serps-processed.csv
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
    """
    Return a normalized host for a URL, lowercased, with common prefixes stripped.
    Example:
        https://m.facebook.com/foo -> m.facebook.com
        https://www.example.com/bar -> example.com
    """
    try:
        u = urlparse(url or "")
        host = (u.netloc or "").lower()
        # strip port if present
        if ":" in host:
            host = host.split(":", 1)[0]
        # strip leading www.
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def _slugify_name(name: str) -> str:
    """
    Normalize a company/brand name for fuzzy domain-contains checks:
    - lowercase
    - remove non-alphanumeric
    - drop very common company suffixes
    """
    s = (name or "").lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    for token in ("inc", "corp", "corporation", "company", "co", "ltd", "plc", "the"):
        s = s.replace(token, "")
    return s


def load_company_domains_from_roster(path: str = ROSTER_CSV) -> Dict[str, str]:
    """
    Expecting roster.csv with columns including at least:
      - company  (display name)
      - domain   (canonical domain like 'example.com')
        (or 'website' which will be normalized to the bare domain)
    Returns: {'acme': 'example.com', ...} keyed by lowercased company
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
            # Normalize website -> bare domain
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
    BRAND CONTROL rules (order matters):
      1) CONTROLLED_HOSTS -> controlled (e.g., facebook.com, apps.apple.com, play.google.com, etc.)
      2) Roster domain match: if host equals or endswith the company's canonical domain
      3) Brand-in-domain: if normalized company slug is contained in the host (dots removed)
      Else -> uncontrolled
    """
    domain = extract_domain(url)
    if not domain:
        return False

    host = domain  # already normalized

    # 1) Explicit controlled hosts (allow subdomains)
    for good in CONTROLLED_HOSTS:
        if host == good or host.endswith("." + good):
            return True

    # 2) Canonical domain from roster
    brand_domain = company_domains.get((company or "").lower())
    if brand_domain:
        bd = brand_domain.lower()
        if host == bd or host.endswith("." + bd):
            return True

    # 3) Brand name is contained in the domain (remove dots for a compact compare)
    brand_slug = _slugify_name(company)
    if brand_slug:
        compact_host = host.replace(".", "")
        if brand_slug and brand_slug in compact_host:
            return True

    return False


@dataclass
class RowResult:
    company: str
    url: str
    sentiment: str  # 'positive' | 'neutral' | 'negative'
    controlled: bool


def read_rows_for_date(date_str: str, company_domains: Dict[str, str]) -> List[RowResult]:
    """
    Read the raw rows CSV for the date and classify control per row.
    The rows file typically includes at least: company,url,sentiment (or label)
    """
    in_path = rows_path_for_date(date_str)
    if not in_path.exists():
        raise FileNotFoundError(f"SERP rows not found: {in_path}")

    results: List[RowResult] = []
    with in_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            company = (r.get("company") or r.get("brand") or "").strip()
            if not company:
                # Some pipelines write company under 'Company'
                company = (r.get("Company") or "").strip()
            url = (r.get("url") or r.get("link") or "").strip()

            # sentiment field can vary; normalize to lower-case 'positive'/'neutral'/'negative'
            sent = (r.get("sentiment") or r.get("label") or r.get("sent") or "").strip().lower()
            if sent not in ("positive", "neutral", "negative"):
                # try numeric polarity if present (e.g., -1/0/1)
                try:
                    s = float(r.get("polarity", ""))
                    sent = "positive" if s > 0 else ("negative" if s < 0 else "neutral")
                except Exception:
                    # default to neutral if unknown
                    sent = "neutral"

            # classify control
            is_ctrl = classify_control(company, url, company_domains)

            # optional: force controlled items to positive sentiment
            if is_ctrl and FORCE_POSITIVE_IF_CONTROLLED:
                sent = "positive"

            results.append(RowResult(company=company, url=url, sentiment=sent, controlled=is_ctrl))
    return results


@dataclass
class BrandDayAgg:
    date: str
    company: str
    total: int
    controlled: int
    negative_serp: int
    neutral_serp: int
    positive_serp: int


def aggregate_by_company(date_str: str, rows: Iterable[RowResult]) -> List[BrandDayAgg]:
    counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {
        "total": 0,
        "controlled": 0,
        "negative": 0,
        "neutral": 0,
        "positive": 0,
    })

    for r in rows:
        key = r.company.strip()
        if not key:
            continue
        c = counts[key]
        c["total"] += 1
        if r.controlled:
            c["controlled"] += 1
        # count sentiment
        if r.sentiment in ("positive", "neutral", "negative"):
            c[r.sentiment] += 1
        else:
            c["neutral"] += 1  # fallback

    out: List[BrandDayAgg] = []
    for comp, c in counts.items():
        out.append(
            BrandDayAgg(
                date=date_str,
                company=comp,
                total=c["total"],
                controlled=c["controlled"],
                negative_serp=c["negative"],
                neutral_serp=c["neutral"],
                positive_serp=c["positive"],
            )
        )
    # stable order: by company
    out.sort(key=lambda a: a.company.lower())
    return out


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
    existing: List[List[str]] = []
    header = ["date", "company", "total", "controlled", "negative_serp", "neutral_serp", "positive_serp"]

    if roll.exists():
        with roll.open(newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            try:
                first = next(reader)
            except StopIteration:
                first = header
            if [h.strip().lower() for h in first] != [h.strip().lower() for h in header]:
                # header mismatch: keep existing as data if any
                existing_rows = [first] + list(reader)
            else:
                existing_rows = list(reader)

        for row in existing_rows:
            if not row:
                continue
            if row[0] != date_str:
                existing.append(row)

    # add new rows
    for r in day_rows:
        existing.append([r.date, r.company, str(r.total), str(r.controlled),
                         str(r.negative_serp), str(r.neutral_serp), str(r.positive_serp)])

    # sort by date then company (date ascending)
    def _sort_key(row: List[str]) -> Tuple[str, str]:
        return (row[0], row[1].lower())

    existing.sort(key=_sort_key)

    roll.parent.mkdir(parents=True, exist_ok=True)
    with roll.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(existing)

    return roll


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    args = parse_args()
    date_str = args.date.strip()
    # Validate date format early
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print(f"ERROR: --date must be YYYY-MM-DD, got {date_str}", file=sys.stderr)
        return 2

    ensure_dirs()
    company_domains = load_company_domains_from_roster()

    # Read raw rows and classify control/sentiment
    rows = read_rows_for_date(date_str, company_domains)

    # Aggregate by company
    aggs = aggregate_by_company(date_str, rows)

    # Write per-day processed
    out_daily = write_processed_for_date(date_str, aggs)
    print(f"[OK] wrote {out_daily}")

    # Update rollup
    out_roll = update_rollup(date_str, aggs)
    print(f"[OK] updated rollup {out_roll}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
