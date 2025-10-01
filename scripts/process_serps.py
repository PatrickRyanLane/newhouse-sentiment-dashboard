#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Process daily CEO SERPs with sentiment & control classification,
and write both row-level and aggregate outputs for the dashboard.

Inputs
------
Raw S3 CSV per day:
  https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-ceo-serps.csv
  NOTE: In raw files, the column named "company" holds the alias text
        (e.g., "Tim Cook Apple"), not the canonical company.

Local maps:
  data/ceo_aliases.csv             (alias, ceo, company)  -> primary
  data/roster.csv or data/ceo_companies.csv (ceo, company [+ optional domain/website/url]) -> fallback + controlled domains

Outputs
-------
Row-level processed SERPs (modal):
  data_ceos/serp_rows/{date}-ceo-serps-rows.csv

Per-CEO daily aggregate:
  data_ceos/processed_serps/{date}-ceo-serps-processed.csv

Rolling index (dashboard table & SERP trend):
  data/serps/ceo_serps_daily.csv

Usage
-----
python scripts/process_serps.py --date 2025-09-17
python scripts/process_serps.py --backfill 2025-09-15 2025-09-30
(no args) -> tries today, then yesterday (skips if before FIRST_AVAILABLE_DATE)
"""

from __future__ import annotations
import argparse
import io
import re
import sys
import datetime as dt
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# --------------------------- Config ---------------------------

S3_TEMPLATE = "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-ceo-serps.csv"

# First day you said CEO SERPs exist
FIRST_AVAILABLE_DATE = dt.date(2025, 9, 15)

ALIASES_PATH = Path("data/ceo_aliases.csv")            # columns: alias, ceo, company
ROSTER_CANDIDATES = [Path("data/roster.csv"), Path("data/ceo_companies.csv")]

OUT_DIR_ROWS = Path("data_ceos/serp_rows")
OUT_DIR_DAILY = Path("data_ceos/processed_serps")
INDEX_DIR = Path("data/serps")
INDEX_PATH = INDEX_DIR / "ceo_serps_daily.csv"

for p in (OUT_DIR_ROWS, OUT_DIR_DAILY, INDEX_DIR):
    p.mkdir(parents=True, exist_ok=True)

# Control rules
CONTROLLED_SOCIAL_DOMAINS = {
    "facebook.com", "linkedin.com", "instagram.com", "twitter.com", "x.com"
}
CONTROLLED_PATH_KEYWORDS = {
    "/leadership/", "/about/", "/governance/", "/team/", "/investors/", "/board-of-directors"
}
UNCONTROLLED_DOMAINS = {
    "wikipedia.org", "youtube.com", "youtu.be", "tiktok.com"
}

# ------------------------ Small helpers -----------------------

def norm(s: str) -> str:
    s = str(s or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

LEGAL_SUFFIXES = {"inc", "inc.", "corp", "co", "co.", "llc", "plc", "ltd", "ltd.", "ag", "sa", "nv"}

def simplify_company(s: str) -> str:
    toks = norm(s).split()
    toks = [t for t in toks if t not in LEGAL_SUFFIXES]
    return " ".join(toks)

def read_csv_safely(text_or_path):
    try:
        if isinstance(text_or_path, str) and "\n" in text_or_path:
            return pd.read_csv(io.StringIO(text_or_path))
        return pd.read_csv(text_or_path)
    except Exception:
        if isinstance(text_or_path, str) and "\n" in text_or_path:
            return pd.read_csv(io.StringIO(text_or_path), engine="python")
        return pd.read_csv(text_or_path, engine="python")

def fetch_csv_text(url: str, timeout=30):
    r = requests.get(url, timeout=timeout)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def load_roster_map() -> dict[str, str]:
    """Return {CEO -> Company} from the first roster-like file we can read."""
    for p in ROSTER_CANDIDATES:
        if p.exists():
            df = read_csv_safely(p)
            cols = {c.lower(): c for c in df.columns}
            ceo_c = next((cols[c] for c in cols if c in ("ceo", "name", "person")), None)
            comp_c = next((cols[c] for c in cols if c in ("company", "brand", "org", "employer")), None)
            if ceo_c and comp_c:
                return {str(r[ceo_c]).strip(): str(r[comp_c]).strip() for _, r in df.iterrows()}
    return {}

def load_alias_index():
    """Build alias map {alias_norm -> (CEO, Company)} and CEO->Company map."""
    alias_map: dict[str, tuple[str, str]] = {}
    ceo_to_company = load_roster_map()

    if ALIASES_PATH.exists():
        a = read_csv_safely(ALIASES_PATH)
        need = {"alias", "ceo", "company"}
        have = {c.lower() for c in a.columns}
        if not need.issubset(have):
            raise SystemExit("data/ceo_aliases.csv must have headers: alias, ceo, company")
        cols = {c.lower(): c for c in a.columns}
        for _, r in a.iterrows():
            alias = str(r[cols["alias"]]).strip()
            ceo = str(r[cols["ceo"]]).strip()
            comp = str(r[cols["company"]]).strip()
            if alias:
                alias_map[norm(alias)] = (ceo, comp)

    # Ensure basic "ceo + company" aliases exist
    for ceo, comp in ceo_to_company.items():
        alias_map.setdefault(norm(f"{ceo} {comp}"), (ceo, comp))

    return alias_map, ceo_to_company

def load_controlled_domains_from_roster() -> set[str]:
    """
    Read domains from roster files and return hostnames treated as controlled.
    Accept columns like: domain, website, url, site, homepage.
    """
    candidates = ROSTER_CANDIDATES
    domains: set[str] = set()

    def norm_host(val: str) -> str:
        try:
            u = urlparse(val if val.startswith("http") else f"https://{val}")
            host = (u.netloc or u.path or "").lower().strip()
            host = host.replace("www.", "")
            return host
        except Exception:
            return ""

    for p in candidates:
        if not p.exists():
            continue
        df = read_csv_safely(p)
        cols = {c.lower(): c for c in df.columns}
        for hdr in ("domain", "website", "url", "site", "homepage"):
            if hdr in cols:
                for v in df[cols[hdr]].dropna().astype(str):
                    host = norm_host(v.strip())
                    if host and "." in host:
                        domains.add(host)
                break  # only use the first matching column

    return domains

# -------------------- Normalization & rules --------------------

def normalize_raw_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize raw fields:
      query_alias  <- raw 'company' (alias text: 'Tim Cook Apple')
      title        <- 'title'/'page_title'/'result'
      url          <- 'url'/'link'
      position     <- 'position'/'rank'/'pos'
      snippet      <- 'snippet'/'description'
    """
    cols = {c.lower(): c for c in df.columns}
    q_c   = cols.get("company") or cols.get("query") or cols.get("search")
    t_c   = cols.get("title") or cols.get("page_title") or cols.get("result")
    u_c   = cols.get("url") or cols.get("link")
    p_c   = cols.get("position") or cols.get("rank") or cols.get("pos")
    sn_c  = cols.get("snippet") or cols.get("description")

    out = pd.DataFrame()
    out["query_alias"] = df[q_c].astype(str).str.strip() if q_c else ""
    out["title"]       = df[t_c].astype(str).str.strip() if t_c else ""
    out["url"]         = df[u_c].astype(str).str.strip() if u_c else ""
    out["position"]    = pd.to_numeric(df[p_c], errors="coerce") if p_c else pd.Series([None]*len(df))
    out["snippet"]     = df[sn_c].astype(str).str.strip() if sn_c else ""
    return out

def resolve_ceo_company(query_alias: str, alias_map: dict[str, tuple[str,str]], ceo_to_company: dict[str,str]) -> tuple[str,str]:
    qn = norm(query_alias)
    if qn in alias_map:
        return alias_map[qn]

    # Fallback: CEO + simplified company tokens all present in alias
    best = None
    best_score = 0
    for ceo, comp in ceo_to_company.items():
        tokens = set(f"{norm(ceo)} {simplify_company(comp)}".split())
        if tokens.issubset(set(qn.split())):
            score = len(tokens)
            if score > best_score:
                best = (ceo, comp)
                best_score = score
    return best if best else ("", "")

def classify_control(url: str, position, company: str, controlled_domains: set[str]) -> bool:
    """
    Return True if URL is 'controlled' by the brand:
      - Host is in controlled_domains (from roster)
      - Company name appears within the hostname (simplified)
      - Known controlled social (FB/LI/IG/Twitter/X)
      - Governance/about-like paths
    Always uncontrolled if host is in UNCONTROLLED_DOMAINS (Wikipedia, YouTube, TikTok).
    """
    try:
        parsed = urlparse(url or "")
        domain = (parsed.netloc or "").lower().replace("www.", "")
        path   = (parsed.path or "").lower()
    except Exception:
        domain, path = "", ""

    # Always uncontrolled
    if any(d in domain for d in UNCONTROLLED_DOMAINS):
        return False

    # Domain from roster
    if domain in controlled_domains:
        return True

    # Company in domain (simplified)
    comp_simple = simplify_company(company)
    if comp_simple and comp_simple.replace(" ", "") in domain.replace(".", ""):
        return True

    # Controlled social
    if any(s in domain for s in CONTROLLED_SOCIAL_DOMAINS):
        return True

    # Controlled paths
    if any(k in path for k in CONTROLLED_PATH_KEYWORDS):
        return True

    return False

def vader_label(analyzer: SentimentIntensityAnalyzer, row) -> str:
    # Only calculate sentiment on TITLE (ignore snippet)
    text = (row.get("title") or "").strip()
    if not text:
        return "neutral"
    score = analyzer.polarity_scores(text)["compound"]
    if score >= 0.05:
        return "positive"
    if score <= -0.15:
        return "negative"
    return "neutral"

# ---------------------------- Core ----------------------------

def process_one_date(date_str: str, alias_map, ceo_to_company):
    day = dt.date.fromisoformat(date_str)
    if day < FIRST_AVAILABLE_DATE:
        print(f"[skip] {date_str} < first available ({FIRST_AVAILABLE_DATE})")
        return None

    url = S3_TEMPLATE.format(date=date_str)
    print(f"[fetch] {url}")
    text = fetch_csv_text(url)
    if text is None:
        print(f"[missing] No S3 file for {date_str}")
        return None

    raw = read_csv_safely(text)
    base = normalize_raw_columns(raw)

    # Map alias -> (CEO, Company)
    mapped = base.copy()
    mapped[["ceo", "company"]] = mapped.apply(
        lambda r: pd.Series(resolve_ceo_company(r["query_alias"], alias_map, ceo_to_company)),
        axis=1,
    )

    # Sentiment + Control per row
    analyzer = SentimentIntensityAnalyzer()

# Neutralize specific terms that should not count as negative
for term in [
    "savage", "flees", "rob", "cancer",
    "nicholas lower", "mad money"
]:
    analyzer.lexicon[term.lower()] = 0.0

    controlled_domains = load_controlled_domains_from_roster()

    mapped["sentiment"] = mapped.apply(lambda r: vader_label(analyzer, r), axis=1)
    mapped["controlled"] = mapped.apply(lambda r: classify_control(r["url"], r["position"], r["company"], controlled_domains), axis=1)

    # Controlled -> positive (override)
    mapped.loc[mapped["controlled"] == True, "sentiment"] = "positive"

    # ---- Row-level output (for modal) ----
    rows_df = pd.DataFrame({
        "date":      date_str,
        "ceo":       mapped["ceo"],
        "company":   mapped["company"],
        "title":     mapped["title"],
        "url":       mapped["url"],
        "position":  mapped["position"],
        "snippet":   mapped["snippet"],
        "sentiment": mapped["sentiment"],
        "controlled":mapped["controlled"],
    })
    rows_path = OUT_DIR_ROWS / f"{date_str}-ceo-serps-rows.csv"
    rows_df.to_csv(rows_path, index=False)
    print(f"[write] {rows_path}")

    # ---- Per-CEO aggregate for the day ----
    # Choose the most frequent non-empty company name per CEO for display
    def majority_company(series: pd.Series) -> str:
        s = pd.Series(series).replace("", pd.NA).dropna()
        if s.empty:
            return ""
        return s.mode().iloc[0]

    ag = mapped.groupby("ceo", dropna=False).agg(
        total=("sentiment", "size"),
        controlled=("controlled", "sum"),
        negative_serp=("sentiment", lambda s: (s == "negative").sum()),
        neutral_serp=("sentiment",  lambda s: (s == "neutral").sum()),
        positive_serp=("sentiment", lambda s: (s == "positive").sum()),
        company=("company", majority_company),
    ).reset_index()
    ag.insert(0, "date", date_str)

    # Save per-day aggregate
    day_path = OUT_DIR_DAILY / f"{date_str}-ceo-serps-processed.csv"
    ag.to_csv(day_path, index=False)
    print(f"[write] {day_path}")

    # ---- Update rolling index ----
    if INDEX_PATH.exists():
        idx = read_csv_safely(INDEX_PATH)
        idx = idx[idx["date"] != date_str]
        idx = pd.concat([idx, ag], ignore_index=True)
    else:
        idx = ag

    idx["date"] = pd.to_datetime(idx["date"], errors="coerce")
    idx = idx.sort_values(["date", "ceo"]).reset_index(drop=True)
    idx["date"] = idx["date"].dt.strftime("%Y-%m-%d")
    idx.to_csv(INDEX_PATH, index=False)
    print(f"[update] {INDEX_PATH} ({len(idx)} rows total)")

    return day_path

def backfill(start: str, end: str, alias_map, ceo_to_company):
    d0 = dt.date.fromisoformat(start)
    d1 = dt.date.fromisoformat(end)
    if d0 > d1:
        d0, d1 = d1, d0
    d = d0
    while d <= d1:
        process_one_date(d.isoformat(), alias_map, ceo_to_company)
        d += dt.timedelta(days=1)

# ---------------------------- CLI ----------------------------

def main():
    ap = argparse.ArgumentParser(description="Process CEO SERPs with sentiment/control and write index + row-level outputs.")
    ap.add_argument("--date", help="Process a single date (YYYY-MM-DD).")
    ap.add_argument("--backfill", nargs=2, metavar=("START", "END"),
                    help="Process an inclusive date range (YYYY-MM-DD YYYY-MM-DD).")
    args = ap.parse_args()

    alias_map, ceo_to_company = load_alias_index()

    if args.date:
        process_one_date(args.date, alias_map, ceo_to_company)
    elif args.backfill:
        backfill(args.backfill[0], args.backfill[1], alias_map, ceo_to_company)
    else:
        today = dt.date.today()
        for cand in (today, today - dt.timedelta(days=1)):
            if process_one_date(cand.isoformat(), alias_map, ceo_to_company):
                break

if __name__ == "__main__":
    sys.exit(main() or 0)
