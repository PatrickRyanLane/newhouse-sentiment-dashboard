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
  rosters/main-roster.csv (CEO, Company, CEO Alias, Website) -> primary source

Outputs
-------
Row-level processed SERPs (modal):
  data/processed_serps/{date}-ceo-serps-modal.csv

Per-CEO daily aggregate:
  data/processed_serps/{date}-ceo-serps-table.csv

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

# First day CEO SERPs exist
FIRST_AVAILABLE_DATE = dt.date(2025, 9, 15)

# Updated to use consolidated roster
MAIN_ROSTER_PATH = Path("rosters/main-roster.csv")

# Updated paths - all CEO SERP files consolidated in data/processed_serps
OUT_DIR_ROWS = Path("data/processed_serps")
OUT_DIR_DAILY = Path("data/processed_serps")
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

# Words/phrases to ignore for title-based sentiment classification
NEUTRALIZE_TITLE_TERMS = [
    r"\bflees\b",
    r"\bsavage\b",
    r"\brob\b",
    r"\bnicholas\s+lower\b",
    r"\bmad\s+money\b",
]
NEUTRALIZE_TITLE_RE = re.compile("|".join(NEUTRALIZE_TITLE_TERMS), flags=re.IGNORECASE)

# ------------------------ Small helpers -----------------------

def strip_neutral_terms_from_title(title: str) -> str:
    s = str(title or "")
    s = NEUTRALIZE_TITLE_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

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
        return pd.read_csv(text_or_path, encoding="utf-8-sig")
    except Exception:
        if isinstance(text_or_path, str) and "\n" in text_or_path:
            return pd.read_csv(io.StringIO(text_or_path), engine="python")
        return pd.read_csv(text_or_path, engine="python", encoding="utf-8-sig")

def fetch_csv_text(url: str, timeout=30):
    r = requests.get(url, timeout=timeout)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def load_roster_data():
    if not MAIN_ROSTER_PATH.exists():
        raise FileNotFoundError(f"Main roster not found: {MAIN_ROSTER_PATH}")

    df = read_csv_safely(MAIN_ROSTER_PATH)
    cols = {c.strip().lower(): c for c in df.columns}
    
    def col(*names):
        for name in names:
            for k, v in cols.items():
                if k == name.lower():
                    return v
        return None

    ceo_col = col("ceo")
    company_col = col("company")
    alias_col = col("ceo alias", "alias")
    website_col = col("website", "domain", "url")

    if not (ceo_col and company_col):
        raise ValueError("Main roster must have CEO and Company columns")

    ceo_to_company = {}
    for _, row in df.iterrows():
        ceo = str(row[ceo_col]).strip()
        company = str(row[company_col]).strip()
        if ceo and company and ceo != "nan" and company != "nan":
            ceo_to_company[ceo] = company

    alias_map = {}
    if alias_col:
        for _, row in df.iterrows():
            alias = str(row[alias_col]).strip()
            ceo = str(row[ceo_col]).strip()
            company = str(row[company_col]).strip()
            if alias and ceo and company and alias != "nan":
                alias_map[norm(alias)] = (ceo, company)

    for ceo, comp in ceo_to_company.items():
        alias_map.setdefault(norm(f"{ceo} {comp}"), (ceo, comp))

    controlled_domains = set()
    if website_col:
        for val in df[website_col].dropna().astype(str):
            val = val.strip()
            if val and val != "nan":
                try:
                    if not val.startswith(("http://", "https://")):
                        val = f"https://{val}"
                    parsed = urlparse(val)
                    host = (parsed.netloc or parsed.path or "").lower().strip()
                    host = host.replace("www.", "")
                    if host and "." in host:
                        controlled_domains.add(host)
                except Exception:
                    pass

    return alias_map, ceo_to_company, controlled_domains

# -------------------- Normalization & rules --------------------

def normalize_raw_columns(df: pd.DataFrame) -> pd.DataFrame:
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

def resolve_ceo_company(query_alias: str, alias_map, ceo_to_company):
    qn = norm(query_alias)
    if qn in alias_map:
        return alias_map[qn]

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

def classify_control(url: str, position, company: str, controlled_domains):
    try:
        parsed = urlparse(url or "")
        domain = (parsed.netloc or "").lower().replace("www.", "")
        path   = (parsed.path or "").lower()
    except Exception:
        domain, path = "", ""

    if any(d in domain for d in UNCONTROLLED_DOMAINS):
        return False

    if domain in controlled_domains:
        return True

    comp_simple = simplify_company(company)
    if comp_simple and comp_simple.replace(" ", "") in domain.replace(".", ""):
        return True

    if any(s in domain for s in CONTROLLED_SOCIAL_DOMAINS):
        return True

    if any(k in path for k in CONTROLLED_PATH_KEYWORDS):
        return True

    return False

def vader_label(analyzer, row):
    raw_text = (row.get("title") or "").strip()
    text = strip_neutral_terms_from_title(raw_text)
    if not text:
        return "neutral"
    score = analyzer.polarity_scores(text)["compound"]
    if score >= 0.05:
        return "positive"
    if score <= -0.15:
        return "negative"
    return "neutral"

# ---------------------------- Core ----------------------------

def process_one_date(date_str: str, alias_map, ceo_to_company, controlled_domains):
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

    mapped = base.copy()
    mapped[["ceo", "company"]] = mapped.apply(
        lambda r: pd.Series(resolve_ceo_company(r["query_alias"], alias_map, ceo_to_company)),
        axis=1,
    )

    analyzer = SentimentIntensityAnalyzer()

    mapped["sentiment"] = mapped.apply(lambda r: vader_label(analyzer, r), axis=1)
    mapped["controlled"] = mapped.apply(lambda r: classify_control(r["url"], r["position"], r["company"], controlled_domains), axis=1)

    mapped.loc[mapped["controlled"] == True, "sentiment"] = "positive"

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
    rows_path = OUT_DIR_ROWS / f"{date_str}-ceo-serps-modal.csv"
    rows_df.to_csv(rows_path, index=False)
    print(f"[write] {rows_path}")

    def majority_company(series):
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

    day_path = OUT_DIR_DAILY / f"{date_str}-ceo-serps-table.csv"
    ag.to_csv(day_path, index=False)
    print(f"[write] {day_path}")

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

def backfill(start: str, end: str, alias_map, ceo_to_company, controlled_domains):
    d0 = dt.date.fromisoformat(start)
    d1 = dt.date.fromisoformat(end)
    if d0 > d1:
        d0, d1 = d1, d0
    d = d0
    while d <= d1:
        process_one_date(d.isoformat(), alias_map, ceo_to_company, controlled_domains)
        d += dt.timedelta(days=1)

def main():
    ap = argparse.ArgumentParser(description="Process CEO SERPs with sentiment/control.")
    ap.add_argument("--date", help="Process a single date (YYYY-MM-DD).")
    ap.add_argument("--backfill", nargs=2, metavar=("START", "END"),
                    help="Process an inclusive date range (YYYY-MM-DD YYYY-MM-DD).")
    args = ap.parse_args()

    alias_map, ceo_to_company, controlled_domains = load_roster_data()

    if args.date:
        process_one_date(args.date, alias_map, ceo_to_company, controlled_domains)
    elif args.backfill:
        backfill(args.backfill[0], args.backfill[1], alias_map, ceo_to_company, controlled_domains)
    else:
        today = dt.date.today()
        for cand in (today, today - dt.timedelta(days=1)):
            if process_one_date(cand.isoformat(), alias_map, ceo_to_company, controlled_domains):
                break

if __name__ == "__main__":
    sys.exit(main() or 0)
