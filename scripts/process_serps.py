#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build daily CEO SERP aggregates and a rolling index for the dashboard.

Inputs
------
- Raw daily CSV on S3:
    https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-ceo-serps.csv
  NOTE: In these raw files, the column named "company" contains the search query
        "CEO Company" (e.g., "Tim Cook Apple"), not the canonical company name.

- data/ceo_aliases.csv  (alias,ceo,company)
    Maps the raw query string ("Tim Cook Apple") to canonical CEO + Company.

- data/roster.csv OR data/ceo_companies.csv  (ceo,company)
    Fallback map used to auto-generate "{ceo} {company}" aliases.

Outputs
-------
- data_ceos/processed_serps/{date}-ceo-serps-processed.csv
- data/serps/ceo_serps_daily.csv   (rolling index used by the dashboard)

Usage
-----
python scripts/process_serps.py --date 2025-09-15
python scripts/process_serps.py --backfill 2025-09-15 2025-09-30
(no args) -> tries today, then yesterday
"""

from __future__ import annotations
import argparse
import io
import re
import sys
import datetime as dt
from pathlib import Path

import pandas as pd
import requests


# --------------------------- Config ---------------------------------

S3_TEMPLATE = "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-ceo-serps.csv"

FIRST_AVAILABLE_DATE = dt.date(2025, 9, 15)  # earliest date you have SERPs for

# Inputs
ALIASES_PATH = Path("data/ceo_aliases.csv")                 # alias,ceo,company
ROSTER_CANDIDATES = [Path("data/roster.csv"), Path("data/ceo_companies.csv")]  # ceo,company

# Outputs
OUT_DIR = Path("data_ceos/processed_serps")
INDEX_DIR = Path("data/serps")
INDEX_PATH = INDEX_DIR / "ceo_serps_daily.csv"

OUT_DIR.mkdir(parents=True, exist_ok=True)
INDEX_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------- Helpers --------------------------------

def norm(s: str) -> str:
    """Normalize for matching: lowercase, keep letters/numbers/spaces, collapse whitespace."""
    s = str(s or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


LEGAL_SUFFIXES = {"inc", "inc.", "corp", "co", "co.", "llc", "plc", "ltd", "ltd.", "ag", "sa", "nv"}


def simplify_company(s: str) -> str:
    """Remove common legal suffixes for more reliable matching."""
    toks = norm(s).split()
    toks = [t for t in toks if t not in LEGAL_SUFFIXES]
    return " ".join(toks)


def read_csv_safely(text_or_path):
    """Read CSV from text or path with a resilient engine selection."""
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
    """Return ceo->company from roster-like files (best-effort)."""
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
    """
    Build alias indexes:
      - alias_norm -> (ceo, company)     from data/ceo_aliases.csv
      - ceo->company                      from roster (fallback)
    Also auto-add "{ceo} {company}" aliases from roster if missing.
    """
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

    # Auto-generate "{ceo} {company}" alias from roster
    for ceo, comp in ceo_to_company.items():
        auto = f"{ceo} {comp}"
        key = norm(auto)
        if key and key not in alias_map:
            alias_map[key] = (ceo, comp)

    return alias_map, ceo_to_company


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    From the raw S3 CSV, extract:
      - query_alias : the raw "CEO Company" string (comes from column named 'company' in S3)
      - sentiment   : negative/neutral/positive
      - controlled  : boolean-ish
    """
    cols = {c.lower(): c for c in df.columns}

    query_c = cols.get("company") or cols.get("query") or cols.get("search")  # S3 uses 'company' for query text
    sent_c = cols.get("sentiment") or cols.get("sentiment_label") or cols.get("serp_sentiment") or cols.get("label")
    ctrl_c = cols.get("control") or cols.get("controlled") or cols.get("is_controlled") or cols.get("serp_control") or cols.get("control_flag")

    out = pd.DataFrame()
    out["query_alias"] = df[query_c].astype(str).str.strip() if query_c else ""

    if sent_c:
        mapping = {
            "neg": "negative", "negative": "negative", "-1": "negative",
            "neu": "neutral",  "neutral":  "neutral",  "0":  "neutral",
            "pos": "positive", "positive": "positive", "1":  "positive",
        }
        out["sentiment"] = (
            df[sent_c].astype(str).str.strip().str.lower().map(lambda s: mapping.get(s, "neutral"))
        )
    else:
        out["sentiment"] = "neutral"

    if ctrl_c:
        v = df[ctrl_c].astype(str).str.strip().str.lower()
        out["controlled"] = v.isin(("1", "true", "t", "yes", "y", "controlled"))
    else:
        out["controlled"] = False

    return out


def resolve_ceo_company(query_alias: str, alias_map: dict[str, tuple[str, str]], ceo_to_company: dict[str, str]) -> tuple[str, str]:
    """
    1) Exact alias match on normalized query.
    2) Lightweight fallback: query contains both CEO name and simplified company name.
    """
    qn = norm(query_alias)
    if qn in alias_map:
        return alias_map[qn]

    # Fallback search
    q_tokens = set(qn.split())
    best = None
    best_score = 0
    for ceo, comp in ceo_to_company.items():
        ceo_n = norm(ceo)
        comp_n = simplify_company(comp)
        t_tokens = set(f"{ceo_n} {comp_n}".split())
        if t_tokens.issubset(q_tokens):
            score = len(t_tokens)
            if score > best_score:
                best = (ceo, comp)
                best_score = score

    return best if best else ("", "")


def dominant_company(series: pd.Series) -> str:
    """Pick the most frequent non-empty company value in a CEO group."""
    s = pd.Series(series).astype(str)
    s = s.replace("", pd.NA).dropna()
    if s.empty:
        return ""
    return s.mode().iloc[0]


# --------------------------- Core -----------------------------------

def process_one_date(date_str: str, alias_map: dict[str, tuple[str, str]], ceo_to_company: dict[str, str]):
    """Process a single YYYY-MM-DD; return output path or None if skipped."""
    try:
        day = dt.date.fromisoformat(date_str)
    except Exception:
        raise SystemExit(f"Bad date format: {date_str}. Use YYYY-MM-DD.")

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
    wf = normalize_columns(raw)

    # Map query_alias -> (ceo, company)
    mapped = wf.copy()
    mapped[["ceo", "company"]] = mapped.apply(
        lambda r: pd.Series(resolve_ceo_company(r["query_alias"], alias_map, ceo_to_company)),
        axis=1,
    )

    # Aggregate per CEO
    if mapped.empty:
        print(f"[warn] No rows after mapping for {date_str}")
        return None

    grouped = mapped.groupby("ceo", dropna=False).agg(
        total=("sentiment", "size"),
        controlled=("controlled", "sum"),
        negative_serp=("sentiment", lambda s: (s == "negative").sum()),
        neutral_serp=("sentiment", lambda s: (s == "neutral").sum()),
        positive_serp=("sentiment", lambda s: (s == "positive").sum()),
        company=("company", dominant_company),
    ).reset_index()

    # If ceo is blank (failed mapping), keep but with empty company; dashboard can ignore/flag later.
    grouped.insert(0, "date", date_str)

    # Write daily processed file
    out_day = OUT_DIR / f"{date_str}-ceo-serps-processed.csv"
    grouped.to_csv(out_day, index=False)
    print(f"[write] {out_day}")

    # Merge into rolling index
    if INDEX_PATH.exists():
        idx = read_csv_safely(INDEX_PATH)
        idx = idx[idx["date"] != date_str]
        idx = pd.concat([idx, grouped], ignore_index=True)
    else:
        idx = grouped

    idx["date"] = pd.to_datetime(idx["date"], errors="coerce")
    idx = idx.sort_values(["date", "ceo"]).reset_index(drop=True)
    idx["date"] = idx["date"].dt.strftime("%Y-%m-%d")
    idx.to_csv(INDEX_PATH, index=False)
    print(f"[update] {INDEX_PATH} ({len(idx)} rows total)")

    return out_day


def backfill(start: str, end: str, alias_map: dict[str, tuple[str, str]], ceo_to_company: dict[str, str]):
    d0 = dt.date.fromisoformat(start)
    d1 = dt.date.fromisoformat(end)
    if d0 > d1:
        d0, d1 = d1, d0
    d = d0
    while d <= d1:
        process_one_date(d.isoformat(), alias_map, ceo_to_company)
        d += dt.timedelta(days=1)


# --------------------------- CLI ------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Process daily CEO SERP files and update daily index (with alias mapping).")
    ap.add_argument("--date", help="Process a single date (YYYY-MM-DD).")
    ap.add_argument("--backfill", nargs=2, metavar=("START", "END"),
                    help="Process a date range inclusive (YYYY-MM-DD YYYY-MM-DD).")
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
