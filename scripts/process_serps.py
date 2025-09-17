#!/usr/bin/env python3
# scripts/process_serps.py
import argparse, os, sys, io
import datetime as dt
from pathlib import Path
import pandas as pd
import requests
import re

S3_TEMPLATE = "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-ceo-serps.csv"

OUT_DIR = Path("data_ceos/processed_serps")
OUT_DIR.mkdir(parents=True, exist_ok=True)

INDEX_DIR = Path("data/serps")
INDEX_DIR.mkdir(parents=True, exist_ok=True)
INDEX_PATH = INDEX_DIR / "ceo_serps_daily.csv"

# Canonical sources
ALIASES_PATH = Path("data/ceo_aliases.csv")     # alias,ceo,company  (alias == raw S3 'company' field)
ROSTER_CANDIDATES = [Path("data/roster.csv"), Path("data/ceo_companies.csv")]  # ceo,company

FIRST_AVAILABLE_DATE = dt.date(2025, 9, 15)  # earliest SERP date you have

def norm(s: str) -> str:
    s = str(s or "").lower().strip()
    # keep letters/numbers/spaces; collapse whitespace
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

LEGAL_SUFFIXES = {"inc", "inc.", "corp", "co", "co.", "llc", "plc", "ltd", "ltd.", "ag", "sa", "nv"}

def simplify_company(s: str) -> str:
    toks = norm(s).split()
    toks = [t for t in toks if t not in LEGAL_SUFFIXES]
    return " ".join(toks)

def load_roster_map():
    """Return ceo->company from roster-like files (best-effort)."""
    for p in ROSTER_CANDIDATES:
        if p.exists():
            df = pd.read_csv(p)
            cols = {c.lower(): c for c in df.columns}
            ceo_c  = next((cols[c] for c in cols if c in ("ceo","name","person")), None)
            comp_c = next((cols[c] for c in cols if c in ("company","brand","org","employer")), None)
            if ceo_c and comp_c:
                return { str(r[ceo_c]).strip(): str(r[comp_c]).strip() for _, r in df.iterrows() }
    return {}

def load_alias_index():
    """
    Build alias indexes:
      - alias_norm -> (ceo, company)  [from data/ceo_aliases.csv]
      - ceo->company (from roster)
    Also auto-add "{ceo} {company}" as an alias if not present.
    """
    alias_map = {}
    ceo_to_company = load_roster_map()

    if ALIASES_PATH.exists():
        a = pd.read_csv(ALIASES_PATH)
        need_cols = {"alias","ceo","company"}
        if not need_cols.issubset({c.lower() for c in a.columns}):
            raise SystemExit("data/ceo_aliases.csv must have columns: alias, ceo, company")
        # tolerate case variants
        cols = {c.lower(): c for c in a.columns}
        for _, r in a.iterrows():
            alias = str(r[cols["alias"]]).strip()
            ceo   = str(r[cols["ceo"]]).strip()
            comp  = str(r[cols["company"]]).strip()
            if alias:
                alias_map[norm(alias)] = (ceo, comp)

    # Auto-generate "{ceo} {company}" aliases from roster if missing
    for ceo, comp in ceo_to_company.items():
        auto = f"{ceo} {comp}"
        na = norm(auto)
        if na and na not in alias_map:
            alias_map[na] = (ceo, comp)

    return alias_map, ceo_to_company

def fetch_csv_text(url: str, timeout=30):
    r = requests.get(url, timeout=timeout)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def normalize_columns(df: pd.DataFrame):
    """
    Map the raw S3 columns into a working frame with:
      query_alias (raw S3 'company' field that actually holds 'CEO Company'),
      sentiment   (negative/neutral/positive),
      controlled  (boolean).
    """
    cols = {c.lower(): c for c in df.columns}

    # In your raw S3, 'company' holds the query "CEO Company"
    query_c   = cols.get("company") or cols.get("query") or cols.get("search")  # tolerant
    # Sentiment/control candidates
    sent_c    = cols.get("sentiment") or cols.get("sentiment_label") or cols.get("serp_sentiment") or cols.get("label")
    control_c = cols.get("control") or cols.get("controlled") or cols.get("is_controlled") or cols.get("serp_control") or cols.get("control_flag")

    out = pd.DataFrame()
    out["query_alias"] = df[query_c].astype(str).str.strip() if query_c else ""

    if sent_c is not None:
        mapping = {
            "neg":"negative", "negative":"negative", "-1":"negative",
            "neu":"neutral",  "neutral":"neutral",   "0":"neutral",
            "pos":"positive", "positive":"positive", "1":"positive",
        }
        out["sentiment"] = df[sent_c].astype(str).str.strip().str.lower().map(lambda s: mapping.get(s, "neutral"))
    else:
        out["sentiment"] = "neutral"

    if control_c is not None:
        v = df[control_c].astype(str).str.strip().str.lower()
        out["controlled"] = v.isin(("1","true","t","yes","y","controlled"))
    else:
        out["controlled"] = False

    return out

def resolve_ceo_company(query_alias: str, alias_map: dict, ceo_to_company: dict):
    """
    1) Exact alias match on the normalized query.
    2) Light fallback: if query contains both the CEO name and the simplified company name, accept it.
    """
    qn = norm(query_alias)
    if qn in alias_map:
        return alias_map[qn]

    # Lightweight heuristic fallback
    # Try to find a (ceo,company) whose "{ceo} {company}" is mostly contained in the query.
    q_tokens = set(qn.split())
    best = None
    best_score = 0
    for ceo, comp in ceo_to_company.items():
        ceo_n = norm(ceo)
        comp_n = simplify_company(comp)
        target = f"{ceo_n} {comp_n}".strip()
        t_tokens = set(target.split())
        if t_tokens.issubset(q_tokens):
            score = len(t_tokens)
            if score > best_score:
                best = (ceo, comp)
                best_score = score
    return best if best else ("", "")  # unresolved -> blanks

def process_one_date(date_str: str, alias_map: dict, ceo_to_company: dict):
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

    raw = pd.read_csv(io.StringIO(text))
    wf = normalize_columns(raw)

    # Map query_alias -> (ceo, company)
    mapped = wf.copy()
    mapped[["ceo","company"]] = mapped.apply(
        lambda r: pd.Series(resolve_ceo_company(r["query_alias"], alias_map, ceo_to_company)),
        axis=1
    )

    # Aggregate to per-CEO metrics
    if mapped.empty:
        print(f"[warn] No rows after mapping for {date_str}")
        return None

    def dominant_company(group):
        s = group["company"].replace("", pd.NA).dropna()
        return s.mode().iloc[0] if len(s) else ""

    grouped = mapped.groupby("ceo", dropna=False).agg(
        total=("sentiment","size"),
        controlled=("controlled","sum"),
        negative_serp=("sentiment", lambda s: (s=="negative").sum()),
        neutral_serp=("sentiment",  lambda s: (s=="neutral").sum()),
        positive_serp=("sentiment", lambda s: (s=="positive").sum()),
        company=("company", dominant_company),
    ).reset_index()

    # Stamp date and write daily processed file
    grouped.insert(0, "date", date_str)
    out_day = OUT_DIR / f"{date_str}-ceo-serps-processed.csv"
    grouped.to_csv(out_day, index=False)
    print(f"[write] {out_day}")

    # Merge into rolling index
    if INDEX_PATH.exists():
        idx = pd.read_csv(INDEX_PATH)
        idx = idx[idx["date"] != date_str]
        idx = pd.concat([idx, grouped], ignore_index=True)
    else:
        idx = grouped

    idx["date"] = pd.to_datetime(idx["date"], errors="coerce")
    idx = idx.sort_values(["date","ceo"]).reset_index(drop=True)
    idx["date"] = idx["date"].dt.strftime("%Y-%m-%d")
    idx.to_csv(INDEX_PATH, index=False)
    print(f"[update] {INDEX_PATH} ({len(idx)} rows total)")
    return out_day

def backfill(start: str, end: str, alias_map: dict, ceo_to_company: dict):
    d0 = dt.date.fromisoformat(start)
    d1 = dt.date.fromisoformat(end)
    if d0 > d1:
        d0, d1 = d1, d0
    d = d0
    while d <= d1:
        process_one_date(d.isoformat(), alias_map, ceo_to_company)
        d += dt.timedelta(days=1)

def main():
    ap = argparse.ArgumentParser(description="Process daily CEO SERP files and update daily index (with alias mapping).")
    ap.add_argument("--date", help="Process a single date (YYYY-MM-DD).")
    ap.add_argument("--backfill", nargs=2, metavar=("START","END"),
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
