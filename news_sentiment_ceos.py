#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Daily CEO News Sentiment — robust loader and normalizer.

Fixes:
- Accepts new ceo_aliases.csv headers (alias,ceo,company) and older formats.
- Robustly parses 'date' in daily counts and normalizes to YYYY-MM-DD.
- Validates/normalizes essential sentiment columns; computes rates if missing.

Inputs (defaults):
  --counts   data_ceos/daily_counts.csv
  --aliases  data/ceo_aliases.csv

Output (default):
  --out      data_ceos/daily_counts.csv     (overwrites with cleaned data)

Usage:
  python news_sentiment_ceos.py
  python news_sentiment_ceos.py --counts path/to/daily_counts.csv --aliases data/ceo_aliases.csv --out path/to/out.csv
"""

from __future__ import annotations
import argparse
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd


# ----------------------------- Helpers ----------------------------- #

def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    # engine='python' fallback avoids csv errors on odd quoting
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.read_csv(path, engine="python")


def normalize_date_column(df: pd.DataFrame, date_col_name: str | None = None) -> pd.DataFrame:
    """
    Parse any reasonable date strings to normalized YYYY-MM-DD in df['date'].
    """
    if date_col_name is None:
        date_col_name = next((c for c in df.columns if c.lower() == "date"), None)
        if date_col_name is None:
            raise ValueError("No 'date' column found in counts file.")

    # First attempt: broadly coerce
    df["date"] = pd.to_datetime(df[date_col_name], errors="coerce")

    # If all NaT, try some common explicit formats
    if df["date"].isna().all():
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y"):
            try:
                dt = pd.to_datetime(df[date_col_name], format=fmt, errors="coerce")
                if not dt.isna().all():
                    df["date"] = dt
                    break
            except Exception:
                pass

    if df["date"].isna().all():
        sample = df[date_col_name].head(5).tolist()
        raise ValueError(f"Could not parse any dates; sample: {sample}")

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df


def load_alias_table(path: Path) -> pd.DataFrame:
    """
    Return a DataFrame with standardized columns: brand, alias.
    Accepts:
      - brand, alias
      - ceo, alias
      - ceo, company        -> builds alias = "CEO Company"
      - alias, ceo[,company]  (your current file)
    """
    df = read_csv(path)
    cols = {c.lower(): c for c in df.columns}

    def col(name: str) -> str | None:
        return cols.get(name)

    # Case 1: brand, alias
    if col("brand") and col("alias"):
        out = df[[col("brand"), col("alias")]].rename(columns={col("brand"): "brand", col("alias"): "alias"})

    # Case 2: ceo, alias
    elif col("ceo") and col("alias"):
        out = df[[col("ceo"), col("alias")]].rename(columns={col("ceo"): "brand", col("alias"): "alias"})

    # Case 3: ceo, company
    elif col("ceo") and col("company"):
        tmp = df[[col("ceo"), col("company")]].copy()
        tmp["alias"] = tmp[col("ceo")].astype(str).str.strip() + " " + tmp[col("company")].astype(str).str.strip()
        out = tmp.rename(columns={col("ceo"): "brand"})[["brand", "alias"]]

    # Case 4: alias, ceo[,company]
    elif col("alias") and col("ceo"):
        tmp = df.copy()
        # If alias blank, fall back to "CEO Company"
        if col("company"):
            fallback = tmp[col("ceo")].astype(str).str.strip() + " " + tmp[col("company")].astype(str).str.strip()
        else:
            fallback = tmp[col("ceo")].astype(str).str.strip()
        tmp["alias"] = tmp[col("alias")].astype(str)
        mask_empty = tmp["alias"].str.strip().eq("") | tmp["alias"].isna()
        tmp.loc[mask_empty, "alias"] = fallback
        out = tmp.rename(columns={col("ceo"): "brand"})[["brand", "alias"]]

    else:
        raise ValueError(
            "ceo_aliases.csv must contain either (brand,alias) OR (ceo,alias) OR (ceo,company) OR (alias,ceo[,company])."
        )

    out["brand"] = out["brand"].astype(str).str.strip()
    out["alias"] = out["alias"].astype(str).str.strip()
    out = out[(out["brand"] != "") & (out["alias"] != "")].drop_duplicates()
    if out.empty:
        raise ValueError("No rows found after normalizing aliases.")
    return out


def normalize_counts_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure standard columns exist:
      date, ceo, company, positive, neutral, negative, total, neg_pct
    Accepts common variants (CEO/name/person; brand/company; pos/positive, etc.)
    Computes 'total' and 'neg_pct' if missing.
    """
    cols = {c.lower(): c for c in df.columns}

    def pick(*names) -> str | None:
        for n in names:
            if n in cols:
                return cols[n]
        return None

    # map identity columns
    ceo_c = pick("ceo", "name", "person", "brand")
    if not ceo_c:
        raise ValueError("Counts file must contain a CEO column (ceo/name/person/brand).")
    company_c = pick("company", "brand", "org", "employer")

    out = pd.DataFrame()
    out["date"] = df["date"]  # already normalized earlier
    out["ceo"] = df[ceo_c].astype(str).str.strip()
    out["company"] = df[company_c].astype(str).str.strip() if company_c else ""

    # sentiment columns (allow many names)
    pos_c = pick("positive", "pos", "pos_count", "pos_articles", "positive_articles")
    neu_c = pick("neutral", "neu", "neutral_count", "neutral_articles")
    neg_c = pick("negative", "neg", "neg_count", "neg_articles", "negative_articles")

    out["positive"] = pd.to_numeric(df[pos_c], errors="coerce").fillna(0).astype(int) if pos_c else 0
    out["neutral"]  = pd.to_numeric(df[neu_c], errors="coerce").fillna(0).astype(int) if neu_c else 0
    out["negative"] = pd.to_numeric(df[neg_c], errors="coerce").fillna(0).astype(int) if neg_c else 0

    # total
    total_c = pick("total", "count", "articles", "n")
    if total_c:
        out["total"] = pd.to_numeric(df[total_c], errors="coerce")
        missing = out["total"].isna() | (out["total"] == 0)
        out.loc[missing, "total"] = out["positive"] + out["neutral"] + out["negative"]
    else:
        out["total"] = out["positive"] + out["neutral"] + out["negative"]

    # negative percentage
    negpct_c = pick("neg_pct", "negative_pct", "neg_percent", "negative_percent")
    if negpct_c:
        out["neg_pct"] = pd.to_numeric(df[negpct_c], errors="coerce")
        needs = out["neg_pct"].isna()
        out.loc[needs, "neg_pct"] = (out["negative"] / out["total"].where(out["total"] > 0, 1) * 100).round(1)
    else:
        out["neg_pct"] = (out["negative"] / out["total"].where(out["total"] > 0, 1) * 100).round(1)

    # optional: theme column passthrough (if present)
    theme_c = pick("theme", "themes", "topic")
    if theme_c:
        out["theme"] = df[theme_c].astype(str)
    else:
        if "theme" in df.columns:
            out["theme"] = df["theme"].astype(str)
        else:
            out["theme"] = ""

    # sort canonical
    out = out.sort_values(["date", "ceo"]).reset_index(drop=True)
    return out


def join_aliases(counts: pd.DataFrame, aliases: pd.DataFrame) -> pd.DataFrame:
    """
    Left-join aliases so downstream tasks can reference the matching alias for each CEO.
    """
    # For clarity, rename alias.brand -> ceo (so the join is explicit)
    alias_map = aliases.rename(columns={"brand": "ceo"})
    out = counts.merge(alias_map, on="ceo", how="left")
    # Ensure 'alias' exists even if not matched
    if "alias" not in out.columns:
        out["alias"] = ""
    return out


# ----------------------------- Main ----------------------------- #

def main():
    ap = argparse.ArgumentParser(description="Normalize CEO daily news sentiment counts with robust date and alias handling.")
    ap.add_argument("--counts", default="data_ceos/daily_counts.csv", help="Path to daily counts CSV (default: data_ceos/daily_counts.csv)")
    ap.add_argument("--aliases", default="data/ceo_aliases.csv", help="Path to ceo aliases CSV (default: data/ceo_aliases.csv)")
    ap.add_argument("--out", default=None, help="Output path (default: overwrite counts path)")
    args = ap.parse_args()

    counts_path = Path(args.counts)
    aliases_path = Path(args.aliases)
    out_path = Path(args.out) if args.out else counts_path

    print("=== CEO Sentiment (normalized) ===")
    print(f"Counts : {counts_path}")
    print(f"Aliases: {aliases_path}")
    print(f"Output : {out_path}")

    # Load & normalize
    counts_raw = read_csv(counts_path)
    counts = normalize_date_column(counts_raw)
    counts = normalize_counts_columns(counts)

    # Load aliases flexibly and left-join (adds 'alias' column)
    aliases = load_alias_table(aliases_path)
    counts = join_aliases(counts, aliases)

    # Write
    out_path.parent.mkdir(parents=True, exist_ok=True)
    counts.to_csv(out_path, index=False)
    print(f"✔ Wrote {len(counts):,} rows → {out_path}")

    # Simple integrity echo
    print("Columns:", ", ".join(counts.columns))
    print("Dates  :", f"{counts['date'].min()} → {counts['date'].max()}")


if __name__ == "__main__":
    raise SystemExit(main() or 0)
