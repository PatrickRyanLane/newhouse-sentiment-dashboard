# scripts/legacy_brand_articles_to_new.py
import csv, os, glob
from pathlib import Path
from collections import Counter, defaultdict

# ==== CONFIG ====
# 1) Where your legacy daily brand article CSVs live:
LEGACY_GLOB = "data/articles_legacy/*.csv"     # e.g., data/articles_legacy/2025-09-15.csv

# 2) Map legacy column names -> new names
# New required columns: company, title, url, source, date, sentiment
COLMAP = {
    "brand": "company",
    "title": "title",
    "url": "url",
    "domain": "source", 
    "date": "date",
    "sentiment": "sentiment",
}

# ==== OUTPUT LOCATIONS (new format) ====
ARTICLES_DIR = Path("data/articles")
PROCESSED_DIR = Path("data/processed_articles")
ROLLING_FILE = PROCESSED_DIR / "daily_counts.csv"

ARTICLES_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def normalize_row(row):
    out = {k: "" for k in ["company","title","url","source","date","sentiment"]}
    for old, val in row.items():
        old_l = old.strip().lower()
        if old_l in COLMAP:
            out[COLMAP[old_l]] = val.strip()
    # minimal cleanup
    out["sentiment"] = (out["sentiment"] or "").strip().lower()
    if out["sentiment"] not in {"positive","neutral","negative"}:
        out["sentiment"] = "neutral"
    return out

def write_daily_articles(date_str, rows):
    # data/articles/YYYY-MM-DD-articles.csv
    fpath = ARTICLES_DIR / f"{date_str}-articles.csv"
    with fpath.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["company","title","url","source","date","sentiment"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

def write_daily_processed(date_str, rows):
    # data/processed_articles/YYYY-MM-DD.csv (company, total, positive, neutral, negative)
    counts = defaultdict(lambda: Counter())
    for r in rows:
        co = r["company"].strip()
        if not co: 
            continue
        counts[co]["total"] += 1
        counts[co][r["sentiment"]] += 1

    fpath = PROCESSED_DIR / f"{date_str}.csv"
    with fpath.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","company","total","positive","neutral","negative"])
        w.writeheader()
        for co, c in sorted(counts.items()):
            w.writerow({
                "date": date_str,
                "company": co,
                "total": c["total"],
                "positive": c["positive"],
                "neutral": c["neutral"],
                "negative": c["negative"],
            })
    return counts

def merge_daily_into_rolling(date_str, day_counts):
    # Update data/processed_articles/daily_counts.csv by replacing rows for date_str
    rows = []
    if ROLLING_FILE.exists():
        with ROLLING_FILE.open("r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                if row.get("date") != date_str:
                    rows.append(row)

    # add this day's rows
    for co, c in sorted(day_counts.items()):
        rows.append({
            "date": date_str,
            "company": co,
            "total": str(c["total"]),
            "positive": str(c["positive"]),
            "neutral": str(c["neutral"]),
            "negative": str(c["negative"]),
        })

    # write back
    with ROLLING_FILE.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","company","total","positive","neutral","negative"])
        w.writeheader()
        for row in rows:
            w.writerow(row)

def infer_date_from_filename(p):
    # expects something like .../2025-09-15.csv
    base = Path(p).stem
    # try smart cases:
    # 1) already an ISO date
    parts = base.split("_")
    for token in parts + [base]:
        if len(token) == 10 and token[4] == "-" and token[7] == "-":
            return token
    raise ValueError(f"Could not infer date from filename: {p}")

def main():
    files = sorted(glob.glob(LEGACY_GLOB))
    if not files:
        print("No legacy files matched. Update LEGACY_GLOB to your old folder/pattern.")
        return

    for path in files:
        date_str = infer_date_from_filename(path)
        print(f"Converting {path} -> {date_str}")

        with open(path, "r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            new_rows = [normalize_row(row) for row in r]

        write_daily_articles(date_str, new_rows)
        day_counts = write_daily_processed(date_str, new_rows)
        merge_daily_into_rolling(date_str, day_counts)

    print("Done. Converted legacy articles & rebuilt processed counts/rolling index.")

if __name__ == "__main__":
    main()
