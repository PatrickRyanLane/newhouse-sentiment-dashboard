#!/usr/bin/env python3
import argparse, csv, sys
from pathlib import Path
from datetime import date, timedelta

ARTICLES_DIR = Path("data/articles")
OUT_DIR      = Path("data/processed_articles")
OUT_DIR.mkdir(parents=True, exist_ok=True)
DAILY_INDEX  = OUT_DIR / "daily_counts.csv"

def iter_dates(from_str: str, to_str: str):
    d0 = date.fromisoformat(from_str)
    d1 = date.fromisoformat(to_str)
    if d1 < d0:
        raise SystemExit(f"--to ({to_str}) is before --from ({from_str})")
    d = d0
    one = timedelta(days=1)
    while d <= d1:
        yield d.isoformat()
        d += one

def read_articles(dstr: str):
    f = ARTICLES_DIR / f"{dstr}-articles.csv"
    if not f.exists():
        print(f"[INFO] No headline file for {dstr} at {f}; nothing to aggregate.", flush=True)
        return []
    rows = []
    with f.open(newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            # expected new columns: company,title,url,source,date,sentiment
            rows.append({
                "company": (row.get("company") or "").strip(),
                "sentiment": (row.get("sentiment") or "").strip().lower(),
            })
    return rows

def aggregate(rows):
    # counts per company {company: {"positive": n, "neutral": n, "negative": n, "total": n}}
    agg = {}
    for r in rows:
        company = r["company"]
        if not company: 
            continue
        s = r["sentiment"]
        bucket = agg.setdefault(company, {"positive":0,"neutral":0,"negative":0,"total":0})
        if s not in ("positive","neutral","negative"):
            s = "neutral"
        bucket[s] += 1
        bucket["total"] += 1
    return agg

def write_daily(dstr: str, agg: dict):
    out = OUT_DIR / f"{dstr}.csv"
    with out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["date","company","positive","neutral","negative","total"])
        for company, c in sorted(agg.items()):
            w.writerow([dstr, company, c["positive"], c["neutral"], c["negative"], c["total"]])
    print(f"[OK] Wrote {out}")

def upsert_daily_index(dstr: str, agg: dict):
    # load existing index
    rows = []
    if DAILY_INDEX.exists():
        with DAILY_INDEX.open(newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))

    # drop any rows for dstr, then append fresh
    rows = [r for r in rows if r.get("date") != dstr]
    for company, c in agg.items():
        rows.append({
            "date": dstr,
            "company": company,
            "positive": str(c["positive"]),
            "neutral": str(c["neutral"]),
            "negative": str(c["negative"]),
            "total":    str(c["total"]),
        })

    # sort by date, then company
    rows.sort(key=lambda r: (r["date"], r["company"]))

    with DAILY_INDEX.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["date","company","positive","neutral","negative","total"])
        w.writeheader()
        w.writerows(rows)
    print(f"[OK] Updated {DAILY_INDEX}")

def process_one(dstr: str):
    print(f"Processing {dstr}...")
    rows = read_articles(dstr)
    if not rows:
        return
    agg = aggregate(rows)
    write_daily(dstr, agg)
    upsert_daily_index(dstr, agg)

def main():
    ap = argparse.ArgumentParser(description="Aggregate brand news sentiment by day/company.")
    ap.add_argument("--date", help="single YYYY-MM-DD to process")
    ap.add_argument("--from", dest="from_date", help="start YYYY-MM-DD (inclusive)")
    ap.add_argument("--to",   dest="to_date",   help="end YYYY-MM-DD (inclusive)")
    args = ap.parse_args()

    dates = []
    if args.date:
        dates = [args.date]
    elif args.from_date and args.to_date:
        dates = list(iter_dates(args.from_date, args.to_date))
    else:
        dates = [date.today().isoformat()]

    for dstr in dates:
        process_one(dstr)

if __name__ == "__main__":
    main()
