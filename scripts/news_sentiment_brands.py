#!/usr/bin/env python3
import csv, os, sys
from datetime import datetime, timezone

DATE = (datetime.now(timezone.utc)).strftime("%Y-%m-%d")
ART_IN  = f"data/articles/{DATE}-articles.csv"
DAY_OUT = f"data/processed_articles/{DATE}.csv"
IDX_OUT = "data/processed_articles/daily_counts.csv"

def main():
    os.makedirs("data/processed_articles", exist_ok=True)
    if not os.path.exists(ART_IN):
        print(f"[INFO] No headline file for {DATE} at {ART_IN}; nothing to aggregate.")
        return

    # aggregate per company
    agg = {}
    with open(ART_IN, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            brand = (row.get("company") or "").strip()
            if not brand: continue
            sent = (row.get("sentiment") or "neutral").strip().lower()
            a = agg.setdefault(brand, {"positive":0,"neutral":0,"negative":0})
            if sent not in a: sent = "neutral"
            a[sent] += 1

    # write per-day file
    day_rows = []
    for brand, c in sorted(agg.items()):
        total = c["positive"] + c["neutral"] + c["negative"]
        neg_pct = (c["negative"]/total) if total else 0.0
        day_rows.append({
            "date": DATE, "company": brand,
            "positive": c["positive"], "neutral": c["neutral"], "negative": c["negative"],
            "total": total, "neg_pct": f"{neg_pct:.4f}"
        })

    with open(DAY_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","company","positive","neutral","negative","total","neg_pct"])
        w.writeheader(); w.writerows(day_rows)
    print(f"Wrote {DAY_OUT} ({len(day_rows)} rows)")

    # upsert date rows into rolling index
    existing = []
    if os.path.exists(IDX_OUT):
        with open(IDX_OUT, newline="", encoding="utf-8") as f:
            existing = list(csv.DictReader(f))

    # drop any existing rows for this date
    keep = [r for r in existing if (r.get("date") != DATE)]
    keep.extend(day_rows)

    with open(IDX_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","company","positive","neutral","negative","total","neg_pct"])
        w.writeheader(); w.writerows(keep)
    print(f"Updated {IDX_OUT} (rows: {len(keep)})")

if __name__ == "__main__":
    main()
