#!/usr/bin/env python3
import csv, os, re, sys, requests, urllib.parse
from datetime import datetime, timezone
from collections import defaultdict
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

DATE = (datetime.now(timezone.utc)).strftime("%Y-%m-%d")
RAW_URL = f"https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{DATE}-brand-serps.csv"

ROWS_OUT = f"data/serp_rows/{DATE}-brand-serps-rows.csv"
AGG_OUT  = f"data/processed_serps/{DATE}-brand-serps-processed.csv"
IDX_OUT  = "data/serps/brand_serps_daily.csv"

os.makedirs("data/serp_rows", exist_ok=True)
os.makedirs("data/processed_serps", exist_ok=True)
os.makedirs("data/serps", exist_ok=True)

# Optional: map brand -> owned domains. If absent, we do a heuristic: domain contains sanitized brand (e.g. "nike").
BRAND_DOMAINS = "brand_domains.csv"  # columns: brand,domain  (optional)
brand_to_domains = defaultdict(set)
if os.path.exists(BRAND_DOMAINS):
    with open(BRAND_DOMAINS, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            b = (r.get("brand") or r.get("company") or "").strip()
            d = (r.get("domain") or "").strip().lower()
            if b and d:
                brand_to_domains[b].add(d)

def hostname(url):
    try:
        return urllib.parse.urlparse(url).hostname or ""
    except Exception:
        return ""

def is_controlled(brand, domain):
    domain = (domain or "").lower()
    if not domain: return False
    if brand_to_domains and brand in brand_to_domains:
        return domain in brand_to_domains[brand]
    # fallback heuristic: domain contains a sanitized token of the brand
    token = re.sub(r"[^a-z0-9]+", "", brand.lower())
    return (token and token in domain)

def classify(text, analyzer):
    s = analyzer.polarity_scores(text or "")
    c = s["compound"]
    if c >= 0.25:  return "positive"
    if c <= -0.25: return "negative"
    return "neutral"

def main():
    print(f"Downloading raw SERPs: {RAW_URL}")
    r = requests.get(RAW_URL, timeout=60)
    r.raise_for_status()
    content = r.content.decode("utf-8", errors="replace").splitlines()
    raw = list(csv.DictReader(content))

    analyzer = SentimentIntensityAnalyzer()
    rows = []
    agg = defaultdict(lambda: {"total":0,"negative_serp":0,"controlled":0})

    for rec in raw:
        brand = (rec.get("company") or rec.get("prompt") or "").strip()
        if not brand: continue
        title = (rec.get("title") or "").strip()
        snippet = (rec.get("snippet") or "").strip()
        url = (rec.get("link") or rec.get("redirect_link") or "").strip()
        dom = (rec.get("displayed_link") or hostname(url)).strip().lower()
        try:
            pos = int(float(rec.get("position") or 9999))
        except Exception:
            pos = 9999
        sent = classify(f"{title}. {snippet}", analyzer)
        ctrl = is_controlled(brand, dom)

        rows.append({
            "date": DATE, "brand": brand, "position": pos, "title": title,
            "url": url, "domain": dom, "snippet": snippet,
            "sentiment": sent, "controlled": "true" if ctrl else "false"
        })
        agg[brand]["total"] += 1
        if sent == "negative": agg[brand]["negative_serp"] += 1
        if ctrl: agg[brand]["controlled"] += 1

    # write rows
    with open(ROWS_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "date","brand","position","title","url","domain","snippet","sentiment","controlled"
        ])
        w.writeheader(); w.writerows(sorted(rows, key=lambda x: (x["brand"], x["position"])))
    print(f"Wrote {ROWS_OUT} ({len(rows)} rows)")

    # write per-day aggregate
    day = [{"date": DATE, "brand": b, **v} for b, v in sorted(agg.items())]
    with open(AGG_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","brand","total","negative_serp","controlled"])
        w.writeheader(); w.writerows(day)
    print(f"Wrote {AGG_OUT} ({len(day)} brands)")

    # upsert rolling index
    existing = []
    if os.path.exists(IDX_OUT):
        with open(IDX_OUT, newline="", encoding="utf-8") as f:
            existing = list(csv.DictReader(f))
    keep = [r for r in existing if r.get("date") != DATE]
    keep.extend(day)
    with open(IDX_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","brand","total","negative_serp","controlled"])
        w.writeheader(); w.writerows(keep)
    print(f"Updated {IDX_OUT} (rows: {len(keep)})")

if __name__ == "__main__":
    main()
