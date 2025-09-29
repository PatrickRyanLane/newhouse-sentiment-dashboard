#!/usr/bin/env python3
import csv, os, re, sys, time, math, urllib.parse, requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

DATE = (datetime.now(timezone.utc)).strftime("%Y-%m-%d")
BRANDS_TXT = "brands.txt"
OUT_DIR = "data/articles"
OUT_FILE = os.path.join(OUT_DIR, f"{DATE}-articles.csv")

# Tunables (env overrides)
MAX_PER_ALIAS = int(os.getenv("ARTICLES_MAX_PER_ALIAS", "50"))

def google_news_rss(q):
    qs = urllib.parse.quote(q)
    return f"https://news.google.com/rss/search?q={qs}&hl=en-US&gl=US&ceid=US:en"

def classify(headline, analyzer):
    s = analyzer.polarity_scores(headline or "")
    c = s["compound"]
    if c >= 0.25:  return "positive"
    if c <= -0.05: return "negative"
    return "neutral"

def fetch_one(brand, analyzer, pause=1.2):
    url = google_news_rss(f'"{brand}"')
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "xml")
    out = []
    for item in soup.find_all("item"):
        title = (item.title.text or "").strip()
        link  = (item.link.text  or "").strip()
        try:
            if "url=" in link:
                link = urllib.parse.parse_qs(urllib.parse.urlparse(link).query).get("url", [link])[0]
        except Exception:
            pass
        source = (item.source.text or "").strip() if item.source else ""
        date   = DATE
        sent   = classify(title, analyzer)
        out.append({
            "company": brand,
            "title": title,
            "url": link,
            "source": source,
            "date": date,
            "sentiment": sent
        })
    time.sleep(pause)  # be respectful
    return out[:MAX_PER_ALIAS]  # cap results

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    if not os.path.exists(BRANDS_TXT):
        print("brands.txt not found", file=sys.stderr); sys.exit(1)
    brands = [b.strip() for b in open(BRANDS_TXT) if b.strip()]
    analyzer = SentimentIntensityAnalyzer()

    rows = []
    for b in brands:
        try:
            rows.extend(fetch_one(b, analyzer))
        except Exception as e:
            print(f"[WARN] {b}: {e}", file=sys.stderr)

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["company","title","url","source","date","sentiment"])
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {OUT_FILE} ({len(rows)} rows)")

if __name__ == "__main__":
    main()
