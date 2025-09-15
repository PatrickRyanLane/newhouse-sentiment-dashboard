#!/usr/bin/env python3
"""
Simplified CEO News Sentiment with robust themes.

Input (exactly one row per CEO):
  ceo_aliases.csv  -> columns: brand,alias   # brand=CEO name, alias=Company

Outputs:
  data_ceos/articles/YYYY-MM-DD.csv
  data_ceos/daily_counts.csv   (includes 'company' and a short 'theme')

Query used per CEO:
  "<CEO NAME>" "<COMPANY>"

Theme logic (negative headlines only):
  1) bigram/trigram with min_df>=2
  2) fallback bigram with min_df>=1
  3) fallback unigrams (top 2–3 words)
All while removing stopwords + CEO/Company tokens + "ceo".
"""

import os, time, re
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urlencode, urlparse

import feedparser
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import CountVectorizer

# ------------------ Config ------------------
HL   = "en-US"
GL   = "US"
CEID = "US:en"

# --- Path setup ---
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = SCRIPT_DIR  # Assuming script is in the repo root

ALIASES_CSV = os.path.join(BASE_DIR, "ceo_aliases.csv")
OUTPUT_BASE  = os.path.join(BASE_DIR, "data_ceos")
ARTICLES_DIR = os.path.join(OUTPUT_BASE, "articles")
COUNTS_CSV   = os.path.join(OUTPUT_BASE, "daily_counts.csv")

REQUEST_PAUSE_SEC     = 1.0   # polite delay between CEOs
MAX_ITEMS_PER_QUERY   = 100    # cap per CEO per day
PURGE_OLDER_THAN_DAYS = 365

MAX_THEME_WORDS = 10

# Sentiment thresholds
SENTIMENT_POS_THRESHOLD = 0.2
SENTIMENT_NEG_THRESHOLD = -0.2

BASE_STOPWORDS = set((
    "the","a","an","and","or","but","of","for","to","in","on","at","by","with","from","as","about","after","over","under",
    "this","that","these","those","it","its","their","his","her","they","we","you","our","your","i",
    "is","are","was","were","be","been","being","has","have","had","do","does","did","will","would","should","can","could","may","might","must",
    "new","update","updates","report","reports","reported","says","say","said","see","sees","seen","watch","market","stock","shares","share","price","prices",
    "wins","loss","losses","gain","gains","up","down","amid","amidst","news","today","latest","analyst","analysts","rating","cut","cuts","downgrade","downgrades",
    "quarter","q1","q2","q3","q4","year","yrs","2024","2025","2026","usd","billion","million","percent","pct","vs","inc","corp","co","ltd","plc"
))

# Filter out low-signal sources
BLOCKED_DOMAINS = {
    "www.prnewswire.com",
    "www.businesswire.com",
    "www.globenewswire.com",
    "investorplace.com",
    "seekingalpha.com",
}

# ------------------ Helpers ------------------

def today_str() -> str:
    return datetime.now(ZoneInfo("US/Eastern")).strftime("%Y-%m-%d")

def load_ceo_company_map(path: str) -> dict:
    """Read ceo_aliases.csv (brand=CEO, alias=Company) -> {CEO: Company}"""
    if not os.path.exists(path):
        return {}
    df = pd.read_csv(path)
    return {str(row.get("brand", "")).strip(): str(row.get("alias", "")).strip() 
            for _, row in df.iterrows() 
            if str(row.get("brand", "")).strip() and str(row.get("alias", "")).strip()}

def google_news_rss_url(query: str) -> str:
    base = "https://news.google.com/rss/search"
    params = {"q": query, "hl": HL, "gl": GL, "ceid": CEID}
    return base + "?" + urlencode(params)

def domain_of(link: str) -> str:
    try:
        return urlparse(link).hostname or ""
    except Exception:
        return ""

def fetch_items_for_query(query: str, cap: int) -> list[dict]:
    try:
        parsed = feedparser.parse(google_news_rss_url(query))
        if parsed.bozo:
            print(f"Warning: Malformed feed for query: {query}")
            return []
        
        out = []
        for e in parsed.entries[:cap]:
            link = e.get("link") or ""
            dom = domain_of(link)
            if dom in BLOCKED_DOMAINS:
                continue
            out.append({
                "title": (e.get("title") or "").strip(),
                "link":  link,
                "published": (e.get("published") or e.get("updated") or "").strip(),
                "domain": dom,
            })
        return out
    except Exception as e:
        print(f"Error fetching or parsing feed for query '{query}': {e}")
        return []

_analyzer = SentimentIntensityAnalyzer()

def label_sentiment(title: str) -> str:
    s = _analyzer.polarity_scores(title or "")
    v = s["compound"]
    if v >= SENTIMENT_POS_THRESHOLD:
        return "positive"
    if v <= SENTIMENT_NEG_THRESHOLD:
        return "negative"
    return "neutral"

def dedup_by_title_domain(items: pd.DataFrame) -> pd.DataFrame:
    return items.drop_duplicates(subset=["title", "domain"])

def clean_for_theme(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s\-']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def tokens_from(s: str) -> set[str]:
    return set(re.findall(r"[a-zA-Z][a-zA-Z-']+", (s or "").lower()))

def best_phrase_from(docs: list[str], stopwords: set[str], ngram_range=(2,3), min_df=2) -> str | None:
    """Return top phrase or None."""
    try:
        vec = CountVectorizer(stop_words=list(stopwords), ngram_range=ngram_range, min_df=min_df)
        X = vec.fit_transform(docs)
        if X.shape[1] == 0:
            return None
        counts = X.sum(axis=0).A1
        vocab  = vec.get_feature_names_out()
        top_i  = counts.argmax()
        phrase = vocab[top_i]
        words  = phrase.split()
        return " ".join(words[:MAX_THEME_WORDS])
    except ValueError:
        return None

def fallback_keywords(docs: list[str], stopwords: set[str], k: int = 3) -> str | None:
    """Return top-k unigrams as a phrase, or None."""
    try:
        vec = CountVectorizer(stop_words=list(stopwords), ngram_range=(1,1), min_df=1)
        X = vec.fit_transform(docs)
        if X.shape[1] == 0:
            return None
        counts = X.sum(axis=0).A1
        vocab  = vec.get_feature_names_out()
        pairs  = sorted(zip(counts, vocab), reverse=True)
        words  = [w for _, w in pairs[:k] if w]
        return " ".join(words[:MAX_THEME_WORDS])
    except ValueError:
        return None

def theme_from_negatives(neg_titles: list[str], ceo: str, company: str) -> str:
    """
    Build a short theme from negative titles, excluding CEO/company tokens.
    Tries: (2–3)-grams min_df>=2, then bigrams min_df>=1, then top unigrams.
    """
    if not neg_titles:
        return "None"

    docs = [clean_for_theme(t) for t in neg_titles if t.strip()]
    if not docs:
        return "None"

    stop = set(BASE_STOPWORDS) | tokens_from(ceo) | tokens_from(company) | {"ceo"}

    p = best_phrase_from(docs, stopwords=stop, ngram_range=(2,3), min_df=2) or \
        best_phrase_from(docs, stopwords=stop, ngram_range=(2,2), min_df=1) or \
        fallback_keywords(docs, stopwords=stop, k=3)
    
    return p if p else "None"

def ensure_dirs():
    os.makedirs(ARTICLES_DIR, exist_ok=True)

def purge_old_files():
    """Purge files and rows older than PURGE_OLDER_THAN_DAYS."""
    cutoff_date = (date.today() - timedelta(days=PURGE_OLDER_THAN_DAYS)).isoformat()

    # Purge article files
    if os.path.exists(ARTICLES_DIR):
        for name in os.listdir(ARTICLES_DIR):
            if name.endswith(".csv") and name.replace(".csv", "") < cutoff_date:
                try:
                    os.remove(os.path.join(ARTICLES_DIR, name))
                except OSError as e:
                    print(f"Error removing file {name}: {e}")

    # Prune daily_counts.csv
    if os.path.exists(COUNTS_CSV):
        df = pd.read_csv(COUNTS_CSV)
        if not df.empty:
            original_rows = len(df)
            df = df[pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d") >= cutoff_date]
            if len(df) < original_rows:
                df.to_csv(COUNTS_CSV, index=False)

# ------------------ Main ------------------

def process_ceo(ceo: str, company: str, today: str) -> tuple[pd.DataFrame, pd.Series]:
    query = f'"{ceo}" "{company}"'
    items = fetch_items_for_query(query, MAX_ITEMS_PER_QUERY)
    if not items:
        return pd.DataFrame(), pd.Series()

    articles_df = pd.DataFrame(items)
    articles_df = dedup_by_title_domain(articles_df)

    articles_df["date"] = today
    articles_df["brand"] = ceo
    articles_df["company"] = company
    articles_df["sentiment"] = articles_df["title"].apply(label_sentiment)

    neg_titles = articles_df[articles_df["sentiment"] == "negative"]["title"].tolist()
    
    sent_counts = articles_df["sentiment"].value_counts()

    counts_row = pd.Series({
        "date": today,
        "brand": ceo,
        "company": company,
        "total": len(articles_df),
        "positive": sent_counts.get("positive", 0),
        "neutral": sent_counts.get("neutral", 0),
        "negative": sent_counts.get("negative", 0),
        "theme": theme_from_negatives(neg_titles, ceo=ceo, company=company),
    })

    return articles_df, counts_row

def main():
    print("=== CEO Sentiment (improved themes) : start ===")
    ensure_dirs()
    purge_old_files()

    ceo_to_company = load_ceo_company_map(ALIASES_CSV)
    if not ceo_to_company:
        raise SystemExit(f"No rows found in {ALIASES_CSV}. Expected header 'brand,alias' with alias=Company.")

    today = today_str()
    all_articles = []
    all_counts = []

    for i, (ceo, company) in enumerate(ceo_to_company.items()):
        articles_df, counts_row = process_ceo(ceo, company, today)
        if not articles_df.empty:
            all_articles.append(articles_df)
        if not counts_row.empty:
            all_counts.append(counts_row)

        if i < len(ceo_to_company) - 1:
            time.sleep(REQUEST_PAUSE_SEC)

    # Write articles
    if all_articles:
        articles_df = pd.concat(all_articles, ignore_index=True)
        daily_articles_path = os.path.join(ARTICLES_DIR, f"{today}.csv")
        articles_df.to_csv(daily_articles_path, index=False)
        print(f"Wrote {len(articles_df)} articles -> {daily_articles_path}")

    # Upsert counts
    if all_counts:
        counts_df = pd.DataFrame(all_counts)
        if os.path.exists(COUNTS_CSV):
            old_counts_df = pd.read_csv(COUNTS_CSV)
            old_counts_df = old_counts_df[old_counts_df["date"] != today]
            counts_df = pd.concat([old_counts_df, counts_df], ignore_index=True)
        
        counts_df.to_csv(COUNTS_CSV, index=False)
        print(f"Upserted {len(counts_df)} rows -> {COUNTS_CSV}")

    print("=== CEO Sentiment (improved themes) : done ===")

if __name__ == "__main__":
    main()
