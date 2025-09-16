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
from typing import Dict

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

def load_ceo_company_map(path: str) -> Dict[str, str]:
    """Read ceo_aliases.csv (brand=CEO, alias=Company) -> {CEO: Company}"""
    if not os.path.exists(path):
        return {}
    df = pd.read_csv(path, header=0, names=['brand', 'alias'])
    df = df.dropna(subset=['brand', 'alias'])
    df["brand"] = df["brand"].str.strip()
    df["alias"] = df["alias"].str.strip()
    return df.set_index('brand')['alias'].to_dict()

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

    # Prune daily_counts.csv in a memory-efficient way
    if os.path.exists(COUNTS_CSV):
        temp_csv = COUNTS_CSV + ".tmp"
        try:
            with open(COUNTS_CSV, 'r', newline='') as infile, open(temp_csv, 'w', newline='') as outfile:
                reader = pd.read_csv(infile, chunksize=1000)
                header_written = False
                for chunk in reader:
                    chunk = chunk[pd.to_datetime(chunk["date"]).dt.strftime("%Y-%m-%d") >= cutoff_date]
                    if not chunk.empty:
                        chunk.to_csv(outfile, header=not header_written, index=False)
                        header_written = True
            os.replace(temp_csv, COUNTS_CSV)
        except Exception as e:
            print(f"Error processing {COUNTS_CSV}: {e}")
            if os.path.exists(temp_csv):
                os.remove(temp_csv)

# ------------------ Main ------------------

def process_ceo_group(ceo_df: pd.DataFrame, today: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_articles = []
    for ceo, company in ceo_df.itertuples(index=False):
        query = f'"{ceo}" "{company}"'
        items = fetch_items_for_query(query, MAX_ITEMS_PER_QUERY)
        if items:
            articles_df = pd.DataFrame(items)
            articles_df["brand"] = ceo
            articles_df["company"] = company
            all_articles.append(articles_df)
        time.sleep(REQUEST_PAUSE_SEC)

    if not all_articles:
        return pd.DataFrame(), pd.DataFrame()

    # Combine, dedup, and analyze
    articles = pd.concat(all_articles, ignore_index=True)
    articles = dedup_by_title_domain(articles)
    articles["date"] = today
    articles["sentiment"] = articles["title"].apply(label_sentiment)

    # Create counts
    counts = articles.groupby(["brand", "company"]).agg(
        total=("sentiment", "size"),
        positive=("sentiment", lambda s: (s == "positive")).sum(),
        neutral=("sentiment", lambda s: (s == "neutral")).sum(),
        negative=("sentiment", lambda s: (s == "negative")).sum(),
    ).reset_index()

    # Add themes
    neg_titles = articles[articles["sentiment"] == "negative"]
    themes = neg_titles.groupby(["brand", "company"])["title"].apply(list).reset_index(name="titles")
    themes["theme"] = themes.apply(lambda row: theme_from_negatives(row["titles"], row["brand"], row["company"] ), axis=1)
    
    counts = counts.merge(themes[["brand", "company", "theme"]], on=["brand", "company"], how="left").fillna({"theme": "None"})
    counts["date"] = today

    return articles, counts

def main():
    print("=== CEO Sentiment (improved themes) : start ===")
    ensure_dirs()
    purge_old_files()

    ceo_map = load_ceo_company_map(ALIASES_CSV)
    if not ceo_map:
        raise SystemExit(f"No rows found in {ALIASES_CSV}. Expected header 'brand,alias' with alias=Company.")

    today = today_str()
    ceo_df = pd.DataFrame(list(ceo_map.items()), columns=["brand", "company"])

    articles_df, counts_df = process_ceo_group(ceo_df, today)

    # Write articles
    if not articles_df.empty:
        daily_articles_path = os.path.join(ARTICLES_DIR, f"{today}.csv")
        articles_df.to_csv(daily_articles_path, index=False)
        print(f"Wrote {len(articles_df)} articles -> {daily_articles_path}")

    # Upsert counts
    if not counts_df.empty:
        today = today_str()
        temp_csv = COUNTS_CSV + ".tmp"
        
        # Write new counts to a temporary file
        counts_df.to_csv(temp_csv, index=False)

        # Append old counts (excluding today) from the original file
        if os.path.exists(COUNTS_CSV):
            with open(COUNTS_CSV, 'r', newline='') as infile, open(temp_csv, 'a', newline='') as outfile:
                reader = pd.read_csv(infile, chunksize=1000)
                for chunk in reader:
                    chunk = chunk[chunk["date"] != today]
                    if not chunk.empty:
                        chunk.to_csv(outfile, header=False, index=False)

        os.replace(temp_csv, COUNTS_CSV)
        print(f"Upserted {len(counts_df)} rows -> {COUNTS_CSV}")

    print("=== CEO Sentiment (improved themes) : done ===")

if __name__ == "__main__":
    main()
