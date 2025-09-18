#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds daily CEO articles from Google News RSS and saves:
  data_ceos/articles/YYYY-MM-DD-articles.csv

Inputs:
- data/ceo_aliases.csv  (must include: alias, ceo, company — case-insensitive)

Output columns:
  ceo, company, title, url, source, sentiment  (sentiment ∈ {positive, neutral, negative})

Notes:
- Uses Google News RSS (no API key). Be respectful; we keep requests tiny.
- Classifies sentiment on the HEADLINE text using VADER (lightweight, works well on short headlines).
"""

from __future__ import annotations
import os, time, html, sys
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote_plus, urlparse

import pandas as pd
import requests
import feedparser
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


BASE = Path(__file__).parent
ALIASES_CSV = BASE / "data" / "ceo_aliases.csv"
OUT_DIR = BASE / "data_ceos" / "articles"
OUT_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = "Mozilla/5.0 (compatible; CEO-NewsBot/1.0; +https://example.com/bot)"
RSS_TMPL = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

# Tunables (env overrides)
MAX_PER_ALIAS = int(os.getenv("ARTICLES_MAX_PER_ALIAS", "25"))
SLEEP_SEC = float(os.getenv("ARTICLES_SLEEP_SEC", "0.35"))   # polite delay between requests
TARGET_DATE = os.getenv("ARTICLES_DATE", "").strip()         # YYYY-MM-DD or empty for today (UTC)


def target_date() -> str:
    if TARGET_DATE:
        try:
            datetime.strptime(TARGET_DATE, "%Y-%m-%d")
            return TARGET_DATE
        except ValueError:
            print(f"WARNING: invalid ARTICLES_DATE={TARGET_DATE!r}; falling back to today.")
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def read_aliases(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing aliases file: {path}")
    df = pd.read_csv(path)
    cols = {c.lower(): c for c in df.columns}

    def col(na
