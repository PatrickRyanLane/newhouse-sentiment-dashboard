

#!/usr/bin/env python3
"""
Processes daily brand SERP data to add sentiment and control analysis.

Input:
- Daily SERP CSV from S3 URL: https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/YYYY-MM-DD-brand-serps.csv
- brands.txt: List of brands to process.
- aliases.csv: Contains brand aliases.

Output:
- Processed SERP CSV with sentiment and control status columns, saved locally to:
  data_brands/processed_serps/YYYY-MM-DD.csv
"""

import os
import sys
import re
import pandas as pd
import requests
from datetime import datetime
from urllib.parse import urlparse
from typing import Union
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# --- Config ---
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = SCRIPT_DIR

BRANDS_TXT = os.path.join(BASE_DIR, "brands.txt")

RAW_SERP_URL_TEMPLATE = "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-brand-serps.csv"


CONTROLLED_SOCIAL_DOMAINS = {"facebook.com", "linkedin.com", "instagram.com", "twitter.com"}
CONTROLLED_PATH_KEYWORDS = {"/leadership/", "/about/", "/governance/"}
UNCONTROLLED_DOMAINS = {"wikipedia.org"}

# --- Functions ---

def get_target_date() -> str:
    """
    Returns date from CLI args (YYYY-MM-DD) or today's date.
    Usage: python process_serps.py [YYYY-MM-DD]
    """
    if len(sys.argv) > 1:
        try:
            datetime.strptime(sys.argv[1], "%Y-%m-%d")
            return sys.argv[1]
        except ValueError:
            print(f"Warning: Invalid date format '{sys.argv[1]}'. Using today's date.")
    return datetime.now().strftime("%Y-%m-%d")

def load_brands(path: str) -> list[str]:
    """Loads a list of brands from a text file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Brand file not found at: {path}")
    with open(path, 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

def fetch_serp_data(date: str) -> Union[pd.DataFrame, None]:
    """Fetches the SERP data for a given date from S3."""
    url = RAW_SERP_URL_TEMPLATE.format(date=date)
    try:
        response = requests.get(url)
        response.raise_for_status()
        return pd.read_csv(url)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching SERP data from {url}: {e}")
        return None
    except Exception as e:
        print(f"Error processing SERP data from {url}: {e}")
        return None

def classify_control(row: pd.Series, brand_name: str) -> str:
    """Classifies a URL as controlled or uncontrolled based on a set of rules."""
    url = row["link"]
    position = row["position"]
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.replace("www.", "")

    # Rule: Wikipedia is always uncontrolled
    if any(uncontrolled_domain in domain for uncontrolled_domain in UNCONTROLLED_DOMAINS):
        return "uncontrolled"

    # Rule: Brand name in domain
    if brand_name.lower() in domain.lower():
        return "controlled"

    # Rule: Controlled social media domains
    if any(social_domain in domain for social_domain in CONTROLLED_SOCIAL_DOMAINS):
        return "controlled"

    # Rule: Controlled path keywords
    if any(keyword in parsed_url.path for keyword in CONTROLLED_PATH_KEYWORDS):
        return "controlled"

    # Rule: Rank 1 is controlled
    if position == 1:
        return "controlled"

    # Rule: All other URLs are uncontrolled
    return "uncontrolled"

def get_sentiment_label(score: float) -> str:
    """Converts a VADER compound score to a sentiment label."""
    if score >= 0.05:
        return "positive"
    if score <= -0.05:
        return "negative"
    return "neutral"

def main():
    """Main function to process SERP data."""
    target_date = get_target_date()
    print(f"Starting brand SERP data processing for {target_date}...")

    # Load brands
    try:
        brands = load_brands(BRANDS_TXT)
    except FileNotFoundError as e:
        print(e)
        return

    # Fetch SERP data
    serp_df = fetch_serp_data(target_date)
    if serp_df is None:
        print("No SERP data to process.")
        return

    # Initialize sentiment analyzer
    analyzer = SentimentIntensityAnalyzer()

    # Process each brand
    processed_dfs = []
    for brand in brands:
        brand_serps = serp_df[serp_df["company"].str.contains(brand, case=False, na=False)].copy()

        if not brand_serps.empty:
            # Apply control classification
            brand_serps["control_status"] = brand_serps.apply(
                lambda row: classify_control(row, brand), axis=1
            )

            # Apply sentiment analysis
            brand_serps["sentiment_score"] = brand_serps["snippet"].apply(
                lambda snippet: analyzer.polarity_scores(str(snippet))["compound"]
            )
            brand_serps["sentiment"] = brand_serps["sentiment_score"].apply(get_sentiment_label)
            processed_dfs.append(brand_serps)

    if not processed_dfs:
        print("No SERP data processed.")
        return

    # Combine processed data
    final_df = pd.concat(processed_dfs, ignore_index=True)

    # Save processed data
    output_path = os.path.join(BASE_DIR, "data_brands", "processed_serps", f"{target_date}.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_df.to_csv(output_path, index=False)

    print(f"Processed SERP data saved to: {output_path}")
    print("SERP data processing complete.")

if __name__ == "__main__":
    main()