

#!/usr/bin/env python3
"""
Processes daily CEO SERP data to add sentiment and control analysis.

Input:
- Daily SERP CSV from S3 URL: https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/YYYY-MM-DD-ceo-serps.csv
- roster.csv: Contains company names for control analysis.

Output:
- Processed SERP CSV with sentiment and control status columns, saved locally to:
  data_ceos/processed_serps/YYYY-MM-DD.csv
"""

import os
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

ROSTER_CSV = os.path.join(BASE_DIR, "data", "roster.csv")

RAW_SERP_URL_TEMPLATE = "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-ceo-serps.csv"


CONTROLLED_SOCIAL_DOMAINS = {"facebook.com", "linkedin.com", "instagram.com", "twitter.com"}
CONTROLLED_PATH_KEYWORDS = {"/leadership/", "/about/", "/governance/"}
UNCONTROLLED_DOMAINS = {"wikipedia.org"}

# --- Functions ---

def get_today_date() -> str:
    """Returns the current date in YYYY-MM-DD format."""
    return datetime.now().strftime("%Y-%m-%d")

def load_roster(path: str) -> pd.DataFrame:
    """Loads the roster CSV into a DataFrame."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Roster file not found at: {path}")
    return pd.read_csv(path)

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

def classify_control(row: pd.Series, company_name: str) -> str:
    """Classifies a URL as controlled or uncontrolled based on a set of rules."""
    url = row["link"]
    position = row["position"]
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.replace("www.", "")

    # Rule 5: Wikipedia is always uncontrolled
    if any(uncontrolled_domain in domain for uncontrolled_domain in UNCONTROLLED_DOMAINS):
        return "uncontrolled"

    # Rule 1: Company name in domain
    if company_name.lower() in domain.lower():
        return "controlled"

    # Rule 2: Controlled social media domains
    if any(social_domain in domain for social_domain in CONTROLLED_SOCIAL_DOMAINS):
        return "controlled"

    # Rule 3: Controlled path keywords
    if any(keyword in parsed_url.path for keyword in CONTROLLED_PATH_KEYWORDS):
        return "controlled"

    # Rule 4: Rank 1 is controlled
    if position == 1:
        return "controlled"

    # Rule 6: All other URLs are uncontrolled
    return "uncontrolled"

def main():
    """Main function to process SERP data."""
    print("Starting SERP data processing...")
    today = get_today_date()

    # Load roster
    try:
        roster_df = load_roster(ROSTER_CSV)
    except FileNotFoundError as e:
        print(e)
        return

    # Fetch SERP data
    serp_df = fetch_serp_data(today)
    if serp_df is None:
        print("No SERP data to process.")
        return

    # Initialize sentiment analyzer
    analyzer = SentimentIntensityAnalyzer()

    # Process each company in the roster
    processed_dfs = []
    for _, roster_row in roster_df.iterrows():
        ceo_name = roster_row["CEO"]
        company_name = roster_row["Company"]
        company_serps = serp_df[serp_df["company"].str.contains(ceo_name, case=False, na=False)].copy()

        if not company_serps.empty:
            # Apply control classification
            company_serps["control_status"] = company_serps.apply(
                lambda row: classify_control(row, company_name), axis=1
            )

            # Apply sentiment analysis
            company_serps["sentiment"] = company_serps["snippet"].apply(
                lambda snippet: analyzer.polarity_scores(str(snippet))["compound"]
            )
            processed_dfs.append(company_serps)

    if not processed_dfs:
        print("No SERP data processed.")
        return

    # Combine processed data
    final_df = pd.concat(processed_dfs, ignore_index=True)

    # Save processed data
    output_path = os.path.join(BASE_DIR, "data_ceos", "processed_serps", f"{today}.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_df.to_csv(output_path, index=False)

    print(f"Processed SERP data saved to: {output_path}")
    print("SERP data processing complete.")

if __name__ == "__main__":
    main()