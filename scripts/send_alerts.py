#!/usr/bin/env python3
"""
Reads the latest daily_counts.csv files for each entity type (brands, CEOs, ROCs)
and triggers email alerts if the negative sentiment threshold is met.

This script is intended to be run after the main data processing scripts
(news_sentiment.py, news_sentiment_ceos.py, news_sentiment_roc.py) have completed.
"""
from __future__ import annotations

import os
from typing import List, Dict, Any
import pandas as pd

from email_utils import check_and_send_alerts

def _load_counts(csv_path: str) -> pd.DataFrame | None:
    if not os.path.exists(csv_path):
        print(f"Info: {csv_path} not found; skipping.")
        return None
    try:
        df = pd.read_csv(csv_path)
        # Expect at least: date, name, neg, tot
        required = {"date", "name", "neg", "tot"}
        missing = required - set(df.columns)
        if missing:
            print(f"Warning: {csv_path} missing columns: {sorted(missing)}; skipping.")
            return None
        # Coerce types
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["neg"] = pd.to_numeric(df["neg"], errors="coerce").fillna(0).astype(int)
        df["tot"] = pd.to_numeric(df["tot"], errors="coerce").fillna(0).astype(int)
        return df
    except Exception as e:
        print(f"Error reading {csv_path}: {e}")
        return None


def _prepare_entities_for_date(df: pd.DataFrame) -> tuple[List[Dict[str, Any]], str] | None:
    if df.empty:
        return None
    most_recent = df["date"].max()
    # filter to the most recent date
    cur = df[df["date"] == most_recent].copy()
    # Aggregate in case of duplicates
    cur = cur.groupby("name", as_index=False).agg({"neg": "sum", "tot": "sum"})
    entities = cur.to_dict(orient="records")
    run_date_str = most_recent.isoformat()
    return entities, run_date_str


def main() -> None:
    MAILGUN_API_KEY = os.environ.get("MAILGUN_API_KEY")
    MAILGUN_DOMAIN = os.environ.get("MAILGUN_DOMAIN")  # e.g., mg.example.com
    MAILGUN_FROM = os.environ.get("MAILGUN_FROM")      # e.g., "Alerts <alerts@mg.example.com>"
    MAILGUN_TO = os.environ.get("MAILGUN_TO")          # comma-separated list
    MAILGUN_REGION = os.environ.get("MAILGUN_REGION")  # set to 'eu' for EU domains, else leave blank
    ALERT_SEND_MODE = os.environ.get("ALERT_SEND_MODE", "same_morning")

    if not (MAILGUN_API_KEY and MAILGUN_DOMAIN and MAILGUN_FROM and MAILGUN_TO):
        raise SystemExit("Error: MAILGUN_API_KEY, MAILGUN_DOMAIN, MAILGUN_FROM, and MAILGUN_TO must be set.")

    recipients = [addr.strip() for addr in MAILGUN_TO.split(",") if addr.strip()]

    # (entity_type, csv_path) pairs — adjust paths to match your repo layout
    targets = [
        ("Brand", "data/daily_counts.csv"),
        ("CEO", "data_ceos/daily_counts.csv"),
        ("ROC", "data_roc/daily_counts.csv"),
    ]

    any_sent = False

    for entity_type, csv_path in targets:
        df = _load_counts(csv_path)
        if df is None:
            continue

        prepared = _prepare_entities_for_date(df)
        if not prepared:
            print(f"Info: no rows for {csv_path}; skipping.")
            continue

        entities, run_date_str = prepared
        check_and_send_alerts(
            entities,
            run_date_str,
            MAILGUN_API_KEY,
            MAILGUN_DOMAIN,
            MAILGUN_FROM,
            recipients,
            entity_type=entity_type,
            region=MAILGUN_REGION,
            schedule_mode=ALERT_SEND_MODE,
        )
        any_sent = True

    if not any_sent:
        print("Nothing to send across configured targets.")


if __name__ == "__main__":
    main()
