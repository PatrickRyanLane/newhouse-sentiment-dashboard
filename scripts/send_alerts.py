#!/usr/bin/env python3
"""
Reads the latest daily_counts.csv files for each entity type (brands, CEOs, ROCs)
and triggers email alerts if the negative sentiment threshold is met.
"""
from __future__ import annotations

import os
from typing import List, Dict, Any
import pandas as pd

# Prefer package import if run as a module; fall back when executed directly from scripts/
try:
    from scripts.email_utils import check_and_send_alerts  # type: ignore
except Exception:  # noqa: BLE001
    from email_utils import check_and_send_alerts  # type: ignore

def _load_counts(csv_path: str) -> pd.DataFrame | None:
    if not os.path.exists(csv_path):
        print(f"Info: {csv_path} not found; skipping.")
        return None
    try:
        df = pd.read_csv(csv_path)
        required = {"date", "name", "neg", "tot"}
        missing = required - set(df.columns)
        if missing:
            print(f"Warning: {csv_path} missing columns: {sorted(missing)}; skipping.")
            return None
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df["neg"] = pd.to_numeric(df["neg"], errors="coerce").fillna(0).astype(int)
        df["tot"] = pd.to_numeric(df["tot"], errors="coerce").fillna(0).astype(int)
        return df
    except Exception as e:  # noqa: BLE001
        print(f"Error reading {csv_path}: {e}")
        return None

def _prepare_entities_for_date(df: pd.DataFrame) -> tuple[List[Dict[str, Any]], str] | None:
    if df.empty:
        return None
    most_recent = df["date"].max()
    cur = df[df["date"] == most_recent].copy()
    cur = cur.groupby("name", as_index=False).agg({"neg": "sum", "tot": "sum"})
    entities = cur.to_dict(orient="records")
    return entities, most_recent.isoformat()

def main() -> None:
    MAILGUN_API_KEY = os.environ.get("MAILGUN_API_KEY")
    MAILGUN_DOMAIN = os.environ.get("MAILGUN_DOMAIN")
    MAILGUN_FROM = os.environ.get("MAILGUN_FROM")
    MAILGUN_TO = os.environ.get("MAILGUN_TO")
    MAILGUN_REGION = os.environ.get("MAILGUN_REGION")
    ALERT_SEND_MODE = os.environ.get("ALERT_SEND_MODE", "same_morning")

    if not (MAILGUN_API_KEY and MAILGUN_DOMAIN and MAILGUN_FROM and MAILGUN_TO):
        raise SystemExit("Error: MAILGUN_API_KEY, MAILGUN_DOMAIN, MAILGUN_FROM, and MAILGUN_TO must be set.")

    recipients = [addr.strip() for addr in MAILGUN_TO.split(",") if addr.strip()]

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
