#!/usr/bin/env python3
"""
Utilities for handling email alerts and cooldown periods.
"""

import os
import json
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ------------- Config -----------------
NEGATIVE_THRESHOLD = 0.4  # 40%
ALERT_COOLDOWN_DAYS = 180 # 6 months

# Soft time-shift so very-early runs still write "yesterday"
EASTERN = ZoneInfo("US/Eastern")
SOFT_SHIFT_HOURS = 6  # if before 6am ET, use previous date


def read_last_alert_dates() -> dict[str, str]:
    """Reads the last alert dates from data/last_alert_dates.json."""
    path = os.path.join("data", "last_alert_dates.json")
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}


def write_last_alert_dates(dates: dict[str, str]) -> None:
    """Writes the last alert dates to data/last_alert_dates.json."""
    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", "last_alert_dates.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(dates, f, indent=2)


def send_kit_summary_broadcast(run_date_str: str, entities_to_alert: list[dict], KIT_API_KEY: str, KIT_TAG_ID: str, entity_type: str = "Brand") -> bool:
    """Send a single broadcast to Kit for multiple entities, scheduled for 9am ET the next day. Returns True on success."""
    if not (KIT_API_KEY and KIT_TAG_ID):
        print("Warning: KIT_API_KEY or KIT_TAG_ID is not set. Skipping broadcast.")
        return False

    # Calculate the timestamp for 9am Eastern on the day after the run_date
    run_date = datetime.fromisoformat(run_date_str).date()
    send_day = run_date + timedelta(days=1)
    send_time = datetime(send_day.year, send_day.month, send_day.day, 9, 0, 0, tzinfo=EASTERN)
    send_at_iso = send_time.isoformat()

    entity_count = len(entities_to_alert)
    plural_entity = entity_type + "s" if entity_count > 1 else entity_type
    subject = f"High Negative Sentiment Alert for {entity_count} {plural_entity}"

    content_html = f"<p>The following {entity_type.lower()}s have high negative sentiment for {run_date_str}:</p><ul>"
    for entity_info in entities_to_alert:
        entity_name = entity_info.get("name")
        neg = entity_info.get("neg")
        tot = entity_info.get("tot")
        if entity_name and neg is not None and tot is not None and tot > 0:
            pct = round((neg / tot) * 100)
            content_html += f"<li><strong>{entity_name}:</strong> {neg}/{tot} ({pct}%) negative articles.</li>"
    content_html += "</ul>"

    try:
        tag_id = int(KIT_TAG_ID)
    except (ValueError, TypeError):
        print(f"Error: KIT_TAG_ID '{KIT_TAG_ID}' is not a valid integer.")
        return False

    url = "https://api.kit.com/v4/broadcasts"
    headers = {
        "X-Kit-Api-Key": KIT_API_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "subject": subject,
        "content": content_html,
        "tag_ids": [tag_id],
        "send_at": send_at_iso
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        print(f"Scheduled Kit summary broadcast for {entity_count} {plural_entity.lower()} to be sent at {send_at_iso}.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error scheduling Kit summary broadcast: {e}")
        return False


def now_eastern_date_str() -> str:
    """Returns the current date string in YYYY-MM-DD format for US/Eastern, shifting back if before SOFT_SHIFT_HOURS."""
    now = datetime.now(EASTERN)
    if now.hour < SOFT_SHIFT_HOURS:
        now = now - timedelta(hours=SOFT_SHIFT_HOURS)
    return now.date().isoformat()

def check_and_send_alerts(entities: list[dict], run_date: str, KIT_API_KEY: str, KIT_TAG_ID: str, entity_type: str = "Brand"):
    """
    Checks which entities meet the alert criteria and sends a summary broadcast.
    Updates the cooldown dates for alerted entities.
    """
    last_alert_dates = read_last_alert_dates()
    entities_to_alert: list[dict] = []

    for entity_data in entities:
        entity_name = entity_data.get("brand") # The daily_counts csv uses 'brand' for the entity name
        neg = entity_data.get("negative")
        tot = entity_data.get("total")

        if not (entity_name and isinstance(neg, int) and isinstance(tot, int) and tot > 0):
            continue

        if (neg / tot) < NEGATIVE_THRESHOLD:
            continue

        # Create a unique key for the entity
        entity_key = f"{entity_type}:{entity_name}"

        last_alert_date_str = last_alert_dates.get(entity_key) # Use the new key
        if last_alert_date_str:
            last_alert_date = datetime.fromisoformat(last_alert_date_str).date()
            days_since_last_alert = (datetime.now(EASTERN).date() - last_alert_date).days
            if days_since_last_alert < ALERT_COOLDOWN_DAYS:
                continue  # Still in cooldown

        entities_to_alert.append({"name": entity_name, "neg": neg, "tot": tot})

    if entities_to_alert:
        if send_kit_summary_broadcast(run_date, entities_to_alert, KIT_API_KEY, KIT_TAG_ID, entity_type):
            today_str = now_eastern_date_str()
            for alert_info in entities_to_alert:
                # Also use the unique key here
                entity_key = f"{entity_type}:{alert_info['name']}"
                last_alert_dates[entity_key] = today_str
            write_last_alert_dates(last_alert_dates)