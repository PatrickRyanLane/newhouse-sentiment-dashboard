#!/usr/bin/env python3
"""
Cleanup script to standardize article daily-counts-chart sheets

This script:
1. Reads data from existing brand-articles-daily-counts-chart and ceo-articles-daily-counts-chart tabs
2. Renames columns to use standardized naming: positive_articles, neutral_articles, negative_articles
3. Writes back to the sheets with the new column structure

Why: We're standardizing column names to match SERP naming convention and make the schemas clear.

Before:
  date, company, positive, neutral, negative, total, neg_pct

After:
  date, company, positive_articles, neutral_articles, negative_articles, total, neg_pct
"""

import os
from pathlib import Path

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    SHEETS_AVAILABLE = True
except ImportError:
    SHEETS_AVAILABLE = False
    print("[ERROR] Google Sheets packages not installed. Cannot run cleanup.")
    exit(1)

SPREADSHEET_ID = os.environ.get('GOOGLE_SHEET_ID', 'YOUR_SHEET_ID_HERE')
CREDENTIALS_PATH = os.environ.get('GOOGLE_CREDENTIALS_PATH', 'credentials/google-sheets-credentials.json')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def get_sheets_service():
    """Create and return Google Sheets API service."""
    if not os.path.exists(CREDENTIALS_PATH):
        raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_PATH}")
    
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH, scopes=SCOPES
    )
    service = build('sheets', 'v4', credentials=credentials)
    return service

def cleanup_sheet(sheet_name: str, is_ceo: bool = False):
    """
    Clean up a daily-counts-chart sheet by standardizing column names.
    
    Args:
        sheet_name: Name of the sheet tab (e.g., 'brand-articles-daily-counts-chart')
        is_ceo: If True, use 'ceo' column name instead of 'company'
    """
    service = get_sheets_service()
    
    try:
        # Read existing data
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A:ZZ'
        ).execute()
        
        values = result.get('values', [])
        if not values or len(values) < 2:
            print(f"[INFO] Sheet '{sheet_name}' is empty or doesn't exist - skipping")
            return False
        
        headers = values[0]
        data_rows = values[1:]
        
        print(f"[INFO] Read {len(data_rows)} rows from '{sheet_name}'")
        print(f"[DEBUG] Original headers: {headers}")
        
        # Map old column names to new ones
        column_mapping = {
            'positive': 'positive_articles',
            'neutral': 'neutral_articles',
            'negative': 'negative_articles',
        }
        
        # Create new headers
        new_headers = []
        for col in headers:
            if col in column_mapping:
                new_headers.append(column_mapping[col])
            else:
                new_headers.append(col)
        
        # Check if renaming actually needed
        if new_headers == headers:
            print(f"[INFO] Sheet '{sheet_name}' already has correct column names - no action needed")
            return True
        
        print(f"[DEBUG] New headers: {new_headers}")
        
        # Create new values array with updated headers
        new_values = [new_headers] + data_rows
        
        # Clear and write back
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A1:ZZ',
            body={}
        ).execute()
        
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A1',
            valueInputOption='RAW',
            body={'values': new_values}
        ).execute()
        
        rows_updated = result.get('updatedRows', 0)
        print(f"[OK] Successfully updated '{sheet_name}' - {rows_updated} rows written")
        print(f"[OK] Column mapping: positive → positive_articles, neutral → neutral_articles, negative → negative_articles")
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to cleanup '{sheet_name}': {e}")
        return False

def main():
    print("=" * 70)
    print("Article Daily Counts Sheet Standardization Cleanup")
    print("=" * 70)
    print()
    print("This script standardizes column names in your article daily-counts-chart")
    print("sheets to match the SERP naming convention:")
    print()
    print("  positive  → positive_articles")
    print("  neutral   → neutral_articles")
    print("  negative  → negative_articles")
    print()
    print("=" * 70)
    print()
    
    if not SHEETS_AVAILABLE:
        print("[ERROR] Google Sheets packages not installed")
        return False
    
    print(f"[DEBUG] Sheet ID: {SPREADSHEET_ID}")
    print(f"[DEBUG] Credentials: {CREDENTIALS_PATH}")
    print()
    
    # Test connection
    try:
        service = get_sheets_service()
        spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        title = spreadsheet.get('properties', {}).get('title', 'Unknown')
        print(f"[OK] Connected to spreadsheet: {title}")
        print()
    except Exception as e:
        print(f"[ERROR] Could not connect to Google Sheets: {e}")
        return False
    
    # Clean up both sheets
    success = all([
        cleanup_sheet('brand-articles-daily-counts-chart', is_ceo=False),
        cleanup_sheet('ceo-articles-daily-counts-chart', is_ceo=True),
    ])
    
    print()
    if success:
        print("[OK] ✅ Cleanup complete! Your sheets now use standardized column names.")
        print()
        print("Next steps:")
        print("  1. The next pipeline run will write data with the new column names")
        print("  2. Your manual edits (sentiment, controlled flags) will be preserved")
        print("  3. All new data will use: positive_articles, neutral_articles, negative_articles")
    else:
        print("[ERROR] ❌ Some sheets could not be updated - check errors above")
    
    return success

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
