#!/usr/bin/env python3
"""
Google Sheets Helper Module - COMPLETE VERSION

Provides convenience functions for writing ALL dashboard data to Google Sheets:
  - Brand SERPs (row-level, daily, rollup)
  - CEO SERPs (row-level, daily, rollup)
  - Brand Articles/News (row-level, daily, rollup)
  - CEO Articles/News (row-level, daily, rollup)

Environment Variables Expected:
  - GOOGLE_SHEET_ID: Your Google Sheet ID
  - GOOGLE_CREDENTIALS_PATH: Path to credentials file
"""

import os
from typing import Optional
import pandas as pd

# Try to import Google Sheets packages - fail gracefully if not available
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    SHEETS_AVAILABLE = True
except ImportError:
    SHEETS_AVAILABLE = False
    print("[INFO] Google Sheets packages not installed.")
    print("      Install: pip install google-auth google-api-python-client")
    print("[INFO] Scripts will work but only write to CSV.")

# ========================================
# CONFIGURATION
# ========================================

SPREADSHEET_ID = os.environ.get('GOOGLE_SHEET_ID', 'YOUR_SHEET_ID_HERE')
CREDENTIALS_PATH = os.environ.get('GOOGLE_CREDENTIALS_PATH', 'credentials/google-sheets-credentials.json')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

if __name__ != '__main__' and SHEETS_AVAILABLE:
    sheet_id_display = SPREADSHEET_ID[:20] + "..." if len(SPREADSHEET_ID) > 20 else SPREADSHEET_ID
    print(f"[DEBUG] Sheet ID: {sheet_id_display}")
    print(f"[DEBUG] Credentials: {CREDENTIALS_PATH}")


# ========================================
# CORE FUNCTIONS
# ========================================

def get_sheets_service():
    """Create and return Google Sheets API service."""
    if not SHEETS_AVAILABLE:
        raise ImportError("Google Sheets packages not installed")
    
    if not os.path.exists(CREDENTIALS_PATH):
        raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_PATH}")
    
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH, scopes=SCOPES
    )
    service = build('sheets', 'v4', credentials=credentials)
    return service


def dataframe_to_sheet_values(df: pd.DataFrame) -> list:
    """Convert pandas DataFrame to Google Sheets format."""
    headers = df.columns.tolist()
    values = [headers] + df.values.tolist()
    return values


def write_to_sheet(
    df: pd.DataFrame, 
    sheet_name: str, 
    date: Optional[str] = None,
    clear_first: bool = False
) -> bool:
    """Write a pandas DataFrame to a Google Sheet tab."""
    if not SHEETS_AVAILABLE:
        print(f"[SKIP] Sheets not available - skipping {sheet_name}")
        return False
    
    full_sheet_name = f"{date}-{sheet_name}" if date else sheet_name
    
    try:
        service = get_sheets_service()
        
        # Check if sheet tab exists, create if not
        spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheet_exists = any(s['properties']['title'] == full_sheet_name for s in spreadsheet['sheets'])
        
        if not sheet_exists:
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={'requests': [{'addSheet': {'properties': {'title': full_sheet_name}}}]}
            ).execute()
            print(f"[INFO] Created sheet tab: {full_sheet_name}")
        
        # Clear if requested
        if clear_first:
            service.spreadsheets().values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f'{full_sheet_name}!A1:ZZ',
                body={}
            ).execute()
        
        # Write data
        values = dataframe_to_sheet_values(df)
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{full_sheet_name}!A1',
            valueInputOption='RAW',
            body={'values': values}
        ).execute()
        
        rows_updated = result.get('updatedRows', 0)
        print(f"[OK] Wrote {rows_updated} rows to: {full_sheet_name}")
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to write {full_sheet_name}: {e}")
        return False


def update_rollup_sheet(
    new_data_df: pd.DataFrame,
    sheet_name: str = 'DailyCounts',
    date_column: str = 'date'
) -> bool:
    """Update rolling index sheet (removes old date data, adds new)."""
    if not SHEETS_AVAILABLE:
        print(f"[SKIP] Sheets not available - skipping rollup")
        return False
    
    try:
        service = get_sheets_service()
        
        # Read existing data
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A:ZZ'
        ).execute()
        
        existing_values = result.get('values', [])
        
        if existing_values:
            headers = existing_values[0]
            data_rows = existing_values[1:]
            existing_df = pd.DataFrame(data_rows, columns=headers)
            
            # Remove rows for dates we're updating
            dates_to_update = new_data_df[date_column].unique()
            existing_df = existing_df[~existing_df[date_column].isin(dates_to_update)]
            
            # Combine and sort
            combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
            combined_df = combined_df.sort_values(date_column).reset_index(drop=True)
        else:
            combined_df = new_data_df
        
        return write_to_sheet(combined_df, sheet_name, clear_first=True)
        
    except Exception as e:
        print(f"[ERROR] Failed to update rollup: {e}")
        return False


# ========================================
# CONVENIENCE FUNCTIONS - BRAND SERPS
# ========================================

def write_brand_serps_to_sheets(
    rows_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    rollup_df: pd.DataFrame,
    target_date: str
) -> bool:
    """Write all three brand SERP dataframes to Google Sheets."""
    if not SHEETS_AVAILABLE:
        print(f"[SKIP] Sheets not available - brand SERPs only in CSV")
        return False
    
    print(f"\n[INFO] Writing brand SERP data to Google Sheets ({target_date})...")
    
    success = all([
        write_to_sheet(rows_df, 'BrandSERPs-Modal', date=target_date, clear_first=True),
        write_to_sheet(daily_df, 'BrandSERPs-Table', date=target_date, clear_first=True),
        update_rollup_sheet(rollup_df, 'BrandSERPs-DailyCounts', date_column='date')
    ])
    
    return success


# ========================================
# CONVENIENCE FUNCTIONS - CEO SERPS
# ========================================

def write_ceo_serps_to_sheets(
    rows_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    rollup_df: pd.DataFrame,
    target_date: str
) -> bool:
    """Write all three CEO SERP dataframes to Google Sheets."""
    if not SHEETS_AVAILABLE:
        print(f"[SKIP] Sheets not available - CEO SERPs only in CSV")
        return False
    
    print(f"\n[INFO] Writing CEO SERP data to Google Sheets ({target_date})...")
    
    success = all([
        write_to_sheet(rows_df, 'CEOSERPs-Modal', date=target_date, clear_first=True),
        write_to_sheet(daily_df, 'CEOSERPs-Table', date=target_date, clear_first=True),
        update_rollup_sheet(rollup_df, 'CEOSERPs-DailyCounts', date_column='date')
    ])
    
    return success


# ========================================
# CONVENIENCE FUNCTIONS - BRAND ARTICLES
# ========================================

def write_brand_articles_to_sheets(
    daily_df: pd.DataFrame,
    rollup_df: pd.DataFrame,
    target_date: str
) -> bool:
    """Write brand article sentiment data to Google Sheets."""
    if not SHEETS_AVAILABLE:
        print(f"[SKIP] Sheets not available - brand articles only in CSV")
        return False
    
    print(f"\n[INFO] Writing brand article data to Google Sheets ({target_date})...")
    
    success = all([
        write_to_sheet(daily_df, 'BrandArticles-Table', date=target_date, clear_first=True),
        update_rollup_sheet(rollup_df, 'BrandArticles-DailyCounts', date_column='date')
    ])
    
    return success


# ========================================
# CONVENIENCE FUNCTIONS - CEO ARTICLES
# ========================================

def write_ceo_articles_to_sheets(
    daily_df: pd.DataFrame,
    rollup_df: pd.DataFrame,
    target_date: str
) -> bool:
    """Write CEO article sentiment data to Google Sheets."""
    if not SHEETS_AVAILABLE:
        print(f"[SKIP] Sheets not available - CEO articles only in CSV")
        return False
    
    print(f"\n[INFO] Writing CEO article data to Google Sheets ({target_date})...")
    
    success = all([
        write_to_sheet(daily_df, 'CEOArticles-Table', date=target_date, clear_first=True),
        update_rollup_sheet(rollup_df, 'CEOArticles-DailyCounts', date_column='date')
    ])
    
    return success


# ========================================
# BACKWARDS COMPATIBILITY
# ========================================

# Alias for backwards compatibility
write_serps_to_sheets = write_brand_serps_to_sheets
write_articles_to_sheets = write_brand_articles_to_sheets


# ========================================
# TESTING
# ========================================

def test_connection():
    """Test connection to Google Sheets."""
    if not SHEETS_AVAILABLE:
        print("❌ Google Sheets packages not installed")
        print("\nInstall with:")
        print("  pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client")
        return False
    
    print(f"[DEBUG] Sheet ID: {SPREADSHEET_ID}")
    print(f"[DEBUG] Credentials: {CREDENTIALS_PATH}")
    
    try:
        service = get_sheets_service()
        spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        
        title = spreadsheet.get('properties', {}).get('title', 'Unknown')
        sheets = spreadsheet.get('sheets', [])
        
        print(f"\n✅ Successfully connected to: {title}")
        print(f"✅ Sheet ID: {SPREADSHEET_ID}")
        print(f"✅ Found {len(sheets)} sheet tabs:")
        for sheet in sheets:
            print(f"   - {sheet['properties']['title']}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        print(f"\nTroubleshooting:")
        print(f"  1. Verify GOOGLE_SHEET_ID: {SPREADSHEET_ID}")
        print(f"  2. Check credentials file: {CREDENTIALS_PATH}")
        print(f"  3. Ensure service account has Editor access to sheet")
        return False


if __name__ == '__main__':
    print("Testing Google Sheets connection...\n")
    test_connection()
