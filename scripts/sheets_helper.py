#!/usr/bin/env python3
"""
Google Sheets Helper Module - UPDATED VERSION

Provides convenience functions for writing ALL dashboard data to Google Sheets:
  - Brand SERPs (row-level, daily, rollup)
  - CEO SERPs (row-level, daily, rollup)
  - Brand Articles/News (row-level, daily, rollup)
  - CEO Articles/News (row-level, daily, rollup)

Environment Variables Expected:
  - GOOGLE_SHEET_ID_BRAND: Your Brand Google Sheet ID
  - GOOGLE_SHEET_ID_CEO: Your CEO Google Sheet ID
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

# Get the correct sheet ID based on data type
def get_spreadsheet_id(sheet_type: str = 'brand') -> str:
    """
    Get the appropriate Google Sheet ID based on the sheet type.
    
    Args:
        sheet_type: Either 'brand' or 'ceo'
    
    Returns:
        The spreadsheet ID for the requested type
    """
    if sheet_type.lower() == 'ceo':
        return os.environ.get('GOOGLE_SHEET_ID_CEO', 'YOUR_CEO_SHEET_ID_HERE')
    else:  # default to brand
        return os.environ.get('GOOGLE_SHEET_ID_BRAND', 'YOUR_BRAND_SHEET_ID_HERE')


CREDENTIALS_PATH = os.environ.get('GOOGLE_CREDENTIALS_PATH', 'credentials/google-sheets-credentials.json')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


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
    clear_first: bool = False,
    sheet_type: str = 'brand'
) -> bool:
    """
    Write a pandas DataFrame to a Google Sheet tab.
    
    Args:
        df: DataFrame to write
        sheet_name: Name of the sheet tab
        date: Optional date prefix for the tab name
        clear_first: Whether to clear existing data first
        sheet_type: Either 'brand' or 'ceo' to determine which sheet to write to
    """
    if not SHEETS_AVAILABLE:
        print(f"[SKIP] Sheets not available - skipping {sheet_name}")
        return False
    
    spreadsheet_id = get_spreadsheet_id(sheet_type)
    full_sheet_name = f"{date}-{sheet_name}" if date else sheet_name
    
    try:
        service = get_sheets_service()
        
        # Check if sheet tab exists, create if not
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_exists = any(s['properties']['title'] == full_sheet_name for s in spreadsheet['sheets'])
        
        if not sheet_exists:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': [{'addSheet': {'properties': {'title': full_sheet_name}}}]}
            ).execute()
            print(f"[INFO] Created sheet tab: {full_sheet_name}")
        
        # Clear if requested
        if clear_first:
            service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f'{full_sheet_name}!A1:ZZ',
                body={}
            ).execute()
        
        # Write data
        values = dataframe_to_sheet_values(df)
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
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
    date_column: str = 'date',
    sheet_type: str = 'brand'
) -> bool:
    """
    Update rolling index sheet (removes old date data, adds new).
    
    Args:
        new_data_df: New data to add/update
        sheet_name: Name of the rollup sheet
        date_column: Name of the date column
        sheet_type: Either 'brand' or 'ceo' to determine which sheet to write to
    """
    if not SHEETS_AVAILABLE:
        print(f"[SKIP] Sheets not available - skipping rollup")
        return False
    
    spreadsheet_id = get_spreadsheet_id(sheet_type)
    
    try:
        service = get_sheets_service()
        
        # Read existing data
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
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
        
        return write_to_sheet(combined_df, sheet_name, clear_first=True, sheet_type=sheet_type)
        
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
        write_to_sheet(rows_df, 'BrandSERPs-Modal', date=target_date, clear_first=True, sheet_type='brand'),
        write_to_sheet(daily_df, 'BrandSERPs-Table', date=target_date, clear_first=True, sheet_type='brand'),
        update_rollup_sheet(rollup_df, 'BrandSERPs-DailyCounts', date_column='date', sheet_type='brand')
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
        write_to_sheet(rows_df, 'CEOSERPs-Modal', date=target_date, clear_first=True, sheet_type='ceo'),
        write_to_sheet(daily_df, 'CEOSERPs-Table', date=target_date, clear_first=True, sheet_type='ceo'),
        update_rollup_sheet(rollup_df, 'CEOSERPs-DailyCounts', date_column='date', sheet_type='ceo')
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
        write_to_sheet(daily_df, 'BrandArticles-Table', date=target_date, clear_first=True, sheet_type='brand'),
        update_rollup_sheet(rollup_df, 'BrandArticles-DailyCounts', date_column='date', sheet_type='brand')
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
        write_to_sheet(daily_df, 'CEOArticles-Table', date=target_date, clear_first=True, sheet_type='ceo'),
        update_rollup_sheet(rollup_df, 'CEOArticles-DailyCounts', date_column='date', sheet_type='ceo')
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

def test_connection(sheet_type: str = 'brand'):
    """
    Test connection to Google Sheets.
    
    Args:
        sheet_type: Either 'brand' or 'ceo' to test that sheet
    """
    if not SHEETS_AVAILABLE:
        print("❌ Google Sheets packages not installed")
        print("\nInstall with:")
        print("  pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client")
        return False
    
    spreadsheet_id = get_spreadsheet_id(sheet_type)
    print(f"[DEBUG] Testing {sheet_type.upper()} Sheet ID: {spreadsheet_id}")
    print(f"[DEBUG] Credentials: {CREDENTIALS_PATH}")
    
    try:
        service = get_sheets_service()
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        
        title = spreadsheet.get('properties', {}).get('title', 'Unknown')
        sheets = spreadsheet.get('sheets', [])
        
        print(f"\n✅ Successfully connected to {sheet_type.upper()} sheet: {title}")
        print(f"✅ Sheet ID: {spreadsheet_id}")
        print(f"✅ Found {len(sheets)} sheet tabs:")
        for sheet in sheets:
            print(f"   - {sheet['properties']['title']}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        print(f"\nTroubleshooting:")
        print(f"  1. Verify GOOGLE_SHEET_ID_{sheet_type.upper()}: {spreadsheet_id}")
        print(f"  2. Check credentials file: {CREDENTIALS_PATH}")
        print(f"  3. Ensure service account has Editor access to sheet")
        return False


if __name__ == '__main__':
    print("Testing Google Sheets connections...\n")
    print("=" * 50)
    test_connection('brand')
    print("\n" + "=" * 50)
    test_connection('ceo')
