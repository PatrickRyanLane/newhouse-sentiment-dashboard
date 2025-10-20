#!/usr/bin/env python3
"""
Google Sheets Helper Module - WITH EDIT PRESERVATION

This version PRESERVES student edits by:
1. Reading existing data from Sheets before writing
2. Merging new data with existing data
3. Keeping student edits for matching rows (by URL)
4. Only adding new rows that don't exist yet

This ensures student manual corrections persist across daily script runs!
"""

import os
from typing import Optional
import pandas as pd

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    SHEETS_AVAILABLE = True
except ImportError:
    SHEETS_AVAILABLE = False
    print("[INFO] Google Sheets packages not installed.")

SPREADSHEET_ID = os.environ.get('GOOGLE_SHEET_ID', 'YOUR_SHEET_ID_HERE')
CREDENTIALS_PATH = os.environ.get('GOOGLE_CREDENTIALS_PATH', 'credentials/google-sheets-credentials.json')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

if __name__ != '__main__' and SHEETS_AVAILABLE:
    sheet_id_display = SPREADSHEET_ID[:20] + "..." if len(SPREADSHEET_ID) > 20 else SPREADSHEET_ID
    print(f"[DEBUG] Sheet ID: {sheet_id_display}")

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
    """Convert pandas DataFrame to Google Sheets format.
    
    Handles NaN values by replacing them with empty strings,
    since Google Sheets API doesn't support NaN in JSON.
    
    Why: When pandas reads CSV files, missing values become NaN (Not a Number).
    Google Sheets API converts data to JSON for uploading, but JSON doesn't
    support NaN, so we replace NaN with empty strings.
    """
    # Replace NaN with empty strings (Google Sheets doesn't support NaN in JSON)
    df = df.fillna('')
    
    headers = df.columns.tolist()
    values = [headers] + df.values.tolist()
    return values

def read_from_sheet(sheet_name: str, date: Optional[str] = None, sheet_id: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Read existing data from a Google Sheet tab.
    Returns DataFrame if tab exists, None if it doesn't.
    
    Args:
        sheet_name: Name of the sheet tab
        date: Optional date prefix for tab name
        sheet_id: Optional Google Sheet ID to override default
    """
    if not SHEETS_AVAILABLE:
        return None
    
    target_sheet_id = sheet_id if sheet_id else SPREADSHEET_ID
    full_sheet_name = f"{date}-{sheet_name}" if date else sheet_name
    
    try:
        service = get_sheets_service()
        
        # Check if tab exists
        spreadsheet = service.spreadsheets().get(spreadsheetId=target_sheet_id).execute()
        sheet_exists = any(s['properties']['title'] == full_sheet_name for s in spreadsheet['sheets'])
        
        if not sheet_exists:
            return None
        
        # Read data
        result = service.spreadsheets().values().get(
            spreadsheetId=target_sheet_id,
            range=f'{full_sheet_name}!A:ZZ'
        ).execute()
        
        values = result.get('values', [])
        if not values or len(values) < 2:
            return None
        
        # Convert to DataFrame
        headers = values[0]
        data = values[1:]
        df = pd.DataFrame(data, columns=headers)
        
        print(f"[INFO] Read {len(df)} existing rows from: {full_sheet_name}")
        return df
        
    except Exception as e:
        print(f"[WARN] Could not read from sheet {full_sheet_name}: {e}")
        return None

def merge_preserving_edits(
    new_df: pd.DataFrame,
    existing_df: Optional[pd.DataFrame],
    key_column: str = 'url',
    preserve_columns: list = ['sentiment', 'controlled']
) -> pd.DataFrame:
    """
    Merge new data with existing data, preserving student edits.
    
    Logic:
    - For rows that exist in both (matched by key_column, e.g. URL):
      → Keep the existing values for preserve_columns (student edits)
      → Update other columns with fresh data
    - For rows only in new_df:
      → Add them (new articles/SERPs discovered)
    - For rows only in existing_df:
      → Keep them (might be manually added or from old data)
    
    Args:
        new_df: Fresh data from scripts (algorithmic classification)
        existing_df: Data currently in Google Sheets (may have student edits)
        key_column: Column to match rows (usually 'url')
        preserve_columns: Columns to preserve from existing (student-edited values)
    
    Returns:
        Merged DataFrame with student edits preserved
    """
    if existing_df is None or existing_df.empty:
        # No existing data, just use new data
        print(f"[INFO] No existing data - using fresh data")
        return new_df
    
    # Ensure key column exists
    if key_column not in new_df.columns or key_column not in existing_df.columns:
        print(f"[WARN] Key column '{key_column}' not found - can't preserve edits")
        return new_df
    
    # Create lookup of existing data by key
    existing_dict = {}
    for _, row in existing_df.iterrows():
        key = str(row.get(key_column, '')).strip()
        if key:
            existing_dict[key] = row.to_dict()
    
    # Merge logic
    merged_rows = []
    edits_preserved = 0
    new_rows_added = 0
    
    for _, new_row in new_df.iterrows():
        key = str(new_row.get(key_column, '')).strip()
        
        if key and key in existing_dict:
            # Row exists - preserve student edits
            existing_row = existing_dict[key]
            merged_row = new_row.to_dict()
            
            # Preserve specified columns from existing data
            for col in preserve_columns:
                if col in existing_row and col in merged_row:
                    # Keep the existing value (student may have edited it)
                    merged_row[col] = existing_row[col]
                    edits_preserved += 1
            
            merged_rows.append(merged_row)
            # Remove from dict so we can track rows only in existing
            del existing_dict[key]
        else:
            # New row - add it
            merged_rows.append(new_row.to_dict())
            new_rows_added += 1
    
    # Add any rows that were in existing but not in new
    # (might be manually added or old data)
    for key, existing_row in existing_dict.items():
        merged_rows.append(existing_row)
    
    merged_df = pd.DataFrame(merged_rows)
    
    # Preserve column order from new_df
    cols = [c for c in new_df.columns if c in merged_df.columns]
    merged_df = merged_df[cols]
    
    print(f"[INFO] Merge complete: {edits_preserved} edits preserved, {new_rows_added} new rows added")
    
    return merged_df

def write_to_sheet(
    df: pd.DataFrame, 
    sheet_name: str, 
    date: Optional[str] = None,
    preserve_edits: bool = True,
    key_column: str = 'url',
    preserve_columns: list = None,
    sheet_id: Optional[str] = None
) -> bool:
    """
    Write DataFrame to Google Sheet, optionally preserving student edits.
    
    Args:
        preserve_edits: If True, read existing data and preserve edits
        key_column: Column to match rows (default: 'url')
        preserve_columns: Columns to preserve (default: ['sentiment', 'controlled'])
        sheet_id: Optional Google Sheet ID to override default (for smart routing)
    """
    if not SHEETS_AVAILABLE:
        print(f"[SKIP] Sheets not available - skipping {sheet_name}")
        return False
    
    if preserve_columns is None:
        preserve_columns = ['sentiment', 'controlled']
    
    # Use provided sheet_id or fall back to default
    target_sheet_id = sheet_id if sheet_id else SPREADSHEET_ID
    
    full_sheet_name = f"{date}-{sheet_name}" if date else sheet_name
    
    try:
        service = get_sheets_service()
        
        # Check if sheet tab exists
        spreadsheet = service.spreadsheets().get(spreadsheetId=target_sheet_id).execute()
        sheet_exists = any(s['properties']['title'] == full_sheet_name for s in spreadsheet['sheets'])
        
        # If preserving edits and sheet exists, read existing data
        if preserve_edits and sheet_exists:
            existing_df = read_from_sheet(sheet_name, date, sheet_id=target_sheet_id)
            df = merge_preserving_edits(df, existing_df, key_column, preserve_columns)
        
        # Create tab if doesn't exist
        if not sheet_exists:
            service.spreadsheets().batchUpdate(
                spreadsheetId=target_sheet_id,
                body={'requests': [{'addSheet': {'properties': {'title': full_sheet_name}}}]}
            ).execute()
            print(f"[INFO] Created sheet tab: {full_sheet_name}")
        
        # Clear and write
        service.spreadsheets().values().clear(
            spreadsheetId=target_sheet_id,
            range=f'{full_sheet_name}!A1:ZZ',
            body={}
        ).execute()
        
        values = dataframe_to_sheet_values(df)
        result = service.spreadsheets().values().update(
            spreadsheetId=target_sheet_id,
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
    sheet_id: Optional[str] = None
) -> bool:
    """Update rolling index sheet (creates if doesn't exist).
    
    Args:
        new_data_df: DataFrame with new data to add/update
        sheet_name: Name of the rollup sheet tab
        date_column: Column name containing dates
        sheet_id: Optional Google Sheet ID to override default
    """
    if not SHEETS_AVAILABLE:
        return False
    
    target_sheet_id = sheet_id if sheet_id else SPREADSHEET_ID
    
    try:
        service = get_sheets_service()
        
        # First, check if the sheet exists
        spreadsheet = service.spreadsheets().get(spreadsheetId=target_sheet_id).execute()
        sheet_exists = any(s['properties']['title'] == sheet_name for s in spreadsheet['sheets'])
        
        combined_df = new_data_df.copy()
        
        # If sheet exists, read and merge data
        if sheet_exists:
            try:
                result = service.spreadsheets().values().get(
                    spreadsheetId=target_sheet_id,
                    range=f'{sheet_name}!A:ZZ'
                ).execute()
                
                existing_values = result.get('values', [])
                
                if existing_values and len(existing_values) > 1:
                    headers = existing_values[0]
                    data_rows = existing_values[1:]
                    existing_df = pd.DataFrame(data_rows, columns=headers)
                    
                    # Remove rows for dates we're updating
                    dates_to_update = new_data_df[date_column].unique()
                    existing_df = existing_df[~existing_df[date_column].isin(dates_to_update)]
                    
                    # Combine and sort
                    combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
                    combined_df = combined_df.sort_values(date_column).reset_index(drop=True)
                    print(f"[INFO] Merged rollup data: {len(existing_df)} existing + {len(new_data_df)} new = {len(combined_df)} total")
            except Exception as read_error:
                print(f"[WARN] Could not read existing rollup data: {read_error}")
                print(f"[INFO] Will use fresh data only")
                combined_df = new_data_df.copy()
        else:
            print(f"[INFO] Rollup sheet '{sheet_name}' doesn't exist yet - will create")
        
        return write_to_sheet(combined_df, sheet_name, preserve_edits=False, sheet_id=target_sheet_id)
        
    except Exception as e:
        print(f"[ERROR] Failed to update rollup: {e}")
        return False

# ========================================
# CONVENIENCE FUNCTIONS
# ========================================

def write_brand_serps_to_sheets(
    rows_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    rollup_df: pd.DataFrame,
    target_date: str
) -> bool:
    """Write brand SERP data, preserving student edits on sentiment and control."""
    if not SHEETS_AVAILABLE:
        return False
    
    print(f"\n[INFO] Writing brand SERP data to Google Sheets ({target_date})...")
    print(f"[INFO] Preserving student edits for existing rows...")
    
    success = all([
        # Modal: Preserve edits (students edit these!)
        write_to_sheet(rows_df, 'brand-serps-modal', date=target_date, 
                      preserve_edits=True, key_column='url', 
                      preserve_columns=['sentiment', 'controlled']),
        # Table: Don't preserve (auto-calculated)
        write_to_sheet(daily_df, 'brand-serps-table', date=target_date, preserve_edits=False),
        # Rollup: Update with fresh data
        update_rollup_sheet(rollup_df, 'brand-serps-daily-counts-chart', date_column='date')
    ])
    
    return success

def write_ceo_serps_to_sheets(
    rows_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    rollup_df: pd.DataFrame,
    target_date: str
) -> bool:
    """Write CEO SERP data, preserving student edits."""
    if not SHEETS_AVAILABLE:
        return False
    
    print(f"\n[INFO] Writing CEO SERP data to Google Sheets ({target_date})...")
    print(f"[INFO] Preserving student edits for existing rows...")
    
    success = all([
        write_to_sheet(rows_df, 'ceo-serps-modal', date=target_date,
                      preserve_edits=True, key_column='url',
                      preserve_columns=['sentiment', 'controlled']),
        write_to_sheet(daily_df, 'ceo-serps-table', date=target_date, preserve_edits=False),
        update_rollup_sheet(rollup_df, 'ceo-serps-daily-counts-chart', date_column='date')
    ])
    
    return success

def write_brand_articles_to_sheets(
    rows_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    rollup_df: pd.DataFrame,
    target_date: str
) -> bool:
    """
    Write all three brand article dataframes to Google Sheets (mirrors SERP pattern).
    
    Args:
        rows_df: Individual articles (modal sheet) - preserves student edits
        daily_df: Aggregated counts by company and sentiment (table sheet)
        rollup_df: Rolling index over time (chart sheet)
        target_date: Date string (YYYY-MM-DD)
    """
    if not SHEETS_AVAILABLE:
        return False
    
    print(f"\n[INFO] Writing brand article data to Google Sheets ({target_date})...")
    print(f"[INFO] Preserving student sentiment edits for existing articles...")
    
    success = all([
        # Modal: Preserve sentiment edits (students correct these!)
        write_to_sheet(rows_df, 'brand-articles-modal', date=target_date, 
                      preserve_edits=True, key_column='url',
                      preserve_columns=['sentiment']),
        # Table: Fresh aggregated data (auto-calculated)
        write_to_sheet(daily_df, 'brand-articles-table', date=target_date, preserve_edits=False),
        # Rollup: Update rolling index
        update_rollup_sheet(rollup_df, 'brand-articles-daily-counts-chart', date_column='date')
    ])
    
    return success

def write_ceo_articles_to_sheets(
    rows_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    rollup_df: pd.DataFrame,
    target_date: str
) -> bool:
    """
    Write all three CEO article dataframes to Google Sheets (mirrors SERP pattern).
    
    Args:
        rows_df: Individual articles (modal sheet) - preserves student edits
        daily_df: Aggregated counts by CEO and sentiment (table sheet)
        rollup_df: Rolling index over time (chart sheet)
        target_date: Date string (YYYY-MM-DD)
    """
    if not SHEETS_AVAILABLE:
        return False
    
    print(f"\n[INFO] Writing CEO article data to Google Sheets ({target_date})...")
    print(f"[INFO] Preserving student sentiment edits for existing articles...")
    
    success = all([
        # Modal: Preserve sentiment edits (students correct these!)
        write_to_sheet(rows_df, 'ceo-articles-modal', date=target_date,
                      preserve_edits=True, key_column='url',
                      preserve_columns=['sentiment']),
        # Table: Fresh aggregated data (auto-calculated)
        write_to_sheet(daily_df, 'ceo-articles-table', date=target_date, preserve_edits=False),
        # Rollup: Update rolling index
        update_rollup_sheet(rollup_df, 'ceo-articles-daily-counts-chart', date_column='date')
    ])
    
    return success

# Special function for article Modal files (called by news_articles scripts)
def write_articles_modal_to_sheets(
    df: pd.DataFrame,
    sheet_name: str,
    target_date: str,
    is_ceo: bool = False
) -> bool:
    """
    Write articles modal file, preserving sentiment edits.
    Used by news_articles_brands.py and news_articles_ceos.py
    """
    if not SHEETS_AVAILABLE:
        return False
    
    data_type = "CEO" if is_ceo else "Brand"
    print(f"\n[INFO] Writing {data_type} articles modal to Google Sheets...")
    print(f"[INFO] Preserving student sentiment edits for existing articles...")
    
    return write_to_sheet(
        df, 
        sheet_name, 
        date=target_date,
        preserve_edits=True,
        key_column='url',
        preserve_columns=['sentiment']  # Only preserve sentiment for articles
    )

# Backwards compatibility
write_serps_to_sheets = write_brand_serps_to_sheets
write_articles_to_sheets = write_brand_articles_to_sheets

def test_connection():
    """Test connection to Google Sheets."""
    if not SHEETS_AVAILABLE:
        print("❌ Google Sheets packages not installed")
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
        print(f"✅ Found {len(sheets)} sheet tabs")
        print(f"✅ Tab limit remaining: {200 - len(sheets)}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        return False

if __name__ == '__main__':
    print("Testing Google Sheets connection...\n")
    test_connection()
