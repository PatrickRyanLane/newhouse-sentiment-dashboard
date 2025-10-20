#!/usr/bin/env python3
"""
Bulk CSV to Google Sheets Uploader with Smart Routing

This script reads multiple CSV files and uploads each one as a separate tab
in the appropriate Google Sheet based on filename keywords.

Smart Routing:
- Files with "brand" in filename → GOOGLE_SHEET_ID_BRAND
- Files with "ceo" in filename → GOOGLE_SHEET_ID_CEO
- Files with neither → Uses GOOGLE_SHEET_ID (default)

Environment Variables:
- GOOGLE_SHEET_ID_BRAND: Google Sheet ID for brand data
- GOOGLE_SHEET_ID_CEO: Google Sheet ID for CEO data
- GOOGLE_SHEET_ID: Default Google Sheet ID (fallback)

You can set these in:
1. GitHub Secrets (for CI/CD workflows)
2. Terminal: export GOOGLE_SHEET_ID_BRAND="..."
3. .env file in project root (for local development)

Usage:
    python bulk_csv_uploader.py --folder ./data/my_csvs

Or with custom routing (overrides filename detection):
    python bulk_csv_uploader.py --folder ./data --sheet-type brand
"""

import os
import sys
import glob
import argparse
import pandas as pd
from pathlib import Path

# Try to load from .env file if it exists (for local development)
try:
    from dotenv import load_dotenv
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"[INFO] Loaded environment variables from .env file")
except ImportError:
    pass  # python-dotenv not installed, that's fine

# Import the sheets_helper from the same directory
from sheets_helper import write_to_sheet, get_sheets_service

# Get sheet IDs from environment variables (GitHub Secrets or .env)
SHEET_ID_BRAND = os.environ.get('GOOGLE_SHEET_ID_BRAND')
SHEET_ID_CEO = os.environ.get('GOOGLE_SHEET_ID_CEO')
SHEET_ID_DEFAULT = os.environ.get('GOOGLE_SHEET_ID')

def detect_sheet_type(filename):
    """
    Detect which Google Sheet this file should go to based on filename.
    
    Why: When you have many CSVs, you want to organize them intelligently.
    By looking at the filename, we can route files to the right sheet
    without manual configuration.
    
    Args:
        filename: CSV filename (including path)
        
    Returns:
        Tuple of (sheet_type, sheet_id) - e.g., ('brand', 'sheet_id_abc123')
    """
    basename = os.path.basename(filename).lower()
    
    # Check for CEO first (since "ceo" is more specific than "brand")
    if 'ceo' in basename:
        if not SHEET_ID_CEO:
            print(f"⚠️  WARNING: File appears to be CEO data but GOOGLE_SHEET_ID_CEO not set")
            return ('unknown', SHEET_ID_DEFAULT)
        return ('ceo', SHEET_ID_CEO)
    
    # Check for brand
    if 'brand' in basename:
        if not SHEET_ID_BRAND:
            print(f"⚠️  WARNING: File appears to be brand data but GOOGLE_SHEET_ID_BRAND not set")
            return ('unknown', SHEET_ID_DEFAULT)
        return ('brand', SHEET_ID_BRAND)
    
    # Default
    return ('default', SHEET_ID_DEFAULT)

def find_csv_files(folder_path):
    """
    Find all CSV files in a folder.
    
    Args:
        folder_path: Path to search for CSV files
        
    Returns:
        List of CSV file paths, sorted alphabetically
    """
    csv_pattern = os.path.join(folder_path, '*.csv')
    csv_files = sorted(glob.glob(csv_pattern))
    
    if not csv_files:
        print(f"❌ No CSV files found in: {folder_path}")
        return []
    
    print(f"✅ Found {len(csv_files)} CSV file(s):\n")
    for f in csv_files:
        print(f"   - {os.path.basename(f)}")
    print()
    
    return csv_files

def csv_to_sheet_name(csv_filename):
    """
    Convert CSV filename to a valid Google Sheet tab name.
    
    Why: CSV names like '2025-01-15-brand-sentiment.csv' need to become
    a sheet name that Google Sheets accepts.
    
    Google Sheets tab names can't contain certain characters, so we
    clean them up while keeping them readable.
    
    Args:
        csv_filename: Name of the CSV file (without path)
        
    Returns:
        A cleaned sheet name suitable for Google Sheets
    """
    # Remove .csv extension
    name = csv_filename.replace('.csv', '')
    
    # Remove problematic characters (Google Sheets doesn't allow: ? * [ ] ! # @ $ %)
    invalid_chars = ['?', '*', '[', ']', '!', '#', '@', '$', '%']
    for char in invalid_chars:
        name = name.replace(char, '')
    
    # Truncate to 100 characters (Google's limit for sheet names)
    name = name[:100]
    
    return name

def upload_csvs_to_sheet(folder_path, sheet_type_override=None, preserve_edits=False, verbose=True):
    """
    Main function: upload all CSVs from a folder as separate sheet tabs.
    
    Smart routing logic:
    1. If sheet_type_override provided, use that for ALL files
    2. Otherwise, detect sheet type from each filename
    
    This function:
    1. Finds all CSV files in the folder
    2. Detects which Google Sheet each belongs to
    3. Reads each CSV into pandas
    4. Writes to the appropriate sheet tab
    
    Args:
        folder_path: Folder containing CSV files
        sheet_type_override: Force all files to 'brand', 'ceo', or None (auto-detect)
        preserve_edits: Whether to preserve existing data if tab already exists
        verbose: Print progress messages
        
    Returns:
        Dictionary with results (successful, failed, skipped)
    """
    
    # Expand home directory if user provided ~/path
    folder_path = os.path.expanduser(folder_path)
    
    # Check folder exists
    if not os.path.isdir(folder_path):
        print(f"❌ Folder not found: {folder_path}")
        return {'successful': 0, 'failed': 0, 'skipped': 0}
    
    if verbose:
        print(f"\n📁 Looking for CSVs in: {folder_path}\n")
    
    csv_files = find_csv_files(folder_path)
    if not csv_files:
        return {'successful': 0, 'failed': 0, 'skipped': 0}
    
    # Validate sheet IDs are configured
    sheet_ids_configured = {
        'brand': bool(SHEET_ID_BRAND),
        'ceo': bool(SHEET_ID_CEO),
        'default': bool(SHEET_ID_DEFAULT)
    }
    
    if not any(sheet_ids_configured.values()):
        print("❌ ERROR: No Google Sheet IDs configured!")
        print("\n   Please set one or more of these environment variables:")
        print("   - GOOGLE_SHEET_ID_BRAND")
        print("   - GOOGLE_SHEET_ID_CEO")
        print("   - GOOGLE_SHEET_ID")
        print("\n   Options:")
        print("   1. GitHub Secrets (for workflows)")
        print("   2. Terminal: export GOOGLE_SHEET_ID_BRAND='...'")
        print("   3. .env file (for local development)")
        print()
        return {'successful': 0, 'failed': 0, 'skipped': 0}
    
    if verbose:
        print("📊 Sheet routing configuration:")
        if sheet_ids_configured['brand']:
            print(f"   ✓ Brand sheet configured")
        if sheet_ids_configured['ceo']:
            print(f"   ✓ CEO sheet configured")
        if sheet_ids_configured['default']:
            print(f"   ✓ Default sheet configured")
        if sheet_type_override:
            print(f"   ⚠️  Override: All files → {sheet_type_override}")
        print()
    
    results = {'successful': 0, 'failed': 0, 'skipped': 0, 'by_type': {'brand': 0, 'ceo': 0, 'default': 0}}
    
    for csv_path in csv_files:
        csv_name = os.path.basename(csv_path)
        sheet_name = csv_to_sheet_name(csv_name)
        
        # Determine target sheet
        if sheet_type_override:
            sheet_type = sheet_type_override
            if sheet_type == 'brand':
                target_sheet_id = SHEET_ID_BRAND
            elif sheet_type == 'ceo':
                target_sheet_id = SHEET_ID_CEO
            else:
                target_sheet_id = SHEET_ID_DEFAULT
        else:
            sheet_type, target_sheet_id = detect_sheet_type(csv_path)
        
        # Skip if sheet ID not configured
        if not target_sheet_id:
            print(f"⏭️  Skipping: {csv_name} (no sheet ID for type '{sheet_type}')\n")
            results['skipped'] += 1
            continue
        
        try:
            # Read the CSV file
            if verbose:
                print(f"📖 Reading: {csv_name}")
            
            df = pd.read_csv(csv_path)
            rows_count = len(df)
            cols_count = len(df.columns)
            
            if verbose:
                sheet_type_display = f" [{sheet_type}]" if sheet_type != 'default' else ""
                print(f"   └─ Loaded {rows_count} rows, {cols_count} columns{sheet_type_display}")
            
            # Write to Google Sheets
            if verbose:
                print(f"📤 Uploading to sheet: '{sheet_name}'...")
            
            success = write_to_sheet(
                df,
                sheet_name,
                preserve_edits=preserve_edits,
                sheet_id=target_sheet_id
            )
            
            if success:
                print(f"   ✅ Success!\n")
                results['successful'] += 1
                results['by_type'][sheet_type] += 1
            else:
                print(f"   ❌ Failed!\n")
                results['failed'] += 1
                
        except Exception as e:
            print(f"   ❌ Error: {e}\n")
            results['failed'] += 1
    
    # Print summary
    print("\n" + "="*60)
    print("📊 UPLOAD SUMMARY")
    print("="*60)
    print(f"✅ Successful: {results['successful']}")
    if results['by_type']['brand'] > 0:
        print(f"   • Brand sheets: {results['by_type']['brand']}")
    if results['by_type']['ceo'] > 0:
        print(f"   • CEO sheets: {results['by_type']['ceo']}")
    if results['by_type']['default'] > 0:
        print(f"   • Default sheets: {results['by_type']['default']}")
    print(f"❌ Failed:     {results['failed']}")
    print(f"⏭️  Skipped:    {results['skipped']}")
    print("="*60 + "\n")
    
    return results

if __name__ == '__main__':
    # Set up command-line arguments
    parser = argparse.ArgumentParser(
        description='Upload multiple CSV files to Google Sheets with smart routing based on filename'
    )
    parser.add_argument(
        '--folder',
        help='Folder containing CSV files to upload',
        default='./data'  # Default to ./data folder
    )
    parser.add_argument(
        '--sheet-type',
        choices=['brand', 'ceo', 'default'],
        help='Force all files to upload to specific sheet type (overrides filename detection)'
    )
    parser.add_argument(
        '--preserve-edits',
        action='store_true',
        help='Preserve existing data if tab already exists'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress progress messages'
    )
    
    args = parser.parse_args()
    
    print("\n🚀 Bulk CSV to Google Sheets Uploader (Smart Routing)\n")
    
    # Run the upload
    results = upload_csvs_to_sheet(
        args.folder,
        sheet_type_override=args.sheet_type,
        preserve_edits=args.preserve_edits,
        verbose=not args.quiet
    )
    
    # Exit with appropriate code
    if results['failed'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)
