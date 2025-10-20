#!/usr/bin/env python3
"""
Bulk CSV to Google Sheets Uploader

This script reads multiple CSV files and uploads each one as a separate tab
in your Google Sheet. It's perfect for initial data setup!

Usage:
    python bulk_csv_uploader.py --folder ./data/my_csvs --sheet-id YOUR_SHEET_ID

Or configure defaults in this script and just run:
    python bulk_csv_uploader.py
"""

import os
import sys
import glob
import argparse
import pandas as pd
from pathlib import Path

# Import the sheets_helper from the project
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from scripts.sheets_helper import write_to_sheet, get_sheets_service

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

def upload_csvs_to_sheet(folder_path, preserve_edits=False, verbose=True):
    """
    Main function: upload all CSVs from a folder as separate sheet tabs.
    
    This function:
    1. Finds all CSV files in the folder
    2. Reads each CSV into pandas
    3. Creates a new tab in Google Sheets for each CSV
    4. Writes the data to that tab
    
    Args:
        folder_path: Folder containing CSV files
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
    
    results = {'successful': 0, 'failed': 0, 'skipped': 0}
    
    for csv_path in csv_files:
        csv_name = os.path.basename(csv_path)
        sheet_name = csv_to_sheet_name(csv_name)
        
        try:
            # Read the CSV file
            if verbose:
                print(f"📖 Reading: {csv_name}")
            
            df = pd.read_csv(csv_path)
            rows_count = len(df)
            cols_count = len(df.columns)
            
            if verbose:
                print(f"   └─ Loaded {rows_count} rows, {cols_count} columns")
            
            # Write to Google Sheets
            if verbose:
                print(f"📤 Uploading to sheet: '{sheet_name}'...")
            
            success = write_to_sheet(
                df,
                sheet_name,
                preserve_edits=preserve_edits
            )
            
            if success:
                print(f"   ✅ Success!\n")
                results['successful'] += 1
            else:
                print(f"   ❌ Failed!\n")
                results['failed'] += 1
                
        except Exception as e:
            print(f"   ❌ Error: {e}\n")
            results['failed'] += 1
    
    # Print summary
    print("\n" + "="*50)
    print("📊 UPLOAD SUMMARY")
    print("="*50)
    print(f"✅ Successful: {results['successful']}")
    print(f"❌ Failed:     {results['failed']}")
    print(f"⏭️  Skipped:    {results['skipped']}")
    print("="*50 + "\n")
    
    return results

if __name__ == '__main__':
    # Set up command-line arguments
    parser = argparse.ArgumentParser(
        description='Upload multiple CSV files to Google Sheets as separate tabs'
    )
    parser.add_argument(
        '--folder',
        help='Folder containing CSV files to upload',
        default='./data'  # Default to ./data folder
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
    
    print("\n🚀 Bulk CSV to Google Sheets Uploader\n")
    
    # Run the upload
    results = upload_csvs_to_sheet(
        args.folder,
        preserve_edits=args.preserve_edits,
        verbose=not args.quiet
    )
    
    # Exit with appropriate code
    if results['failed'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)
