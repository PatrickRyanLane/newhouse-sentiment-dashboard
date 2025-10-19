#!/usr/bin/env python3
"""
Google Sheets Tab Cleanup Script

Automatically deletes tabs older than KEEP_DAYS to prevent hitting the 200 tab limit.

Usage:
  # Delete tabs older than 30 days
  python scripts/cleanup_old_tabs.py

  # Keep only last 7 days
  KEEP_DAYS=7 python scripts/cleanup_old_tabs.py

  # Dry run (see what would be deleted without deleting)
  DRY_RUN=true python scripts/cleanup_old_tabs.py
"""

import os
import re
from datetime import datetime, timedelta

try:
    from sheets_helper import get_sheets_service
    SHEETS_AVAILABLE = True
except ImportError:
    SHEETS_AVAILABLE = False
    print("[ERROR] sheets_helper not available")
    print("       Make sure sheets_helper.py is in the same directory")
    exit(1)

# Configuration
SPREADSHEET_ID = os.environ.get('GOOGLE_SHEET_ID', 'YOUR_SHEET_ID_HERE')
KEEP_DAYS = int(os.environ.get('KEEP_DAYS', '30'))
DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'

# Tabs to NEVER delete (rolling indices)
PROTECTED_TABS = {
    'BrandArticles-DailyCounts',
    'BrandSERPs-DailyCounts',
    'CEOArticles-DailyCounts',
    'CEOSERPs-DailyCounts',
}

def is_dated_tab(title: str) -> tuple[bool, str]:
    """
    Check if tab has date prefix (YYYY-MM-DD-...)
    Returns: (is_dated, date_string)
    """
    # Match YYYY-MM-DD at start of title
    match = re.match(r'^(\d{4}-\d{2}-\d{2})-', title)
    if match:
        return True, match.group(1)
    return False, None

def cleanup_old_tabs():
    """Delete tabs older than KEEP_DAYS."""
    
    if not SHEETS_AVAILABLE:
        print("[ERROR] Google Sheets packages not available")
        return False
    
    print(f"{'[DRY RUN] ' if DRY_RUN else ''}Cleaning up tabs older than {KEEP_DAYS} days...")
    print(f"Protected tabs (never deleted): {', '.join(PROTECTED_TABS)}")
    print()
    
    try:
        service = get_sheets_service()
        spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        
        cutoff_date = (datetime.now() - timedelta(days=KEEP_DAYS)).strftime('%Y-%m-%d')
        print(f"Cutoff date: {cutoff_date}")
        print(f"Deleting tabs dated before: {cutoff_date}")
        print()
        
        tabs_to_delete = []
        tabs_kept = []
        
        for sheet in spreadsheet['sheets']:
            title = sheet['properties']['title']
            sheet_id = sheet['properties']['sheetId']
            
            # Never delete protected tabs
            if title in PROTECTED_TABS:
                print(f"  [KEEP] {title} (protected)")
                continue
            
            # Check if tab has a date
            is_dated, tab_date = is_dated_tab(title)
            
            if is_dated:
                if tab_date < cutoff_date:
                    tabs_to_delete.append({
                        'title': title,
                        'sheetId': sheet_id,
                        'date': tab_date
                    })
                    print(f"  [DELETE] {title} (date: {tab_date})")
                else:
                    tabs_kept.append(title)
                    print(f"  [KEEP] {title} (date: {tab_date})")
            else:
                # Non-dated tabs (keep them)
                tabs_kept.append(title)
                print(f"  [KEEP] {title} (no date prefix)")
        
        print()
        print(f"Summary:")
        print(f"  Total tabs: {len(spreadsheet['sheets'])}")
        print(f"  Tabs to keep: {len(tabs_kept)}")
        print(f"  Tabs to delete: {len(tabs_to_delete)}")
        print()
        
        if not tabs_to_delete:
            print("✅ No tabs to delete - all are recent or protected")
            return True
        
        if DRY_RUN:
            print(f"[DRY RUN] Would delete {len(tabs_to_delete)} tabs, but DRY_RUN=true")
            print("Set DRY_RUN=false to actually delete")
            return True
        
        # Confirm before deleting
        print(f"⚠️  About to delete {len(tabs_to_delete)} tabs!")
        
        # Build batch delete request
        delete_requests = [
            {'deleteSheet': {'sheetId': t['sheetId']}} 
            for t in tabs_to_delete
        ]
        
        # Execute deletion
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={'requests': delete_requests}
        ).execute()
        
        print(f"✅ Successfully deleted {len(tabs_to_delete)} old tabs")
        print(f"   Remaining tabs: {len(tabs_kept)}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Cleanup failed: {e}")
        return False

if __name__ == '__main__':
    success = cleanup_old_tabs()
    exit(0 if success else 1)
