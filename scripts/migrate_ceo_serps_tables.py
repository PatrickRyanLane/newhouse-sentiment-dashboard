#!/usr/bin/env python3
"""
One-time migration script to consolidate and rename CEO SERP table files.

OLD: data_ceos/processed_serps/YYYY-MM-DD-ceo-serps-processed.csv
NEW: data/processed_serps/YYYY-MM-DD-ceo-serps-table.csv

This script:
1. Moves CEO SERP table files to consolidated directory
2. Renames them to use -table.csv suffix for consistency
3. Prints a summary of changes

Usage:
  python scripts/migrate_ceo_serps_tables.py           # dry run (preview)
  python scripts/migrate_ceo_serps_tables.py --apply   # actually move files
"""

import argparse
import re
from pathlib import Path

SOURCE_DIR = Path("data_ceos/processed_serps")
TARGET_DIR = Path("data/processed_serps")
OLD_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})-ceo-serps-processed\.csv$")
NEW_TEMPLATE = "{date}-ceo-serps-table.csv"

def find_files_to_migrate():
    """Find all CEO SERP processed files"""
    if not SOURCE_DIR.exists():
        print(f"ERROR: {SOURCE_DIR} does not exist")
        return []
    
    files_to_move = []
    for file in SOURCE_DIR.iterdir():
        if file.is_file():
            match = OLD_PATTERN.match(file.name)
            if match:
                date_str = match.group(1)
                old_path = file
                new_name = NEW_TEMPLATE.format(date=date_str)
                new_path = TARGET_DIR / new_name
                files_to_move.append((old_path, new_path))
    
    # Sort by date for cleaner output
    files_to_move.sort(key=lambda x: x[0].name)
    return files_to_move

def preview_migration(files_to_move):
    """Show what would be moved"""
    print(f"\n{'='*80}")
    print(f"CEO SERP TABLE FILES MIGRATION PREVIEW")
    print(f"{'='*80}\n")
    print(f"Found {len(files_to_move)} CEO SERP table files to move:\n")
    
    for old_path, new_path in files_to_move:
        print(f"  {old_path}")
        print(f"  → {new_path}")
        print()
    
    print(f"{'='*80}")
    print(f"Total: {len(files_to_move)} files")
    print(f"{'='*80}\n")
    print("Run with --apply to execute the migration")

def execute_migration(files_to_move):
    """Actually move and rename the files"""
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    
    success = 0
    errors = []
    
    print(f"\n{'='*80}")
    print(f"EXECUTING CEO SERP TABLE FILES MIGRATION")
    print(f"{'='*80}\n")
    
    for old_path, new_path in files_to_move:
        try:
            # Move the file
            old_path.rename(new_path)
            print(f"✓ {old_path.name} → {new_path.name}")
            success += 1
        except Exception as e:
            error_msg = f"✗ Failed to move {old_path.name}: {e}"
            print(error_msg)
            errors.append(error_msg)
    
    print(f"\n{'='*80}")
    print(f"MIGRATION COMPLETE")
    print(f"{'='*80}")
    print(f"Successfully moved: {success} files")
    if errors:
        print(f"Errors: {len(errors)} files")
        for err in errors:
            print(f"  {err}")
    print(f"{'='*80}\n")
    
    # Check if old directory is empty
    if SOURCE_DIR.exists() and not any(SOURCE_DIR.iterdir()):
        print(f"NOTE: {SOURCE_DIR} is now empty and can be removed")

def main():
    parser = argparse.ArgumentParser(
        description="Migrate CEO SERP table files to consolidated directory"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually execute the migration (default is dry-run preview)"
    )
    args = parser.parse_args()
    
    files_to_move = find_files_to_migrate()
    
    if not files_to_move:
        print("No CEO SERP table files found to migrate.")
        return
    
    if args.apply:
        execute_migration(files_to_move)
    else:
        preview_migration(files_to_move)

if __name__ == "__main__":
    main()
