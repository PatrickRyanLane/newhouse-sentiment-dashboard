#!/usr/bin/env python3
"""
One-time migration script to consolidate and rename SERP row files.

OLD STRUCTURE:
  data/serp_rows/YYYY-MM-DD-brand-serps-rows.csv
  data_ceos/serp_rows/YYYY-MM-DD-ceo-serps-rows.csv

NEW STRUCTURE:
  data/processed_serps/YYYY-MM-DD-brand-serps-modal.csv
  data/processed_serps/YYYY-MM-DD-ceo-serps-modal.csv

This script:
1. Creates the target directory if it doesn't exist
2. Moves and renames both brand and CEO SERP row files
3. Prints a summary of changes

Usage:
  python scripts/migrate_serp_rows.py           # dry run (preview)
  python scripts/migrate_serp_rows.py --apply   # actually move files
"""

import argparse
import re
from pathlib import Path

# Source directories
BRAND_ROWS_DIR = Path("data/serp_rows")
CEO_ROWS_DIR = Path("data_ceos/serp_rows")

# Target directory (consolidated)
TARGET_DIR = Path("data/processed_serps")

# Patterns
BRAND_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})-brand-serps-rows\.csv$")
CEO_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})-ceo-serps-rows\.csv$")

def find_files_to_migrate():
    """Find all SERP row files to migrate"""
    files_to_move = []
    
    # Find brand SERP row files
    if BRAND_ROWS_DIR.exists():
        for file in BRAND_ROWS_DIR.iterdir():
            if file.is_file():
                match = BRAND_PATTERN.match(file.name)
                if match:
                    date_str = match.group(1)
                    old_path = file
                    new_name = f"{date_str}-brand-serps-modal.csv"
                    new_path = TARGET_DIR / new_name
                    files_to_move.append(("BRAND", old_path, new_path))
    
    # Find CEO SERP row files
    if CEO_ROWS_DIR.exists():
        for file in CEO_ROWS_DIR.iterdir():
            if file.is_file():
                match = CEO_PATTERN.match(file.name)
                if match:
                    date_str = match.group(1)
                    old_path = file
                    new_name = f"{date_str}-ceo-serps-modal.csv"
                    new_path = TARGET_DIR / new_name
                    files_to_move.append(("CEO", old_path, new_path))
    
    # Sort by type then date for cleaner output
    files_to_move.sort(key=lambda x: (x[0], x[1].name))
    return files_to_move

def preview_migration(files_to_move):
    """Show what would be moved"""
    print(f"\n{'='*80}")
    print(f"SERP ROWS MIGRATION PREVIEW")
    print(f"{'='*80}\n")
    
    brand_files = [f for f in files_to_move if f[0] == "BRAND"]
    ceo_files = [f for f in files_to_move if f[0] == "CEO"]
    
    if brand_files:
        print(f"BRAND SERP Row Files ({len(brand_files)}):\n")
        for _, old_path, new_path in brand_files:
            print(f"  {old_path}")
            print(f"  → {new_path}")
            print()
    
    if ceo_files:
        print(f"CEO SERP Row Files ({len(ceo_files)}):\n")
        for _, old_path, new_path in ceo_files:
            print(f"  {old_path}")
            print(f"  → {new_path}")
            print()
    
    print(f"{'='*80}")
    print(f"Total Brand files: {len(brand_files)}")
    print(f"Total CEO files: {len(ceo_files)}")
    print(f"Grand Total: {len(files_to_move)} files")
    print(f"{'='*80}\n")
    print("Run with --apply to execute the migration")

def execute_migration(files_to_move):
    """Actually move and rename the files"""
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    
    success = 0
    errors = []
    
    print(f"\n{'='*80}")
    print(f"EXECUTING SERP ROWS MIGRATION")
    print(f"{'='*80}\n")
    
    for file_type, old_path, new_path in files_to_move:
        try:
            # Move the file
            old_path.rename(new_path)
            print(f"✓ [{file_type}] {old_path.name} → {new_path.name}")
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
    
    # Check if old directories are empty
    if BRAND_ROWS_DIR.exists() and not any(BRAND_ROWS_DIR.iterdir()):
        print(f"NOTE: {BRAND_ROWS_DIR} is now empty and can be removed")
    if CEO_ROWS_DIR.exists() and not any(CEO_ROWS_DIR.iterdir()):
        print(f"NOTE: {CEO_ROWS_DIR} is now empty and can be removed")

def main():
    parser = argparse.ArgumentParser(
        description="Migrate SERP row files to consolidated processed_serps directory"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually execute the migration (default is dry-run preview)"
    )
    args = parser.parse_args()
    
    files_to_move = find_files_to_migrate()
    
    if not files_to_move:
        print("No SERP row files found to migrate.")
        return
    
    if args.apply:
        execute_migration(files_to_move)
    else:
        preview_migration(files_to_move)

if __name__ == "__main__":
    main()
