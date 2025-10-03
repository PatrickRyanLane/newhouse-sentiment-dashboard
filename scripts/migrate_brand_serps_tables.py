#!/usr/bin/env python3
"""
One-time migration script to rename brand SERP processed/aggregate files.

OLD: data/processed_serps/YYYY-MM-DD-brand-serps-processed.csv
NEW: data/processed_serps/YYYY-MM-DD-brand-serps-table.csv

Usage:
  python scripts/migrate_brand_serps_tables.py           # dry run (preview)
  python scripts/migrate_brand_serps_tables.py --apply   # actually rename files
"""

import argparse
import re
from pathlib import Path

TARGET_DIR = Path("data/processed_serps")
OLD_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})-brand-serps-processed\.csv$")
NEW_TEMPLATE = "{date}-brand-serps-table.csv"

def find_files_to_migrate():
    if not TARGET_DIR.exists():
        print(f"ERROR: {TARGET_DIR} does not exist")
        return []
    
    files_to_rename = []
    for file in TARGET_DIR.iterdir():
        if file.is_file():
            match = OLD_PATTERN.match(file.name)
            if match:
                date_str = match.group(1)
                old_path = file
                new_name = NEW_TEMPLATE.format(date=date_str)
                new_path = TARGET_DIR / new_name
                files_to_rename.append((old_path, new_path))
    
    files_to_rename.sort(key=lambda x: x[0].name)
    return files_to_rename

def preview_migration(files_to_rename):
    print(f"\n{'='*80}")
    print(f"BRAND SERP TABLE FILES MIGRATION PREVIEW")
    print(f"{'='*80}\n")
    print(f"Found {len(files_to_rename)} brand SERP table files to rename:\n")
    
    for old_path, new_path in files_to_rename:
        print(f"  {old_path.name}")
        print(f"  → {new_path.name}")
        print()
    
    print(f"{'='*80}")
    print(f"Total: {len(files_to_rename)} files")
    print(f"{'='*80}\n")
    print("Run with --apply to execute the migration")

def execute_migration(files_to_rename):
    success = 0
    errors = []
    
    print(f"\n{'='*80}")
    print(f"EXECUTING BRAND SERP TABLE FILES MIGRATION")
    print(f"{'='*80}\n")
    
    for old_path, new_path in files_to_rename:
        try:
            old_path.rename(new_path)
            print(f"✓ {old_path.name} → {new_path.name}")
            success += 1
        except Exception as e:
            error_msg = f"✗ Failed to rename {old_path.name}: {e}"
            print(error_msg)
            errors.append(error_msg)
    
    print(f"\n{'='*80}")
    print(f"MIGRATION COMPLETE")
    print(f"{'='*80}")
    print(f"Successfully renamed: {success} files")
    if errors:
        print(f"Errors: {len(errors)} files")
        for err in errors:
            print(f"  {err}")
    print(f"{'='*80}\n")

def main():
    parser = argparse.ArgumentParser(
        description="Migrate brand SERP table files to new naming convention"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually execute the migration (default is dry-run preview)"
    )
    args = parser.parse_args()
    
    files_to_rename = find_files_to_migrate()
    
    if not files_to_rename:
        print("No brand SERP table files found to migrate.")
        return
    
    if args.apply:
        execute_migration(files_to_rename)
    else:
        preview_migration(files_to_rename)

if __name__ == "__main__":
    main()
