#!/usr/bin/env python3
"""
One-time migration script to rename brand article aggregate/table files.

OLD: data/processed_articles/YYYY-MM-DD.csv
NEW: data/processed_articles/YYYY-MM-DD-brand-articles-table.csv

This script:
1. Renames all daily aggregate files
2. Skips files that don't match the pattern (like daily_counts.csv)
3. Prints a summary of changes

Usage:
  python scripts/migrate_brand_tables.py           # dry run (preview)
  python scripts/migrate_brand_tables.py --apply   # actually rename files
"""

import argparse
import re
from pathlib import Path

TARGET_DIR = Path("data/processed_articles")
# Match YYYY-MM-DD.csv but NOT files with additional text like daily_counts.csv
OLD_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})\.csv$")
NEW_TEMPLATE = "{date}-brand-articles-table.csv"

def find_files_to_migrate():
    """Find all files matching the pattern YYYY-MM-DD.csv"""
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
    
    # Sort by date for cleaner output
    files_to_rename.sort(key=lambda x: x[0].name)
    return files_to_rename

def preview_migration(files_to_rename):
    """Show what would be renamed"""
    print(f"\n{'='*80}")
    print(f"MIGRATION PREVIEW - Table Files")
    print(f"{'='*80}\n")
    print(f"Found {len(files_to_rename)} table files to rename:\n")
    
    for old_path, new_path in files_to_rename:
        print(f"  {old_path.name}")
        print(f"  → {new_path.name}")
        print()
    
    print(f"{'='*80}")
    print(f"Total: {len(files_to_rename)} files")
    print(f"{'='*80}\n")
    print("Run with --apply to execute the migration")

def execute_migration(files_to_rename):
    """Actually rename the files"""
    success = 0
    errors = []
    
    print(f"\n{'='*80}")
    print(f"EXECUTING MIGRATION - Table Files")
    print(f"{'='*80}\n")
    
    for old_path, new_path in files_to_rename:
        try:
            # Rename the file
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
        description="Migrate brand table files to new naming convention"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually execute the migration (default is dry-run preview)"
    )
    args = parser.parse_args()
    
    files_to_rename = find_files_to_migrate()
    
    if not files_to_rename:
        print("No table files found to migrate.")
        return
    
    if args.apply:
        execute_migration(files_to_rename)
    else:
        preview_migration(files_to_rename)

if __name__ == "__main__":
    main()
