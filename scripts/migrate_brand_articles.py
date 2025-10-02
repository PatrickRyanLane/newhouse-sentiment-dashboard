#!/usr/bin/env python3
"""
One-time migration script to move and rename brand article files.

OLD: data/articles/YYYY-MM-DD-articles.csv
NEW: data/processed_articles/YYYY-MM-DD-brand-articles-modal.csv

This script:
1. Creates the new directory if it doesn't exist
2. Moves and renames all matching files
3. Prints a summary of changes

Usage:
  python scripts/migrate_brand_articles.py           # dry run (preview)
  python scripts/migrate_brand_articles.py --apply   # actually move files
"""

import argparse
import os
import re
from pathlib import Path

OLD_DIR = Path("data/articles")
NEW_DIR = Path("data/processed_articles")
OLD_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})-articles\.csv$")
NEW_TEMPLATE = "{date}-brand-articles-modal.csv"

def find_files_to_migrate():
    """Find all files matching the old pattern"""
    if not OLD_DIR.exists():
        print(f"ERROR: {OLD_DIR} does not exist")
        return []
    
    files_to_move = []
    for file in OLD_DIR.iterdir():
        if file.is_file():
            match = OLD_PATTERN.match(file.name)
            if match:
                date_str = match.group(1)
                old_path = file
                new_name = NEW_TEMPLATE.format(date=date_str)
                new_path = NEW_DIR / new_name
                files_to_move.append((old_path, new_path))
    
    return files_to_move

def preview_migration(files):
    """Show what would be moved"""
    print(f"\n{'='*80}")
    print(f"MIGRATION PREVIEW")
    print(f"{'='*80}\n")
    print(f"Found {len(files)} files to migrate:\n")
    
    for old_path, new_path in files:
        print(f"  {old_path}")
        print(f"  → {new_path}")
        print()
    
    print(f"{'='*80}")
    print(f"Total: {len(files)} files")
    print(f"{'='*80}\n")
    print("Run with --apply to execute the migration")

def execute_migration(files):
    """Actually move and rename the files"""
    NEW_DIR.mkdir(parents=True, exist_ok=True)
    
    success = 0
    errors = []
    
    print(f"\n{'='*80}")
    print(f"EXECUTING MIGRATION")
    print(f"{'='*80}\n")
    
    for old_path, new_path in files:
        try:
            # Use rename (move) operation
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

def main():
    parser = argparse.ArgumentParser(
        description="Migrate brand article files to new location and naming convention"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually execute the migration (default is dry-run preview)"
    )
    args = parser.parse_args()
    
    files_to_move = find_files_to_migrate()
    
    if not files_to_move:
        print("No files found to migrate.")
        return
    
    if args.apply:
        execute_migration(files_to_move)
    else:
        preview_migration(files)

if __name__ == "__main__":
    main()
