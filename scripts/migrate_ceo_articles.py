#!/usr/bin/env python3
"""
One-time migration script to consolidate and rename CEO article files.

OLD STRUCTURE:
  data_ceos/articles/YYYY-MM-DD-articles.csv       (modal/individual articles)
  data_ceos/YYYY-MM-DD.csv                          (table/aggregates)

NEW STRUCTURE:
  data/processed_articles/YYYY-MM-DD-ceo-articles-modal.csv
  data/processed_articles/YYYY-MM-DD-ceo-articles-table.csv

This script:
1. Creates the target directory if it doesn't exist
2. Moves and renames both modal and table CEO article files
3. Prints a summary of changes

Usage:
  python scripts/migrate_ceo_articles.py           # dry run (preview)
  python scripts/migrate_ceo_articles.py --apply   # actually move files
"""

import argparse
import re
from pathlib import Path

# Source directories
MODAL_SOURCE_DIR = Path("data_ceos/articles")
TABLE_SOURCE_DIR = Path("data_ceos")

# Target directory (consolidated with brand articles)
TARGET_DIR = Path("data/processed_articles")

# Patterns
MODAL_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})-articles\.csv$")
TABLE_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})\.csv$")

def find_files_to_migrate():
    """Find all CEO article files to migrate"""
    files_to_move = []
    
    # Find modal files (individual articles)
    if MODAL_SOURCE_DIR.exists():
        for file in MODAL_SOURCE_DIR.iterdir():
            if file.is_file():
                match = MODAL_PATTERN.match(file.name)
                if match:
                    date_str = match.group(1)
                    old_path = file
                    new_name = f"{date_str}-ceo-articles-modal.csv"
                    new_path = TARGET_DIR / new_name
                    files_to_move.append(("MODAL", old_path, new_path))
    
    # Find table files (aggregates) - only YYYY-MM-DD.csv files, not daily_counts.csv
    if TABLE_SOURCE_DIR.exists():
        for file in TABLE_SOURCE_DIR.iterdir():
            if file.is_file():
                match = TABLE_PATTERN.match(file.name)
                if match:
                    date_str = match.group(1)
                    old_path = file
                    new_name = f"{date_str}-ceo-articles-table.csv"
                    new_path = TARGET_DIR / new_name
                    files_to_move.append(("TABLE", old_path, new_path))
    
    # Sort by type then date for cleaner output
    files_to_move.sort(key=lambda x: (x[0], x[1].name))
    return files_to_move

def preview_migration(files_to_move):
    """Show what would be moved"""
    print(f"\n{'='*80}")
    print(f"CEO ARTICLES MIGRATION PREVIEW")
    print(f"{'='*80}\n")
    
    modal_files = [f for f in files_to_move if f[0] == "MODAL"]
    table_files = [f for f in files_to_move if f[0] == "TABLE"]
    
    if modal_files:
        print(f"CEO Article Modal Files ({len(modal_files)}):\n")
        for _, old_path, new_path in modal_files:
            print(f"  {old_path}")
            print(f"  → {new_path}")
            print()
    
    if table_files:
        print(f"CEO Article Table Files ({len(table_files)}):\n")
        for _, old_path, new_path in table_files:
            print(f"  {old_path}")
            print(f"  → {new_path}")
            print()
    
    print(f"{'='*80}")
    print(f"Total Modal files: {len(modal_files)}")
    print(f"Total Table files: {len(table_files)}")
    print(f"Grand Total: {len(files_to_move)} files")
    print(f"{'='*80}\n")
    print("Run with --apply to execute the migration")

def execute_migration(files_to_move):
    """Actually move and rename the files"""
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    
    success = 0
    errors = []
    
    print(f"\n{'='*80}")
    print(f"EXECUTING CEO ARTICLES MIGRATION")
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
    if MODAL_SOURCE_DIR.exists() and not any(MODAL_SOURCE_DIR.iterdir()):
        print(f"NOTE: {MODAL_SOURCE_DIR} is now empty and can be removed")

def main():
    parser = argparse.ArgumentParser(
        description="Migrate CEO article files to consolidated processed_articles directory"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually execute the migration (default is dry-run preview)"
    )
    args = parser.parse_args()
    
    files_to_move = find_files_to_migrate()
    
    if not files_to_move:
        print("No CEO article files found to migrate.")
        return
    
    if args.apply:
        execute_migration(files_to_move)
    else:
        preview_migration(files_to_move)

if __name__ == "__main__":
    main()
