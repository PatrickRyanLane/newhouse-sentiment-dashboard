#!/usr/bin/env python3
"""
One-time migration script to consolidate all daily counts/rolling index files.

OLD LOCATIONS:
  data/processed_articles/daily_counts.csv
  data/serps/brand_serps_daily.csv
  data_ceos/daily_counts.csv
  data/serps/ceo_serps_daily.csv

NEW LOCATIONS (consolidated in /daily_counts/):
  daily_counts/brand-articles-daily-counts-chart.csv
  daily_counts/brand-serps-daily-counts-chart.csv
  daily_counts/ceo-articles-daily-counts-chart.csv
  daily_counts/ceo-serps-daily-counts-chart.csv

This script:
1. Creates the daily_counts directory
2. Moves all 4 rolling index files
3. Renames them with clear, consistent naming
4. Prints a summary of changes

Usage:
  python scripts/migrate_daily_counts.py           # dry run (preview)
  python scripts/migrate_daily_counts.py --apply   # actually move files
"""

import argparse
from pathlib import Path

TARGET_DIR = Path("daily_counts")

# Define all files to migrate: (old_path, new_name)
FILES_TO_MIGRATE = [
    (Path("data/processed_articles/daily_counts.csv"), "brand-articles-daily-counts-chart.csv"),
    (Path("data/serps/brand_serps_daily.csv"), "brand-serps-daily-counts-chart.csv"),
    (Path("data_ceos/daily_counts.csv"), "ceo-articles-daily-counts-chart.csv"),
    (Path("data/serps/ceo_serps_daily.csv"), "ceo-serps-daily-counts-chart.csv"),
]

def find_files_to_migrate():
    """Find which files exist and need migration"""
    files_to_move = []
    
    for old_path, new_name in FILES_TO_MIGRATE:
        if old_path.exists():
            new_path = TARGET_DIR / new_name
            files_to_move.append((old_path, new_path))
        else:
            print(f"[SKIP] File not found: {old_path}")
    
    return files_to_move

def preview_migration(files_to_move):
    """Show what would be moved"""
    print(f"\n{'='*80}")
    print(f"DAILY COUNTS CONSOLIDATION PREVIEW")
    print(f"{'='*80}\n")
    print(f"Found {len(files_to_move)} daily count files to migrate:\n")
    
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
    print(f"EXECUTING DAILY COUNTS CONSOLIDATION")
    print(f"{'='*80}\n")
    
    for old_path, new_path in files_to_move:
        try:
            # Move the file
            old_path.rename(new_path)
            print(f"✓ {old_path.name} → {new_path.name}")
            success += 1
        except Exception as e:
            error_msg = f"✗ Failed to move {old_path}: {e}"
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
        description="Migrate all daily count files to consolidated directory"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually execute the migration (default is dry-run preview)"
    )
    args = parser.parse_args()
    
    files_to_move = find_files_to_migrate()
    
    if not files_to_move:
        print("No daily count files found to migrate.")
        return
    
    if args.apply:
        execute_migration(files_to_move)
    else:
        preview_migration(files_to_move)

if __name__ == "__main__":
    main()
