#!/usr/bin/env python3
"""
Migration script to move daily_counts folder under data/

This script:
1. Copies all files from daily_counts/ to data/daily_counts/
2. Verifies the copy was successful
3. Removes the old daily_counts/ folder

Run this script from the repository root:
    python scripts/migrate_daily_counts_to_data.py
"""

import shutil
from pathlib import Path

# Define paths
OLD_DIR = Path("daily_counts")
NEW_DIR = Path("data/daily_counts")

def main():
    if not OLD_DIR.exists():
        print(f"❌ Source directory {OLD_DIR} does not exist!")
        return 1
    
    # Create new directory
    NEW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✓ Created directory: {NEW_DIR}")
    
    # Get all CSV files from old directory
    csv_files = list(OLD_DIR.glob("*.csv"))
    
    if not csv_files:
        print(f"⚠️  No CSV files found in {OLD_DIR}")
        return 0
    
    print(f"\nMoving {len(csv_files)} files:")
    
    # Copy each file
    for old_file in csv_files:
        new_file = NEW_DIR / old_file.name
        print(f"  {old_file} → {new_file}")
        shutil.copy2(old_file, new_file)
        
        # Verify the copy
        if not new_file.exists():
            print(f"    ❌ Failed to copy {old_file.name}")
            return 1
        
        if new_file.stat().st_size != old_file.stat().st_size:
            print(f"    ❌ Size mismatch for {old_file.name}")
            return 1
        
        print(f"    ✓ Verified")
    
    print(f"\n✓ All files copied successfully!")
    print(f"\nRemoving old directory: {OLD_DIR}")
    
    # Remove old directory and its contents
    shutil.rmtree(OLD_DIR)
    print(f"✓ Removed {OLD_DIR}")
    
    print("\n✅ Migration complete!")
    print(f"   Files are now in: {NEW_DIR}")
    
    return 0

if __name__ == "__main__":
    exit(main())
