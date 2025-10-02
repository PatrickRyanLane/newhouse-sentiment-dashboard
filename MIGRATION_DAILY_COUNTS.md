# Daily Counts Migration Summary

## Overview
This document tracks the migration of the `daily_counts/` folder to `data/daily_counts/` as part of the repository reorganization.

## Changes Made

### 1. Script Updates
All scripts have been updated to reference the new path `data/daily_counts/`:

- ✅ `scripts/news_sentiment_ceos.py`
  - Changed `DEFAULT_OUT` from `"daily_counts/ceo-articles-daily-counts-chart.csv"` 
  - To: `"data/daily_counts/ceo-articles-daily-counts-chart.csv"`

- ✅ `scripts/news_sentiment_brands.py`
  - Changed `DAILY_INDEX` from `Path("daily_counts") / "brand-articles-daily-counts-chart.csv"`
  - To: `Path("data/daily_counts") / "brand-articles-daily-counts-chart.csv"`

- ✅ `scripts/process_serps.py`
  - Changed `INDEX_DIR` from `Path("daily_counts")`
  - To: `Path("data/daily_counts")`

- ✅ `scripts/process_serps_brands.py`
  - Changed `OUT_ROLLUP` from `"daily_counts/brand-serps-daily-counts-chart.csv"`
  - To: `"data/daily_counts/brand-serps-daily-counts-chart.csv"`

### 2. Workflow Updates
- ✅ `.github/workflows/daily_ceos.yml`
  - Removed the `--out` parameter override (now uses script default)
  - The script now correctly writes to `data/daily_counts/`

### 3. Migration Script
- ✅ Created `scripts/migrate_daily_counts_to_data.py`
  - This script will move the CSV files from `daily_counts/` to `data/daily_counts/`
  - Includes verification of file integrity
  - Removes the old directory after successful migration

## Files to be Moved

The following CSV files need to be moved from `daily_counts/` to `data/daily_counts/`:

1. `brand-articles-daily-counts-chart.csv` (1.7 MB)
2. `brand-serps-daily-counts-chart.csv` (427 KB)
3. `ceo-articles-daily-counts-chart.csv` (1.4 MB)
4. `ceo-serps-daily-counts-chart.csv` (608 KB)

## Next Steps

### To Complete the Migration:

1. **Run the migration script locally:**
   ```bash
   python scripts/migrate_daily_counts_to_data.py
   ```

2. **Commit and push the changes:**
   ```bash
   git add -A
   git commit -m "chore: move daily_counts folder under data/"
   git push origin reorganize-data-structure
   ```

3. **Verify the migration:**
   - Check that all files exist in `data/daily_counts/`
   - Check that the old `daily_counts/` folder is removed
   - Test running the scripts to ensure they write to the correct location

### Alternative: Manual Migration via GitHub Web Interface

If you prefer to do this through the GitHub web interface, you'll need to:

1. Download each CSV file from `daily_counts/`
2. Upload each file to `data/daily_counts/` using "Add file" → "Upload files"
3. Delete the old `daily_counts/` folder

## Impact

### Scripts Affected:
- `news_sentiment_ceos.py` - Writes CEO article sentiment counts
- `news_sentiment_brands.py` - Writes brand article sentiment counts  
- `process_serps.py` - Writes CEO SERP sentiment counts
- `process_serps_brands.py` - Writes brand SERP sentiment counts

### Workflows Affected:
- `daily_ceos.yml` - Daily CEO processing workflow
- `daily_brands.yml` - Daily brand processing workflow (indirectly)
- `serp-data.yml` - SERP processing workflow (indirectly)

All affected workflows will automatically use the new paths once the migration is complete.

## Rollback Plan

If needed, you can rollback by:
1. Reverting the commits on this branch
2. Or manually moving files back to `daily_counts/`
3. Reverting the script changes

## Notes

- The migration script ensures file integrity by verifying file sizes
- All scripts maintain backward compatibility - they create the directory if it doesn't exist
- The workflows will continue to work without modification after the file move
