# Roster Consolidation Migration

## Overview
This migration consolidates all roster/list files into a single source of truth in the `/rosters` directory, eliminating the need for sync scripts and reducing data duplication.

## Changes Made

### New Directory Structure
```
/rosters/
├── main-roster.csv          # Single source of truth for all CEO/company data
├── boards-roster.csv        # Board members data (template)
└── README.md               # Documentation
```

### Updated Scripts

#### 1. `scripts/news_articles_brands.py`
- **Before:** Read from `brands.txt` in root
- **After:** Reads Company column from `rosters/main-roster.csv`
- **Changes:** New `load_companies_from_roster()` function extracts unique companies

#### 2. `scripts/news_articles_ceos.py`  
- **Before:** Read from `data/ceo_aliases.csv`
- **After:** Reads CEO, Company, and CEO Alias from `rosters/main-roster.csv`
- **Changes:** Updated `read_roster()` function with flexible column matching

#### 3. `scripts/news_sentiment_ceos.py`
- **Before:** Read from `data/ceo_aliases.csv`
- **After:** Reads from `rosters/main-roster.csv`
- **Changes:** Updated `load_roster()` function, CLI flag now `--roster` instead of `--aliases`

### Deleted Files

The following files are **no longer needed** and should be removed:

#### Scripts (no longer needed):
- `scripts/sync_ceo_lists.py`
- `scripts/sync_brands_from_roster.py`

#### Data Files (replaced by main-roster.csv):
- `brands.txt`
- `ceo_aliases.csv`
- `ceo_companies.csv`
- `data/roster.csv`

#### Workflows (no longer needed):
- `.github/workflows/sync_lists.yml`

## Migration Steps

### Step 1: Create New Structure
```bash
# Upload your main-roster.csv to rosters/ directory
mkdir rosters
mv main-roster.csv rosters/

# Create boards-roster.csv template
# (provided in this PR)
```

### Step 2: Update Scripts
All three Python scripts have been updated (provided in this PR):
- `scripts/news_articles_brands.py`
- `scripts/news_articles_ceos.py`
- `scripts/news_sentiment_ceos.py`

### Step 3: Remove Old Files
After verifying the new scripts work:
```bash
# Remove deprecated sync scripts
git rm scripts/sync_ceo_lists.py
git rm scripts/sync_brands_from_roster.py

# Remove deprecated data files
git rm brands.txt
git rm ceo_aliases.csv
git rm ceo_companies.csv
git rm data/roster.csv

# Remove deprecated workflow
git rm .github/workflows/sync_lists.yml
```

### Step 4: Update Workflows
The daily workflows (`daily_ceos.yml`, `daily_brands.yml`) should continue to work without changes, as the updated scripts maintain the same output format.

## Benefits

1. **Single Source of Truth:** All data in one place (`rosters/main-roster.csv`)
2. **No Sync Scripts:** Eliminates maintenance overhead
3. **Simpler Workflows:** No need for list synchronization workflow
4. **Easier Updates:** Edit one file instead of multiple
5. **Better Organization:** Clear `/rosters` directory structure

## Testing Checklist

After deploying these changes, verify:

- [ ] Brands news collection works (`scripts/news_articles_brands.py`)
- [ ] CEO news collection works (`scripts/news_articles_ceos.py`)
- [ ] CEO sentiment aggregation works (`scripts/news_sentiment_ceos.py`)
- [ ] Daily workflows run successfully
- [ ] Dashboard displays data correctly
- [ ] All old files have been removed

## Rollback Plan

If issues arise, you can temporarily rollback by:
1. Reverting to the previous commit
2. Restoring the old file structure
3. Re-enabling sync workflows

However, this should not be necessary as the updated scripts maintain full backward compatibility in terms of output format.

## Future Enhancements

With this consolidated structure, future enhancements become easier:
- Add more columns to `main-roster.csv` as needed
- Implement board member tracking using `boards-roster.csv`
- Add additional roster types (e.g., `executives-roster.csv`)
- All scripts can be updated to read from the same source
