# Repository Reorganization Guide

## Overview

This branch (`reorganize-data-structure`) consolidates all roster files and reorganizes brand article data for better clarity and maintainability.

## Two Major Changes

### 1. Roster Consolidation
**All roster data now lives in `/rosters/main-roster.csv`**

**Replaces:**
- `brands.txt`
- `ceo_aliases.csv`
- `ceo_companies.csv`
- `data/roster.csv`
- `data/ceo_aliases.csv`

**Updated Scripts:**
- `scripts/news_articles_brands.py` - Reads Company column
- `scripts/news_articles_ceos.py` - Reads CEO, Company, CEO Alias
- `scripts/news_sentiment_ceos.py` - Reads CEO, Company, CEO Alias
- `scripts/process_serps.py` - Reads all CEO data + Website for domains
- `scripts/process_serps_brands.py` - Reads Website for controlled domains

### 2. Brand Articles Reorganization
**Brand article files moved and renamed for clarity**

**Old:** `data/articles/YYYY-MM-DD-articles.csv`
**New:** `data/processed_articles/YYYY-MM-DD-brand-articles-modal.csv`

**Updated Scripts:**
- `scripts/news_articles_brands.py` - Writes to new location
- `scripts/news_sentiment_brands.py` - Reads from new location

---

## Quick Start

### Step 1: Upload Your main-roster.csv

```bash
git checkout reorganize-data-structure

# Add your main-roster.csv file
cp /path/to/your/main-roster.csv rosters/
git add rosters/main-roster.csv
git commit -m "feat: add main-roster.csv with CEO and company data"
git push origin reorganize-data-structure
```

### Step 2: Run Brand Articles Migration

```bash
# Preview what will be moved (dry run)
python scripts/migrate_brand_articles.py

# Execute the migration
python scripts/migrate_brand_articles.py --apply

# Commit the changes
git add data/processed_articles/
git rm data/articles/*.csv
git commit -m "chore: migrate brand articles to new structure"
git push origin reorganize-data-structure
```

### Step 3: Test All Scripts

```bash
# Test brand scripts
python scripts/news_articles_brands.py
python scripts/news_sentiment_brands.py

# Test CEO scripts
python scripts/news_articles_ceos.py
python scripts/news_sentiment_ceos.py

# Test SERP scripts (if S3 data available)
python scripts/process_serps.py
python scripts/process_serps_brands.py
```

### Step 4: Clean Up Old Files

After confirming everything works:

```bash
# Remove deprecated roster files
git rm brands.txt ceo_aliases.csv ceo_companies.csv data/roster.csv

# Remove sync scripts (no longer needed)
git rm scripts/sync_ceo_lists.py scripts/sync_brands_from_roster.py

# Remove sync workflow (no longer needed)
git rm .github/workflows/sync_lists.yml

# Remove empty articles directory (if empty)
rmdir data/articles  # or: git rm -r data/articles if it has .gitkeep

git commit -m "chore: remove deprecated files"
git push origin reorganize-data-structure
```

### Step 5: Create Pull Request

Go to: https://github.com/PatrickRyanLane/news-sentiment-dashboard/compare/main...reorganize-data-structure

---

## What's Already Done ✅

All scripts have been updated in this branch:

- ✅ Rosters directory structure created
- ✅ All 6 scripts updated to use `rosters/main-roster.csv`
- ✅ Brand scripts updated to use `data/processed_articles/`
- ✅ Brand scripts updated to use new naming: `*-brand-articles-modal.csv`
- ✅ Migration helper script created
- ✅ Documentation added

## What You Need to Do 📝

1. **Upload your `main-roster.csv`** to `rosters/` directory
2. **Run the migration script** to move brand article files
3. **Test all scripts** to verify they work
4. **Clean up old files** (after testing)
5. **Merge the PR** to main

---

## Benefits

### Roster Consolidation
- ✅ Single source of truth for all data
- ✅ No sync scripts needed
- ✅ Easier to maintain and update
- ✅ Better organization

### Brand Articles Reorganization
- ✅ Clearer naming: "-brand-articles-modal" is self-documenting
- ✅ Consistent with CEO articles structure
- ✅ Processed data in dedicated directory
- ✅ Future-proof for adding other article types

---

## Rollback

If issues arise, simply:
1. Don't merge the PR
2. Continue using `main` branch
3. Or revert specific commits

All changes are isolated in this branch and won't affect production until merged.

---

## Files Changed

### New Files
- `rosters/main-roster.csv` (you need to upload)
- `rosters/boards-roster.csv`
- `rosters/README.md`
- `scripts/migrate_brand_articles.py`
- `MIGRATION_GUIDE.md`

### Updated Files
- `scripts/news_articles_brands.py`
- `scripts/news_articles_ceos.py`
- `scripts/news_sentiment_brands.py`
- `scripts/news_sentiment_ceos.py`
- `scripts/process_serps.py`
- `scripts/process_serps_brands.py`

### Files to Delete (after testing)
- `brands.txt`
- `ceo_aliases.csv`
- `ceo_companies.csv`
- `data/roster.csv`
- `scripts/sync_ceo_lists.py`
- `scripts/sync_brands_from_roster.py`
- `.github/workflows/sync_lists.yml`
- `data/articles/` (directory, after migration)
