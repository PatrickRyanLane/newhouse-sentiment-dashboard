# Repository Reorganization - Complete Guide

## 🎯 What This Branch Does

This branch (`reorganize-data-structure`) implements two major improvements:

1. **Roster Consolidation** - Single source of truth for all CEO/company data
2. **Brand Articles Reorganization** - Clearer naming and better organization

---

## 📁 New Directory Structure

```
news-sentiment-dashboard/
├── rosters/
│   ├── main-roster.csv              ⭐ SINGLE SOURCE OF TRUTH
│   ├── boards-roster.csv            (template for future use)
│   └── README.md
├── data/
│   ├── processed_articles/          🆕 RENAMED from articles/
│   │   └── YYYY-MM-DD-brand-articles-modal.csv
│   ├── processed_serps/
│   └── serps/
├── data_ceos/
│   ├── articles/
│   │   └── YYYY-MM-DD-articles.csv      (CEO articles, unchanged)
│   └── processed_serps/
└── scripts/
    ├── news_articles_brands.py      ✅ UPDATED
    ├── news_articles_ceos.py        ✅ UPDATED
    ├── news_sentiment_brands.py     ✅ UPDATED
    ├── news_sentiment_ceos.py       ✅ UPDATED
    ├── process_serps_ceos.py        ✅ UPDATED (renamed from process_serps.py)
    ├── process_serps_brands.py      ✅ UPDATED
    └── migrate_brand_articles.py    🆕 NEW
```

---

## ✅ What's Already Been Done

All scripts have been updated and committed to this branch:

### Updated Scripts (6 total)
1. ✅ `scripts/news_articles_brands.py`
   - Now reads from: `rosters/main-roster.csv`
   - Now writes to: `data/processed_articles/{DATE}-brand-articles-modal.csv`

2. ✅ `scripts/news_articles_ceos.py`
   - Now reads from: `rosters/main-roster.csv`

3. ✅ `scripts/news_sentiment_brands.py`
   - Now reads from: `data/processed_articles/{date}-brand-articles-modal.csv`

4. ✅ `scripts/news_sentiment_ceos.py`
   - Now reads from: `rosters/main-roster.csv`

5. ✅ `scripts/process_serps_ceos.py` (renamed from process_serps.py)
   - Now reads from: `rosters/main-roster.csv`
   - Uses Website column for controlled domain detection

6. ✅ `scripts/process_serps_brands.py`
   - Now reads from: `rosters/main-roster.csv`
   - Uses Website column for controlled domain detection

### New Files Created
- ✅ `rosters/boards-roster.csv` - Template for board tracking
- ✅ `rosters/README.md` - Documentation
- ✅ `scripts/migrate_brand_articles.py` - Migration helper
- ✅ `MIGRATION_GUIDE.md` - Step-by-step instructions
- ✅ `README_REORGANIZATION.md` - This file

---

## 📝 What You Need to Do

### Step 1: Upload main-roster.csv ⭐ REQUIRED

```bash
git checkout reorganize-data-structure

# Add your main-roster.csv file to rosters/
cp /path/to/your/main-roster.csv rosters/

git add rosters/main-roster.csv
git commit -m "feat: add main-roster.csv with all CEO and company data"
git push origin reorganize-data-structure
```

**Your main-roster.csv should have these columns:**
- CEO
- Company
- CEO Alias
- Website
- Stock
- Sector

### Step 2: Migrate Brand Article Files

```bash
# Preview the migration (shows what will happen)
python scripts/migrate_brand_articles.py

# Execute the migration (moves 27 files)
python scripts/migrate_brand_articles.py --apply

# Commit the migrated files
git add data/processed_articles/
git status  # verify all 27 files are staged
git commit -m "chore: migrate brand articles to new structure"

# Remove old directory
git rm -r data/articles/
git commit -m "chore: remove old articles directory"

git push origin reorganize-data-structure
```

### Step 3: Test Everything Works

```bash
# Test brand pipeline
python scripts/news_articles_brands.py
python scripts/news_sentiment_brands.py

# Test CEO pipeline
python scripts/news_articles_ceos.py
python scripts/news_sentiment_ceos.py

# Test SERP processing (if S3 data available)
python scripts/process_serps_ceos.py
python scripts/process_serps_brands.py
```

### Step 4: Clean Up Deprecated Files

**Only do this after confirming all scripts work!**

```bash
# Remove old roster files
git rm brands.txt
git rm ceo_aliases.csv
git rm ceo_companies.csv
git rm data/roster.csv

# Remove sync scripts (no longer needed)
git rm scripts/sync_ceo_lists.py
git rm scripts/sync_brands_from_roster.py

# Remove sync workflow (no longer needed)
git rm .github/workflows/sync_lists.yml

# Commit cleanup
git commit -m "chore: remove deprecated roster files and sync infrastructure"
git push origin reorganize-data-structure
```

### Step 5: Create Pull Request

Go to: https://github.com/PatrickRyanLane/news-sentiment-dashboard/compare/main...reorganize-data-structure

Review all changes and merge when ready!

---

## 🔍 Quick Reference

### Before ❌
```
brands.txt                    →  Companies list
ceo_aliases.csv               →  CEO search aliases
ceo_companies.csv             →  CEO to company mapping
data/roster.csv               →  Main roster
data/articles/*.csv           →  Brand articles
scripts/sync_*.py             →  Sync scripts
scripts/process_serps.py      →  CEO SERP processing
```

### After ✅
```
rosters/main-roster.csv       →  Everything in one place!
data/processed_articles/*.csv →  Brand articles (clearer naming)
scripts/process_serps_ceos.py →  CEO SERP processing (consistent naming)
(sync scripts deleted)        →  No longer needed!
```

---

## 📊 Impact Summary

### Files Moved/Renamed
- 27 brand article CSV files will be moved and renamed
- 1 script renamed for consistency (process_serps.py → process_serps_ceos.py)

### Scripts Updated
- 6 Python scripts updated to use new structure

### Files to Delete
- 5 deprecated roster files
- 2 sync scripts
- 1 sync workflow

### Net Result
- **Simpler:** Single source of truth for data
- **Clearer:** Better naming conventions
- **Easier:** No sync scripts to maintain
- **Organized:** Logical directory structure
- **Consistent:** All scripts follow same naming pattern

---

## ⚠️ Important Notes

1. **main-roster.csv is REQUIRED** - The branch won't work without it
2. **Test before cleaning up** - Make sure all scripts work before deleting old files
3. **Migration is reversible** - The migration script doesn't delete, it moves
4. **Workflows unchanged** - GitHub Actions workflows will work with updated scripts

---

## 🔄 Rollback

If you need to rollback:

```bash
# Don't merge the PR, or:
git checkout main

# Or revert specific commits
git revert <commit-hash>
```

---

## 👥 Need Help?

If you encounter issues:

1. Check `MIGRATION_GUIDE.md` for detailed instructions
2. Check `rosters/README.md` for roster structure info
3. Run migration script in preview mode first: `python scripts/migrate_brand_articles.py`
4. Check individual script documentation in file headers

---

## ✅ Success Criteria

You'll know everything is working when:

- [ ] All 6 scripts run without errors
- [ ] New article files appear in `data/processed_articles/`
- [ ] Dashboard displays data correctly
- [ ] Daily workflows run successfully
- [ ] Old files have been removed
