# Article Column Standardization

## Overview

We've standardized the column naming conventions in the **article daily-counts-chart** Google Sheets tabs to match the naming pattern used in SERP sheets and make the data structure clearer.

## What Changed

### Brand Articles Daily Counts Tab
**Before:**
```
date, company, positive, neutral, negative, total, neg_pct
```

**After:**
```
date, company, positive_articles, neutral_articles, negative_articles, total, neg_pct
```

### CEO Articles Daily Counts Tab  
**Before:**
```
date, ceo, company, positive_articles, neutral_articles, negative_articles, total, neg_pct, theme, alias
```

**After:**
```
date, ceo, company, positive_articles, neutral_articles, negative_articles, total, neg_pct, theme, alias
```

### SERP Daily Counts Tabs (No Change)
These remain as-is with their own naming convention:
```
date, company, total, controlled, negative_serp, neutral_serp, positive_serp
```

## Why This Matters (For Learning)

This is a great example of **data schema consistency**, which is important in real-world data work:

1. **Clarity**: The `_articles` suffix makes it immediately clear what metric you're looking at (articles vs SERPs)
2. **Consistency**: Both article and SERP tabs now follow the same naming pattern
3. **Maintainability**: When future developers (or your students) read the code, the structure is obvious
4. **Alignment**: The column names match what the Python scripts are actually calculating

## How to Apply the Changes

### Option 1: Run the Cleanup Script (Recommended)
```bash
python scripts/cleanup_article_columns.py
```

This script will:
- Read your existing Google Sheets tabs
- Rename `positive` → `positive_articles`, `neutral` → `neutral_articles`, `negative` → `negative_articles`
- Write the data back with new column headers
- Preserve all your existing data

### Option 2: Manual Cleanup in Google Sheets
1. Open your `brand-articles-daily-counts-chart` tab
2. Edit the header row to change:
   - `positive` → `positive_articles`
   - `neutral` → `neutral_articles`
   - `negative` → `negative_articles`
3. Repeat for `ceo-articles-daily-counts-chart` tab

## Code Changes

### Updated Files

**`scripts/news_sentiment_brands.py`**
- Changed `INDEX_FIELDS` to use new column names
- Updated `write_daily()` to create DataFrames with new column names
- Updated `upsert_daily_index()` to write with new column names

**`scripts/news_sentiment_ceos.py`**
- Updated `aggregate_counts()` to output standardized column names
- Updated `upsert_master_index()` to use new expected columns

**New File: `scripts/cleanup_article_columns.py`**
- Utility script to fix existing Google Sheets tabs
- Connects to Google Sheets API
- Renames columns while preserving all data

## Going Forward

After applying these changes:

1. **Next pipeline run** will write data using the new column names
2. **Your manual edits** (sentiment corrections, etc.) are preserved automatically via the `merge_preserving_edits()` function in `sheets_helper.py`
3. **CSV backups** also use the new column names for consistency
4. **Dashboards** reading from these sheets will see consistent, clear column names

## Questions for Learning

If you're studying this code, here are some learning questions:

1. **Why use suffixes like `_articles` instead of just `positive`?**
   - Because your spreadsheet has multiple data types (articles, SERPs, etc.) and the suffix clarifies which metric you're looking at

2. **How does `merge_preserving_edits()` protect student work?**
   - It reads existing data, finds matching rows by URL, and keeps student-edited values while updating calculated fields

3. **What would happen if you didn't standardize these columns?**
   - Over time, your sheets would become harder to understand, dashboards might break, and it's harder to add new features

---

**Last Updated:** October 21, 2025  
**Related Files:** `sheets_helper.py`, `news_sentiment_brands.py`, `news_sentiment_ceos.py`
