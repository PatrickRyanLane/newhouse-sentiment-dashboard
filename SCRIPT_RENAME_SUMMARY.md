# Script Rename: process_serps.py → process_serps_ceos.py

## Overview
Renamed `scripts/process_serps.py` to `scripts/process_serps_ceos.py` for consistency with the `process_serps_brands.py` naming convention.

## Changes Made

### 1. **Script File**
- ✅ Created: `scripts/process_serps_ceos.py` (copy of process_serps.py with updates)
- 🗑️ To delete: `scripts/process_serps.py` (after PR merge)

### 2. **Script Output Filenames**
Updated to match standardized naming convention:

**Before**:
- `data/processed_serps/{date}-ceo-serps-modal.csv` (rows)
- `data/processed_serps/{date}-ceo-serps-table.csv` (aggregates)

**After**:
- `data/processed_serps/{date}-ceo-serps-rows.csv` (rows)
- `data/processed_serps/{date}-ceo-serps-processed.csv` (aggregates)

This aligns with brand SERP naming:
- `{date}-brand-serps-rows.csv`
- `{date}-brand-serps-table.csv`

### 3. **Workflow Files Updated**
- ✅ `.github/workflows/daily_ceos.yml`
  - Changed: `python scripts/process_serps.py` → `python scripts/process_serps_ceos.py`
  - Updated idempotent check filename: `-modal.csv` → `-rows.csv`

- ✅ `.github/workflows/backfill_serps.yml`
  - Changed: `python scripts/process_serps.py` → `python scripts/process_serps_ceos.py`

### 4. **Documentation Updated**
- ✅ `WORKFLOWS.md` - Updated all references to process_serps_ceos.py
- ✅ `README_REORGANIZATION.md` - Updated script listing

---

## Naming Convention Summary

### Current Script Naming (After Rename):

**News Articles Collection**:
- `news_articles_brands.py` - Fetch brand/company news
- `news_articles_ceos.py` - Fetch CEO news

**Sentiment Analysis**:
- `news_sentiment_brands.py` - Analyze brand sentiment
- `news_sentiment_ceos.py` - Analyze CEO sentiment
- `news_sentiment_roc.py` - Analyze Roc Nation sentiment

**SERP Processing**:
- `process_serps_brands.py` - Process brand SERPs
- `process_serps_ceos.py` - Process CEO SERPs ← **RENAMED**

**Migrations**:
- `migrate_brand_articles.py`
- `migrate_brand_serps_tables.py`
- `migrate_brand_tables.py`
- `migrate_ceo_articles.py`
- `migrate_ceo_serps_tables.py`
- `migrate_daily_counts.py`
- `migrate_daily_counts_to_data.py`
- `migrate_serp_rows.py`

**Utilities**:
- `email_utils.py`
- `send_alerts.py`
- `sync_brands_from_roster.py`
- `sync_ceo_lists.py`

---

## Output File Naming Conventions

### Processed Articles:
```
data/processed_articles/
├── {date}-brand-articles-modal.csv   (detailed rows for modals)
├── {date}-brand-articles-table.csv   (aggregated for tables)
├── {date}-ceo-articles-modal.csv     (detailed rows for modals)
└── {date}-ceo-articles-table.csv     (aggregated for tables)
```

### Processed SERPs:
```
data/processed_serps/
├── {date}-brand-serps-rows.csv       (detailed rows for modals)
├── {date}-brand-serps-table.csv      (aggregated for tables)
├── {date}-ceo-serps-rows.csv         (detailed rows for modals) ← UPDATED
└── {date}-ceo-serps-processed.csv    (aggregated for tables)   ← UPDATED
```

**Note**: Brand uses `-table.csv` while CEO uses `-processed.csv` for aggregates. Consider standardizing to one pattern in future.

---

## Breaking Changes

### For Manual Script Runs:
If you manually run the CEO SERP script, update your command:

**Old**:
```bash
python scripts/process_serps.py --date 2025-09-17
python scripts/process_serps.py --backfill 2025-09-15 2025-09-30
```

**New**:
```bash
python scripts/process_serps_ceos.py --date 2025-09-17
python scripts/process_serps_ceos.py --backfill 2025-09-15 2025-09-30
```

### For Programmatic Calls:
If any other scripts import or call `process_serps.py`, update those references:

```python
# Old
from scripts.process_serps import process_one_date

# New
from scripts.process_serps_ceos import process_one_date
```

---

## Migration Steps

### Immediate (This PR):
1. ✅ Create `process_serps_ceos.py` with updated output filenames
2. ✅ Update `daily_ceos.yml` workflow
3. ✅ Update `backfill_serps.yml` workflow
4. ✅ Update documentation

### After PR Merge:
1. Delete old `scripts/process_serps.py` file
2. Verify all workflows run successfully
3. Check for any other references to the old script name

---

## Testing Checklist

Before merging:
- [ ] Run `process_serps_ceos.py` manually for a test date
- [ ] Verify outputs appear with new filenames:
  - `{date}-ceo-serps-rows.csv`
  - `{date}-ceo-serps-processed.csv`
- [ ] Test `daily_ceos.yml` workflow (manual trigger)
- [ ] Test `backfill_serps.yml` workflow (small date range)
- [ ] Verify no import errors in other scripts

After merging:
- [ ] Monitor next scheduled `daily_ceos.yml` run
- [ ] Check for errors in workflow logs
- [ ] Verify CEO dashboard displays SERP data correctly
- [ ] Delete old `process_serps.py` file

---

## Rationale

### Why Rename?

**Consistency**: All other scripts follow `{function}_{entity}.py` pattern:
- `news_articles_brands.py` / `news_articles_ceos.py`
- `news_sentiment_brands.py` / `news_sentiment_ceos.py`
- `process_serps_brands.py` / `process_serps_ceos.py` ← Now consistent!

**Clarity**: Makes it immediately obvious which entity the script processes

**Maintainability**: Easier to find and organize scripts when naming is consistent

### Why Update Output Filenames?

**Consistency**: Align CEO SERP file naming with conventions:
- "rows" suffix for detailed data (used in modals)
- "processed" suffix for aggregated data (used in tables)
- Matches pattern used across other processed files

---

## Related Documentation

- `WORKFLOWS.md` - Complete workflow reference
- `HTML_REFACTORING_SUMMARY.md` - HTML dashboard path updates
- `MIGRATION_GUIDE.md` - Data migration instructions
- `README_REORGANIZATION.md` - Overall reorganization guide
