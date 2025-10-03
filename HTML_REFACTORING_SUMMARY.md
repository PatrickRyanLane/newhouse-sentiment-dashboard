# HTML Dashboard Refactoring Summary

## Overview
This refactoring updates all HTML dashboard files to reference the new consolidated data structure established in previous migrations.

## Files Updated

### 1. **ceo-dashboard.html**
Updated to reference the new data paths:

**Chart Data (for time series visualizations):**
- ✅ Daily counts: `data_ceos/daily_counts.csv` → `data/daily_counts/ceo-articles-daily-counts-chart.csv`
- ✅ SERP daily: `data/serps/ceo_serps_daily.csv` → `data/daily_counts/ceo-serps-daily-counts-chart.csv`

**Table Data (for per-date tables):**
- ✅ Articles table: `data_ceos/{date}.csv` → `data/processed_articles/{date}-ceo-articles-table.csv`
- ✅ SERP processed: `data_ceos/processed_serps/{date}` → `data/processed_serps/{date}-ceo-serps-processed.csv`

**Modal Data (for detailed views):**
- ✅ Headlines modal: `data_ceos/articles/{date}-articles.csv` → `data/processed_articles/{date}-ceo-articles-modal.csv`
- ✅ SERP rows: `data_ceos/serp_rows/{date}-ceo-serps-rows.csv` → `data/processed_serps/{date}-ceo-serps-rows.csv`

**Roster Data:**
- ✅ Boards: `data_ceos/ceo_boards.csv` → `rosters/boards_roster.csv`
- ✅ Roster: `data/serps/roster.csv`, `data/roster.csv` → `rosters/main_roster.csv`, `rosters/roster.csv`

### 2. **brand-dashboard.html**
Updated to reference the new data paths:

**Chart Data:**
- ✅ Daily counts: `data/processed_articles/daily_counts.csv` → `data/daily_counts/brand-articles-daily-counts-chart.csv`
- ✅ SERP daily: `data/serps/brand_serps_daily.csv` → `data/daily_counts/brand-serps-daily-counts-chart.csv`

**Table Data:**
- ✅ Articles table: `data/processed_articles/{date}.csv` → `data/processed_articles/{date}-brand-articles-table.csv`
- ✅ SERP processed: `data/processed_serps/{date}-brand-serps-processed.csv` → `data/processed_serps/{date}-brand-serps-table.csv`

**Modal Data:**
- ✅ Headlines modal: `data/articles/{date}-articles.csv` → `data/processed_articles/{date}-brand-articles-modal.csv`
- ✅ SERP rows: `data/serp_rows/{date}-brand-serps-rows.csv` → `data/processed_serps/{date}-brand-serps-rows.csv`

### 3. **sectors.html**
Updated to reference the new data paths:

**Chart Data:**
- ✅ Daily counts: `data/processed_articles/daily_counts.csv` → `data/daily_counts/brand-articles-daily-counts-chart.csv`
- ✅ SERP daily: `data/serps/brand_serps_daily.csv` → `data/daily_counts/brand-serps-daily-counts-chart.csv`

**Roster Data:**
- ✅ Roster: `data/serps/roster.csv`, `data/roster.csv` → `rosters/main_roster.csv`, `rosters/roster.csv`

## Files Not Updated

### **roc-dashboard.html**
- Status: ✅ No changes needed
- Reason: ROC data uses separate structure (`data_roc/`) that has not been reorganized
- Current paths remain valid:
  - `data_roc/daily_counts.csv`
  - `data_roc/articles/{date}.csv`

### **combined.html**
- Status: ✅ No changes needed
- Reason: This file only loads other dashboard HTML files via iframe - contains no direct data paths

### **index.html**
- Status: ✅ No changes needed
- Reason: Simple landing page with no data dependencies

## New Data Structure Reference

```
data/
├── daily_counts/
│   ├── brand-articles-daily-counts-chart.csv
│   ├── brand-serps-daily-counts-chart.csv
│   ├── ceo-articles-daily-counts-chart.csv
│   └── ceo-serps-daily-counts-chart.csv
├── processed_articles/
│   ├── {date}-brand-articles-table.csv
│   ├── {date}-brand-articles-modal.csv
│   ├── {date}-ceo-articles-table.csv
│   └── {date}-ceo-articles-modal.csv
└── processed_serps/
    ├── {date}-brand-serps-table.csv
    ├── {date}-brand-serps-rows.csv
    ├── {date}-ceo-serps-processed.csv
    └── {date}-ceo-serps-rows.csv

rosters/
├── main_roster.csv
├── roster.csv
└── boards_roster.csv
```

## Testing Checklist

Before merging, verify that each dashboard:

### CEO Dashboard
- [ ] Loads and displays data correctly
- [ ] Date selector populates with available dates
- [ ] Charts render with correct time series data
- [ ] Table shows current date's CEO data
- [ ] Headlines modal opens with article details
- [ ] SERP modal opens with search results
- [ ] Boards modal displays board memberships
- [ ] Filtering works correctly
- [ ] Chart pagination works
- [ ] CEO selection drives chart filtering

### Brand Dashboard
- [ ] Loads and displays data correctly
- [ ] Date selector populates with available dates
- [ ] Charts render with correct time series data
- [ ] Table shows current date's brand data
- [ ] Headlines modal opens with article details
- [ ] SERP modal opens with search results
- [ ] Filtering works correctly
- [ ] Chart pagination works
- [ ] Company selection drives chart filtering

### Sectors Dashboard
- [ ] Loads and displays data correctly
- [ ] Date selector populates with available dates
- [ ] Charts render with aggregated sector data
- [ ] Table shows sector-level aggregations
- [ ] Filtering works correctly
- [ ] Chart pagination works
- [ ] Sector selection drives chart filtering

## Validation Notes

All updated paths follow the new naming conventions:
- Chart data files use descriptive names: `{entity}-{datatype}-daily-counts-chart.csv`
- Table data files use format: `{date}-{entity}-{datatype}-table.csv`
- Modal data files use format: `{date}-{entity}-{datatype}-modal.csv`
- SERP files distinguish between processed aggregates and individual rows

## Rollback Plan

If issues are discovered after merging:
1. Revert this PR
2. The old paths remain in the codebase via git history
3. Data files exist in both old and new locations (migration scripts preserved originals)
4. No data loss - only path references changed

## Next Steps

After this PR is merged and verified:
1. Monitor dashboards for any loading errors
2. Check browser console for any 404s or failed data loads
3. Verify all modals and drill-down features work correctly
4. Once confirmed stable, old data files can be archived/removed in a future cleanup PR
