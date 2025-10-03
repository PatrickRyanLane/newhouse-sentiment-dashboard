# GitHub Workflows Documentation

## Overview
This repository uses GitHub Actions workflows to automate daily data collection, sentiment analysis, SERP processing, and alerting for Fortune 1000 brands and CEOs.

## Active Workflows

### 1. **daily_brands.yml** - Daily Brand/Company Pipeline
**Purpose**: Collects and analyzes news sentiment for Fortune 1000 brands/companies

**Schedule**: Daily at 09:10 UTC

**Process Flow**:
```
1. Fetch brand news articles (Google News RSS)
   ↓ outputs: data/processed_articles/{date}-brand-articles-modal.csv
   
2. Aggregate brand news sentiment  
   ↓ outputs: data/processed_articles/{date}-brand-articles-table.csv
   ↓ updates: data/daily_counts/brand-articles-daily-counts-chart.csv
   
3. Process brand SERPs (search results analysis)
   ↓ outputs: data/processed_serps/{date}-brand-serps-table.csv
   ↓ outputs: data/processed_serps/{date}-brand-serps-rows.csv
   ↓ updates: data/daily_counts/brand-serps-daily-counts-chart.csv
```

**Scripts Used**:
- `scripts/news_articles_brands.py` - Fetches articles
- `scripts/news_sentiment_brands.py` - Analyzes sentiment
- `scripts/process_serps_brands.py` - Processes SERP data

**Features**:
- Idempotent SERP processing (skips if data exists)
- Safe commit/push with rebase retry
- Manual trigger with optional date override
- Uses `data-writes` concurrency group to prevent conflicts

**Outputs**:
- Daily article data with sentiment labels
- Aggregated sentiment counts for charts
- SERP analysis (negative %, control %)

---

### 2. **daily_ceos.yml** - Daily CEO News & SERP Pipeline
**Purpose**: Collects and analyzes news sentiment and search results for Fortune 1000 CEOs

**Schedule**: Daily at 12:00 UTC (~8am ET)

**Process Flow**:
```
1. Fetch CEO news articles (Google News RSS)
   ↓ outputs: data/processed_articles/{date}-ceo-articles-modal.csv
   
2. Aggregate CEO news sentiment
   ↓ outputs: data/processed_articles/{date}-ceo-articles-table.csv
   ↓ updates: data/daily_counts/ceo-articles-daily-counts-chart.csv

3. Process CEO SERPs (search results analysis)
   ↓ outputs: data/processed_serps/{date}-ceo-serps-processed.csv
   ↓ outputs: data/processed_serps/{date}-ceo-serps-rows.csv
   ↓ updates: data/daily_counts/ceo-serps-daily-counts-chart.csv
```

**Scripts Used**:
- `scripts/news_articles_ceos.py` - Fetches CEO articles
- `scripts/news_sentiment_ceos.py` - Analyzes sentiment and themes
- `scripts/process_serps_ceos.py` - Processes CEO SERP data

**Configuration**:
- `ARTICLES_MAX_PER_ALIAS: 25` - Max articles per CEO
- `ARTICLES_SLEEP_SEC: 0.35` - Rate limiting between requests

**Roster Files**:
- `rosters/ceo_aliases.csv` - CEO name variations for searching
- `rosters/main_roster.csv` - CEO to company mappings

**Features**:
- Idempotent SERP processing (skips if data exists)
- Manual trigger with optional date override
- Complete daily processing (articles + sentiment + SERPs)

---

### 3. **daily_roc.yml** - Daily Roc Nation News Pipeline
**Purpose**: Separate pipeline for tracking Roc Nation individuals (celebrities, artists, executives)

**Schedule**: Daily at 12:00 UTC (~8am ET)

**Process Flow**:
```
1. Fetch & analyze Roc Nation news
   ↓ outputs: data_roc/daily_counts.csv
   ↓ outputs: data_roc/articles/{date}.csv
```

**Scripts Used**:
- `scripts/news_sentiment_roc.py` - All-in-one processing

**Roster Files**:
- `roc.txt` - List of Roc Nation individuals
- `roc_aliases.csv` - Name variations

**Features**:
- Completely separate data structure (`data_roc/`)
- Uploads artifacts for debugging
- Auto-triggers on changes to ROC-related files

**Note**: Independent from Fortune 1000 data - different roster, different output structure

---

### 4. **backfill_serps.yml** - Historical SERP Backfill
**Purpose**: Process SERP data for historical date ranges (one-time or periodic catch-up)

**Schedule**: Manual trigger only

**Process Flow**:
```
For each date in range:
  1. Process brand SERPs
     ↓ outputs: data/processed_serps/{date}-brand-serps-*.csv
     
  2. Process CEO SERPs (batch mode)
     ↓ outputs: data/processed_serps/{date}-ceo-serps-*.csv
     ↓ updates: data/daily_counts/ceo-serps-daily-counts-chart.csv
```

**Scripts Used**:
- `scripts/process_serps_brands.py --date {date}` - Per-date brand processing
- `scripts/process_serps_ceos.py --backfill {start} {end}` - Batch CEO processing

**Input Parameters**:
- `start`: Start date (YYYY-MM-DD) - default: 2025-09-15
- `end`: End date (YYYY-MM-DD) - default: today

**Use Cases**:
- Initial SERP data population
- Reprocessing after algorithm changes
- Filling gaps in historical data
- Recovery after data loss

**Behavior**:
- Continues on errors (some dates may lack raw SERP data)
- Commits all processed dates in single batch
- Safe push with rebase retry

---

### 5. **send_alerts.yml** - Sentiment Alert Notifications
**Purpose**: Sends email alerts when CEO/brand sentiment exceeds risk thresholds

**Schedule**: 
- Triggers after `Daily Data Pipeline` or `Sentiment ETL` complete
- Can also be triggered manually or via API

**Process Flow**:
```
1. Download previous cooldown state
   ↓ (tracks when each entity was last alerted)
   
2. Analyze latest sentiment data
   ↓ checks against threshold (default: 40% negative)
   ↓ applies cooldown period (default: 180 days)
   ↓ implements "soft shift" logic for early runs
   
3. Send email alerts via Mailgun
   ↓ includes dashboard links
   ↓ shows sentiment metrics
   
4. Upload new cooldown state for next run
```

**Scripts Used**:
- `scripts/send_alerts.py` - Main alerting logic

**Configuration** (via GitHub Secrets & Variables):

**Required Secrets**:
- `MAILGUN_API_KEY` - Mailgun API key
- `MAILGUN_DOMAIN` - Your Mailgun domain
- `MAILGUN_FROM` - Sender email address
- `MAILGUN_TO` - Recipient email address(es)
- `MAILGUN_REGION` - Mailgun region (us/eu)

**Optional Variables** (can override per run):
- `ALERT_SEND_MODE`: `same_morning` or `next_morning` (default: same_morning)
- `NEGATIVE_THRESHOLD`: 0.0-1.0 (default: 0.4 = 40%)
- `ALERT_COOLDOWN_DAYS`: Days between alerts (default: 180)
- `SOFT_SHIFT_HOURS`: Hours before which to treat as previous day (default: 6)

**Manual Trigger Options**:
All configuration values can be overridden when manually triggering

**Cooldown Mechanism**:
- Stores `data/last_alert_dates.json` as GitHub artifact
- Prevents alert spam by tracking last notification date per entity
- 365-day retention for cooldown state

---

### 6. **serp-data.yml** - [DEPRECATED]
**Status**: ⚠️ **DISABLED AND DEPRECATED**

**Reason for Deprecation**:
- Overlaps with SERP processing in `daily_brands.yml` and `daily_ceos.yml`
- Uses outdated data paths
- Creates potential for duplicate processing and data conflicts

**Replaced By**:
- `daily_brands.yml` - Daily brand SERP processing
- `daily_ceos.yml` - Daily CEO SERP processing
- `backfill_serps.yml` - Historical SERP backfills

**Action**: Will fail immediately with deprecation notice if triggered

---

## Workflow Execution Schedule

### Daily Execution Order:
```
09:10 UTC - daily_brands.yml starts
    ↓ (brand articles + sentiment + SERPs)
    
12:00 UTC - daily_ceos.yml starts  
12:00 UTC - daily_roc.yml starts (parallel)
    ↓ (CEO articles + sentiment + SERPs)
    ↓ (ROC sentiment)
    
On completion - send_alerts.yml triggers
    ↓ (analyzes all new data, sends notifications)
```

### Concurrency Groups:
- `data-writes`: Prevents brands/CEOs/ROC from conflicting (brands pipeline uses this)
- `daily-ceos-${{ github.ref }}`: CEO pipeline isolation
- `daily-roc-${{ github.ref_name }}`: ROC pipeline isolation
- `send-alerts`: Alert sending isolation

---

## Data Structure Outputs

### Article Data:
```
data/processed_articles/
├── {date}-brand-articles-table.csv   (aggregated for dashboard table)
├── {date}-brand-articles-modal.csv   (detailed for headlines modal)
├── {date}-ceo-articles-table.csv     (aggregated for dashboard table)
└── {date}-ceo-articles-modal.csv     (detailed for headlines modal)
```

### SERP Data:
```
data/processed_serps/
├── {date}-brand-serps-table.csv      (aggregated metrics)
├── {date}-brand-serps-rows.csv       (individual search results)
├── {date}-ceo-serps-processed.csv    (aggregated metrics)
└── {date}-ceo-serps-rows.csv         (individual search results)
```

### Chart Data (Time Series):
```
data/daily_counts/
├── brand-articles-daily-counts-chart.csv  (brand sentiment over time)
├── brand-serps-daily-counts-chart.csv     (brand SERP metrics over time)
├── ceo-articles-daily-counts-chart.csv    (CEO sentiment over time)
└── ceo-serps-daily-counts-chart.csv       (CEO SERP metrics over time)
```

### ROC Data (Separate):
```
data_roc/
├── daily_counts.csv          (Roc Nation sentiment over time)
└── articles/{date}.csv       (daily article details)
```

### Roster Data:
```
rosters/
├── main_roster.csv           (companies/CEOs with sectors)
├── roster.csv                (legacy fallback)
├── boards_roster.csv         (CEO board memberships)
└── ceo_aliases.csv           (CEO name variations)
```

---

## Troubleshooting

### Common Issues:

**1. Workflow fails with "folder not found"**
- Solution: The workflow creates necessary folders in "Ensure folders" step
- Check that your Python scripts output to the correct new paths

**2. SERP processing skipped**
- Reason: Idempotent check finds existing data for that date
- Solution: Delete the existing file if you need to reprocess

**3. Alerts not sending**
- Check: GitHub Secrets are configured correctly
- Check: Cooldown period hasn't been triggered recently
- Check: Sentiment threshold is being exceeded
- Debug: Run manually with lower threshold

**4. Push conflicts/failures**
- Workflows use rebase retry logic
- If still failing: check for file locks or merge conflicts
- Manual resolution may be needed

### Manual Workflow Triggers:

All workflows support manual triggering via GitHub Actions UI:
1. Go to Actions tab
2. Select workflow
3. Click "Run workflow"
4. Configure optional parameters
5. Click "Run workflow" button

---

## Future Improvements

### Suggested Enhancements:

1. **Consolidate daily pipelines**:
   - Combine `daily_brands.yml` and `daily_ceos.yml` into single workflow
   - Use matrix strategy for parallel processing

2. **Improve error handling**:
   - Add Slack/email notifications on workflow failures
   - Implement retry logic for transient API failures

3. **Performance optimization**:
   - Cache Python dependencies across runs
   - Use artifacts to share data between jobs
   - Parallel processing where possible

4. **Monitoring**:
   - Add workflow status badges to README
   - Track processing times and success rates
   - Alert on stale data (missed runs)

---

## Maintenance

### Regular Tasks:

**Weekly**:
- Review workflow run history for failures
- Check data completeness (no missing dates)

**Monthly**:
- Review cooldown state artifact size
- Verify roster files are up to date
- Check for deprecated dependencies

**As Needed**:
- Update Python version (currently 3.11)
- Update action versions (@v4, @v5, etc.)
- Adjust rate limits if hitting API quotas
- Update alert thresholds based on feedback

### File Dependencies:

Workflows monitor these files and auto-trigger on changes:
- `daily_roc.yml`: Triggers on changes to ROC scripts, rosters, or dashboard
- Other workflows: Manual or scheduled only

---

## Quick Reference

| Workflow | Frequency | Purpose | Outputs |
|----------|-----------|---------|---------|
| daily_brands.yml | Daily 09:10 UTC | Brand news & SERPs | Brand articles, sentiment, SERP metrics |
| daily_ceos.yml | Daily 12:00 UTC | CEO news & SERPs | CEO articles, sentiment, SERP metrics |
| daily_roc.yml | Daily 12:00 UTC | Roc Nation news | ROC sentiment data |
| backfill_serps.yml | Manual | Historical SERP processing | Backfilled SERP data |
| send_alerts.yml | After pipelines | Email notifications | Alerts for high-risk entities |
| serp-data.yml | ❌ DEPRECATED | Legacy SERP processing | (disabled) |

---

## Contact & Support

For workflow issues or questions:
1. Check workflow run logs in GitHub Actions tab
2. Review this documentation
3. Check individual script documentation in `scripts/` folder
4. Review migration documentation in `MIGRATION_*.md` files
