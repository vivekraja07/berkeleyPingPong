# Scheduled Import Script Guide

## Overview

The `scheduled_import.py` script is designed to automatically import new round robin tournaments from the Berkeley Table Tennis website. It's meant to run weekly (e.g., every Friday at midnight) to keep the database up-to-date with the latest tournament results.

## What It Does

### Step-by-Step Process

1. **Calculates Date Range**
   - Sets the start date to **2 weeks ago** from the current date
   - This ensures the script catches any tournaments that might have been missed if it failed to run the previous week
   - Example: If run on Dec 15, 2024, it will import tournaments from Dec 1, 2024 onwards

2. **Scrapes Tournament Links**
   - Fetches the results page from `https://berkeleytabletennis.org/results`
   - Parses all tournament links (HTML and PDF formats)
   - Identifies three types of tournament formats:
     - **HTML format**: `/results/rr_results_YYYYmonDD` or `/results/RR_Results YYYYMonDD.html`
     - **PDF format**: `/results/RR_Results YYYYMonDD.pdf`
     - **Old PDF format**: `/results/XXX.pdf` (dates extracted from PDF content)

3. **Filters by Date**
   - Only processes tournaments from the last 2 weeks (or from the `start_date` if specified)
   - Sorts tournaments by date (newest first)

4. **Checks for Existing Tournaments**
   - Queries the database to see which tournaments have already been imported
   - Skips tournaments that already exist (based on date matching)

5. **Imports New Tournaments in Parallel**
   - Uses up to 5 parallel workers to process multiple tournaments simultaneously
   - For each tournament:
     - Downloads and parses the tournament data (HTML or PDF)
     - Extracts player information, groups, and match results
     - Validates the data
     - Inserts into the database (tournaments, players, groups, matches, stats)
   - For old PDFs without dates in filename, extracts the date from the PDF content first

6. **Reports Statistics**
   - Shows total tournaments processed
   - Counts newly imported tournaments
   - Counts skipped tournaments (already existed)
   - Counts failed imports
   - Exits with error code 1 if any failures occurred (useful for monitoring)

## Key Features

- **Idempotent**: Safe to run multiple times - won't duplicate data
- **Parallel Processing**: Uses 5 workers to import tournaments concurrently
- **Error Handling**: Continues processing even if some tournaments fail
- **Date-Based Filtering**: Only processes recent tournaments to avoid re-processing old data
- **Automatic Date Extraction**: For old PDFs, extracts dates from PDF content on-the-fly

## How to Test It

### Prerequisites

1. **Environment Setup**
   ```bash
   # Make sure you have a .env file with:
   SUPABASE_URL=your_supabase_project_url
   SUPABASE_KEY=your_supabase_anon_key
   ```

2. **Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

### Test Methods

#### Method 1: Dry Run (Safest - No Database Changes)

Test what the script would do without actually importing:

```bash
# Test the underlying importer with dry-run
python3 scripts/import_all_tournaments.py --dry-run --start-date 2024-12-01
```

This will:
- Scrape all tournament links
- Show you what tournaments would be imported
- **NOT** modify the database

#### Method 2: Limited Import (Test with Small Dataset)

Import only a few recent tournaments:

```bash
# Import only the 3 most recent tournaments
python3 scripts/import_all_tournaments.py --limit 3 --start-date 2024-12-01
```

#### Method 3: Test with Specific Date Range

Test with a specific date range:

```bash
# Import tournaments from a specific date onwards
python3 scripts/import_all_tournaments.py --start-date 2024-12-01 --limit 5
```

#### Method 4: Run the Actual Scheduled Script (Limited)

Run the scheduled script but limit the number of tournaments:

You can modify the script temporarily or create a test version. However, the scheduled script doesn't have a limit parameter, so you'd need to:

**Option A**: Modify the script temporarily to add a limit:
```python
stats = importer.import_all(
    limit=3,  # Add this for testing
    skip_existing=True,
    start_date=two_weeks_ago,
    max_workers=2  # Reduce workers for testing
)
```

**Option B**: Test the underlying importer directly (recommended):
```bash
# This mimics what scheduled_import.py does
python3 scripts/import_all_tournaments.py \
    --start-date $(date -v-14d +%Y-%m-%d) \
    --max-workers 2 \
    --limit 3
```

#### Method 5: Full Test Run (Production-like)

Run the actual scheduled script (will import all new tournaments from last 2 weeks):

```bash
python3 scripts/scheduled_import.py
```

**Warning**: This will import all new tournaments from the last 2 weeks into your database!

### Verification Steps

After running a test, verify the results:

1. **Check Database**:
   ```bash
   # Use the check_tables script to see what was imported
   python3 scripts/check_tables.py
   ```

2. **Check Import Statistics**:
   The script outputs a summary:
   ```
   ============================================================
   Import Complete
   ============================================================
   Total processed: 5
   ✅ Newly imported: 3
   ⏭️  Already existed: 2
   ❌ Failed: 0
   ============================================================
   ```

3. **Check Exit Code**:
   ```bash
   python3 scripts/scheduled_import.py
   echo $?  # Should be 0 if successful, 1 if failures occurred
   ```

## Testing Checklist

- [ ] Environment variables are set correctly
- [ ] Database connection works (test with a simple query)
- [ ] Dry run shows expected tournaments
- [ ] Limited import works correctly
- [ ] Skipped tournaments are properly identified
- [ ] New tournaments are imported successfully
- [ ] Statistics are reported accurately
- [ ] Exit codes work correctly (0 for success, 1 for failures)

## Common Issues and Solutions

### Issue: "SUPABASE_URL and SUPABASE_KEY must be set"
**Solution**: Create a `.env` file in the project root with your Supabase credentials

### Issue: "No tournaments found"
**Solution**: 
- Check your internet connection
- Verify the berkeleytabletennis.org website is accessible
- Check if the results page structure has changed

### Issue: "Failed to import tournaments"
**Solution**:
- Check database permissions
- Verify database tables exist (run `sql/round_robin_schema.sql`)
- Check error messages in the output for specific issues

### Issue: Script runs but imports nothing
**Solution**:
- All tournaments from the last 2 weeks may already be imported
- Try with a longer date range: `--start-date 2024-11-01`
- Check the database to see what dates are already imported

## Production Deployment

For production, you would typically:

1. **Set up a cron job** (Linux/Mac):
   ```bash
   # Run every Friday at midnight PST
   0 0 * * 5 cd /path/to/project && python3 scripts/scheduled_import.py >> logs/import.log 2>&1
   ```

2. **Use a scheduler service** (e.g., Render Cron, GitHub Actions, etc.)

3. **Monitor the exit code** to alert on failures

4. **Set up logging** to track import history

## Example Output

```
============================================================
Scheduled Tournament Import
Started at: 2024-12-15 00:00:00
============================================================

Importing tournaments from 2024-12-01 onwards...
(This ensures we catch any tournaments from the past 2 weeks)

Scraping tournament links from: https://berkeleytabletennis.org/results
Found 45 unique tournaments:
  - HTML format: 30
  - PDF format: 12
  - Old PDF format: 3

Filtered to tournaments from 2024-12-01 onwards: 5 tournaments

Will process 5 tournaments in parallel (max 5 workers)
Starting from: 2024 Dec 15

[Processing output for each tournament...]

============================================================
Import Complete
============================================================
Total processed: 5
✅ Newly imported: 3
⏭️  Already existed: 2
❌ Failed: 0
============================================================
```

