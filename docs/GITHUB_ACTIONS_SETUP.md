# GitHub Actions Scheduled Import Setup

## Overview

The scheduled tournament import runs automatically in **GitHub Actions** on a **weekly** schedule. That job **refreshes data** from the Berkeley Table Tennis site: it **scrapes** tournament links, downloads HTML/PDF results, and imports new tournaments into **Supabase**. The workflow runs **Fridays at 08:00 UTC** (midnight Pacific during PST) and can also be started manually.

For how this fits with **Render** (web UI) and optional **FastCron**, see **[INFRASTRUCTURE.md](./INFRASTRUCTURE.md)**.

## Setup Instructions

### 1. Add GitHub Secrets

You need to add your Supabase credentials as GitHub Secrets:

1. Go to your GitHub repository
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret** and add:
   - `SUPABASE_URL`: Your Supabase project URL
   - `SUPABASE_KEY`: Your Supabase anon/service key

### 2. Workflow File

The workflow is located at `.github/workflows/scheduled-import.yml` and:
- Runs every Friday at **08:00 UTC** (intended as end-of-week import; aligns with midnight Pacific when PST is in effect—see cron in the workflow file)
- Can be manually triggered via the GitHub Actions UI
- Automatically creates a GitHub issue if the import fails

### 3. Manual Trigger

You can manually trigger the workflow:
1. Go to **Actions** tab in your GitHub repository
2. Select **Scheduled Tournament Import** workflow
3. Click **Run workflow** button

## Validation Changes

The validation threshold has been updated to **50%** (previously 20%). This means tournaments will only fail validation if more than 50% of expected matches are missing.

## Scripts Cleanup

The following scripts have been removed as they were redundant:
- `import_tournaments.py` - Replaced by `import_all_tournaments.py` (which now includes validation)
- `import_round_robin.py` - Functionality available in `reimport_tournament.py`

## Remaining Scripts

Essential scripts that remain:
- `scheduled_import.py` - Main script for scheduled imports
- `import_all_tournaments.py` - Core importer with validation (used by scheduled import)
- `reimport_tournament.py` - Utility for re-importing specific tournaments
- `test_scheduled_import.py` - Testing utility
- `check_setup.py` - Environment verification
- `check_tables.py` - Database table verification

## Monitoring

The workflow will:
- Create a GitHub issue automatically if the import fails
- Exit with code 1 if any tournaments fail to import
- Log all output in the GitHub Actions logs

## Testing Locally

Before deploying, test the scheduled import script locally:

```bash
# Set environment variables
export SUPABASE_URL=your_url
export SUPABASE_KEY=your_key

# Run the scheduled import script
python3 scripts/scheduled_import.py
```

