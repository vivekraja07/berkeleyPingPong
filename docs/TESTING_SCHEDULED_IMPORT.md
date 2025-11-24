# Quick Testing Guide for Scheduled Import

## Quick Start

### 1. Test Database Connection
```bash
python3 scripts/test_scheduled_import.py --mode db-test
```

### 2. Test Scraping (No Database Changes)
```bash
python3 scripts/test_scheduled_import.py --mode scraping
```

### 3. Dry Run (See What Would Be Imported)
```bash
python3 scripts/test_scheduled_import.py --mode dry-run
```

### 4. Limited Import Test (Import 3 tournaments)
```bash
python3 scripts/test_scheduled_import.py --mode limited --limit 3
```

### 5. Simulate Full Scheduled Run (Limited)
```bash
python3 scripts/test_scheduled_import.py --mode simulate --limit 5
```

## What Each Test Does

| Mode | What It Does | Database Changes? |
|------|--------------|-------------------|
| `db-test` | Tests database connection | ❌ No |
| `scraping` | Tests scraping tournament links | ❌ No |
| `dry-run` | Shows what would be imported | ❌ No |
| `limited` | Imports a few tournaments | ✅ Yes |
| `simulate` | Simulates full scheduled run | ✅ Yes |

## Recommended Testing Order

1. **Start with `db-test`** - Make sure database works
2. **Then `scraping`** - Make sure website scraping works
3. **Then `dry-run`** - See what would be imported
4. **Finally `limited`** - Test actual import with 1-3 tournaments

## Advanced Options

### Custom Date Range
```bash
# Look back 30 days instead of 14
python3 scripts/test_scheduled_import.py --mode dry-run --days-back 30
```

### More Workers (Faster)
```bash
# Use 5 workers instead of 2
python3 scripts/test_scheduled_import.py --mode limited --limit 5 --max-workers 5
```

### Simulate Full Run (No Limit)
```bash
# WARNING: This will import ALL tournaments from last 2 weeks!
python3 scripts/scheduled_import.py
```

## Understanding the Output

### Success Output
```
✅ Newly imported: 3
⏭️  Already existed: 2
❌ Failed: 0
```
- **Newly imported**: Tournaments successfully added to database
- **Already existed**: Tournaments that were skipped (already in database)
- **Failed**: Tournaments that couldn't be imported (check error messages)

### Exit Codes
- `0` = Success (no failures)
- `1` = Failure (some tournaments failed to import)

## Troubleshooting

### "No tournaments found"
- Check internet connection
- Website might be down
- Date range might be too narrow (try `--days-back 30`)

### "Database connection failed"
- Check `.env` file exists with `SUPABASE_URL` and `SUPABASE_KEY`
- Verify database tables exist (run `sql/round_robin_schema.sql`)

### "All tournaments already exist"
- This is normal if you've already imported recent tournaments
- Try a longer date range: `--days-back 60`

## Full Documentation

See [SCHEDULED_IMPORT_GUIDE.md](./SCHEDULED_IMPORT_GUIDE.md) for detailed explanation of how the scheduled import works.

