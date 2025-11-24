# Quick Start Guide

Follow these steps to set up and run the round robin tournament importer:

## Step 1: Set Up Environment Variables

Create a `.env` file in the project root:

```bash
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_anon_key
```

**Where to find these:**
1. Go to your Supabase project dashboard
2. Click on Settings (gear icon)
3. Go to API section
4. Copy:
   - **Project URL** → `SUPABASE_URL`
   - **anon/public key** → `SUPABASE_KEY`

## Step 2: Create Database Tables

1. Open your Supabase project dashboard
2. Go to **SQL Editor**
3. Click **New Query**
4. Open the file `sql/round_robin_schema.sql` in this project
5. Copy all the SQL content
6. Paste it into the SQL Editor
7. Click **Run** (or press Cmd/Ctrl + Enter)

This will create all necessary tables, views, and functions.

## Step 3: Test the Parser (Dry Run)

Test that the parser works without inserting data:

```bash
python3 scripts/import_round_robin.py --url https://berkeleytabletennis.org/results/rr_results_2025nov07 --dry-run
```

This should show you:
- Tournament information
- Number of groups
- Number of players
- Number of matches

## Step 4: Import Tournament Data

Once the database tables are created, import the data:

```bash
python3 scripts/import_round_robin.py --url https://berkeleytabletennis.org/results/rr_results_2025nov07
```

## Step 5: Start the Web UI

Start the Flask web application to view statistics:

```bash
python3 backend/app.py
```

Or use the convenience script:

```bash
./scripts/start_ui.sh
```

The UI will be available at `http://localhost:5001` (or the port specified in your environment).

## Common Commands

### Import from URL
```bash
python3 scripts/import_round_robin.py --url <url>
```

### Import from local file
```bash
python3 scripts/import_round_robin.py --file path/to/file.html
```

### Query player statistics
```bash
python3 scripts/import_round_robin.py --query-player "Player Name"
```

### List all players
```bash
python3 scripts/import_round_robin.py --list-players
```

## Troubleshooting

### "SUPABASE_URL and SUPABASE_KEY must be set"
- Make sure you created a `.env` file in the project root
- Check that the variable names are exactly `SUPABASE_URL` and `SUPABASE_KEY`
- Make sure there are no spaces around the `=` sign

### "relation does not exist" or table errors
- Make sure you ran the SQL schema in Supabase SQL Editor
- Check that all tables were created successfully

### Import errors
- Try a dry run first: `--dry-run` flag
- Check that the URL is accessible
- Verify your Supabase credentials are correct


