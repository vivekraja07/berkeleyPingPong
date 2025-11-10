# Berkeley Ping Pong History Parser

A Python parser that extracts ping pong match history from PDF and HTML files/websites and stores them in a Supabase database.

## Features

- Parse ping pong match data from PDF files (local or URLs)
- Parse ping pong match data from HTML pages (local or URLs)
- Extract match information including:
  - Player names
  - Scores
  - Match dates
  - Winners
- Store parsed data in Supabase database
- Support for batch processing multiple files

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up Supabase

1. Create a Supabase project at [supabase.com](https://supabase.com)
2. Get your project URL and anon key from the project settings
3. Create a `.env` file in the project root:

```bash
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_anon_key
```

### 3. Create Database Table

Run this SQL in your Supabase SQL Editor to create the matches table:

```sql
CREATE TABLE IF NOT EXISTS ping_pong_matches (
    id BIGSERIAL PRIMARY KEY,
    player1 TEXT NOT NULL,
    player2 TEXT NOT NULL,
    score1 INTEGER NOT NULL DEFAULT 0,
    score2 INTEGER NOT NULL DEFAULT 0,
    match_date DATE,
    winner TEXT,
    source TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_player1 ON ping_pong_matches(player1);
CREATE INDEX IF NOT EXISTS idx_player2 ON ping_pong_matches(player2);
CREATE INDEX IF NOT EXISTS idx_match_date ON ping_pong_matches(match_date);
CREATE INDEX IF NOT EXISTS idx_winner ON ping_pong_matches(winner);
```

## Usage

### Basic Usage

Parse PDF files:
```bash
python main.py --pdf file1.pdf file2.pdf
```

Parse HTML files:
```bash
python main.py --html page1.html page2.html
```

Parse from URLs:
```bash
python main.py --pdf https://example.com/matches.pdf --html https://example.com/matches.html
```

Combine multiple sources:
```bash
python main.py --pdf matches.pdf --html https://example.com/history.html
```

### Dry Run (Test Without Database)

To test parsing without inserting into the database:
```bash
python main.py --pdf matches.pdf --dry-run
```

## Project Structure

```
berkeleyPingPong/
├── parsers/
│   ├── __init__.py
│   ├── pdf_parser.py      # PDF parsing logic
│   └── html_parser.py     # HTML parsing logic
├── db/
│   ├── __init__.py
│   └── supabase_client.py # Supabase integration
├── main.py                 # Main entry point
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables (not in git)
├── .gitignore
└── README.md
```

## Parser Details

### PDF Parser

The PDF parser uses `pdfplumber` (primary) and `PyPDF2` (fallback) to extract text from PDF files. It looks for:
- Date patterns (MM/DD/YYYY, DD-MM-YYYY, etc.)
- Player names (format: "Player1 vs Player2")
- Scores (format: "X - Y" or "X:Y")
- Winner information

### HTML Parser

The HTML parser uses `BeautifulSoup` to parse HTML content. It:
- First tries to extract data from HTML tables
- Falls back to extracting from divs/sections with match-related classes
- Finally extracts from plain text if structured elements aren't found

## Customization

The parsers use regex patterns to extract match data. You can customize these patterns in:
- `parsers/pdf_parser.py` - `match_patterns` and extraction methods
- `parsers/html_parser.py` - `match_patterns` and extraction methods

## Troubleshooting

### Connection Issues

If you get connection errors:
1. Verify your `.env` file has correct `SUPABASE_URL` and `SUPABASE_KEY`
2. Check that your Supabase project is active
3. Ensure the table `ping_pong_matches` exists in your database

### Parsing Issues

If matches aren't being extracted correctly:
1. Use `--dry-run` to see what data is being extracted
2. Check the sample matches printed in the output
3. Adjust the regex patterns in the parser files to match your data format

### Database Errors

If inserts fail:
1. Verify the table schema matches the SQL provided
2. Check that your Supabase key has INSERT permissions
3. Review the error messages for specific field issues

## License

MIT

