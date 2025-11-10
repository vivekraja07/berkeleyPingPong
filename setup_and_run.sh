#!/bin/bash
# Setup and Run Script for Round Robin Tournament Importer

echo "=========================================="
echo "Round Robin Tournament Database Setup"
echo "=========================================="
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  .env file not found!"
    echo ""
    echo "Please create a .env file with your Supabase credentials:"
    echo ""
    echo "SUPABASE_URL=your_supabase_project_url"
    echo "SUPABASE_KEY=your_supabase_anon_key"
    echo ""
    echo "You can find these in your Supabase project:"
    echo "  Settings > API > Project URL and anon/public key"
    echo ""
    read -p "Press Enter after you've created the .env file..."
fi

# Check if .env has the required variables
if [ -f .env ]; then
    if ! grep -q "SUPABASE_URL" .env || ! grep -q "SUPABASE_KEY" .env; then
        echo "⚠️  .env file exists but missing SUPABASE_URL or SUPABASE_KEY"
        echo "Please add them to your .env file"
        exit 1
    fi
    echo "✅ .env file found"
else
    echo "❌ .env file still not found. Exiting."
    exit 1
fi

echo ""
echo "=========================================="
echo "Step 1: Test Parser (Dry Run)"
echo "=========================================="
echo ""

# Test the parser first
python3 import_round_robin.py --url https://berkeleytabletennis.org/results/rr_results_2025nov07 --dry-run

echo ""
echo "=========================================="
echo "Step 2: Database Setup"
echo "=========================================="
echo ""
echo "⚠️  IMPORTANT: Before importing data, you need to:"
echo ""
echo "1. Open your Supabase project dashboard"
echo "2. Go to SQL Editor"
echo "3. Copy and paste the contents of round_robin_schema.sql"
echo "4. Run the SQL to create all tables"
echo ""
echo "The schema file is located at:"
echo "  $(pwd)/round_robin_schema.sql"
echo ""
read -p "Press Enter after you've run the SQL schema in Supabase..."

echo ""
echo "=========================================="
echo "Step 3: Import Tournament Data"
echo "=========================================="
echo ""

# Import the data
python3 import_round_robin.py --url https://berkeleytabletennis.org/results/rr_results_2025nov07

echo ""
echo "=========================================="
echo "Step 4: Test Queries"
echo "=========================================="
echo ""

# Run example queries
python3 example_queries.py

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "You can now:"
echo "  - Query player stats: python3 import_round_robin.py --query-player 'Player Name'"
echo "  - List all players: python3 import_round_robin.py --list-players"
echo "  - Import more tournaments: python3 import_round_robin.py --url <url>"
echo ""

