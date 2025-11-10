#!/usr/bin/env python3
"""
Check if round robin tables exist
"""
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')
client = create_client(supabase_url, supabase_key)

required_tables = [
    'players',
    'tournaments',
    'round_robin_groups',
    'player_tournament_stats',
    'player_rating_history',
    'matches'
]

print("=" * 60)
print("Checking Round Robin Tables")
print("=" * 60)
print()

missing_tables = []

for table in required_tables:
    try:
        result = client.table(table).select('*').limit(1).execute()
        print(f"✅ {table} - exists")
    except Exception as e:
        error_str = str(e).lower()
        if 'relation' in error_str or 'does not exist' in error_str or 'pgrst205' in error_str or 'schema cache' in error_str:
            print(f"❌ {table} - MISSING")
            missing_tables.append(table)
        else:
            print(f"⚠️  {table} - Error: {e}")

print()

if missing_tables:
    print("=" * 60)
    print("⚠️  Missing Tables Detected")
    print("=" * 60)
    print()
    print("Please run the SQL schema to create the tables:")
    print()
    print("1. Open your Supabase dashboard")
    print("2. Go to SQL Editor")
    print("3. Copy the contents of round_robin_schema.sql")
    print("4. Paste and run the SQL")
    print()
    print(f"Missing tables: {', '.join(missing_tables)}")
else:
    print("=" * 60)
    print("✅ All tables exist!")
    print("=" * 60)
    print()
    print("You can now import tournament data:")
    print("  python3 import_round_robin.py --url <url>")

