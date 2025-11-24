#!/usr/bin/env python3
"""
Check Setup - Verify environment and database connection
"""
import os
from dotenv import load_dotenv

load_dotenv()

print("=" * 60)
print("Setup Check")
print("=" * 60)
print()

# Check .env file
env_file_exists = os.path.exists('.env')
print(f"✅ .env file exists: {env_file_exists}")

if not env_file_exists:
    print("\n⚠️  Please create a .env file with:")
    print("   SUPABASE_URL=your_supabase_project_url")
    print("   SUPABASE_KEY=your_supabase_anon_key")
    print()
    exit(1)

# Check environment variables
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')

print(f"✅ SUPABASE_URL set: {bool(supabase_url)}")
if supabase_url:
    print(f"   URL: {supabase_url[:30]}...")

print(f"✅ SUPABASE_KEY set: {bool(supabase_key)}")
if supabase_key:
    print(f"   Key: {supabase_key[:20]}...")

if not supabase_url or not supabase_key:
    print("\n⚠️  Please set SUPABASE_URL and SUPABASE_KEY in your .env file")
    print()
    exit(1)

# Test database connection
print("\n" + "=" * 60)
print("Testing Database Connection")
print("=" * 60)
print()

try:
    from supabase import create_client, Client
    
    client: Client = create_client(supabase_url, supabase_key)
    
    # Try to query a table (this will fail if tables don't exist, but connection works)
    try:
        result = client.table('players').select('id').limit(1).execute()
        print("✅ Database connection successful!")
        print("✅ Tables exist - you can import data")
    except Exception as e:
        if 'relation' in str(e).lower() or 'does not exist' in str(e).lower():
            print("⚠️  Database connection works, but tables don't exist yet")
            print("   Please run the SQL schema in Supabase SQL Editor:")
            print("   1. Open Supabase dashboard")
            print("   2. Go to SQL Editor")
            print("   3. Copy contents of round_robin_schema.sql")
            print("   4. Run the SQL")
        else:
            print(f"❌ Database error: {e}")
            raise
    
except ImportError:
    print("❌ supabase package not installed")
    print("   Run: pip install -r requirements.txt")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print("   Check your SUPABASE_URL and SUPABASE_KEY")
    exit(1)

print("\n" + "=" * 60)
print("✅ Setup check complete!")
print("=" * 60)


