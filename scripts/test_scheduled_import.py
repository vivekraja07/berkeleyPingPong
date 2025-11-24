#!/usr/bin/env python3
"""
Test Script for Scheduled Import
Provides various testing options for the scheduled import functionality
"""
import argparse
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.import_all_tournaments import TournamentImporter


def test_dry_run(days_back=14):
    """Test what would be imported without actually importing"""
    print("="*60)
    print("DRY RUN TEST - No data will be imported")
    print("="*60)
    print()
    
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    print(f"Would import tournaments from {start_date} onwards...")
    print()
    
    importer = TournamentImporter()
    tournaments = importer.scrape_tournament_links()
    
    if not tournaments:
        print("No tournaments found!")
        return
    
    # Filter by date
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    filtered = [t for t in tournaments if t['date'] and t['date'] >= start_datetime]
    
    print(f"Found {len(tournaments)} total tournaments")
    print(f"Would process {len(filtered)} tournaments from {start_date} onwards")
    print()
    
    if filtered:
        print("First 10 tournaments that would be imported:")
        for i, t in enumerate(filtered[:10], 1):
            print(f"  {i}. {t['display']} ({t['format']}) - {t['url']}")
        if len(filtered) > 10:
            print(f"  ... and {len(filtered) - 10} more")
    else:
        print("No tournaments found in the date range.")


def test_limited_import(limit=3, days_back=14, max_workers=2):
    """Test with a limited number of tournaments"""
    print("="*60)
    print(f"LIMITED IMPORT TEST - Will import up to {limit} tournaments")
    print("="*60)
    print()
    
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    print(f"Importing tournaments from {start_date} onwards...")
    print(f"Limit: {limit} tournaments")
    print(f"Workers: {max_workers}")
    print()
    
    importer = TournamentImporter()
    stats = importer.import_all(
        limit=limit,
        skip_existing=True,
        start_date=start_date,
        max_workers=max_workers
    )
    
    print()
    print("="*60)
    print("Test Import Complete")
    print("="*60)
    print(f"Total processed: {stats['total']}")
    print(f"✅ Newly imported: {stats['imported']}")
    print(f"⏭️  Already existed: {stats['skipped']}")
    print(f"❌ Failed: {stats['failed']}")
    print("="*60)
    
    return stats['failed'] == 0


def test_scraping_only():
    """Test just the scraping functionality"""
    print("="*60)
    print("SCRAPING TEST - Testing tournament link scraping")
    print("="*60)
    print()
    
    importer = TournamentImporter()
    tournaments = importer.scrape_tournament_links()
    
    if not tournaments:
        print("❌ No tournaments found!")
        return False
    
    print(f"✅ Found {len(tournaments)} tournaments")
    print()
    
    # Count by format
    html_count = sum(1 for t in tournaments if t.get('format') == 'html')
    pdf_count = sum(1 for t in tournaments if t.get('format') == 'pdf')
    pdf_old_count = sum(1 for t in tournaments if t.get('format') == 'pdf_old')
    
    print(f"Format breakdown:")
    print(f"  - HTML: {html_count}")
    print(f"  - PDF (with dates): {pdf_count}")
    print(f"  - Old PDF (no dates): {pdf_old_count}")
    print()
    
    # Show first 5
    print("First 5 tournaments:")
    for i, t in enumerate(tournaments[:5], 1):
        date_str = t['date'].strftime('%Y-%m-%d') if t.get('date') else 'No date'
        print(f"  {i}. {t['display']} ({date_str}) - {t['format']}")
    
    return True


def test_database_connection():
    """Test database connection"""
    print("="*60)
    print("DATABASE CONNECTION TEST")
    print("="*60)
    print()
    
    try:
        from backend.db.round_robin_client import RoundRobinClient
        client = RoundRobinClient()
        
        # Try a simple query
        result = client.client.table('tournaments').select('id, date').limit(1).execute()
        
        print("✅ Database connection successful!")
        print(f"✅ Can query tournaments table")
        
        # Count existing tournaments
        count_result = client.client.table('tournaments').select('id', count='exact').execute()
        print(f"✅ Found {count_result.count} existing tournaments in database")
        
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print()
        print("Make sure you have:")
        print("  1. A .env file with SUPABASE_URL and SUPABASE_KEY")
        print("  2. Database tables created (run sql/round_robin_schema.sql)")
        return False


def simulate_scheduled_run(days_back=14, limit=None):
    """Simulate the actual scheduled import script"""
    print("="*60)
    print("SIMULATED SCHEDULED IMPORT")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    print()
    
    two_weeks_ago = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    print(f"Importing tournaments from {two_weeks_ago} onwards...")
    print("(This ensures we catch any tournaments from the past 2 weeks)")
    print()
    
    if limit:
        print(f"⚠️  TEST MODE: Limited to {limit} tournaments")
        print()
    
    importer = TournamentImporter()
    
    stats = importer.import_all(
        limit=limit,
        skip_existing=True,
        start_date=two_weeks_ago,
        max_workers=5 if not limit else 2  # Use fewer workers in test mode
    )
    
    print()
    print("="*60)
    print("Import Complete")
    print("="*60)
    print(f"Total processed: {stats['total']}")
    print(f"✅ Newly imported: {stats['imported']}")
    print(f"⏭️  Already existed: {stats['skipped']}")
    print(f"❌ Failed: {stats['failed']}")
    print("="*60)
    
    # Exit with error code if there were failures (matching scheduled_import.py behavior)
    if stats['failed'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description='Test the scheduled import script with various options'
    )
    parser.add_argument(
        '--mode',
        choices=['dry-run', 'limited', 'scraping', 'db-test', 'simulate'],
        default='dry-run',
        help='Test mode to run (default: dry-run)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=3,
        help='Limit number of tournaments to import (for limited mode, default: 3)'
    )
    parser.add_argument(
        '--days-back',
        type=int,
        default=14,
        help='Number of days back to look for tournaments (default: 14)'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=2,
        help='Maximum number of parallel workers (default: 2)'
    )
    
    args = parser.parse_args()
    
    if args.mode == 'dry-run':
        test_dry_run(args.days_back)
    elif args.mode == 'limited':
        success = test_limited_import(args.limit, args.days_back, args.max_workers)
        sys.exit(0 if success else 1)
    elif args.mode == 'scraping':
        success = test_scraping_only()
        sys.exit(0 if success else 1)
    elif args.mode == 'db-test':
        success = test_database_connection()
        sys.exit(0 if success else 1)
    elif args.mode == 'simulate':
        simulate_scheduled_run(args.days_back, args.limit if args.limit != 3 else None)


if __name__ == '__main__':
    main()

