#!/usr/bin/env python3
"""
Re-import a specific tournament by date
Useful for re-importing tournaments that failed validation or had errors
"""
import argparse
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.import_all_tournaments import TournamentImporter
from backend.db.round_robin_client import RoundRobinClient


def find_tournament_by_date(target_date: str) -> dict:
    """Find tournament URL by date"""
    print(f"Searching for tournament on {target_date}...")
    
    importer = TournamentImporter()
    tournaments = importer.scrape_tournament_links()
    
    # Parse target date
    try:
        target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
    except ValueError:
        # Try other formats
        try:
            target_datetime = datetime.strptime(target_date, '%m/%d/%Y')
        except ValueError:
            print(f"Error: Invalid date format. Use YYYY-MM-DD or MM/DD/YYYY")
            return None
    
    # Find matching tournament
    for tournament in tournaments:
        if tournament.get('date') and tournament['date'].date() == target_datetime.date():
            return tournament
    
    print(f"❌ No tournament found for date {target_date}")
    return None


def delete_tournament_by_date(date_str: str) -> bool:
    """Delete existing tournament entry by date (to allow re-import)"""
    try:
        client = RoundRobinClient()
        
        # Find tournament
        result = client.client.table('tournaments').select('id').eq('date', date_str).execute()
        
        if not result.data:
            print(f"ℹ️  No existing tournament found for {date_str}")
            return True
        
        tournament_id = result.data[0]['id']
        print(f"Found existing tournament (ID: {tournament_id})")
        
        # Delete related data first (foreign key constraints)
        print("Deleting related data...")
        
        # Delete matches
        client.client.table('matches').delete().eq('tournament_id', tournament_id).execute()
        
        # Delete player tournament stats
        client.client.table('player_tournament_stats').delete().eq('tournament_id', tournament_id).execute()
        
        # Delete round robin groups
        client.client.table('round_robin_groups').delete().eq('tournament_id', tournament_id).execute()
        
        # Delete tournament
        client.client.table('tournaments').delete().eq('id', tournament_id).execute()
        
        print(f"✅ Deleted existing tournament and related data")
        return True
        
    except Exception as e:
        print(f"⚠️  Error deleting tournament: {e}")
        print("Will attempt to re-import anyway (may cause duplicate key errors)")
        return False


def reimport_tournament_by_date(date_str: str, delete_first: bool = True):
    """Re-import a tournament by date"""
    print("="*60)
    print("Re-import Tournament by Date")
    print("="*60)
    print()
    
    # Find tournament
    tournament = find_tournament_by_date(date_str)
    if not tournament:
        return False
    
    print(f"✅ Found tournament: {tournament['display']}")
    print(f"   URL: {tournament['url']}")
    print(f"   Format: {tournament['format']}")
    print()
    
    # Delete existing entry if requested
    if delete_first:
        date_for_db = tournament['date'].strftime('%Y-%m-%d')
        delete_tournament_by_date(date_for_db)
        print()
    
    # Import the tournament
    print("Importing tournament...")
    print()
    
    importer = TournamentImporter()
    
    # Import with skip_existing=False to force import
    display, status = importer.import_tournament(
        tournament,
        skip_existing=False,  # Force import even if exists
        index=1,
        total=1
    )
    
    print()
    print("="*60)
    if status == 'imported':
        print("✅ Successfully re-imported tournament!")
    elif status == 'skipped':
        print("⏭️  Tournament was skipped (may still exist in database)")
    else:
        print(f"❌ Failed to import tournament: {status}")
    print("="*60)
    
    return status == 'imported'


def reimport_tournament_by_url(url: str, delete_first: bool = True):
    """Re-import a tournament by URL"""
    print("="*60)
    print("Re-import Tournament by URL")
    print("="*60)
    print()
    
    # Parse URL to get date
    import re
    from datetime import datetime
    
    # Try to extract date from URL
    date_match = re.search(r'(\d{4})([a-z]{3})(\d{2})', url, re.I)
    if date_match:
        year = int(date_match.group(1))
        month_str = date_match.group(2)
        day = int(date_match.group(3))
        
        month_map = {
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }
        month = month_map.get(month_str.lower(), 1)
        
        try:
            date = datetime(year, month, day)
            date_str = date.strftime('%Y-%m-%d')
            
            # Delete first if requested
            if delete_first:
                delete_tournament_by_date(date_str)
                print()
        except ValueError:
            pass
    
    # Create tournament dict
    tournament = {
        'url': url,
        'date': date if 'date' in locals() else None,
        'format': 'pdf' if url.endswith('.pdf') else 'html',
        'display': url.split('/')[-1]
    }
    
    print(f"Importing tournament from URL: {url}")
    print()
    
    importer = TournamentImporter()
    display, status = importer.import_tournament(
        tournament,
        skip_existing=False,
        index=1,
        total=1
    )
    
    print()
    print("="*60)
    if status == 'imported':
        print("✅ Successfully re-imported tournament!")
    else:
        print(f"❌ Failed to import tournament: {status}")
    print("="*60)
    
    return status == 'imported'


def main():
    parser = argparse.ArgumentParser(
        description='Re-import a tournament by date or URL'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Tournament date (YYYY-MM-DD or MM/DD/YYYY format, e.g., 2025-11-21 or 11/21/2025)'
    )
    parser.add_argument(
        '--url',
        type=str,
        help='Tournament URL to re-import'
    )
    parser.add_argument(
        '--no-delete',
        action='store_true',
        help='Do not delete existing tournament entry first (may cause errors)'
    )
    
    args = parser.parse_args()
    
    if not args.date and not args.url:
        parser.print_help()
        sys.exit(1)
    
    delete_first = not args.no_delete
    
    if args.date:
        success = reimport_tournament_by_date(args.date, delete_first)
    else:
        success = reimport_tournament_by_url(args.url, delete_first)
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

