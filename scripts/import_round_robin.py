#!/usr/bin/env python3
"""
Import Round Robin Tournament Data
Parses round robin results and imports them into Supabase
"""
import argparse
import sys
import os

# Add parent directory to path to import backend modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.parsers.round_robin_parser import RoundRobinParser
from backend.db.round_robin_client import RoundRobinClient


def import_round_robin(url: str, dry_run: bool = False):
    """
    Import round robin tournament data from a URL
    
    Args:
        url: URL to the round robin results page
        dry_run: If True, parse but don't insert into database
    """
    print(f"Parsing round robin results from: {url}")
    
    # Parse the data
    parser = RoundRobinParser()
    try:
        results = parser.parse_url(url)
    except Exception as e:
        print(f"Error parsing URL: {e}")
        sys.exit(1)
    
    tournament_info = results.get('tournament', {})
    groups = results.get('groups', [])
    
    print(f"\nTournament: {tournament_info.get('name', 'Unknown')}")
    print(f"Date: {tournament_info.get('date_string', 'Unknown')}")
    print(f"Found {len(groups)} groups")
    
    # Print summary
    total_players = sum(len(g.get('players', [])) for g in groups)
    total_matches = sum(len(g.get('matches', [])) for g in groups)
    
    print(f"\nSummary:")
    print(f"  Total players: {total_players}")
    print(f"  Total matches: {total_matches}")
    
    for group in groups:
        print(f"\n  Group {group.get('group_number')}: "
              f"{len(group.get('players', []))} players, "
              f"{len(group.get('matches', []))} matches")
    
    if dry_run:
        print("\n--- DRY RUN MODE - Not inserting into database ---")
        return
    
    # Insert into database
    print("\nInserting data into database...")
    try:
        client = RoundRobinClient()
        result = client.insert_round_robin_data(results)
        
        print(f"\n✓ Successfully imported tournament!")
        print(f"  Tournament ID: {result['tournament_id']}")
        print(f"  Groups imported: {len(result['groups'])}")
        
        for group_result in result['groups']:
            print(f"\n  Group {group_result['group_number']}:")
            print(f"    Players: {group_result['players_inserted']}")
            print(f"    Matches: {group_result['matches_inserted']}")
        
    except Exception as e:
        print(f"\n✗ Error importing data: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def query_player_stats(player_name: str):
    """Query and display player statistics"""
    print(f"\nFetching statistics for: {player_name}")
    
    try:
        client = RoundRobinClient()
        
        # Get rating history
        rating_history = client.get_player_rating_history(player_name)
        print(f"\nRating History ({len(rating_history)} tournaments):")
        for entry in rating_history:
            print(f"  {entry.get('tournament_date')}: "
                  f"{entry.get('rating_pre')} → {entry.get('rating_post')} "
                  f"({entry.get('rating_change', 0):+d})")
        
        # Get match statistics
        match_stats = client.get_player_match_stats(player_name)
        if match_stats:
            print(f"\nMatch Statistics:")
            print(f"  Total matches: {match_stats.get('total_matches', 0)}")
            print(f"  Wins: {match_stats.get('wins', 0)}")
            print(f"  Losses: {match_stats.get('losses', 0)}")
            print(f"  Draws: {match_stats.get('draws', 0)}")
            print(f"  Win percentage: {match_stats.get('win_percentage', 0)}%")
        
        # Get tournament stats
        tournament_stats = client.get_player_stats_by_tournament(player_name)
        if tournament_stats:
            print(f"\nTournament Statistics ({len(tournament_stats)} tournaments):")
            for stat in tournament_stats[:5]:  # Show first 5
                print(f"  {stat.get('tournament_date')} - {stat.get('tournament_name')}: "
                      f"Rating {stat.get('rating_pre')} → {stat.get('rating_post')}, "
                      f"{stat.get('matches_won', 0)} matches won")
        
    except Exception as e:
        print(f"Error querying player stats: {e}")
        import traceback
        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(
        description='Import round robin tournament data into Supabase'
    )
    parser.add_argument(
        '--url',
        type=str,
        help='URL to the round robin results page'
    )
    parser.add_argument(
        '--file',
        type=str,
        help='Path to local HTML file with round robin results'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Parse files but do not insert into database'
    )
    parser.add_argument(
        '--query-player',
        type=str,
        help='Query statistics for a specific player'
    )
    parser.add_argument(
        '--list-players',
        action='store_true',
        help='List all players in the database'
    )
    
    args = parser.parse_args()
    
    if args.query_player:
        query_player_stats(args.query_player)
    elif args.list_players:
        try:
            client = RoundRobinClient()
            players = client.get_all_players()
            print(f"\nFound {len(players)} players:")
            for player in players:
                print(f"  - {player.get('name')}")
        except Exception as e:
            print(f"Error listing players: {e}")
    elif args.url:
        import_round_robin(args.url, args.dry_run)
    elif args.file:
        # Parse from file
        from backend.parsers.round_robin_parser import RoundRobinParser
        parser = RoundRobinParser()
        results = parser.parse_file(args.file)
        
        tournament_info = results.get('tournament', {})
        print(f"\nTournament: {tournament_info.get('name', 'Unknown')}")
        print(f"Date: {tournament_info.get('date_string', 'Unknown')}")
        print(f"Found {len(results.get('groups', []))} groups")
        
        if not args.dry_run:
            client = RoundRobinClient()
            result = client.insert_round_robin_data(results)
            print(f"\n✓ Successfully imported!")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()

