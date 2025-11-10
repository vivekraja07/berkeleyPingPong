#!/usr/bin/env python3
"""
Example Queries for Round Robin Tournament Data
Demonstrates how to query data for charts and statistics
"""
from db.round_robin_client import RoundRobinClient
from datetime import datetime
import json


def example_rating_history():
    """Example: Get player rating history for charting"""
    print("=" * 60)
    print("Example 1: Player Rating History")
    print("=" * 60)
    
    client = RoundRobinClient()
    player_name = "GuangPeng Chen"
    
    rating_history = client.get_player_rating_history(player_name)
    
    print(f"\nRating history for {player_name}:")
    print(f"{'Date':<15} {'Pre':<8} {'Post':<8} {'Change':<10} {'Tournament':<30}")
    print("-" * 80)
    
    for entry in rating_history:
        date = entry.get('tournament_date', '')
        pre = entry.get('rating_pre', 'N/A')
        post = entry.get('rating_post', 'N/A')
        change = entry.get('rating_change', 0)
        tournament = entry.get('tournament_name', '')[:28]
        
        change_str = f"{change:+d}" if change else "0"
        print(f"{date:<15} {pre!s:<8} {post!s:<8} {change_str:<10} {tournament:<30}")
    
    # Format for charting (JSON)
    chart_data = {
        'labels': [r.get('tournament_date') for r in rating_history],
        'ratings': [r.get('rating_post') for r in rating_history],
        'changes': [r.get('rating_change', 0) for r in rating_history]
    }
    
    print(f"\nChart data (JSON format):")
    print(json.dumps(chart_data, indent=2))


def example_match_stats():
    """Example: Get player match statistics"""
    print("\n" + "=" * 60)
    print("Example 2: Player Match Statistics")
    print("=" * 60)
    
    client = RoundRobinClient()
    player_name = "GuangPeng Chen"
    
    match_stats = client.get_player_match_stats(player_name)
    
    if match_stats:
        print(f"\nMatch statistics for {player_name}:")
        print(f"  Total matches: {match_stats.get('total_matches', 0)}")
        print(f"  Wins: {match_stats.get('wins', 0)}")
        print(f"  Losses: {match_stats.get('losses', 0)}")
        print(f"  Draws: {match_stats.get('draws', 0)}")
        print(f"  Win percentage: {match_stats.get('win_percentage', 0)}%")
        
        # Format for pie chart
        chart_data = {
            'labels': ['Wins', 'Losses', 'Draws'],
            'data': [
                match_stats.get('wins', 0),
                match_stats.get('losses', 0),
                match_stats.get('draws', 0)
            ]
        }
        
        print(f"\nChart data (pie chart format):")
        print(json.dumps(chart_data, indent=2))


def example_tournament_stats():
    """Example: Get player statistics by tournament"""
    print("\n" + "=" * 60)
    print("Example 3: Player Statistics by Tournament")
    print("=" * 60)
    
    client = RoundRobinClient()
    player_name = "GuangPeng Chen"
    
    tournament_stats = client.get_player_stats_by_tournament(player_name)
    
    print(f"\nTournament statistics for {player_name}:")
    print(f"{'Date':<15} {'Tournament':<40} {'Rating':<20} {'Matches Won':<12}")
    print("-" * 90)
    
    for stat in tournament_stats[:10]:  # Show first 10
        date = stat.get('tournament_date', '')
        tournament = stat.get('tournament_name', '')[:38]
        rating_pre = stat.get('rating_pre', 'N/A')
        rating_post = stat.get('rating_post', 'N/A')
        matches_won = stat.get('matches_won', 0)
        
        rating_str = f"{rating_pre} → {rating_post}"
        print(f"{date:<15} {tournament:<40} {rating_str:<20} {matches_won:<12}")


def example_all_players():
    """Example: List all players"""
    print("\n" + "=" * 60)
    print("Example 4: List All Players")
    print("=" * 60)
    
    client = RoundRobinClient()
    players = client.get_all_players()
    
    print(f"\nFound {len(players)} players:")
    for i, player in enumerate(players, 1):
        print(f"  {i}. {player.get('name')}")


def example_player_matches():
    """Example: Get recent matches for a player"""
    print("\n" + "=" * 60)
    print("Example 5: Recent Matches for Player")
    print("=" * 60)
    
    client = RoundRobinClient()
    player_name = "GuangPeng Chen"
    
    matches = client.get_player_matches(player_name, limit=10)
    
    print(f"\nRecent matches for {player_name}:")
    print(f"{'Date':<15} {'Opponent':<30} {'Score':<15} {'Result':<10}")
    print("-" * 75)
    
    for match in matches:
        date = match.get('tournament_date', '')
        opponent = match.get('player2_name') if match.get('player1_name') == player_name else match.get('player1_name')
        score1 = match.get('player1_score', 0)
        score2 = match.get('player2_score', 0)
        
        if match.get('player1_name') == player_name:
            score_str = f"{score1}-{score2}"
            result = "Win" if match.get('winner_name') == player_name else "Loss"
        else:
            score_str = f"{score2}-{score1}"
            result = "Win" if match.get('winner_name') == player_name else "Loss"
        
        print(f"{date:<15} {opponent:<30} {score_str:<15} {result:<10}")


def example_rating_trend():
    """Example: Calculate rating trend over time"""
    print("\n" + "=" * 60)
    print("Example 6: Rating Trend Analysis")
    print("=" * 60)
    
    client = RoundRobinClient()
    player_name = "GuangPeng Chen"
    
    rating_history = client.get_player_rating_history(player_name)
    
    if len(rating_history) >= 2:
        first_rating = rating_history[0].get('rating_post')
        last_rating = rating_history[-1].get('rating_post')
        total_change = last_rating - first_rating if first_rating and last_rating else 0
        
        print(f"\nRating trend for {player_name}:")
        print(f"  Starting rating: {first_rating}")
        print(f"  Current rating: {last_rating}")
        print(f"  Total change: {total_change:+d}")
        print(f"  Tournaments played: {len(rating_history)}")
        
        if len(rating_history) > 1:
            avg_change = sum(r.get('rating_change', 0) or 0 for r in rating_history) / len(rating_history)
            print(f"  Average change per tournament: {avg_change:+.2f}")


def main():
    """Run all examples"""
    try:
        example_rating_history()
        example_match_stats()
        example_tournament_stats()
        example_all_players()
        example_player_matches()
        example_rating_trend()
        
        print("\n" + "=" * 60)
        print("All examples completed!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError running examples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()

