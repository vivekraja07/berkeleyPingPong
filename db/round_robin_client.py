"""
Round Robin Database Client
Handles insertion and querying of round robin tournament data
"""
from supabase import create_client, Client
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv

load_dotenv()


class RoundRobinClient:
    """Client for interacting with round robin tournament data in Supabase"""
    
    def __init__(self):
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        if not supabase_url or not supabase_key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY must be set in environment variables or .env file"
            )
        
        self.client: Client = create_client(supabase_url, supabase_key)
        self._player_cache = {}
        self._tournament_cache = {}
        self._group_cache = {}
    
    def insert_round_robin_data(self, parsed_data: Dict) -> Dict:
        """
        Insert complete round robin tournament data
        
        Args:
            parsed_data: Dictionary from RoundRobinParser containing tournament, groups, players, and matches
            
        Returns:
            Dictionary with inserted IDs
        """
        tournament_info = parsed_data.get('tournament', {})
        groups = parsed_data.get('groups', [])
        
        # Insert tournament
        tournament_id = self._get_or_create_tournament(
            tournament_info.get('name'),
            tournament_info.get('date')
        )
        
        if not tournament_id:
            raise ValueError("Failed to create tournament")
        
        result = {
            'tournament_id': tournament_id,
            'groups': []
        }
        
        # Process each group
        for group in groups:
            group_result = self._insert_group(tournament_id, group)
            result['groups'].append(group_result)
        
        return result
    
    def _get_or_create_tournament(self, name: str, date: Optional[str]) -> Optional[int]:
        """Get or create a tournament"""
        if not name or not date:
            return None
        
        cache_key = f"{name}_{date}"
        if cache_key in self._tournament_cache:
            return self._tournament_cache[cache_key]
        
        try:
            # Try to find existing tournament
            result = self.client.table('tournaments').select('id').eq('name', name).eq('date', date).execute()
            
            if result.data:
                tournament_id = result.data[0]['id']
                self._tournament_cache[cache_key] = tournament_id
                return tournament_id
            
            # Create new tournament
            result = self.client.table('tournaments').insert({
                'name': name,
                'date': date
            }).execute()
            
            if result.data:
                tournament_id = result.data[0]['id']
                self._tournament_cache[cache_key] = tournament_id
                return tournament_id
        except Exception as e:
            print(f"Error getting/creating tournament {name}: {e}")
        
        return None
    
    def _get_or_create_player(self, name: str) -> Optional[int]:
        """Get or create a player"""
        if not name:
            return None
        
        name = name.strip()
        if name in self._player_cache:
            return self._player_cache[name]
        
        try:
            # Try to find existing player
            result = self.client.table('players').select('id').eq('name', name).execute()
            
            if result.data:
                player_id = result.data[0]['id']
                self._player_cache[name] = player_id
                return player_id
            
            # Create new player
            result = self.client.table('players').insert({'name': name}).execute()
            
            if result.data:
                player_id = result.data[0]['id']
                self._player_cache[name] = player_id
                return player_id
        except Exception as e:
            print(f"Error getting/creating player {name}: {e}")
        
        return None
    
    def _get_or_create_group(self, tournament_id: int, group_number: int, group_name: str) -> Optional[int]:
        """Get or create a round robin group"""
        cache_key = f"{tournament_id}_{group_number}"
        if cache_key in self._group_cache:
            return self._group_cache[cache_key]
        
        try:
            # Try to find existing group
            result = self.client.table('round_robin_groups').select('id').eq('tournament_id', tournament_id).eq('group_number', group_number).execute()
            
            if result.data:
                group_id = result.data[0]['id']
                self._group_cache[cache_key] = group_id
                return group_id
            
            # Create new group
            result = self.client.table('round_robin_groups').insert({
                'tournament_id': tournament_id,
                'group_number': group_number,
                'group_name': group_name
            }).execute()
            
            if result.data:
                group_id = result.data[0]['id']
                self._group_cache[cache_key] = group_id
                return group_id
        except Exception as e:
            print(f"Error getting/creating group {group_name}: {e}")
        
        return None
    
    def _insert_group(self, tournament_id: int, group: Dict) -> Dict:
        """Insert a group with all its players and matches"""
        group_number = group.get('group_number')
        group_name = group.get('group_name', f"#{group_number}")
        players = group.get('players', [])
        matches = group.get('matches', [])
        
        # Create group
        group_id = self._get_or_create_group(tournament_id, group_number, group_name)
        if not group_id:
            raise ValueError(f"Failed to create group {group_name}")
        
        result = {
            'group_id': group_id,
            'group_number': group_number,
            'players_inserted': 0,
            'matches_inserted': 0
        }
        
        # Insert players and their stats
        player_ids = {}
        stats_to_insert = []
        rating_history_to_insert = []
        
        for player in players:
            player_name = player.get('name')
            if not player_name:
                continue
            
            player_id = self._get_or_create_player(player_name)
            if not player_id:
                continue
            
            player_ids[player.get('player_number')] = player_id
            
            # Prepare player tournament stats
            stats = {
                'player_id': player_id,
                'tournament_id': tournament_id,
                'group_id': group_id,
                'player_number': player.get('player_number'),
                'rating_pre': player.get('rating_pre'),
                'rating_post': player.get('rating_post'),
                'rating_change': player.get('rating_change'),
                'matches_won': player.get('matches_won'),
                'games_won': player.get('games_won'),
                'bonus_points': player.get('bonus_points'),
                'change_w_bonus': player.get('change_w_bonus')
            }
            stats_to_insert.append(stats)
            
            # Prepare rating history
            if player.get('rating_pre') is not None or player.get('rating_post') is not None:
                rating_history = {
                    'player_id': player_id,
                    'tournament_id': tournament_id,
                    'group_id': group_id,
                    'rating_pre': player.get('rating_pre'),
                    'rating_post': player.get('rating_post'),
                    'rating_change': player.get('rating_change')
                }
                rating_history_to_insert.append(rating_history)
        
        # Insert player stats
        if stats_to_insert:
            try:
                self.client.table('player_tournament_stats').upsert(
                    stats_to_insert,
                    on_conflict='player_id,tournament_id,group_id'
                ).execute()
                result['players_inserted'] = len(stats_to_insert)
            except Exception as e:
                print(f"Error inserting player stats: {e}")
        
        # Insert rating history
        if rating_history_to_insert:
            try:
                self.client.table('player_rating_history').insert(rating_history_to_insert).execute()
            except Exception as e:
                print(f"Error inserting rating history: {e}")
        
        # Insert matches
        matches_to_insert = []
        for match in matches:
            player1_num = match.get('player1_number')
            player2_num = match.get('player2_number')
            
            if player1_num not in player_ids or player2_num not in player_ids:
                continue
            
            match_data = {
                'tournament_id': tournament_id,
                'group_id': group_id,
                'player1_id': player_ids[player1_num],
                'player2_id': player_ids[player2_num],
                'player1_score': match.get('player1_score', 0),
                'player2_score': match.get('player2_score', 0)
            }
            matches_to_insert.append(match_data)
        
        if matches_to_insert:
            try:
                self.client.table('matches').insert(matches_to_insert).execute()
                result['matches_inserted'] = len(matches_to_insert)
            except Exception as e:
                print(f"Error inserting matches: {e}")
        
        return result
    
    # ============================================================================
    # QUERY METHODS FOR CHARTS AND STATISTICS
    # ============================================================================
    
    def get_player_rating_history(self, player_name: str) -> List[Dict]:
        """Get player rating history for charting"""
        try:
            result = self.client.rpc('get_player_rating_history', {
                'player_name_param': player_name
            }).execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Error getting rating history for {player_name}: {e}")
            # Fallback to direct query
            return self._get_player_rating_history_direct(player_name)
    
    def _get_player_rating_history_direct(self, player_name: str) -> List[Dict]:
        """Direct query for player rating history"""
        try:
            result = (
                self.client.table('player_rating_chart_view')
                .select('*')
                .eq('player_name', player_name)
                .order('tournament_date', desc=False)
                .execute()
            )
            return result.data if result.data else []
        except Exception as e:
            print(f"Error in direct rating history query: {e}")
            return []
    
    def get_player_match_stats(self, player_name: str, days_back: Optional[int] = None) -> Dict:
        """Get player match statistics, optionally filtered by days_back"""
        try:
            from datetime import datetime, timedelta
            
            # Build queries for matches where player is player1
            query1 = (
                self.client.table('match_results_view')
                .select('match_id,winner_name,tournament_date,tournament_id,group_id,player1_id,player2_id,player1_name,player2_name', count='exact')
                .eq('player1_name', player_name)
            )
            
            # Build queries for matches where player is player2
            query2 = (
                self.client.table('match_results_view')
                .select('match_id,winner_name,tournament_date,tournament_id,group_id,player1_id,player2_id,player1_name,player2_name', count='exact')
                .eq('player2_name', player_name)
            )
            
            # Apply date filter if needed
            if days_back:
                cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
                query1 = query1.gte('tournament_date', cutoff_date)
                query2 = query2.gte('tournament_date', cutoff_date)
            
            # Execute both queries
            result1 = query1.execute()
            result2 = query2.execute()
            
            print(f"Match query results for {player_name}: query1={len(result1.data) if result1.data else 0}, query2={len(result2.data) if result2.data else 0}")
            
            # Combine results
            all_matches = []
            if result1.data:
                all_matches.extend(result1.data)
            if result2.data:
                all_matches.extend(result2.data)
            
            print(f"Total matches before deduplication: {len(all_matches)}")
            
            # Remove duplicates
            seen_ids = set()
            unique_matches = []
            for match in all_matches:
                match_id = match.get('match_id')
                if match_id and match_id not in seen_ids:
                    seen_ids.add(match_id)
                    unique_matches.append(match)
                elif not match_id:
                    # If no match_id, still include it (shouldn't happen but handle gracefully)
                    unique_matches.append(match)
            
            print(f"Unique matches after deduplication: {len(unique_matches)}")
            
            # Count unique tournaments (respecting days_back filter)
            unique_tournament_ids = set()
            for match in unique_matches:
                tournament_id = match.get('tournament_id')
                if tournament_id:
                    unique_tournament_ids.add(tournament_id)
            total_tournaments = len(unique_tournament_ids)
            
            # Count statistics
            all_winners = [match.get('winner_name') for match in unique_matches]
            total_matches = len(all_winners)
            wins = sum(1 for winner in all_winners if winner == player_name)
            draws = sum(1 for winner in all_winners if winner is None)
            losses = total_matches - wins - draws
            
            # Calculate win percentage
            win_percentage = 0.0
            if total_matches > 0:
                win_percentage = round((wins / total_matches) * 100, 2)
            
            # Get highest rating in timeframe from rating history
            # Use the same method as get_player_rating_history (RPC first, then fallback)
            highest_rating = None
            top_rated_win = None
            date_joined = None
            
            try:
                # Get rating history using the same method that works for the chart
                rating_history = self.get_player_rating_history(player_name)
                print(f"Rating history for {player_name}: {len(rating_history) if rating_history else 0} entries")
                
                if rating_history and len(rating_history) > 0:
                    print(f"First entry keys: {list(rating_history[0].keys())}")
                    print(f"First entry: {rating_history[0]}")
                    print(f"Sample rating_post values: {[r.get('rating_post') for r in rating_history[:5]]}")
                    
                    # Get earliest tournament date (date joined) from all history
                    # RPC returns 'date', direct query returns 'tournament_date'
                    dates = []
                    for r in rating_history:
                        date_val = r.get('tournament_date') or r.get('date')
                        if date_val:
                            dates.append(date_val)
                    if dates:
                        date_joined = min(dates)
                        print(f"Date joined: {date_joined}")
                    
                    # Filter by date if needed for highest rating
                    if days_back:
                        cutoff_date = (datetime.now() - timedelta(days=days_back))
                        filtered_history = []
                        for r in rating_history:
                            # RPC returns 'date', direct query returns 'tournament_date'
                            date_str = r.get('tournament_date') or r.get('date')
                            if date_str:
                                try:
                                    # Try parsing different date formats
                                    if isinstance(date_str, str):
                                        # Remove timezone info if present
                                        date_str_clean = date_str.split('T')[0].split(' ')[0]
                                        entry_date = datetime.strptime(date_str_clean, '%Y-%m-%d')
                                    else:
                                        entry_date = date_str
                                    
                                    if entry_date >= cutoff_date:
                                        filtered_history.append(r)
                                except Exception as e:
                                    print(f"Error parsing date {date_str}: {e}")
                                    continue
                    else:
                        filtered_history = rating_history
                    
                    # Find highest rating_post (check for None specifically, not falsy, since 0 is valid)
                    if filtered_history:
                        ratings = []
                        for r in filtered_history:
                            # Get rating_post value
                            rating_val = r.get('rating_post')
                            if rating_val is not None and rating_val != '':  # 0 is a valid rating
                                try:
                                    rating_int = int(rating_val)
                                    ratings.append(rating_int)
                                except (ValueError, TypeError):
                                    print(f"Invalid rating value: {rating_val} (type: {type(rating_val)})")
                                    continue
                        
                        if ratings:
                            highest_rating = max(ratings)
                            print(f"Found highest rating: {highest_rating} for {player_name} (from {len(ratings)} ratings)")
                        else:
                            print(f"No valid ratings found in filtered history (filtered: {len(filtered_history)} entries)")
                else:
                    print(f"No rating history found for {player_name}")
            except Exception as e:
                print(f"Error getting highest rating for {player_name}: {e}")
                import traceback
                traceback.print_exc()
            
            # Get top rated win in timeframe (highest rated opponent they beat)
            wins_in_timeframe = [m for m in unique_matches if m.get('winner_name') == player_name]
            
            top_rated_win_info = None  # Store full info: rating, opponent name, date
            
            if wins_in_timeframe:
                # Get opponent ratings for wins - limit to first 50 to avoid too many queries
                for win_match in wins_in_timeframe[:50]:
                    tournament_id = win_match.get('tournament_id')
                    group_id = win_match.get('group_id')
                    opponent_id = None
                    opponent_name = None
                    match_date = win_match.get('tournament_date')
                    
                    if win_match.get('player1_name') == player_name:
                        opponent_id = win_match.get('player2_id')
                        opponent_name = win_match.get('player2_name')
                    else:
                        opponent_id = win_match.get('player1_id')
                        opponent_name = win_match.get('player1_name')
                    
                    if tournament_id and group_id and opponent_id:
                        try:
                            opponent_stats = (
                                self.client.table('player_tournament_stats')
                                .select('rating_pre')
                                .eq('player_id', opponent_id)
                                .eq('tournament_id', tournament_id)
                                .eq('group_id', group_id)
                                .execute()
                            )
                            if opponent_stats.data and len(opponent_stats.data) > 0:
                                opponent_rating = opponent_stats.data[0].get('rating_pre')
                                if opponent_rating is not None:
                                    if top_rated_win_info is None or opponent_rating > top_rated_win_info.get('rating', 0):
                                        top_rated_win_info = {
                                            'rating': opponent_rating,
                                            'opponent_name': opponent_name,
                                            'date': match_date
                                        }
                                        top_rated_win = opponent_rating
                        except Exception as e:
                            print(f"Error getting opponent rating for top win (opponent: {opponent_name}, tournament: {tournament_id}): {e}")
                            continue
            
            # date_joined is now calculated above with rating_history
            
            # Get last match date (ever) - query both player1 and player2
            last_match_date = None
            try:
                last_match_query1 = (
                    self.client.table('match_results_view')
                    .select('tournament_date')
                    .eq('player1_name', player_name)
                    .order('tournament_date', desc=True)
                    .limit(1)
                    .execute()
                )
                last_match_query2 = (
                    self.client.table('match_results_view')
                    .select('tournament_date')
                    .eq('player2_name', player_name)
                    .order('tournament_date', desc=True)
                    .limit(1)
                    .execute()
                )
                
                dates = []
                if last_match_query1.data and len(last_match_query1.data) > 0:
                    date1 = last_match_query1.data[0].get('tournament_date')
                    if date1:
                        dates.append(date1)
                if last_match_query2.data and len(last_match_query2.data) > 0:
                    date2 = last_match_query2.data[0].get('tournament_date')
                    if date2:
                        dates.append(date2)
                
                if dates:
                    # Get the most recent date
                    last_match_date = max(dates)
            except Exception as e:
                print(f"Error getting last match date for {player_name}: {e}")
            
            result = {
                'total_matches': total_matches,
                'total_tournaments': total_tournaments,
                'wins': wins,
                'losses': losses,
                'draws': draws,
                'win_percentage': win_percentage,
                'highest_rating': highest_rating,
                'top_rated_win': top_rated_win,
                'top_rated_win_info': top_rated_win_info,  # Full info: rating, opponent_name, date
                'date_joined': date_joined,
                'last_match_date': last_match_date
            }
            print(f"Returning stats for {player_name}: highest_rating={highest_rating}, top_rated_win={top_rated_win}, top_rated_win_info={top_rated_win_info}, date_joined={date_joined}, last_match_date={last_match_date}")
            return result
        except Exception as e:
            print(f"Error getting match stats for {player_name}: {e}")
            import traceback
            traceback.print_exc()
            return {
                'total_matches': 0,
                'total_tournaments': 0,
                'wins': 0,
                'losses': 0,
                'draws': 0,
                'win_percentage': 0.0,
                'highest_rating': None,
                'top_rated_win': None,
                'top_rated_win_info': None,
                'date_joined': None,
                'last_match_date': None
            }
    
    def get_player_stats_by_tournament(self, player_name: str) -> List[Dict]:
        """Get all player statistics grouped by tournament"""
        try:
            result = (
                self.client.table('player_stats_view')
                .select('*')
                .eq('player_name', player_name)
                .order('tournament_date', desc=True)
                .execute()
            )
            return result.data if result.data else []
        except Exception as e:
            print(f"Error getting player stats: {e}")
            return []
    
    def get_all_players(self) -> List[Dict]:
        """Get all players"""
        try:
            result = self.client.table('players').select('*').order('name').execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Error getting players: {e}")
            return []
    
    def get_player_matches(self, player_name: str, limit: Optional[int] = None, days_back: Optional[int] = None) -> List[Dict]:
        """Get all matches for a player (backward compatibility)"""
        result = self.get_player_matches_paginated(player_name, page=1, page_size=limit or 100, days_back=days_back)
        return result['matches']
    
    def get_player_matches_paginated(self, player_name: str, page: int = 1, page_size: int = 20, days_back: Optional[int] = None, tournament_id: Optional[int] = None) -> Dict:
        """Get paginated matches for a player"""
        try:
            from datetime import datetime, timedelta
            import math
            
            # First, try to get matches where player is player1
            query1 = (
                self.client.table('match_results_view')
                .select('*', count='exact')
                .eq('player1_name', player_name)
            )
            
            # Then get matches where player is player2
            query2 = (
                self.client.table('match_results_view')
                .select('*', count='exact')
                .eq('player2_name', player_name)
            )
            
            # Apply tournament filter if needed
            if tournament_id:
                query1 = query1.eq('tournament_id', tournament_id)
                query2 = query2.eq('tournament_id', tournament_id)
            
            # Apply date filter if needed
            if days_back:
                cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
                query1 = query1.gte('tournament_date', cutoff_date)
                query2 = query2.gte('tournament_date', cutoff_date)
            
            # Execute both queries
            result1 = query1.execute()
            result2 = query2.execute()
            
            # Combine results
            all_matches = []
            if result1.data:
                all_matches.extend(result1.data)
            if result2.data:
                all_matches.extend(result2.data)
            
            # Remove duplicates (in case of any edge cases)
            seen_ids = set()
            unique_matches = []
            for match in all_matches:
                match_id = match.get('match_id')
                if match_id and match_id not in seen_ids:
                    seen_ids.add(match_id)
                    unique_matches.append(match)
            
            # Sort by tournament date descending
            unique_matches.sort(key=lambda x: x.get('tournament_date', ''), reverse=True)
            
            # Calculate pagination
            total = len(unique_matches)
            total_pages = math.ceil(total / page_size) if page_size > 0 else 1
            
            # Apply pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            paginated_matches = unique_matches[start_idx:end_idx]
            
            # Get ratings for each match from player_tournament_stats
            for match in paginated_matches:
                tournament_id = match.get('tournament_id')
                group_id = match.get('group_id')
                player1_id = match.get('player1_id')
                player2_id = match.get('player2_id')
                
                if tournament_id and group_id and player1_id and player2_id:
                    try:
                        # Get player1 rating_pre for this tournament/group
                        stats1 = (
                            self.client.table('player_tournament_stats')
                            .select('rating_pre')
                            .eq('player_id', player1_id)
                            .eq('tournament_id', tournament_id)
                            .eq('group_id', group_id)
                            .execute()
                        )
                        if stats1.data and len(stats1.data) > 0:
                            match['player1_rating'] = stats1.data[0].get('rating_pre')
                        
                        # Get player2 rating_pre for this tournament/group
                        stats2 = (
                            self.client.table('player_tournament_stats')
                            .select('rating_pre')
                            .eq('player_id', player2_id)
                            .eq('tournament_id', tournament_id)
                            .eq('group_id', group_id)
                            .execute()
                        )
                        if stats2.data and len(stats2.data) > 0:
                            match['player2_rating'] = stats2.data[0].get('rating_pre')
                    except Exception as e:
                        print(f"Error getting ratings for match {match.get('match_id')}: {e}")
                        # Continue without ratings if there's an error
            
            return {
                'matches': paginated_matches,
                'total': total,
                'total_pages': total_pages,
                'page': page,
                'page_size': page_size
            }
        except Exception as e:
            print(f"Error getting matches for {player_name}: {e}")
            import traceback
            traceback.print_exc()
            return {
                'matches': [],
                'total': 0,
                'total_pages': 0,
                'page': page,
                'page_size': page_size
            }
    
    def get_player_tournaments(self, player_name: str) -> List[Dict]:
        """Get all tournaments that a player has participated in"""
        try:
            # Get unique tournaments from player_tournament_stats
            result = (
                self.client.table('player_stats_view')
                .select('tournament_id,tournament_name,tournament_date')
                .eq('player_name', player_name)
                .order('tournament_date', desc=True)
                .execute()
            )
            
            if not result.data:
                return []
            
            # Get unique tournaments
            seen_tournaments = {}
            tournaments = []
            for stat in result.data:
                tournament_id = stat.get('tournament_id')
                if tournament_id and tournament_id not in seen_tournaments:
                    seen_tournaments[tournament_id] = True
                    tournaments.append({
                        'tournament_id': tournament_id,
                        'tournament_name': stat.get('tournament_name'),
                        'tournament_date': stat.get('tournament_date')
                    })
            
            return tournaments
        except Exception as e:
            print(f"Error getting tournaments for {player_name}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_head_to_head_matches(self, player1_name: str, player2_name: str) -> List[Dict]:
        """Get all matches between two players with ratings at match time"""
        try:
            # Get matches where player1 is player1_name and player2 is player2_name
            query1 = (
                self.client.table('match_results_view')
                .select('*')
                .eq('player1_name', player1_name)
                .eq('player2_name', player2_name)
                .order('tournament_date', desc=True)
            )
            
            # Get matches where player1 is player2_name and player2 is player1_name
            query2 = (
                self.client.table('match_results_view')
                .select('*')
                .eq('player1_name', player2_name)
                .eq('player2_name', player1_name)
                .order('tournament_date', desc=True)
            )
            
            # Execute both queries
            result1 = query1.execute()
            result2 = query2.execute()
            
            # Combine results
            all_matches = []
            if result1.data:
                all_matches.extend(result1.data)
            if result2.data:
                all_matches.extend(result2.data)
            
            # Remove duplicates (in case of any edge cases)
            seen_ids = set()
            unique_matches = []
            for match in all_matches:
                match_id = match.get('match_id')
                if match_id and match_id not in seen_ids:
                    seen_ids.add(match_id)
                    unique_matches.append(match)
            
            # Get ratings for each match from player_tournament_stats
            for match in unique_matches:
                tournament_id = match.get('tournament_id')
                group_id = match.get('group_id')
                player1_id = match.get('player1_id')
                player2_id = match.get('player2_id')
                
                if tournament_id and group_id and player1_id and player2_id:
                    try:
                        # Get player1 rating stats for this tournament/group
                        stats1 = (
                            self.client.table('player_tournament_stats')
                            .select('rating_pre,rating_post,rating_change')
                            .eq('player_id', player1_id)
                            .eq('tournament_id', tournament_id)
                            .eq('group_id', group_id)
                            .execute()
                        )
                        if stats1.data and len(stats1.data) > 0:
                            match['player1_rating'] = stats1.data[0].get('rating_pre')
                            match['player1_rating_post'] = stats1.data[0].get('rating_post')
                            match['player1_rating_change'] = stats1.data[0].get('rating_change')
                        
                        # Get player2 rating stats for this tournament/group
                        stats2 = (
                            self.client.table('player_tournament_stats')
                            .select('rating_pre,rating_post,rating_change')
                            .eq('player_id', player2_id)
                            .eq('tournament_id', tournament_id)
                            .eq('group_id', group_id)
                            .execute()
                        )
                        if stats2.data and len(stats2.data) > 0:
                            match['player2_rating'] = stats2.data[0].get('rating_pre')
                            match['player2_rating_post'] = stats2.data[0].get('rating_post')
                            match['player2_rating_change'] = stats2.data[0].get('rating_change')
                    except Exception as e:
                        print(f"Error getting ratings for match {match_id}: {e}")
                        # Continue without ratings if there's an error
            
            # Sort by tournament date descending
            unique_matches.sort(key=lambda x: x.get('tournament_date', ''), reverse=True)
            
            return unique_matches
        except Exception as e:
            print(f"Error getting head-to-head matches for {player1_name} vs {player2_name}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_opponents(self, player_name: str) -> List[Dict]:
        """Get all opponents that a player has played against, with match counts"""
        try:
            # Get matches where player is player1 - only select opponent names
            query1 = (
                self.client.table('match_results_view')
                .select('player2_name', count='exact')
                .eq('player1_name', player_name)
            )
            
            # Get matches where player is player2 - only select opponent names
            query2 = (
                self.client.table('match_results_view')
                .select('player1_name', count='exact')
                .eq('player2_name', player_name)
            )
            
            # Execute both queries
            result1 = query1.execute()
            result2 = query2.execute()
            
            # Count matches per opponent
            opponent_counts = {}
            
            # Count from player1 matches (opponent is player2)
            if result1.data:
                for match in result1.data:
                    opponent = match.get('player2_name')
                    if opponent and opponent != player_name:
                        if opponent not in opponent_counts:
                            opponent_counts[opponent] = 0
                        opponent_counts[opponent] += 1
            
            # Count from player2 matches (opponent is player1)
            if result2.data:
                for match in result2.data:
                    opponent = match.get('player1_name')
                    if opponent and opponent != player_name:
                        if opponent not in opponent_counts:
                            opponent_counts[opponent] = 0
                        opponent_counts[opponent] += 1
            
            # Convert to list of dicts with name and match_count, sorted by match count descending
            opponents = [
                {'name': name, 'match_count': count}
                for name, count in opponent_counts.items()
            ]
            opponents.sort(key=lambda x: x['match_count'], reverse=True)
            
            return opponents
        except Exception as e:
            print(f"Error getting opponents for {player_name}: {e}")
            import traceback
            traceback.print_exc()
            return []

