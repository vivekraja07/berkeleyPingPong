"""
Round Robin Database Client
Handles insertion and querying of round robin tournament data
"""
from supabase import create_client, Client
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv

# Only load environment variables from a local .env file if they haven't 
# already been set by the CI/CD pipeline or system environment.
# if not os.getenv('SUPABASE_URL') or not os.getenv('SUPABASE_KEY'):
#     # This line will only run if the variables aren't already available
#     # (i.e., when running locally without exporting the vars)
#     load_dotenv()



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
    
    def insert_round_robin_data(self, parsed_data: Dict, source_url: Optional[str] = None, 
                                parsing_status: str = 'success', parse_error: Optional[str] = None) -> Dict:
        """
        Insert complete round robin tournament data
        
        Args:
            parsed_data: Dictionary from RoundRobinParser containing tournament, groups, players, and matches
            source_url: Optional URL where the tournament data was extracted from
            parsing_status: Parsing status ('success', 'parsing_failed', 'validation_failed', 'db_error')
            parse_error: Optional error message if parsing failed
            
        Returns:
            Dictionary with inserted IDs
        """
        tournament_info = parsed_data.get('tournament', {})
        groups = parsed_data.get('groups', [])
        
        # Insert tournament
        tournament_id = self._get_or_create_tournament(
            tournament_info.get('name'),
            tournament_info.get('date'),
            source_url=source_url,
            parsing_status=parsing_status,
            parse_error=parse_error
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
        
        # Refresh the materialized views after successful import
        try:
            self.refresh_player_rankings_view()
            print(f"Refreshed player rankings view after importing tournament {tournament_id}")
        except Exception as e:
            # Log but don't fail the import if refresh fails
            print(f"Warning: Could not refresh player rankings view: {e}")
            # Don't raise - import was successful, view refresh is optional
        
        try:
            self.refresh_player_match_stats_view()
            print(f"Refreshed player match stats view after importing tournament {tournament_id}")
        except Exception as e:
            # Log but don't fail the import if refresh fails
            print(f"Warning: Could not refresh player match stats view: {e}")
            # Don't raise - import was successful, view refresh is optional
        
        return result
    
    def _get_or_create_tournament(self, name: str, date: Optional[str], source_url: Optional[str] = None, 
                                  parsing_status: Optional[str] = None, parse_error: Optional[str] = None) -> Optional[int]:
        """Get or create a tournament (thread-safe with race condition handling)
        Note: name parameter is kept for backward compatibility but is no longer used
        
        Args:
            name: Tournament name (kept for backward compatibility)
            date: Tournament date (required)
            source_url: Optional URL where tournament data was extracted from
            parsing_status: Optional parsing status ('success', 'parsing_failed', 'validation_failed', 'db_error')
            parse_error: Optional error message if parsing failed
        """
        if not date:
            return None
        
        # Use date as cache key (name is no longer used)
        cache_key = f"tournament_{date}"
        if cache_key in self._tournament_cache:
            # Update URL and parsing status if provided
            update_data = {}
            if source_url:
                update_data['source_url'] = source_url
            if parsing_status:
                update_data['parsing_status'] = parsing_status
            if parse_error:
                update_data['parse_error'] = parse_error
            if update_data:
                try:
                    self.client.table('tournaments').update(update_data).eq('id', self._tournament_cache[cache_key]).execute()
                except Exception:
                    pass  # Ignore update errors
            return self._tournament_cache[cache_key]
        
        try:
            # Try to find existing tournament by date only
            result = self.client.table('tournaments').select('id').eq('date', date).execute()
            
            if result.data:
                tournament_id = result.data[0]['id']
                # Update URL and parsing status if provided
                update_data = {}
                if source_url:
                    update_data['source_url'] = source_url
                if parsing_status:
                    update_data['parsing_status'] = parsing_status
                if parse_error:
                    update_data['parse_error'] = parse_error
                if update_data:
                    try:
                        self.client.table('tournaments').update(update_data).eq('id', tournament_id).execute()
                    except Exception:
                        pass  # Ignore update errors
                self._tournament_cache[cache_key] = tournament_id
                return tournament_id
            
            # Create new tournament (with name, URL, and parsing status if provided)
            insert_data = {'date': date}
            if name:
                insert_data['name'] = name
            if source_url:
                insert_data['source_url'] = source_url
            if parsing_status:
                insert_data['parsing_status'] = parsing_status
            if parse_error:
                insert_data['parse_error'] = parse_error
            result = self.client.table('tournaments').insert(insert_data).execute()
            
            if result.data:
                tournament_id = result.data[0]['id']
                self._tournament_cache[cache_key] = tournament_id
                return tournament_id
        except Exception as e:
            # Handle race condition: if another thread created the tournament between our check and insert
            error_dict = e.__dict__ if hasattr(e, '__dict__') else {}
            error_code = error_dict.get('code') or str(e)
            
            # Check if it's a duplicate key error (PostgreSQL error code 23505)
            if '23505' in str(error_code) or 'duplicate key' in str(e).lower() or 'already exists' in str(e).lower():
                # Another thread created it, just fetch it
                try:
                    result = self.client.table('tournaments').select('id').eq('date', date).execute()
                    if result.data:
                        tournament_id = result.data[0]['id']
                        self._tournament_cache[cache_key] = tournament_id
                        return tournament_id
                except Exception as fetch_error:
                    print(f"Error fetching tournament for date {date} after duplicate key error: {fetch_error}")
            else:
                print(f"Error getting/creating tournament for date {date}: {e}")
        
        return None
    
    def _get_or_create_player(self, name: str) -> Optional[int]:
        """
        Get or create a player (thread-safe with race condition handling)
        
        Handles concurrent inserts by multiple threads:
        1. Check cache (per-instance, so each thread has its own)
        2. Check database for existing player
        3. If not found, try to insert
        4. If insert fails with duplicate key error, another thread created it - fetch it
        5. Database unique constraint on player name prevents actual duplicates
        """
        if not name:
            return None
        
        name = name.strip()
        if name in self._player_cache:
            return self._player_cache[name]
        
        # Retry loop to handle race conditions
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Try to find existing player first
                result = self.client.table('players').select('id').eq('name', name).execute()
                
                if result.data:
                    player_id = result.data[0]['id']
                    self._player_cache[name] = player_id
                    return player_id
                
                # Not found, try to create
                result = self.client.table('players').insert({'name': name}).execute()
                
                if result.data:
                    player_id = result.data[0]['id']
                    self._player_cache[name] = player_id
                    return player_id
                    
            except Exception as e:
                # Handle race condition: if another thread created the player between our check and insert
                error_str = str(e).lower()
                error_dict = e.__dict__ if hasattr(e, '__dict__') else {}
                error_code = error_dict.get('code') or str(e)
                
                # Check if it's a duplicate key error (PostgreSQL error code 23505)
                # Also check for Supabase-specific error messages
                is_duplicate = (
                    '23505' in str(error_code) or 
                    'duplicate key' in error_str or 
                    'already exists' in error_str or
                    'unique constraint' in error_str or
                    'duplicate' in error_str
                )
                
                if is_duplicate:
                    # Another thread created it, fetch it
                    try:
                        result = self.client.table('players').select('id').eq('name', name).execute()
                        if result.data:
                            player_id = result.data[0]['id']
                            self._player_cache[name] = player_id
                            return player_id
                    except Exception as fetch_error:
                        # If fetch fails, retry the whole operation
                        if attempt < max_retries - 1:
                            continue
                        print(f"Error fetching player {name} after duplicate key error: {fetch_error}")
                else:
                    # Not a duplicate error, something else went wrong
                    if attempt < max_retries - 1:
                        continue  # Retry
                    print(f"Error getting/creating player {name}: {e}")
        
        return None
    
    def _get_or_create_group(self, tournament_id: int, group_number: int, group_name: str) -> Optional[int]:
        """
        Get or create a round robin group (thread-safe with race condition handling)
        
        Handles concurrent inserts by multiple threads using the same pattern as _get_or_create_player
        """
        cache_key = f"{tournament_id}_{group_number}"
        if cache_key in self._group_cache:
            return self._group_cache[cache_key]
        
        # Retry loop to handle race conditions
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Try to find existing group first
                result = self.client.table('round_robin_groups').select('id').eq('tournament_id', tournament_id).eq('group_number', group_number).execute()
                
                if result.data:
                    group_id = result.data[0]['id']
                    self._group_cache[cache_key] = group_id
                    return group_id
                
                # Not found, try to create
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
                # Handle race condition: if another thread created the group between our check and insert
                error_str = str(e).lower()
                error_dict = e.__dict__ if hasattr(e, '__dict__') else {}
                error_code = error_dict.get('code') or str(e)
                
                # Check if it's a duplicate key error
                is_duplicate = (
                    '23505' in str(error_code) or 
                    'duplicate key' in error_str or 
                    'already exists' in error_str or
                    'unique constraint' in error_str or
                    'duplicate' in error_str
                )
                
                if is_duplicate:
                    # Another thread created it, fetch it
                    try:
                        result = self.client.table('round_robin_groups').select('id').eq('tournament_id', tournament_id).eq('group_number', group_number).execute()
                        if result.data:
                            group_id = result.data[0]['id']
                            self._group_cache[cache_key] = group_id
                            return group_id
                    except Exception as fetch_error:
                        # If fetch fails, retry the whole operation
                        if attempt < max_retries - 1:
                            continue
                        print(f"Error fetching group {group_name} after duplicate key error: {fetch_error}")
                else:
                    # Not a duplicate error, something else went wrong
                    if attempt < max_retries - 1:
                        continue  # Retry
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
                raise ValueError(f"Group {group_name}: Player is missing name (player_number: {player.get('player_number')})")
            
            player_id = self._get_or_create_player(player_name)
            if not player_id:
                raise ValueError(f"Group {group_name}: Failed to create/get player '{player_name}'")
            
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
                raise ValueError(f"Group {group_name}: Error inserting player stats: {e}")
        
        # Insert rating history (check for duplicates first)
        if rating_history_to_insert:
            try:
                # Batch check for existing rating history entries to avoid duplicates
                # Get unique (player_id, tournament_id, group_id) combinations
                entry_keys = {
                    (entry['player_id'], entry['tournament_id'], entry.get('group_id'))
                    for entry in rating_history_to_insert
                }
                
                existing_keys = set()
                # Query existing entries for this tournament/group combination
                if entry_keys:
                    # Get tournament_id and group_id from first entry (all should be same)
                    sample_entry = rating_history_to_insert[0]
                    tournament_id = sample_entry['tournament_id']
                    group_id = sample_entry.get('group_id')
                    
                    # Query all rating history for this tournament/group
                    check_result = (
                        self.client.table('player_rating_history')
                        .select('player_id,tournament_id,group_id')
                        .eq('tournament_id', tournament_id)
                        .eq('group_id', group_id)
                        .execute()
                    )
                    
                    if check_result.data:
                        existing_keys = {
                            (entry['player_id'], entry['tournament_id'], entry.get('group_id'))
                            for entry in check_result.data
                        }
                
                # Only insert new entries
                new_entries = [
                    entry for entry in rating_history_to_insert
                    if (entry['player_id'], entry['tournament_id'], entry.get('group_id')) not in existing_keys
                ]
                
                if new_entries:
                    self.client.table('player_rating_history').insert(new_entries).execute()
            except Exception as e:
                raise ValueError(f"Group {group_name}: Error inserting rating history: {e}")
        
        # Insert matches
        matches_to_insert = []
        for match in matches:
            player1_num = match.get('player1_number')
            player2_num = match.get('player2_number')
            
            if player1_num not in player_ids:
                raise ValueError(f"Group {group_name}: Match references player1_number {player1_num} which doesn't exist")
            if player2_num not in player_ids:
                raise ValueError(f"Group {group_name}: Match references player2_number {player2_num} which doesn't exist")
            
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
                # Batch check for existing matches to avoid duplicates
                # Query all existing matches for this tournament/group
                sample_match = matches_to_insert[0]
                tournament_id = sample_match['tournament_id']
                group_id = sample_match.get('group_id')
                
                existing_matches = set()
                check_result = (
                    self.client.table('matches')
                    .select('player1_id,player2_id')
                    .eq('tournament_id', tournament_id)
                    .eq('group_id', group_id)
                    .execute()
                )
                
                if check_result.data:
                    existing_matches = {
                        (m['player1_id'], m['player2_id'])
                        for m in check_result.data
                    }
                
                # Only insert new matches
                new_matches = [
                    m for m in matches_to_insert
                    if (m['player1_id'], m['player2_id']) not in existing_matches
                ]
                
                duplicates_skipped = len(matches_to_insert) - len(new_matches)
                
                if new_matches:
                    insert_result = self.client.table('matches').insert(new_matches).execute()
                    # Verify the insert actually worked
                    if insert_result.data:
                        result['matches_inserted'] = len(insert_result.data)
                    else:
                        result['matches_inserted'] = 0
                else:
                    result['matches_inserted'] = 0
                
                # Only raise error if we tried to insert new matches but got fewer back
                # (duplicates being skipped is expected and fine)
                if new_matches and result['matches_inserted'] != len(new_matches):
                    raise ValueError(f"Group {group_name}: Match insertion failed - tried to insert {len(new_matches)} new matches, but only {result['matches_inserted']} were inserted")
                
                # Log if duplicates were skipped (only in debug mode to reduce noise)
                # Duplicates are expected when re-importing or when tournament already exists
                # if duplicates_skipped > 0:
                #     print(f"  ℹ️  Group {group_name}: Skipped {duplicates_skipped} duplicate matches")
            except Exception as e:
                raise ValueError(f"Group {group_name}: Error inserting matches: {e}")
        else:
            result['matches_inserted'] = 0
        
        # Verify: total matches should equal inserted + duplicates (if any)
        # But we don't raise an error if duplicates were skipped - that's expected
        total_expected = len(matches)
        total_inserted = result['matches_inserted']
        
        # Only raise error if we expected to insert matches but got none
        # (This catches actual insertion failures, not duplicate skips)
        if total_expected > 0 and total_inserted == 0 and matches_to_insert:
            # Check if all were duplicates
            try:
                sample_match = matches_to_insert[0]
                tournament_id = sample_match['tournament_id']
                group_id = sample_match.get('group_id')
                check_result = (
                    self.client.table('matches')
                    .select('id')
                    .eq('tournament_id', tournament_id)
                    .eq('group_id', group_id)
                    .execute()
                )
                existing_count = len(check_result.data) if check_result.data else 0
                
                # If we have existing matches, they might all be duplicates - that's fine
                if existing_count >= total_expected:
                    # Don't log - duplicates are expected when re-importing
                    # print(f"  ℹ️  Group {group_name}: All {total_expected} matches already exist (duplicates)")
                    result['matches_inserted'] = total_expected  # Treat as success
                else:
                    raise ValueError(f"Group {group_name}: Match insertion failed - expected {total_expected} matches, but 0 were inserted and only {existing_count} exist in DB")
            except ValueError:
                raise  # Re-raise ValueError
            except Exception:
                # If we can't check, assume it's a real failure
                raise ValueError(f"Group {group_name}: Match insertion failed - expected {total_expected} matches, but 0 were inserted")
        
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
    
    def get_player_ranking_and_percentile(self, player_name: str, active_days: int = 365) -> Dict:
        """Get player ranking and percentile based on current rating among active players"""
        try:
            from datetime import datetime, timedelta
            
            # Get cutoff date for active players (last year)
            cutoff_date = (datetime.now() - timedelta(days=active_days)).strftime('%Y-%m-%d')
            
            # Get all players who have played in the last year
            # Include players from BOTH match_results_view AND rating history (same logic as get_all_players_with_rankings)
            active_player_names = set()
            
            # Method 1: Get active players from matches
            active_players_query = (
                self.client.table('match_results_view')
                .select('player1_name,player2_name,tournament_date')
                .gte('tournament_date', cutoff_date)
                .execute()
            )
            
            if active_players_query.data:
                for match in active_players_query.data:
                    if match.get('player1_name'):
                        active_player_names.add(match['player1_name'])
                    if match.get('player2_name'):
                        active_player_names.add(match['player2_name'])
            
            # Method 2: Also include players who have rating history in the last year
            # This catches players like Vin Reddy who have ratings but no match records
            try:
                # Get all players first to query their rating history
                all_players = self.get_all_players()
                all_player_names = [p.get('name') for p in all_players if p.get('name')]
                
                # Batch query rating history (Supabase .in_() limit is ~100)
                batch_size = 100
                all_rating_history_data = []
                
                for i in range(0, len(all_player_names), batch_size):
                    batch_names = all_player_names[i:i + batch_size]
                    try:
                        batch_result = (
                            self.client.table('player_rating_chart_view')
                            .select('player_name,tournament_date,rating_post,rating_pre')
                            .in_('player_name', batch_names)
                            .order('tournament_date', desc=True)
                            .execute()
                        )
                        if batch_result.data:
                            all_rating_history_data.extend(batch_result.data)
                    except Exception as e:
                        print(f"Error fetching rating history batch in get_player_ranking_and_percentile: {e}")
                        continue
                
                # Add players with rating history in the last year to active players
                if all_rating_history_data:
                    cutoff_date_obj = datetime.strptime(cutoff_date, '%Y-%m-%d')
                    for entry in all_rating_history_data:
                        p_name = entry.get('player_name')
                        entry_date = entry.get('tournament_date')
                        
                        if p_name and entry_date:
                            try:
                                if isinstance(entry_date, str):
                                    entry_date_obj = datetime.strptime(entry_date.split('T')[0], '%Y-%m-%d')
                                else:
                                    entry_date_obj = entry_date
                                
                                if entry_date_obj >= cutoff_date_obj:
                                    active_player_names.add(p_name)
                            except Exception as e:
                                continue
            except Exception as e:
                print(f"Error fetching rating history for active players: {e}")
            
            if not active_player_names:
                return {'rank': None, 'total_players': 0, 'players_better_than': None}
            
            # OPTIMIZED: Get all rating history entries for active players in batches
            # Then process in memory to get the latest rating for each player
            player_ratings = {}
            try:
                # Batch query rating history (Supabase .in_() limit is ~100)
                batch_size = 100
                active_player_list = list(active_player_names)
                all_rating_history_data = []
                
                for i in range(0, len(active_player_list), batch_size):
                    batch_names = active_player_list[i:i + batch_size]
                    try:
                        batch_result = (
                            self.client.table('player_rating_chart_view')
                            .select('player_name,tournament_date,rating_post,rating_pre')
                            .in_('player_name', batch_names)
                            .order('tournament_date', desc=True)
                            .execute()
                        )
                        if batch_result.data:
                            all_rating_history_data.extend(batch_result.data)
                    except Exception as e:
                        print(f"Error batch fetching rating history: {e}")
                        continue
                
                # Sort by date descending (most recent first)
                all_rating_history_data.sort(key=lambda x: (
                    x.get('tournament_date') or '',
                    x.get('player_name') or ''
                ), reverse=True)
                
                if all_rating_history_data:
                    # Group by player and get the most recent rating_post (or rating_pre as fallback)
                    seen_players = set()
                    for entry in all_rating_history_data:
                        p_name = entry.get('player_name')
                        if not p_name or p_name in seen_players:
                            continue
                        
                        # Try rating_post first, fallback to rating_pre if rating_post is missing
                        rating_post = entry.get('rating_post')
                        rating_pre = entry.get('rating_pre')
                        rating_to_use = None
                        
                        if rating_post is not None and rating_post != '':
                            rating_to_use = rating_post
                        elif rating_pre is not None and rating_pre != '':
                            rating_to_use = rating_pre
                        
                        if rating_to_use is not None:
                            try:
                                rating_int = int(rating_to_use)
                                player_ratings[p_name] = rating_int
                                seen_players.add(p_name)
                            except (ValueError, TypeError):
                                continue
            except Exception as e:
                print(f"Error batch fetching rating history: {e}")
                return {'rank': None, 'total_players': 0, 'players_better_than': None}
            
            if not player_ratings:
                return {'rank': None, 'total_players': 0, 'players_better_than': None}
            
            # Get current player's rating
            current_player_rating = player_ratings.get(player_name)
            if current_player_rating is None:
                return {'rank': None, 'total_players': len(player_ratings), 'players_better_than': None}
            
            # Sort players by rating (descending)
            sorted_players = sorted(player_ratings.items(), key=lambda x: x[1], reverse=True)
            
            # Find player's rank (1-indexed)
            rank = None
            for idx, (name, rating) in enumerate(sorted_players, start=1):
                if name == player_name:
                    rank = idx
                    break
            
            if rank is None:
                return {'rank': None, 'total_players': len(player_ratings), 'players_better_than': None}
            
            # Calculate how many players this player is better than
            players_better_than = rank - 1
            total_players = len(sorted_players)
            
            return {
                'rank': rank,
                'total_players': total_players,
                'players_better_than': players_better_than
            }
        except Exception as e:
            print(f"Error getting ranking and percentile for {player_name}: {e}")
            import traceback
            traceback.print_exc()
            return {'rank': None, 'total_players': 0, 'players_better_than': None}
    
    def _get_player_rating_history_direct(self, player_name: str) -> List[Dict]:
        """Direct query for player rating history"""
        try:
            # Try exact match first
            result = (
                self.client.table('player_rating_chart_view')
                .select('*')
                .eq('player_name', player_name)
                .order('tournament_date', desc=False)
                .execute()
            )
            
            # If no results, try case-insensitive search (for debugging)
            if not result.data or len(result.data) == 0:
                print(f"Warning: No rating history found for exact match '{player_name}'")
                # Try case-insensitive search to see if there's a name mismatch
                try:
                    all_players = (
                        self.client.table('player_rating_chart_view')
                        .select('player_name')
                        .ilike('player_name', f'%{player_name.split()[0] if player_name.split() else player_name}%')
                        .limit(10)
                        .execute()
                    )
                    if all_players.data:
                        unique_names = list(set([p.get('player_name') for p in all_players.data]))
                        print(f"Found similar player names: {unique_names[:5]}")
                except Exception:
                    pass
            
            return result.data if result.data else []
        except Exception as e:
            print(f"Error in direct rating history query: {e}")
            return []
    
    def get_player_match_stats(self, player_name: str, days_back: Optional[int] = None, 
                                include_top_rated_win: bool = False, rating_history: Optional[List[Dict]] = None) -> Dict:
        """Get player match statistics, optionally filtered by days_back
        
        Uses materialized view for base stats if available, falls back to old method.
        For date-filtered stats (days_back), still queries match_results_view.
        """
        try:
            from datetime import datetime, timedelta
            
            # Try to get base stats from materialized view first (fast)
            base_stats = None
            try:
                result = (
                    self.client.table('player_match_stats_view')
                    .select('*')
                    .eq('player_name', player_name)
                    .execute()
                )
                if result.data and len(result.data) > 0:
                    base_stats = result.data[0]
                    print(f"Using materialized view for {player_name} base stats")
            except Exception as e:
                print(f"Could not use materialized view for {player_name}: {e}")
                base_stats = None
            
            # If we have days_back filter, we need to query matches for filtered stats
            # Otherwise, we can use the materialized view stats directly
            if days_back is None and base_stats is not None:
                # No date filter, use materialized view stats directly
                # Also get ranking from player_rankings_view
                ranking_info = None
                try:
                    ranking_result = (
                        self.client.table('player_rankings_view')
                        .select('ranking,current_rating')
                        .eq('player_name', player_name)
                        .execute()
                    )
                    if ranking_result.data and len(ranking_result.data) > 0:
                        ranking_data = ranking_result.data[0]
                        ranking = ranking_data.get('ranking')
                        # Get total active players for percentile calculation
                        if ranking is not None:
                            # Count total active players with rankings
                            total_active = (
                                self.client.table('player_rankings_view')
                                .select('player_id', count='exact')
                                .not_.is_('ranking', 'null')
                                .execute()
                            )
                            total_players = total_active.count if hasattr(total_active, 'count') and total_active.count is not None else None
                            ranking_info = {
                                'rank': ranking,
                                'total_players': total_players,
                                'players_better_than': ranking - 1 if ranking and total_players else None
                            }
                except Exception as e:
                    print(f"Error getting ranking from view for {player_name}: {e}")
                    ranking_info = None
                
                result = {
                    'total_matches': base_stats.get('total_matches', 0),
                    'wins': base_stats.get('total_wins', 0),
                    'losses': base_stats.get('total_losses', 0),
                    'draws': base_stats.get('total_draws', 0),
                    'win_percentage': float(base_stats.get('win_percentage', 0)),
                    'total_tournaments': base_stats.get('total_tournaments', 0),
                    'highest_rating': base_stats.get('highest_rating'),
                    'date_joined': str(base_stats.get('date_joined')) if base_stats.get('date_joined') else None,
                    'last_match_date': str(base_stats.get('last_tournament_date')) if base_stats.get('last_tournament_date') else None,
                    'ranking': ranking_info.get('rank') if ranking_info else None,
                    'total_players': ranking_info.get('total_players') if ranking_info else None,
                    'players_better_than': ranking_info.get('players_better_than') if ranking_info else None,
                    'top_rated_win': None,
                    'top_rated_win_info': None
                }
                
                # If top_rated_win is requested, we still need to calculate it
                # (it's expensive and not in the materialized view, so fall through to old method)
                if include_top_rated_win:
                    # Fall through to old method for top_rated_win calculation
                    base_stats = None  # Force use of old method
                else:
                    return result
            
            # For date-filtered stats or if view unavailable, use old method
            # Get matches where player is player1
            query1 = (
                self.client.table('match_results_view')
                .select('match_id,winner_name,tournament_date,tournament_id,group_id,player1_id,player2_id,player1_name,player2_name')
                .eq('player1_name', player_name)
            )
            
            # Get matches where player is player2
            query2 = (
                self.client.table('match_results_view')
                .select('match_id,winner_name,tournament_date,tournament_id,group_id,player1_id,player2_id,player1_name,player2_name')
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
            
            # Combine results
            all_matches = []
            if result1.data:
                all_matches.extend(result1.data)
            if result2.data:
                all_matches.extend(result2.data)
            
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
            
            # Calculate win percentage (excluding draws from denominator)
            win_percentage = 0.0
            non_draw_matches = wins + losses
            if non_draw_matches > 0:
                win_percentage = round((wins / non_draw_matches) * 100, 2)
            
            # Get highest rating in timeframe from rating history
            # OPTIMIZED: Use provided rating_history if available, otherwise fetch
            highest_rating = None
            top_rated_win = None
            date_joined = None
            
            try:
                # Use provided rating_history or fetch if not provided
                if rating_history is None:
                    rating_history = self.get_player_rating_history(player_name)
                
                if rating_history and len(rating_history) > 0:
                    # Get earliest tournament date (date joined) from all history
                    # RPC returns 'date', direct query returns 'tournament_date'
                    dates = []
                    for r in rating_history:
                        date_val = r.get('tournament_date') or r.get('date')
                        if date_val:
                            dates.append(date_val)
                    if dates:
                        date_joined = min(dates)
                    
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
            except Exception as e:
                print(f"Error getting highest rating for {player_name}: {e}")
            
            # Get top rated win in timeframe (highest rated opponent they beat)
            # OPTIMIZED: Skip this expensive calculation unless explicitly requested
            top_rated_win_info = None  # Store full info: rating, opponent name, date
            top_rated_win = None
            
            if include_top_rated_win:
                wins_in_timeframe = [m for m in unique_matches if m.get('winner_name') == player_name]
                
                if wins_in_timeframe:
                    # Collect all win matches with their opponent info first
                    win_matches_with_opponents = []
                    unique_opponent_keys = set()  # Track unique (opponent_id, tournament_id, group_id) combinations
                    
                    for win_match in wins_in_timeframe:
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
                            key = (opponent_id, tournament_id, group_id)
                            if key not in unique_opponent_keys:
                                unique_opponent_keys.add(key)
                                win_matches_with_opponents.append({
                                    'tournament_id': tournament_id,
                                    'group_id': group_id,
                                    'opponent_id': opponent_id,
                                    'opponent_name': opponent_name,
                                    'match_date': match_date
                                })
                    
                    # OPTIMIZED: Fetch all opponent ratings in minimal queries
                    # Strategy: Fetch all stats for all tournaments involved, then filter in memory
                    if win_matches_with_opponents:
                        # Build a lookup map: (opponent_id, tournament_id, group_id) -> rating
                        rating_lookup = {}
                        
                        # Create a set of all unique (opponent_id, tournament_id, group_id) tuples we need
                        needed_keys = set()
                        for w in win_matches_with_opponents:
                            needed_keys.add((w['opponent_id'], w['tournament_id'], w['group_id']))
                        
                        # Get all unique tournaments and groups
                        tournament_ids = list(set([w['tournament_id'] for w in win_matches_with_opponents]))
                        all_opponent_ids = list(set([w['opponent_id'] for w in win_matches_with_opponents]))
                        
                        try:
                            # OPTIMIZED: Fetch ALL stats for ALL tournaments at once using .in_() for tournament_id
                            # This reduces queries from N (one per tournament) to just a few batches
                            if tournament_ids and all_opponent_ids:
                                # Batch tournaments if there are many (Supabase .in_() limit is ~100)
                                tournament_batch_size = 50  # Conservative limit
                                opponent_batch_size = 50    # Conservative limit
                                
                                # Fetch in batches: for each tournament batch, fetch opponent batches
                                for t_batch_start in range(0, len(tournament_ids), tournament_batch_size):
                                    tournament_batch = tournament_ids[t_batch_start:t_batch_start + tournament_batch_size]
                                    
                                    for o_batch_start in range(0, len(all_opponent_ids), opponent_batch_size):
                                        opponent_batch = all_opponent_ids[o_batch_start:o_batch_start + opponent_batch_size]
                                        
                                        try:
                                            # Fetch all stats for this batch of tournaments and opponents
                                            batch_stats = (
                                                self.client.table('player_tournament_stats')
                                                .select('player_id,tournament_id,group_id,rating_pre')
                                                .in_('tournament_id', tournament_batch)
                                                .in_('player_id', opponent_batch)
                                                .execute()
                                            )
                                            
                                            if batch_stats.data:
                                                for stat in batch_stats.data:
                                                    key = (stat['player_id'], stat['tournament_id'], stat['group_id'])
                                                    if key in needed_keys:
                                                        rating_lookup[key] = stat.get('rating_pre')
                                        except Exception as e:
                                            # If .in_() with multiple columns fails, fall back to per-tournament queries
                                            print(f"Error batch fetching ratings (trying fallback): {e}")
                                            # Fallback: query per tournament
                                            for tournament_id in tournament_batch:
                                                tournament_opponent_ids = list(set([
                                                    w['opponent_id'] for w in win_matches_with_opponents 
                                                    if w['tournament_id'] == tournament_id
                                                ]))
                                                
                                                if not tournament_opponent_ids:
                                                    continue
                                                
                                                for o_batch_start in range(0, len(tournament_opponent_ids), opponent_batch_size):
                                                    batch_opponent_ids = tournament_opponent_ids[o_batch_start:o_batch_start + opponent_batch_size]
                                                    
                                                    try:
                                                        batch_stats = (
                                                            self.client.table('player_tournament_stats')
                                                            .select('player_id,tournament_id,group_id,rating_pre')
                                                            .eq('tournament_id', tournament_id)
                                                            .in_('player_id', batch_opponent_ids)
                                                            .execute()
                                                        )
                                                        
                                                        if batch_stats.data:
                                                            for stat in batch_stats.data:
                                                                key = (stat['player_id'], stat['tournament_id'], stat['group_id'])
                                                                if key in needed_keys:
                                                                    rating_lookup[key] = stat.get('rating_pre')
                                                    except Exception as e2:
                                                        print(f"Error in fallback query for tournament {tournament_id}: {e2}")
                                                        continue
                                            break  # Exit opponent batch loop, continue with next tournament batch
                        except Exception as e:
                            print(f"Error batch fetching opponent ratings: {e}")
                        
                        # Now find the highest rated win using the lookup
                        for win_info in win_matches_with_opponents:
                            key = (win_info['opponent_id'], win_info['tournament_id'], win_info['group_id'])
                            opponent_rating = rating_lookup.get(key)
                            
                            if opponent_rating is not None:
                                try:
                                    opponent_rating_int = int(opponent_rating)
                                    current_top_rating = top_rated_win_info.get('rating', 0) if top_rated_win_info else 0
                                    if top_rated_win_info is None or opponent_rating_int > current_top_rating:
                                        top_rated_win_info = {
                                            'rating': opponent_rating_int,
                                            'opponent_name': win_info['opponent_name'],
                                            'date': win_info['match_date']
                                        }
                                        top_rated_win = opponent_rating_int
                                except (ValueError, TypeError):
                                    print(f"Invalid opponent rating value: {opponent_rating} (type: {type(opponent_rating)})")
                                    continue
            
            # date_joined is now calculated above with rating_history
            
            # OPTIMIZED: Get last match date using single OR query instead of two separate queries
            last_match_date = None
            try:
                # Use the matches we already fetched if available, otherwise query
                if unique_matches:
                    # Extract dates from already fetched matches
                    dates = [m.get('tournament_date') for m in unique_matches if m.get('tournament_date')]
                    if dates:
                        last_match_date = max(dates)
                else:
                    # Fallback: two separate queries
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
                        dates.append(last_match_query1.data[0].get('tournament_date'))
                    if last_match_query2.data and len(last_match_query2.data) > 0:
                        dates.append(last_match_query2.data[0].get('tournament_date'))
                    
                    if dates:
                        last_match_date = max([d for d in dates if d])
            except Exception as e:
                print(f"Error getting last match date for {player_name}: {e}")
            
            # Get ranking and percentile (only if not filtering by days_back, or if days_back is None)
            ranking_info = None
            if days_back is None:  # Only calculate for all-time stats
                try:
                    ranking_info = self.get_player_ranking_and_percentile(player_name)
                except Exception as e:
                    print(f"Error getting ranking info: {e}")
                    ranking_info = None
            
            return {
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
                'last_match_date': last_match_date,
                'ranking': ranking_info.get('rank') if ranking_info else None,
                'total_players': ranking_info.get('total_players') if ranking_info else None,
                'players_better_than': ranking_info.get('players_better_than') if ranking_info else None
            }
        except Exception as e:
            print(f"Error getting match stats for {player_name}: {e}")
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
        """Get all players (paginated to handle Supabase's 1000 row limit)"""
        try:
            # First, get the total count to know how many players we need to fetch
            count_result = (
                self.client.table('players')
                .select('id', count='exact')
                .execute()
            )
            total_count = count_result.count if hasattr(count_result, 'count') and count_result.count is not None else None
            
            print(f"Total players in database: {total_count}")
            
            all_players = []
            page_size = 1000
            offset = 0
            
            while True:
                # Use range with explicit calculation
                # Supabase range is inclusive on both ends
                # If we're getting 999 instead of 1000, try using end = start + page_size (inclusive)
                start_idx = offset
                end_idx = offset + page_size  # Try inclusive end to get page_size rows
                
                result = (
                    self.client.table('players')
                    .select('*')
                    .order('name')
                    .range(start_idx, end_idx)
                    .execute()
                )
                
                if not result.data:
                    # If we still need more players, try a smaller range
                    if total_count and len(all_players) < total_count:
                        # Try fetching remaining players one by one or in smaller batches
                        remaining = total_count - len(all_players)
                        if remaining > 0 and remaining <= 10:
                            # Try fetching the remaining players with a smaller range
                            small_start = len(all_players)
                            small_end = small_start + remaining - 1
                            small_result = (
                                self.client.table('players')
                                .select('*')
                                .order('name')
                                .range(small_start, small_end)
                                .execute()
                            )
                            if small_result.data:
                                print(f"Fetched remaining {len(small_result.data)} players (range {small_start}-{small_end})")
                                all_players.extend(small_result.data)
                    print(f"No more data at offset {offset} (range {start_idx}-{end_idx})")
                    break
                
                batch_size = len(result.data)
                print(f"Fetched batch: offset {offset}, got {batch_size} players (range {start_idx}-{end_idx}), total so far: {len(all_players) + batch_size}")
                all_players.extend(result.data)
                
                # If we know the total count, check if we've fetched all
                if total_count:
                    if len(all_players) >= total_count:
                        print(f"Fetched all {total_count} players")
                        break
                    # Continue fetching even if this batch was smaller, as long as we haven't reached total
                    if batch_size < page_size and len(all_players) < total_count:
                        print(f"Got {batch_size} players but total is {total_count}, continuing...")
                        # Move to next batch - start from where we left off
                        offset = len(all_players)
                        continue
                
                # If we don't know total count and got fewer than page_size, we've reached the end
                if batch_size < page_size:
                    print(f"Got fewer than {page_size} players and no total count, assuming reached end")
                    break
                
                # Move to next batch - start from where we left off
                offset = len(all_players)
            
            print(f"Final count: {len(all_players)} players fetched")
            if total_count and len(all_players) != total_count:
                print(f"WARNING: Expected {total_count} players but fetched {len(all_players)}")
            
            return all_players
        except Exception as e:
            print(f"Error getting players: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_all_players_with_rankings(self, active_days: int = 365, use_view: bool = True) -> List[Dict]:
        """Get all players with their rankings and current ratings
        
        Args:
            active_days: Number of days to consider for active players (default 365)
            use_view: If True, use materialized view (fast). If False or view unavailable, use old method.
        """
        # Try to use materialized view first (much faster)
        if use_view:
            try:
                return self.get_all_players_with_rankings_from_view(active_days=active_days)
            except Exception as e:
                print(f"Failed to use materialized view, falling back to old method: {e}")
                # Fall through to old method
        
        # Old method (slower but always works)
        try:
            from datetime import datetime, timedelta
            
            # Get all players
            all_players = self.get_all_players()
            if not all_players:
                return []
            
            all_player_names = [p.get('name') for p in all_players if p.get('name')]
            print(f"Processing {len(all_player_names)} player names for ratings/rankings")
            
            # Get current ratings for ALL players (not just active ones)
            all_player_ratings = {}
            rating_history_result = None
            try:
                # Supabase .in_() has a limit of ~100 items for the IN clause
                # BUT it also has a default result limit of ~1000 rows per query
                # If we query 100 players and some have many entries, we might hit the row limit
                # and miss players. Solution: use smaller batches (25 players) to ensure
                # we get all results, or paginate the results.
                batch_size = 25  # Reduced from 100 to avoid hitting result limits
                all_rating_history_data = []
                
                for i in range(0, len(all_player_names), batch_size):
                    batch_names = all_player_names[i:i + batch_size]
                    batch_num = i//batch_size + 1
                    
                    # Check if our test player is in this batch
                    test_player_in_batch = any('chitamur' in name.lower() and 'ashwath' in name.lower() for name in batch_names)
                    if test_player_in_batch:
                        matching_names = [name for name in batch_names if 'chitamur' in name.lower() and 'ashwath' in name.lower()]
                        print(f"DEBUG: Test player found in batch {batch_num}: {matching_names}")
                    
                    try:
                        # Paginate results to ensure we get ALL entries, not just the first 1000
                        # Supabase has a default limit of ~1000 rows per query
                        page_size = 1000
                        offset = 0
                        batch_entries = []
                        
                        while True:
                            batch_result = (
                                self.client.table('player_rating_chart_view')
                                .select('player_name,tournament_date,rating_post,rating_pre')
                                .in_('player_name', batch_names)
                                .range(offset, offset + page_size - 1)
                                .execute()
                            )
                            
                            if not batch_result.data:
                                break
                            
                            batch_entries.extend(batch_result.data)
                            
                            # If we got fewer than page_size, we've reached the end
                            if len(batch_result.data) < page_size:
                                break
                            
                            offset += page_size
                        
                        if batch_entries:
                            all_rating_history_data.extend(batch_entries)
                            if test_player_in_batch:
                                # Check if we got data for our test player
                                test_player_data = [e for e in batch_entries if 'chitamur' in e.get('player_name', '').lower() and 'ashwath' in e.get('player_name', '').lower()]
                                if test_player_data:
                                    print(f"DEBUG: Found test player data in batch {batch_num} result: {len(test_player_data)} entries")
                                    print(f"DEBUG: Sample entry: player_name='{test_player_data[0].get('player_name')}', rating_post={test_player_data[0].get('rating_post')}")
                                else:
                                    print(f"DEBUG: WARNING - Test player in batch {batch_num} but NOT in query results!")
                                    print(f"DEBUG: Batch had {len(batch_entries)} entries, checking first few player names...")
                                    sample_names = list(set([e.get('player_name') for e in batch_entries[:10]]))
                                    print(f"DEBUG: Sample player names from batch result: {sample_names}")
                    except Exception as e:
                        print(f"Error fetching rating history batch {batch_num}: {e}")
                        if test_player_in_batch:
                            print(f"DEBUG: ERROR - Batch {batch_num} failed and it contained our test player!")
                        continue
                
                # Sort all rating history by date descending (most recent first)
                # This ensures we get the latest rating for each player
                all_rating_history_data.sort(key=lambda x: (
                    x.get('tournament_date') or '',
                    x.get('player_name') or ''
                ), reverse=True)
                
                # Create a mock result object for compatibility
                class MockResult:
                    def __init__(self, data):
                        self.data = data
                
                rating_history_result = MockResult(all_rating_history_data)
                
                if rating_history_result.data:
                    seen_players = set()
                    for entry in rating_history_result.data:
                        p_name = entry.get('player_name')
                        if not p_name or p_name in seen_players:
                            continue
                        
                        # Try rating_post first, fallback to rating_pre if rating_post is missing
                        rating_post = entry.get('rating_post')
                        rating_pre = entry.get('rating_pre')
                        rating_to_use = None
                        
                        if rating_post is not None and rating_post != '':
                            rating_to_use = rating_post
                        elif rating_pre is not None and rating_pre != '':
                            # Use rating_pre as fallback if rating_post is missing
                            rating_to_use = rating_pre
                        
                        if rating_to_use is not None:
                            try:
                                rating_int = int(rating_to_use)
                                all_player_ratings[p_name] = rating_int
                                seen_players.add(p_name)
                            except (ValueError, TypeError):
                                continue
            except Exception as e:
                print(f"Error batch fetching rating history: {e}")
                import traceback
                traceback.print_exc()
                # Continue with empty ratings dict
            
            # Get cutoff date for active players (for ranking calculation)
            cutoff_date = (datetime.now() - timedelta(days=active_days)).strftime('%Y-%m-%d')
            
            # Get all players who have played in the last year (for ranking)
            # Include players from BOTH match_results_view AND rating history
            active_player_names = set()
            
            # Method 1: Get active players from matches
            active_players_query = (
                self.client.table('match_results_view')
                .select('player1_name,player2_name,tournament_date')
                .gte('tournament_date', cutoff_date)
                .execute()
            )
            
            if active_players_query.data:
                for match in active_players_query.data:
                    p1_name = match.get('player1_name')
                    p2_name = match.get('player2_name')
                    
                    if p1_name:
                        active_player_names.add(p1_name)
                    
                    if p2_name:
                        active_player_names.add(p2_name)
            
            # Method 2: Also include players who have rating history in the last year
            # This catches players like Vin Reddy who have ratings but no match records
            if rating_history_result and rating_history_result.data:
                for entry in rating_history_result.data:
                    p_name = entry.get('player_name')
                    entry_date = entry.get('tournament_date')
                    
                    if p_name and entry_date:
                        try:
                            # Parse date and check if it's within the active period
                            if isinstance(entry_date, str):
                                entry_date_obj = datetime.strptime(entry_date.split('T')[0], '%Y-%m-%d')
                            else:
                                entry_date_obj = entry_date
                            
                            cutoff_date_obj = datetime.strptime(cutoff_date, '%Y-%m-%d')
                            
                            if entry_date_obj >= cutoff_date_obj:
                                active_player_names.add(p_name)
                        except Exception as e:
                            # If date parsing fails, skip this entry
                            continue
            
            # Calculate rankings only for active players
            ranking_map = {}
            if active_player_names:
                # Filter ratings to only active players for ranking
                active_player_ratings = {name: rating for name, rating in all_player_ratings.items() if name in active_player_names}
                
                # Check for active players without ratings
                active_without_ratings = active_player_names - set(all_player_ratings.keys())
                if active_without_ratings:
                    print(f"Warning: Active players without ratings: {sorted(active_without_ratings)}")
                
                # Sort active players by rating (descending) to calculate rankings
                sorted_players = sorted(active_player_ratings.items(), key=lambda x: x[1], reverse=True)
                
                # Create ranking map
                for idx, (name, rating) in enumerate(sorted_players, start=1):
                    ranking_map[name] = idx
                
            # Get last match dates for all players
            # Check both match_results_view and rating history to get the most recent date
            # NOTE: If a player has rating history but no matches, this indicates a data integrity issue
            # (they should have matches if they have rating history). We use rating history as fallback
            # but this should be investigated.
            last_match_dates = {}
            players_with_rating_but_no_matches = set()
            try:
                # Method 1: Query all matches and process in memory
                all_matches_query = (
                    self.client.table('match_results_view')
                    .select('player1_name,player2_name,tournament_date')
                    .order('tournament_date', desc=True)
                    .execute()
                )
                
                players_with_matches = set()
                if all_matches_query.data:
                    # Process matches to find the latest date for each player
                    # Since matches are ordered desc, first occurrence is the latest
                    for match in all_matches_query.data:
                        p1_name = match.get('player1_name')
                        p2_name = match.get('player2_name')
                        match_date = match.get('tournament_date')
                        
                        if p1_name and match_date:
                            players_with_matches.add(p1_name)
                            # Use the most recent date if we already have one from rating history
                            if p1_name not in last_match_dates or match_date > last_match_dates[p1_name]:
                                last_match_dates[p1_name] = match_date
                        if p2_name and match_date:
                            players_with_matches.add(p2_name)
                            if p2_name not in last_match_dates or match_date > last_match_dates[p2_name]:
                                last_match_dates[p2_name] = match_date
                
                # Method 2: Also check rating history (some players may have ratings but no matches)
                # This is a data integrity issue - players with rating history should have matches
                if rating_history_result and rating_history_result.data:
                    # Rating history is already sorted by date descending
                    seen_players_in_history = set()
                    for entry in rating_history_result.data:
                        p_name = entry.get('player_name')
                        entry_date = entry.get('tournament_date')
                        
                        if p_name and entry_date:
                            seen_players_in_history.add(p_name)
                            # Use the most recent date between matches and rating history
                            if p_name not in last_match_dates or entry_date > last_match_dates[p_name]:
                                last_match_dates[p_name] = entry_date
                    
                    # Track players with rating history but no matches (data integrity issue)
                    players_with_rating_but_no_matches = seen_players_in_history - players_with_matches
                    if players_with_rating_but_no_matches:
                        print(f"WARNING: Found {len(players_with_rating_but_no_matches)} players with rating history but no matches (data integrity issue):")
                        for player in sorted(list(players_with_rating_but_no_matches))[:10]:  # Show first 10
                            print(f"  - {player}")
                        if len(players_with_rating_but_no_matches) > 10:
                            print(f"  ... and {len(players_with_rating_but_no_matches) - 10} more")
            except Exception as e:
                print(f"Error getting last match dates: {e}")
                import traceback
                traceback.print_exc()
                # Continue without last match dates
            
            # Build result list with ratings for all players, rankings only for active players
            result = []
            print(f"Building result list from {len(all_players)} players")
            for player in all_players:
                player_name = player.get('name')
                player_info = {
                    'name': player_name,
                    'id': player.get('id'),
                    'ranking': ranking_map.get(player_name),
                    'current_rating': all_player_ratings.get(player_name),
                    'last_match_date': last_match_dates.get(player_name)
                }
                result.append(player_info)
            
            print(f"Returning {len(result)} players in result")
            
            # Debug: Check if specific players are in the result and have ratings/rankings
            test_players = ['Chitamur, Ashwath', 'Ashwath Chitamur', 'Tate Houston']
            for test_name in test_players:
                found = [p for p in result if test_name.lower() in p.get('name', '').lower()]
                if found:
                    player_info = found[0]
                    actual_name = player_info.get('name')
                    print(f"Found test player '{test_name}': {player_info}")
                    # Check if they have rating/ranking data
                    if player_info.get('ranking') is None and player_info.get('current_rating') is None:
                        print(f"  WARNING: '{actual_name}' has no ranking or rating!")
                        # Check if they're in rating history (exact name match)
                        in_rating_history = [e for e in (rating_history_result.data if rating_history_result else []) 
                                           if e.get('player_name') == actual_name]
                        if in_rating_history:
                            print(f"  But they ARE in rating history (exact match): {len(in_rating_history)} entries")
                            print(f"  First entry: {in_rating_history[0]}")
                            # Check if they're in all_player_ratings
                            if actual_name in all_player_ratings:
                                print(f"  And they ARE in all_player_ratings: {all_player_ratings[actual_name]}")
                            else:
                                print(f"  But they're NOT in all_player_ratings!")
                                print(f"  This suggests a bug in the rating extraction logic")
                                print(f"  Sample rating history entries: {in_rating_history[:2]}")
                        else:
                            print(f"  NOT found in rating history (exact match)")
                            # Try case-insensitive match
                            in_rating_history_ci = [e for e in (rating_history_result.data if rating_history_result else []) 
                                                   if e.get('player_name', '').lower() == actual_name.lower()]
                            if in_rating_history_ci:
                                print(f"  Found in rating history (case-insensitive): {len(in_rating_history_ci)} entries")
                                print(f"  Rating history name: '{in_rating_history_ci[0].get('player_name')}' vs actual: '{actual_name}'")
                                print(f"  This suggests a name mismatch issue")
                            else:
                                # Try partial match - but be more specific (check for name components)
                                actual_name_parts = [part.strip() for part in actual_name.lower().split(',')]
                                actual_name_parts.extend([part.strip() for part in actual_name.lower().split()])
                                actual_name_parts = [p for p in actual_name_parts if len(p) > 2]  # Only meaningful parts
                                
                                in_rating_history_partial = []
                                for e in (rating_history_result.data if rating_history_result else []):
                                    rh_name = e.get('player_name', '').lower()
                                    # Check if any meaningful part of the actual name is in the rating history name
                                    if any(part in rh_name for part in actual_name_parts if len(part) > 2):
                                        in_rating_history_partial.append(e)
                                
                                if in_rating_history_partial:
                                    print(f"  Found similar names in rating history: {len(in_rating_history_partial)} entries")
                                    # Get unique player names from matches
                                    unique_names = list(set([e.get('player_name') for e in in_rating_history_partial]))
                                    print(f"  Unique player names found: {unique_names[:10]}")
                                    # Show full entry for first match
                                    if in_rating_history_partial:
                                        print(f"  First entry details: player_name='{in_rating_history_partial[0].get('player_name')}', date={in_rating_history_partial[0].get('tournament_date')}")
                                else:
                                    print(f"  NOT found in rating history at all (checked exact, case-insensitive, and meaningful partial)")
                                    print(f"  Total rating history entries: {len(rating_history_result.data) if rating_history_result else 0}")
                                    # Check if maybe the name is stored differently - search for "Chitamur" or "Ashwath"
                                    search_terms = ['chitamur', 'ashwath']
                                    for term in search_terms:
                                        matches = [e for e in (rating_history_result.data if rating_history_result else []) 
                                                  if term in e.get('player_name', '').lower()]
                                        if matches:
                                            unique_matches = list(set([e.get('player_name') for e in matches[:10]]))
                                            print(f"  Found entries containing '{term}': {unique_matches}")
                                    
                                    # Try querying rating history directly for this player to see what name format is used
                                    try:
                                        direct_query = (
                                            self.client.table('player_rating_chart_view')
                                            .select('player_name,tournament_date,rating_post')
                                            .ilike('player_name', f'%{actual_name.split(",")[0].strip()}%')
                                            .limit(5)
                                            .execute()
                                        )
                                        if direct_query.data:
                                            unique_direct = list(set([e.get('player_name') for e in direct_query.data]))
                                            print(f"  Direct query found: {unique_direct}")
                                            print(f"  This suggests the name in rating history might be: {unique_direct[0] if unique_direct else 'N/A'}")
                                    except Exception as e:
                                        print(f"  Error in direct query: {e}")
                        # Check if they're in active players
                        in_active = actual_name in active_player_names if 'active_player_names' in locals() else False
                        print(f"  In active players: {in_active}")
                        # Check if they're in ranking map
                        in_ranking = actual_name in ranking_map if 'ranking_map' in locals() else False
                        print(f"  In ranking map: {in_ranking}")
                        # Check if they're in all_player_ratings
                        in_ratings = actual_name in all_player_ratings if 'all_player_ratings' in locals() else False
                        print(f"  In all_player_ratings: {in_ratings}")
                        if in_ratings:
                            print(f"  Rating value: {all_player_ratings[actual_name]}")
                else:
                    # Check if it's in all_players but not in result
                    in_all = [p for p in all_players if test_name.lower() in p.get('name', '').lower()]
                    if in_all:
                        print(f"WARNING: '{test_name}' is in all_players but not in result!")
                        print(f"  all_players entry: {in_all[0]}")
            
            return result
        except Exception as e:
            print(f"Error getting players with rankings: {e}")
            import traceback
            traceback.print_exc()
            # Fallback to basic player list
            return [{'name': p.get('name'), 'id': p.get('id'), 'ranking': None, 'current_rating': None} for p in self.get_all_players()]
    
    def refresh_player_rankings_view(self) -> bool:
        """Refresh the materialized view for player rankings"""
        try:
            # Call the PostgreSQL function to refresh the view
            result = self.client.rpc('refresh_player_rankings_view').execute()
            return True
        except Exception as e:
            # If the function doesn't exist, try direct SQL (fallback)
            try:
                # Note: Supabase client doesn't directly support REFRESH MATERIALIZED VIEW
                # So we use the RPC function which should be created by the SQL script
                print(f"Error refreshing player rankings view via RPC: {e}")
                print("Make sure you've run sql/create_player_rankings_view.sql")
                return False
            except Exception as e2:
                print(f"Error refreshing player rankings view: {e2}")
                return False
    
    def refresh_player_match_stats_view(self) -> bool:
        """Refresh the materialized view for player match statistics"""
        try:
            # Call the PostgreSQL function to refresh the view
            result = self.client.rpc('refresh_player_match_stats_view').execute()
            return True
        except Exception as e:
            # If the function doesn't exist, log warning
            print(f"Error refreshing player match stats view via RPC: {e}")
            print("Make sure you've run sql/create_player_stats_view.sql")
            return False
    
    def get_all_players_with_rankings_from_view(self, active_days: int = 365) -> List[Dict]:
        """Get all players with rankings using the materialized view (fast)"""
        try:
            # Get total count first
            count_result = (
                self.client.table('player_rankings_view')
                .select('player_id', count='exact')
                .execute()
            )
            total_count = count_result.count if hasattr(count_result, 'count') and count_result.count is not None else None
            
            if total_count:
                print(f"Total players in materialized view: {total_count}")
            
            # Paginate to handle Supabase's 1000 row limit
            all_players = []
            page_size = 1000
            offset = 0
            
            while True:
                # Use range with explicit calculation (matching get_all_players logic)
                start_idx = offset
                end_idx = offset + page_size  # Try inclusive end to get page_size rows
                
                result = (
                    self.client.table('player_rankings_view')
                    .select('player_id,player_name,current_rating,ranking,last_match_date,is_active')
                    .order('player_name')
                    .range(start_idx, end_idx)
                    .execute()
                )
                
                if not result.data:
                    # If we still need more players, try a smaller range
                    if total_count and len(all_players) < total_count:
                        remaining = total_count - len(all_players)
                        if remaining > 0 and remaining <= 10:
                            # Try fetching remaining players with a smaller range
                            small_start = len(all_players)
                            small_end = small_start + remaining - 1
                            small_result = (
                                self.client.table('player_rankings_view')
                                .select('player_id,player_name,current_rating,ranking,last_match_date,is_active')
                                .order('player_name')
                                .range(small_start, small_end)
                                .execute()
                            )
                            if small_result.data:
                                print(f"Fetched remaining {len(small_result.data)} players (range {small_start}-{small_end})")
                                for row in small_result.data:
                                    all_players.append({
                                        'id': row.get('player_id'),
                                        'name': row.get('player_name'),
                                        'ranking': row.get('ranking'),
                                        'current_rating': row.get('current_rating'),
                                        'last_match_date': str(row.get('last_match_date')) if row.get('last_match_date') else None
                                    })
                    break
                
                batch_size = len(result.data)
                print(f"Fetched batch: offset {offset}, got {batch_size} players (range {start_idx}-{end_idx}), total so far: {len(all_players) + batch_size}")
                
                # Convert to expected format
                for row in result.data:
                    all_players.append({
                        'id': row.get('player_id'),
                        'name': row.get('player_name'),
                        'ranking': row.get('ranking'),
                        'current_rating': row.get('current_rating'),
                        'last_match_date': str(row.get('last_match_date')) if row.get('last_match_date') else None
                    })
                
                # If we know the total count, check if we've fetched all
                if total_count:
                    if len(all_players) >= total_count:
                        print(f"Fetched all {total_count} players from view")
                        break
                    # Continue fetching even if this batch was smaller, as long as we haven't reached total
                    if batch_size < page_size and len(all_players) < total_count:
                        print(f"Got {batch_size} players but total is {total_count}, continuing...")
                        # Move to next batch - start from where we left off
                        offset = len(all_players)
                        continue
                
                # If we don't know total count and got fewer than page_size, we've reached the end
                if batch_size < page_size:
                    print(f"Got fewer than {page_size} players and no total count, assuming reached end")
                    break
                
                # Move to next batch - start from where we left off
                offset = len(all_players)
            
            if not all_players:
                # Fallback to old method if view doesn't exist or is empty
                print("Warning: player_rankings_view not found or empty, falling back to old method")
                return self.get_all_players_with_rankings(active_days=active_days, use_view=False)
            
            print(f"Fetched {len(all_players)} players from materialized view")
            
            # Sort by ranking (NULLs last), then by name
            # This matches the SQL ORDER BY ranking NULLS LAST, name
            all_players.sort(key=lambda p: (
                p.get('ranking') if p.get('ranking') is not None else float('inf'),
                p.get('name', '')
            ))
            
            return all_players
        except Exception as e:
            # If view doesn't exist or query fails, fallback to old method
            print(f"Error querying player_rankings_view: {e}")
            print("Falling back to old method. Make sure you've run sql/create_player_rankings_view.sql")
            return self.get_all_players_with_rankings(active_days=active_days, use_view=False)
    
    def get_total_tournaments(self) -> int:
        """Get total number of tournaments in the database"""
        try:
            result = self.client.table('tournaments').select('id', count='exact').execute()
            return result.count if result.count is not None else 0
        except Exception as e:
            print(f"Error getting total tournaments: {e}")
            return 0
    
    def get_tournament_details(self, tournament_id: int) -> Optional[Dict]:
        """Get complete tournament details including groups, players, and matches
        OPTIMIZED: Uses views and batch queries instead of N+1 queries
        """
        try:
            # Get tournament info
            tournament_result = self.client.table('tournaments').select('*').eq('id', tournament_id).execute()
            if not tournament_result.data:
                return None
            
            tournament = tournament_result.data[0]
            
            # OPTIMIZED: Fetch all players for this tournament in one query using player_tournament_stats with join
            # This gets all player stats with player names in a single query
            all_players_result = (
                self.client.table('player_tournament_stats')
                .select('*, players!inner(name)')
                .eq('tournament_id', tournament_id)
                .order('group_id,player_number')
                .execute()
            )
            
            # OPTIMIZED: Fetch all matches for this tournament in one query using match_results_view
            # This view already includes player names, so no additional queries needed
            all_matches_result = (
                self.client.table('match_results_view')
                .select('*')
                .eq('tournament_id', tournament_id)
                .execute()
            )
            
            # Get all groups for this tournament (still needed for group metadata)
            groups_result = (
                self.client.table('round_robin_groups')
                .select('*')
                .eq('tournament_id', tournament_id)
                .order('group_number')
                .execute()
            )
            
            # Build lookup maps for efficient grouping
            players_by_group = {}
            matches_by_group = {}
            
            # Process all players and group by group_id
            for player_stat in all_players_result.data:
                group_id = player_stat['group_id']
                
                # Extract player name from join
                player_name = None
                if 'players' in player_stat:
                    player_info = player_stat['players']
                    if isinstance(player_info, dict):
                        player_name = player_info.get('name')
                    elif isinstance(player_info, list) and len(player_info) > 0:
                        player_name = player_info[0].get('name')
                
                # Fallback: if join didn't work, we'd need to query, but this should work
                if not player_name:
                    # This shouldn't happen with the join, but handle gracefully
                    player_name = 'Unknown'
                
                if group_id not in players_by_group:
                    players_by_group[group_id] = []
                
                players_by_group[group_id].append({
                    'player_id': player_stat['player_id'],
                    'player_name': player_name,
                    'player_number': player_stat['player_number'],
                    'rating_pre': player_stat.get('rating_pre'),
                    'rating_post': player_stat.get('rating_post'),
                    'rating_change': player_stat.get('rating_change'),
                    'matches_won': player_stat.get('matches_won', 0),
                    'games_won': player_stat.get('games_won', 0),
                    'bonus_points': player_stat.get('bonus_points', 0)
                })
            
            # Process all matches and group by group_id
            for match in all_matches_result.data:
                group_id = match.get('group_id')
                if group_id is None:
                    continue  # Skip matches without a group
                
                if group_id not in matches_by_group:
                    matches_by_group[group_id] = []
                
                matches_by_group[group_id].append({
                    'match_id': match['match_id'],
                    'player1_id': match['player1_id'],
                    'player1_name': match.get('player1_name') or 'Unknown',
                    'player1_score': match.get('player1_score'),
                    'player2_id': match['player2_id'],
                    'player2_name': match.get('player2_name') or 'Unknown',
                    'player2_score': match.get('player2_score'),
                    'winner_id': match.get('winner_id'),
                    'winner_name': match.get('winner_name')
                })
            
            # Build groups list from groups_result, using the pre-grouped players and matches
            groups = []
            for group in groups_result.data:
                group_id = group['id']
                group_number = group['group_number']
                group_name = group['group_name']
                
                # Get players for this group (already sorted by player_number from query)
                players = players_by_group.get(group_id, [])
                
                # Get matches for this group
                matches = matches_by_group.get(group_id, [])
                
                groups.append({
                    'group_id': group_id,
                    'group_number': group_number,
                    'group_name': group_name,
                    'players': players,
                    'matches': matches
                })
            
            return {
                'tournament': tournament,
                'groups': groups
            }
        except Exception as e:
            print(f"Error getting tournament details: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_all_tournaments_with_stats(self) -> List[Dict]:
        """Get all tournaments with statistics (number of players, number of matches)
        Uses SQL RPC function for efficient server-side aggregation
        """
        try:
            # Use RPC to call the SQL function for efficient aggregation
            result = self.client.rpc('get_tournament_stats', params={}).execute()
            
            if not result.data:
                return []
            
            tournaments = []
            for row in result.data:
                tournament_id = int(row.get('tournament_id')) if row.get('tournament_id') is not None else None
                # Get source_url, parsing_status, and parse_error from RPC result (should be included now)
                source_url = row.get('source_url')
                parsing_status = row.get('parsing_status', 'success')
                parse_error = row.get('parse_error')
                tournament_date = row.get('tournament_date')
                
                # If source_url is not in RPC result (old function version), fetch it separately
                if source_url is None and tournament_id:
                    try:
                        url_result = self.client.table('tournaments').select('source_url').eq('id', tournament_id).execute()
                        if url_result.data:
                            source_url = url_result.data[0].get('source_url')
                    except Exception:
                        pass
                
                tournaments.append({
                    'tournament_id': tournament_id,
                    'tournament_date': tournament_date,
                    'num_players': int(row.get('num_players')) if row.get('num_players') is not None else 0,
                    'num_matches': int(row.get('num_matches')) if row.get('num_matches') is not None else 0,
                    'source_url': source_url,
                    'parsing_status': parsing_status,
                    'parse_error': parse_error
                })
            
            return tournaments
        except Exception as e:
            # Fallback to manual counting if RPC function doesn't exist
            print(f"RPC function not available, using fallback method: {e}")
            return self._get_all_tournaments_with_stats_fallback()
    
    def _get_all_tournaments_with_stats_fallback(self) -> List[Dict]:
        """Fallback method that fetches all data and processes in Python"""
        try:
            # Get all tournaments with all needed fields
            tournaments_result = (
                self.client.table('tournaments')
                .select('id,date,source_url,parsing_status,parse_error')
                .order('date', desc=True)
                .execute()
            )
            
            if not tournaments_result.data:
                return []
            
            # Build tournament lookup
            tournaments_dict = {}
            tournament_ids = []
            for tournament in tournaments_result.data:
                tournament_id = int(tournament.get('id')) if tournament.get('id') is not None else None
                if tournament_id is None:
                    continue
                tournament_ids.append(tournament_id)
                tournaments_dict[tournament_id] = {
                    'tournament_id': tournament_id,
                    'tournament_date': tournament.get('date'),
                    'source_url': tournament.get('source_url'),
                    'parsing_status': tournament.get('parsing_status', 'success'),
                    'parse_error': tournament.get('parse_error'),
                    'num_players': 0,
                    'num_matches': 0
                }
            
            # Count players and matches per tournament using batched queries
            # Process tournaments in batches to avoid timeout
            # Supabase .in_() has a limit, so use smaller batches
            batch_size = 25
            for i in range(0, len(tournament_ids), batch_size):
                batch_ids = tournament_ids[i:i + batch_size]
                
                # Count distinct players per tournament in this batch
                try:
                    # Get all player stats for this batch of tournaments
                    player_stats_result = (
                        self.client.table('player_tournament_stats')
                        .select('tournament_id,player_id')
                        .in_('tournament_id', batch_ids)
                        .execute()
                    )
                    
                    if player_stats_result.data:
                        # Accumulate players per tournament
                        for stat in player_stats_result.data:
                            tournament_id = stat.get('tournament_id')
                            player_id = stat.get('player_id')
                            if tournament_id is not None and player_id is not None:
                                try:
                                    tournament_id = int(tournament_id)
                                    player_id = int(player_id)
                                    if tournament_id in tournaments_dict:
                                        # Use a set stored in the tournament dict to accumulate
                                        if 'player_set' not in tournaments_dict[tournament_id]:
                                            tournaments_dict[tournament_id]['player_set'] = set()
                                        tournaments_dict[tournament_id]['player_set'].add(player_id)
                                except (ValueError, TypeError):
                                    continue
                except Exception as e:
                    print(f"Error fetching player stats for batch: {e}")
                
                # Count matches per tournament in this batch
                try:
                    matches_result = (
                        self.client.table('matches')
                        .select('tournament_id')
                        .in_('tournament_id', batch_ids)
                        .execute()
                    )
                    
                    if matches_result.data:
                        # Accumulate match counts
                        for match in matches_result.data:
                            tournament_id = match.get('tournament_id')
                            if tournament_id is not None:
                                try:
                                    tournament_id = int(tournament_id)
                                    if tournament_id in tournaments_dict:
                                        tournaments_dict[tournament_id]['num_matches'] = tournaments_dict[tournament_id].get('num_matches', 0) + 1
                                except (ValueError, TypeError):
                                    continue
                except Exception as e:
                    print(f"Error fetching matches for batch: {e}")
            
            # Finalize player counts from accumulated sets
            for tournament_id in tournaments_dict:
                if 'player_set' in tournaments_dict[tournament_id]:
                    tournaments_dict[tournament_id]['num_players'] = len(tournaments_dict[tournament_id]['player_set'])
                    # Clean up the temporary set
                    del tournaments_dict[tournament_id]['player_set']
            
            # Convert to list
            tournaments = [tournaments_dict[tid] for tid in tournament_ids if tid in tournaments_dict]
            return tournaments
        except Exception as e:
            print(f"Error in fallback method: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_rating_distribution(self, active_days: int = 365) -> Dict:
        """Get rating distribution for active players (histogram data)"""
        try:
            from datetime import datetime, timedelta
            
            # Get active players with their current ratings (reuse existing logic)
            players_with_ratings = self.get_all_players_with_rankings(active_days=active_days)
            
            # Extract ratings (only for players with ratings)
            ratings = []
            for player in players_with_ratings:
                rating = player.get('current_rating')
                if rating is not None and rating != '':
                    try:
                        rating_int = int(rating)
                        ratings.append(rating_int)
                    except (ValueError, TypeError):
                        continue
            
            if not ratings:
                return {
                    'buckets': [],
                    'min_rating': 0,
                    'max_rating': 0,
                    'total_players': 0
                }
            
            # Calculate histogram buckets
            min_rating = min(ratings)
            max_rating = max(ratings)
            
            # Create buckets: every 50 points (e.g., 0-50, 50-100, 100-150, etc.)
            # Round min down to nearest 50 and max up to nearest 50
            bucket_size = 50
            bucket_min = (min_rating // bucket_size) * bucket_size
            bucket_max = ((max_rating // bucket_size) + 1) * bucket_size
            
            # Initialize buckets
            num_buckets = (bucket_max - bucket_min) // bucket_size
            buckets = [0] * num_buckets
            
            # Count ratings in each bucket
            for rating in ratings:
                bucket_index = (rating - bucket_min) // bucket_size
                # Handle edge case where rating equals bucket_max
                if bucket_index >= num_buckets:
                    bucket_index = num_buckets - 1
                buckets[bucket_index] += 1
            
            # Format bucket labels
            bucket_labels = []
            for i in range(num_buckets):
                start = bucket_min + (i * bucket_size)
                end = start + bucket_size
                bucket_labels.append(f"{start}-{end}")
            
            return {
                'buckets': buckets,
                'labels': bucket_labels,
                'min_rating': min_rating,
                'max_rating': max_rating,
                'total_players': len(ratings),
                'bucket_size': bucket_size
            }
        except Exception as e:
            print(f"Error getting rating distribution: {e}")
            import traceback
            traceback.print_exc()
            return {
                'buckets': [],
                'labels': [],
                'min_rating': 0,
                'max_rating': 0,
                'total_players': 0,
                'bucket_size': 50
            }
    
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
                .select('tournament_id,tournament_date')
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
                        'tournament_date': stat.get('tournament_date')
                    })
            
            return tournaments
        except Exception as e:
            print(f"Error getting tournaments for {player_name}: {e}")
            return []
    
    def get_all_tournaments_with_attendance(self, player_name: str) -> List[Dict]:
        """Get all tournaments with attendance information for a specific player"""
        try:
            # Get all tournaments
            tournaments_result = (
                self.client.table('tournaments')
                .select('id,date,source_url')
                .order('date', desc=True)
                .execute()
            )
            
            if not tournaments_result.data:
                return []
            
            # Get tournaments this player attended
            player_tournaments_result = (
                self.client.table('player_stats_view')
                .select('tournament_id')
                .eq('player_name', player_name)
                .execute()
            )
            
            # Create a set of tournament IDs the player attended
            attended_tournament_ids = set()
            if player_tournaments_result.data:
                for stat in player_tournaments_result.data:
                    tournament_id = stat.get('tournament_id')
                    if tournament_id is not None:
                        attended_tournament_ids.add(int(tournament_id))
            
            # Build result with attendance info
            tournaments = []
            for tournament in tournaments_result.data:
                tournament_id = int(tournament.get('id')) if tournament.get('id') is not None else None
                if tournament_id is None:
                    continue
                
                tournaments.append({
                    'tournament_id': tournament_id,
                    'tournament_date': tournament.get('date'),
                    'source_url': tournament.get('source_url'),
                    'attended': tournament_id in attended_tournament_ids
                })
            
            return tournaments
        except Exception as e:
            print(f"Error getting tournaments with attendance for {player_name}: {e}")
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
            return []
    
    def get_head_to_head_matches_paginated(self, player1_name: str, player2_name: str, page: int = 1, page_size: int = 20) -> Dict:
        """Get paginated head-to-head matches between two players with ratings at match time (OPTIMIZED)"""
        try:
            import math
            
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
            
            # Execute both queries (these are fast, main bottleneck is rating queries)
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
            
            # Calculate total head-to-head statistics (across all matches, not just current page)
            player1_wins = 0
            player2_wins = 0
            for match in unique_matches:
                winner_name = match.get('winner_name')
                if winner_name == player1_name:
                    player1_wins += 1
                elif winner_name == player2_name:
                    player2_wins += 1
                # Draws are ignored
            
            # Calculate pagination
            total = len(unique_matches)
            total_pages = math.ceil(total / page_size) if page_size > 0 else 1
            
            # Apply pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            paginated_matches = unique_matches[start_idx:end_idx]
            
            # OPTIMIZATION: Batch fetch all ratings to eliminate N+1 query problem
            # Collect all unique (tournament_id, group_id, player_id) combinations from paginated matches
            rating_keys = set()
            for match in paginated_matches:
                tournament_id = match.get('tournament_id')
                group_id = match.get('group_id')
                player1_id = match.get('player1_id')
                player2_id = match.get('player2_id')
                
                if tournament_id and group_id and player1_id and player2_id:
                    rating_keys.add((tournament_id, group_id, player1_id))
                    rating_keys.add((tournament_id, group_id, player2_id))
            
            # Batch fetch all ratings - fetch all stats for all tournament/group/player combinations at once
            ratings_cache = {}
            if rating_keys:
                # Group by tournament_id and group_id to batch fetch player stats
                # Since we need to match on (tournament_id, group_id, player_id), we'll batch by tournament/group
                tournament_groups = {}
                for tournament_id, group_id, player_id in rating_keys:
                    key = (tournament_id, group_id)
                    if key not in tournament_groups:
                        tournament_groups[key] = set()
                    tournament_groups[key].add(player_id)
                
                # Fetch ratings for each tournament/group combination
                for (tournament_id, group_id), player_ids in tournament_groups.items():
                    try:
                        # Fetch all player stats for this tournament/group in one query
                        stats_result = (
                            self.client.table('player_tournament_stats')
                            .select('player_id,tournament_id,group_id,rating_pre,rating_post,rating_change')
                            .eq('tournament_id', tournament_id)
                            .eq('group_id', group_id)
                            .in_('player_id', list(player_ids))
                            .execute()
                        )
                        
                        if stats_result.data:
                            for stat in stats_result.data:
                                player_id = stat.get('player_id')
                                key = (tournament_id, group_id, player_id)
                                ratings_cache[key] = stat
                    except Exception as e:
                        # If batch query fails (e.g., too many players), fall back to individual queries
                        print(f"Batch query failed for tournament {tournament_id}, group {group_id}: {e}")
                        for player_id in player_ids:
                            try:
                                stats = (
                                    self.client.table('player_tournament_stats')
                                    .select('player_id,tournament_id,group_id,rating_pre,rating_post,rating_change')
                                    .eq('player_id', player_id)
                                    .eq('tournament_id', tournament_id)
                                    .eq('group_id', group_id)
                                    .execute()
                                )
                                if stats.data and len(stats.data) > 0:
                                    key = (tournament_id, group_id, player_id)
                                    ratings_cache[key] = stats.data[0]
                            except Exception as e2:
                                print(f"Error getting rating for player {player_id}, tournament {tournament_id}, group {group_id}: {e2}")
                                continue
            
            # Attach ratings to matches from cache
            for match in paginated_matches:
                tournament_id = match.get('tournament_id')
                group_id = match.get('group_id')
                player1_id = match.get('player1_id')
                player2_id = match.get('player2_id')
                
                if tournament_id and group_id and player1_id and player2_id:
                    # Get player1 rating from cache
                    key1 = (tournament_id, group_id, player1_id)
                    if key1 in ratings_cache:
                        stats1 = ratings_cache[key1]
                        match['player1_rating'] = stats1.get('rating_pre')
                        match['player1_rating_post'] = stats1.get('rating_post')
                        match['player1_rating_change'] = stats1.get('rating_change')
                    
                    # Get player2 rating from cache
                    key2 = (tournament_id, group_id, player2_id)
                    if key2 in ratings_cache:
                        stats2 = ratings_cache[key2]
                        match['player2_rating'] = stats2.get('rating_pre')
                        match['player2_rating_post'] = stats2.get('rating_post')
                        match['player2_rating_change'] = stats2.get('rating_change')
            
            return {
                'matches': paginated_matches,
                'total': total,
                'total_pages': total_pages,
                'page': page,
                'page_size': page_size,
                'player1_wins': player1_wins,
                'player2_wins': player2_wins
            }
        except Exception as e:
            print(f"Error getting head-to-head matches for {player1_name} vs {player2_name}: {e}")
            import traceback
            traceback.print_exc()
            return {
                'matches': [],
                'total': 0,
                'total_pages': 0,
                'page': page,
                'page_size': page_size,
                'player1_wins': 0,
                'player2_wins': 0
            }
    
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
            return []
    
    def get_performance_vs_rating_ranges(self, player_name: str, days_back: Optional[int] = None) -> Dict:
        """Get win rate performance against different rating ranges"""
        try:
            from datetime import datetime, timedelta
            
            # Get all matches for the player
            query1 = (
                self.client.table('match_results_view')
                .select('match_id,winner_name,tournament_id,group_id,player1_id,player2_id,player1_name,player2_name')
                .eq('player1_name', player_name)
            )
            
            query2 = (
                self.client.table('match_results_view')
                .select('match_id,winner_name,tournament_id,group_id,player1_id,player2_id,player1_name,player2_name')
                .eq('player2_name', player_name)
            )
            
            # Apply date filter if needed
            if days_back:
                cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
                query1 = query1.gte('tournament_date', cutoff_date)
                query2 = query2.gte('tournament_date', cutoff_date)
            
            result1 = query1.execute()
            result2 = query2.execute()
            
            # Combine results
            all_matches = []
            if result1.data:
                all_matches.extend(result1.data)
            if result2.data:
                all_matches.extend(result2.data)
            
            # Remove duplicates
            seen_ids = set()
            unique_matches = []
            for match in all_matches:
                match_id = match.get('match_id')
                if match_id and match_id not in seen_ids:
                    seen_ids.add(match_id)
                    unique_matches.append(match)
            
            # Initialize range buckets
            ranges = {
                '100_plus_higher': {'wins': 0, 'total': 0},
                '50_100_higher': {'wins': 0, 'total': 0},
                'similar': {'wins': 0, 'total': 0},
                '50_100_lower': {'wins': 0, 'total': 0},
                '100_plus_lower': {'wins': 0, 'total': 0}
            }
            
            if not unique_matches:
                # Return empty results if no matches
                result = {}
                for range_key in ranges.keys():
                    result[range_key] = {'wins': 0, 'total': 0, 'win_rate': 0.0}
                return result
            
            # OPTIMIZED: Batch fetch all ratings at once
            # Collect all unique (player_id, tournament_id, group_id) tuples we need
            needed_keys = set()
            match_info = []  # Store match info with keys for lookup
            
            for match in unique_matches:
                tournament_id = match.get('tournament_id')
                group_id = match.get('group_id')
                player1_id = match.get('player1_id')
                player2_id = match.get('player2_id')
                winner_name = match.get('winner_name')
                
                if not (tournament_id and group_id and player1_id and player2_id):
                    continue
                
                # Determine if player is player1 or player2
                is_player1 = match.get('player1_name') == player_name
                opponent_id = player2_id if is_player1 else player1_id
                player_id = player1_id if is_player1 else player2_id
                
                # Add keys for both player and opponent
                needed_keys.add((player_id, tournament_id, group_id))
                needed_keys.add((opponent_id, tournament_id, group_id))
                
                match_info.append({
                    'player_id': player_id,
                    'opponent_id': opponent_id,
                    'tournament_id': tournament_id,
                    'group_id': group_id,
                    'winner_name': winner_name
                })
            
            # Build rating lookup map using batched queries
            rating_lookup = {}
            
            # Get all unique tournaments and player IDs
            tournament_ids = list(set([m['tournament_id'] for m in match_info]))
            all_player_ids = set()
            for m in match_info:
                all_player_ids.add(m['player_id'])
                all_player_ids.add(m['opponent_id'])
            all_player_ids = list(all_player_ids)
            
            # Batch fetch ratings (Supabase .in_() limit is ~100)
            tournament_batch_size = 50
            player_batch_size = 50
            
            try:
                for t_batch_start in range(0, len(tournament_ids), tournament_batch_size):
                    tournament_batch = tournament_ids[t_batch_start:t_batch_start + tournament_batch_size]
                    
                    for p_batch_start in range(0, len(all_player_ids), player_batch_size):
                        player_batch = all_player_ids[p_batch_start:p_batch_start + player_batch_size]
                        
                        try:
                            # Fetch all stats for this batch
                            batch_stats = (
                                self.client.table('player_tournament_stats')
                                .select('player_id,tournament_id,group_id,rating_pre')
                                .in_('tournament_id', tournament_batch)
                                .in_('player_id', player_batch)
                                .execute()
                            )
                            
                            if batch_stats.data:
                                for stat in batch_stats.data:
                                    key = (stat['player_id'], stat['tournament_id'], stat['group_id'])
                                    if key in needed_keys:
                                        rating_lookup[key] = stat.get('rating_pre')
                        except Exception as e:
                            # If batch query fails, fall back to per-tournament queries
                            print(f"Error batch fetching ratings (trying fallback): {e}")
                            for tournament_id in tournament_batch:
                                tournament_player_ids = list(set([
                                    m['player_id'] for m in match_info 
                                    if m['tournament_id'] == tournament_id
                                ] + [
                                    m['opponent_id'] for m in match_info 
                                    if m['tournament_id'] == tournament_id
                                ]))
                                
                                for p_batch_start in range(0, len(tournament_player_ids), player_batch_size):
                                    batch_player_ids = tournament_player_ids[p_batch_start:p_batch_start + player_batch_size]
                                    
                                    try:
                                        batch_stats = (
                                            self.client.table('player_tournament_stats')
                                            .select('player_id,tournament_id,group_id,rating_pre')
                                            .eq('tournament_id', tournament_id)
                                            .in_('player_id', batch_player_ids)
                                            .execute()
                                        )
                                        
                                        if batch_stats.data:
                                            for stat in batch_stats.data:
                                                key = (stat['player_id'], stat['tournament_id'], stat['group_id'])
                                                if key in needed_keys:
                                                    rating_lookup[key] = stat.get('rating_pre')
                                    except Exception as e2:
                                        print(f"Error in fallback query for tournament {tournament_id}: {e2}")
                                        continue
                            break  # Exit player batch loop, continue with next tournament batch
            except Exception as e:
                print(f"Error batch fetching ratings: {e}")
            
            # Process matches using the lookup map
            for match in match_info:
                player_id = match['player_id']
                opponent_id = match['opponent_id']
                tournament_id = match['tournament_id']
                group_id = match['group_id']
                winner_name = match['winner_name']
                
                # Get ratings from lookup
                player_key = (player_id, tournament_id, group_id)
                opponent_key = (opponent_id, tournament_id, group_id)
                
                player_rating = rating_lookup.get(player_key)
                opponent_rating = rating_lookup.get(opponent_key)
                
                if player_rating is None or opponent_rating is None:
                    continue
                
                try:
                    player_rating_int = int(player_rating)
                    opponent_rating_int = int(opponent_rating)
                except (ValueError, TypeError):
                    continue
                
                # Calculate rating difference (opponent - player)
                rating_diff = opponent_rating_int - player_rating_int
                
                # Skip draws
                if winner_name is None:
                    continue
                
                # Determine which range this match belongs to
                range_key = None
                if rating_diff >= 100:
                    range_key = '100_plus_higher'
                elif rating_diff >= 50:
                    range_key = '50_100_higher'
                elif rating_diff >= -50:
                    range_key = 'similar'
                elif rating_diff >= -100:
                    range_key = '50_100_lower'
                else:
                    range_key = '100_plus_lower'
                
                # Update range stats
                ranges[range_key]['total'] += 1
                if winner_name == player_name:
                    ranges[range_key]['wins'] += 1
            
            # Calculate win rates
            result = {}
            for range_key, stats in ranges.items():
                win_rate = 0.0
                if stats['total'] > 0:
                    win_rate = round((stats['wins'] / stats['total']) * 100, 1)
                
                result[range_key] = {
                    'wins': stats['wins'],
                    'total': stats['total'],
                    'win_rate': win_rate
                }
            
            return result
        except Exception as e:
            print(f"Error getting performance vs rating ranges for {player_name}: {e}")
            import traceback
            traceback.print_exc()
            return {
                '100_plus_higher': {'wins': 0, 'total': 0, 'win_rate': 0.0},
                '50_100_higher': {'wins': 0, 'total': 0, 'win_rate': 0.0},
                'similar': {'wins': 0, 'total': 0, 'win_rate': 0.0},
                '50_100_lower': {'wins': 0, 'total': 0, 'win_rate': 0.0},
                '100_plus_lower': {'wins': 0, 'total': 0, 'win_rate': 0.0}
            }

