"""
Supabase Database Client
Handles connection and operations with Supabase
"""
from supabase import create_client, Client
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv

load_dotenv()


class SupabaseClient:
    """Client for interacting with Supabase database"""
    
    def __init__(self):
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        if not supabase_url or not supabase_key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY must be set in environment variables or .env file"
            )
        
        # Note: Even if your tables are unrestricted (RLS disabled or public policies),
        # you still need both URL and anon key. The anon key is required by the Supabase
        # client library to authenticate API requests, regardless of table permissions.
        self.client: Client = create_client(supabase_url, supabase_key)
        self.table_name = 'ping_pong_matches'
    
    def create_table_if_not_exists(self):
        """
        Create the matches table if it doesn't exist.
        Note: This requires running SQL in Supabase dashboard.
        The SQL schema is provided in the README.
        """
        # Supabase Python client doesn't support DDL operations
        # Table creation must be done via Supabase dashboard or SQL editor
        pass
    
    def insert_match(self, match_data: Dict) -> Optional[Dict]:
        """
        Insert a single match into the database
        
        Args:
            match_data: Dictionary containing match information
            
        Returns:
            Inserted match data or None if failed
        """
        try:
            # Normalize the data
            normalized = self._normalize_match_data(match_data)
            
            result = self.client.table(self.table_name).insert(normalized).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            print(f"Error inserting match: {e}")
            print(f"Match data: {match_data}")
            return None
    
    def insert_matches(self, matches: List[Dict]) -> List[Dict]:
        """
        Insert multiple matches into the database
        
        Args:
            matches: List of dictionaries containing match information
            
        Returns:
            List of successfully inserted matches
        """
        if not matches:
            return []
        
        # Normalize all matches
        normalized_matches = [self._normalize_match_data(match) for match in matches]
        
        try:
            result = self.client.table(self.table_name).insert(normalized_matches).execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Error inserting matches: {e}")
            # Try inserting one by one
            inserted = []
            for match in normalized_matches:
                try:
                    result = self.client.table(self.table_name).insert(match).execute()
                    if result.data:
                        inserted.extend(result.data)
                except Exception as e2:
                    print(f"Error inserting individual match: {e2}")
            return inserted
    
    def get_all_matches(self) -> List[Dict]:
        """Retrieve all matches from the database"""
        try:
            result = self.client.table(self.table_name).select('*').execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Error retrieving matches: {e}")
            return []
    
    def get_matches_by_player(self, player_name: str) -> List[Dict]:
        """Retrieve all matches for a specific player"""
        try:
            result = (
                self.client.table(self.table_name)
                .select('*')
                .or_(f'player1.eq.{player_name},player2.eq.{player_name}')
                .execute()
            )
            return result.data if result.data else []
        except Exception as e:
            print(f"Error retrieving matches for player {player_name}: {e}")
            return []
    
    def _normalize_match_data(self, match_data: Dict) -> Dict:
        """
        Normalize match data to match database schema
        
        Args:
            match_data: Raw match data from parser
            
        Returns:
            Normalized match data
        """
        normalized = {
            'player1': match_data.get('player1', ''),
            'player2': match_data.get('player2', ''),
            'score1': match_data.get('score1', 0),
            'score2': match_data.get('score2', 0),
            'match_date': match_data.get('date', None),
            'winner': match_data.get('winner', None),
            'source': match_data.get('source', 'unknown'),
        }
        
        # Determine winner if not specified
        if not normalized['winner'] and normalized['score1'] and normalized['score2']:
            if normalized['score1'] > normalized['score2']:
                normalized['winner'] = normalized['player1']
            elif normalized['score2'] > normalized['score1']:
                normalized['winner'] = normalized['player2']
        
        return normalized

