"""
Round Robin PDF Parser
Parses round robin tournament results from PDF files
"""
import re
import pdfplumber
import requests
from typing import List, Dict, Optional
from datetime import datetime
from io import BytesIO


class RoundRobinPDFParser:
    """Parser for extracting round robin tournament results from PDF files"""
    
    def __init__(self):
        self.tables = []
        self.text_content = ""
    
    def parse_url(self, url: str) -> Dict:
        """
        Parse round robin results from a PDF URL
        
        Args:
            url: URL to the PDF file
            
        Returns:
            Dictionary containing tournament info and all groups with players and matches
        """
        response = requests.get(url)
        response.raise_for_status()
        
        pdf_bytes = BytesIO(response.content)
        return self._parse_pdf(pdf_bytes, url)
    
    def parse_file(self, file_path: str) -> Dict:
        """
        Parse round robin results from a local PDF file
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            Dictionary containing tournament info and all groups with players and matches
        """
        with open(file_path, 'rb') as f:
            pdf_bytes = BytesIO(f.read())
        
        return self._parse_pdf(pdf_bytes, file_path)
    
    def _parse_pdf(self, pdf_bytes: BytesIO, source: str) -> Dict:
        """Extract all data from the PDF"""
        with pdfplumber.open(pdf_bytes) as pdf:
            # Extract text from all pages
            self.text_content = ""
            self.tables = []
            
            for page in pdf.pages:
                self.text_content += page.extract_text() or ""
                page_tables = page.extract_tables()
                if page_tables:
                    self.tables.extend(page_tables)
        
        # Extract tournament information
        tournament_info = self._extract_tournament_info(source)
        
        # Extract all groups
        groups = self._extract_all_groups()
        
        return {
            'tournament': tournament_info,
            'groups': groups
        }
    
    def _extract_tournament_info(self, source: str) -> Dict:
        """Extract tournament name and date from the PDF"""
        tournament_info = {}
        
        # Try to extract date from filename/URL first
        date_match = re.search(r'(\d{4})([a-z]{3})(\d{2})', source, re.I)
        if not date_match:
            # Try from PDF text
            date_match = re.search(r'(\d{4})([a-z]{3})(\d{2})', self.text_content[:200], re.I)
        
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
                tournament_info['date'] = date.isoformat()
                tournament_info['date_string'] = date.strftime('%Y %b %d')
                tournament_info['name'] = f"BTTC Round Robin results for {tournament_info['date_string']}"
            except ValueError:
                pass
        
        return tournament_info
    
    def _extract_all_groups(self) -> List[Dict]:
        """Extract all round robin groups from the PDF tables"""
        groups = []
        
        # Each table represents a group
        # Tables start with "#\n1", "#\n2", etc. in the first cell
        for table in self.tables:
            if not table or len(table) < 3:
                continue
            
            # Check if this is a group table (has # in first cell)
            first_row = table[0]
            if not first_row or len(first_row) < 1:
                continue
            
            first_cell = str(first_row[0] or '').strip()
            group_match = re.search(r'#\s*(\d+)', first_cell, re.I)
            
            if group_match:
                group_number = int(group_match.group(1))
                group_data = self._extract_group_from_table(table, group_number)
                if group_data:
                    groups.append(group_data)
        
        return groups
    
    def _extract_group_from_table(self, table: List[List], group_number: int) -> Optional[Dict]:
        """Extract a single group from a table"""
        if not table or len(table) < 3:
            return None
        
        # Row 0: Header with "#\n1", "Name", "Rating\nPre", etc.
        # Row 1: Sub-header with opponent numbers
        # Row 2: Main data row with player info
        # Rows 3+: Additional score rows
        
        players = self._extract_players_from_table(table)
        matches = self._extract_matches_from_table(table, players)
        
        return {
            'group_number': group_number,
            'group_name': f"#{group_number}",
            'players': players,
            'matches': matches
        }
    
    def _extract_players_from_table(self, table: List[List]) -> List[Dict]:
        """Extract players from a table"""
        players = []
        
        if len(table) < 3:
            return []
        
        # Row 2 contains the main player data
        # Column 0: Player numbers (stacked: "1\n2\n3\n4\n5\n6")
        # Column 1: Names and ratings (stacked: "Name RatingPre RatingPost\n...")
        # Columns 2+: Statistics
        
        row2 = table[2]
        if not row2 or len(row2) < 2:
            return []
        
        # Extract player numbers from column 0
        player_nums_cell = str(row2[0] or '').strip()
        player_numbers = []
        for line in player_nums_cell.split('\n'):
            num = line.strip()
            if num.isdigit():
                player_numbers.append(int(num))
        
        # Extract names and ratings from column 1
        names_ratings_cell = str(row2[1] or '').strip()
        names_ratings_lines = [line.strip() for line in names_ratings_cell.split('\n') if line.strip()]
        
        # Extract statistics from later columns
        # Column indices vary, but typically:
        # - Matches Won, Games Won, Rating Change, etc. are in later columns
        
        # Parse each player
        for i, player_num in enumerate(player_numbers):
            if i >= len(names_ratings_lines):
                continue
            
            player_data = {'player_number': player_num}
            
            # Parse name and ratings from the line
            line = names_ratings_lines[i]
            
            # Extract ratings (3-4 digit numbers)
            rating_match = re.search(r'(\d{3,4})\s+(\d{3,4})', line)
            if rating_match:
                player_data['rating_pre'] = int(rating_match.group(1))
                player_data['rating_post'] = int(rating_match.group(2))
                player_data['rating_change'] = player_data['rating_post'] - player_data['rating_pre']
            
            # Extract name (everything before the ratings)
            name_match = re.match(r'^([A-Za-z\s]+?)(?:\s+\d{3,4})', line)
            if name_match:
                player_data['name'] = name_match.group(1).strip()
            else:
                # Fallback: take everything before first number
                name_match = re.match(r'^([A-Za-z\s]+)', line)
                if name_match:
                    player_data['name'] = name_match.group(1).strip()
            
            # Try to extract statistics from later columns
            # Look for columns with matches won, games won, etc.
            for col_idx in range(2, min(len(row2), 25)):
                cell = str(row2[col_idx] or '').strip()
                if not cell:
                    continue
                
                # Check if this column contains statistics
                # The structure varies, so we'll try to extract what we can
                # Matches won, games won are typically single numbers
                if cell.isdigit():
                    # This might be matches won or games won
                    # We'll need to infer from context
                    pass
            
            if player_data.get('name'):
                players.append(player_data)
        
        return players
    
    def _extract_matches_from_table(self, table: List[List], players: List[Dict]) -> List[Dict]:
        """Extract matches from a table"""
        matches = []
        player_map = {p['player_number']: p for p in players}
        
        if len(table) < 3:
            return []
        
        # Row 1 contains opponent numbers in pairs (1, 1, 2, 2, 3, 3, ...)
        # Row 2 contains player 1's scores vs each opponent
        # Row 3 contains player 2's scores vs each opponent
        # etc.
        
        row1 = table[1]  # Opponent numbers
        if not row1 or len(row1) < 4:
            return []
        
        # Find score columns (start after name/rating columns)
        score_start_col = 4  # After #, Name, Rating Pre, Rating Post
        
        # Extract opponent numbers from row 1 (they come in pairs)
        # Map column index to opponent number
        col_to_opponent = {}
        for col_idx in range(score_start_col, len(row1)):
            cell = str(row1[col_idx] or '').strip()
            if cell.isdigit():
                opp_num = int(cell)
                col_to_opponent[col_idx] = opp_num
        
        # Extract matches from all data rows
        # Row 2 contains player 1's matches vs all opponents
        # Rows 3+ contain matches from other players' perspectives
        
        # Track which matchups we've already extracted
        extracted_matchups = set()
        
        # First, extract matches from row 2 (player 1's row)
        row2 = table[2] if len(table) > 2 else None
        if row2:
            player1_num = players[0]['player_number'] if players else None
            if player1_num and player1_num in player_map:
                for col_idx in range(score_start_col, min(len(row2), len(row1))):
                    if col_idx not in col_to_opponent:
                        continue
                    
                    opponent_num = col_to_opponent[col_idx]
                    if opponent_num not in player_map or player1_num >= opponent_num:
                        continue
                    
                    score_cell = str(row2[col_idx] or '').strip()
                    if not score_cell or score_cell == '+':
                        continue
                    
                    # Extract match from score
                    match = self._extract_match_from_score(score_cell, player1_num, opponent_num, player_map)
                    if match:
                        matches.append(match)
                        matchup = (player1_num, opponent_num)
                        extracted_matchups.add(matchup)
        
        # Then extract matches from rows 3+ (other players' matches)
        # These rows contain scores from different players' perspectives
        # Each row typically contains one match, and the column tells us the opponent number
        # We need to determine which player each row belongs to
        
        for row_idx in range(3, len(table)):
            row = table[row_idx]
            if not row or len(row) < score_start_col:
                continue
            
            # Find all scores in this row
            row_scores = []
            for col_idx in range(score_start_col, min(len(row), len(row1))):
                if col_idx not in col_to_opponent:
                    continue
                
                opponent_num = col_to_opponent[col_idx]
                if opponent_num not in player_map:
                    continue
                
                # Check both columns of the pair
                for check_col in [col_idx, col_idx + 1]:
                    if check_col >= len(row):
                        break
                    if check_col not in col_to_opponent:
                        continue
                    if col_to_opponent[check_col] != opponent_num:
                        continue
                    
                    score_cell = str(row[check_col] or '').strip()
                    if score_cell and score_cell != '+':
                        row_scores.append((check_col, opponent_num, score_cell))
                        break  # Found score in this pair, move on
            
            # For each score in this row, try to match it to a player
            for col_idx, opponent_num, score_cell in row_scores:
                # Try all players to see which one this match belongs to
                # The row structure suggests that each row contains matches from a specific player's perspective
                # We'll try all players and see which matchup makes sense
                
                best_match = None
                best_matchup = None
                
                for player in players:
                    player_num = player['player_number']
                    
                    # Skip if same player (diagonal)
                    if player_num == opponent_num:
                        continue
                    
                    # Determine the matchup (always use lower number first)
                    if player_num < opponent_num:
                        matchup = (player_num, opponent_num)
                        swap_scores = False
                    else:
                        matchup = (opponent_num, player_num)
                        swap_scores = True
                    
                    # Check if we already have this matchup
                    if matchup in extracted_matchups:
                        continue
                    
                    # Try to extract match from this score
                    if swap_scores:
                        match = self._extract_match_from_score(score_cell, opponent_num, player_num, player_map, swap=True)
                    else:
                        match = self._extract_match_from_score(score_cell, player_num, opponent_num, player_map)
                    
                    if match:
                        # Prefer matches where the player number is close to the row index
                        # (row 3 might be player 2, row 4 might be player 3, etc.)
                        if best_match is None:
                            best_match = match
                            best_matchup = matchup
                        else:
                            # Prefer the match where player_num is closer to (row_idx - 1)
                            row_player_hint = row_idx - 1
                            current_diff = abs(player_num - row_player_hint)
                            best_diff = abs(best_match['player1_number'] - row_player_hint)
                            if current_diff < best_diff:
                                best_match = match
                                best_matchup = matchup
                
                # Add the best match found
                if best_match and best_matchup:
                    matches.append(best_match)
                    extracted_matchups.add(best_matchup)
        
        return matches
    
    def _extract_match_from_score(self, score_cell: str, player1_num: int, player2_num: int, player_map: Dict, swap: bool = False) -> Optional[Dict]:
        """Extract a match from a score cell"""
        if not score_cell or score_cell == '+':
            return None
        
        # Parse score - might be single line "3 1" or stacked "3 1\n2 3\n..."
        score_lines = [s.strip() for s in score_cell.split('\n') if s.strip()]
        
        # Use the first valid score line
        p1_games = None
        p2_games = None
        
        for line in score_lines:
            # Skip if it looks like statistics (large numbers)
            if len(line) > 10:  # Statistics are usually longer
                continue
            
            score_match = re.match(r'^(\d+)\s+(\d+)$', line)
            if score_match:
                p1 = int(score_match.group(1))
                p2 = int(score_match.group(2))
                
                # Validate: game scores should be reasonable (0-3 typically)
                if 0 <= p1 <= 5 and 0 <= p2 <= 5:
                    if swap:
                        # Swap scores because we're viewing from player2's perspective
                        p1_games = p2
                        p2_games = p1
                    else:
                        p1_games = p1
                        p2_games = p2
                    break
        
        if p1_games is not None and p2_games is not None:
            # Ensure player1_num < player2_num for consistency
            if player1_num > player2_num:
                player1_num, player2_num = player2_num, player1_num
                p1_games, p2_games = p2_games, p1_games
            
            return {
                'player1_number': player1_num,
                'player1_name': player_map[player1_num]['name'],
                'player2_number': player2_num,
                'player2_name': player_map[player2_num]['name'],
                'player1_score': p1_games,
                'player2_score': p2_games
            }
        
        return None
