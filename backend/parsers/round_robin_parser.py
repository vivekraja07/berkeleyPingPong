"""
Round Robin Results Parser
Parses round robin tournament results from berkeleytabletennis.org
"""
import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from datetime import datetime


class RoundRobinParser:
    """Parser for extracting round robin tournament results"""
    
    def __init__(self):
        self.soup: Optional[BeautifulSoup] = None
    
    def parse_url(self, url: str) -> Dict:
        """
        Parse round robin results from a URL
        
        Args:
            url: URL to the round robin results page
            
        Returns:
            Dictionary containing tournament info and all groups with players and matches
        """
        response = requests.get(url)
        response.raise_for_status()
        self.soup = BeautifulSoup(response.content, "html.parser")
        
        return self._parse_results()
    
    def parse_file(self, file_path: str) -> Dict:
        """
        Parse round robin results from a local HTML file
        
        Args:
            file_path: Path to the HTML file
            
        Returns:
            Dictionary containing tournament info and all groups with players and matches
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        self.soup = BeautifulSoup(html_content, "html.parser")
        return self._parse_results()
    
    def _parse_results(self) -> Dict:
        """Extract all data from the parsed HTML"""
        # Extract tournament information
        tournament_info = self._extract_tournament_info()
        
        # Find all round robin groups
        groups = self._extract_all_groups()
        
        return {
            'tournament': tournament_info,
            'groups': groups
        }
    
    def _extract_tournament_info(self) -> Dict:
        """Extract tournament name and date from the page"""
        tournament_info = {}
        
        # Find the h1 tag with tournament name
        h1 = self.soup.find("h1")
        if h1:
            tournament_name = h1.get_text(strip=True)
            tournament_info['name'] = tournament_name
            
            # Extract date from tournament name
            date_match = re.search(r"for (\d{4} \w+ \d+)", tournament_name)
            if date_match:
                date_str = date_match.group(1)  # "2025 Nov 7"
                try:
                    tournament_date = datetime.strptime(date_str, "%Y %b %d")
                    tournament_info['date'] = tournament_date.isoformat()
                    tournament_info['date_string'] = date_str
                except ValueError:
                    tournament_info['date_string'] = date_str
        
        return tournament_info
    
    def _extract_all_groups(self) -> List[Dict]:
        """Extract all round robin groups from the page"""
        groups = []
        
        # Find all bracket containers (each bracket is a group)
        brackets = self.soup.find_all("div", class_="bracket")
        
        for bracket in brackets:
            group_data = self._extract_group_from_bracket(bracket)
            if group_data:
                groups.append(group_data)
        
        return groups
    
    def _extract_group_from_bracket(self, bracket) -> Optional[Dict]:
        """Extract data for a single round robin group from a bracket div"""
        group_header = self._find_group_header(bracket)
        if not group_header:
            return None
        
        group_name = group_header.get_text(strip=True)
        group_number = self._extract_group_number(group_name)
        
        players = self._extract_players_from_bracket(bracket)
        matches = self._extract_matches_from_bracket(bracket, players)
        
        return {
            'group_number': group_number,
            'group_name': group_name,
            'players': players,
            'matches': matches
        }
    
    def _find_group_header(self, bracket) -> Optional:
        """Find the group header in a bracket"""
        group_header = bracket.find("div", class_="row-header", string=re.compile(r'^#\d+$'))
        if group_header:
            return group_header
        
        # Fallback: find by text
        group_header = bracket.find("div", class_="row-header")
        if group_header:
            text = group_header.get_text(strip=True)
            if text.startswith("#"):
                return group_header
        
        return None
    
    def _extract_group_number(self, group_name: str) -> Optional[int]:
        """Extract group number from group name (e.g., '#1' -> 1)"""
        match = re.search(r'#(\d+)', group_name)
        return int(match.group(1)) if match else None
    
    def _extract_players_from_bracket(self, bracket) -> List[Dict]:
        """Extract all players and their statistics from a bracket"""
        players = []
        col_1_divs = bracket.find_all("div", class_="col-1")
        
        for col_1_div in col_1_divs:
            player_data = self._extract_player_from_col1(col_1_div)
            if player_data:
                players.append(player_data)
        
        return players
    
    def _extract_player_from_col1(self, col_1_div) -> Optional[Dict]:
        """Extract player data from a col-1 div"""
        row = col_1_div.find("div", class_="row")
        if not row or row.find("div", class_="row-header"):
            return None  # Skip header rows
        
        player_num_text = row.get_text(strip=True)
        if not player_num_text.isdigit():
            return None
        
        player_num = int(player_num_text)
        player_data = {'player_number': player_num}
        
        # Extract data from sibling columns
        current = col_1_div.find_next_sibling()
        while current:
            if not self._is_valid_div(current):
                current = current.find_next_sibling()
                continue
            
            classes = current.get('class', [])
            
            # Stop when we hit the next player's col-1
            if 'col-1' in classes:
                break
            
            # Extract data based on column type
            self._extract_column_data(current, classes, player_data)
            
            current = current.find_next_sibling()
        
        return player_data if len(player_data) > 1 else None  # Must have more than just player_number
    
    def _is_valid_div(self, element) -> bool:
        """Check if element is a valid div element"""
        return hasattr(element, 'get') and element.name == 'div'
    
    def _extract_column_data(self, column, classes: List[str], player_data: Dict) -> None:
        """Extract data from a column based on its classes"""
        row = column.find("div", class_="row")
        if not row:
            return
        
        text = row.get_text(strip=True)
        
        if 'names' in classes:
            player_data['name'] = text
        
        elif 'rating-pre' in classes:
            player_data['rating_pre'] = self._parse_int(text)
        
        elif 'rating-post' in classes:
            player_data['rating_post'] = self._parse_int(text)
        
        elif 'matches-won' in classes:
            player_data['matches_won'] = self._parse_int(text)
        
        elif 'games-won' in classes:
            player_data['games_won'] = self._parse_int(text)
        
        elif 'rating-change' in classes and 'rating-change-vs' not in classes:
            player_data['rating_change'] = self._parse_signed_int(text)
        
        elif 'bonus-points' in classes:
            player_data['bonus_points'] = self._parse_int(text)
        
        elif 'total-change' in classes:
            player_data['change_w_bonus'] = self._parse_signed_int(text)
    
    def _parse_int(self, text: str) -> Optional[int]:
        """Parse an integer from text, returning None if invalid"""
        try:
            return int(text) if text.isdigit() else None
        except (ValueError, AttributeError):
            return None
    
    def _parse_signed_int(self, text: str) -> Optional[int]:
        """Parse a signed integer from text (e.g., '+12', '-8', '5')"""
        if not text:
            return None
        
        try:
            if text.startswith('+'):
                return int(text[1:])
            elif text.startswith('-'):
                return -int(text[1:])
            else:
                return int(text)
        except (ValueError, AttributeError):
            return None
    
    def _extract_matches_from_bracket(self, bracket, players: List[Dict]) -> List[Dict]:
        """Extract match results from a bracket"""
        matches = []
        player_map = {p['player_number']: p for p in players}
        col_1_divs = bracket.find_all("div", class_="col-1")
        
        for col_1_div in col_1_divs:
            player_num = self._get_player_number_from_col1(col_1_div)
            if not player_num or player_num not in player_map:
                continue
            
            games_col = self._find_games_column(col_1_div)
            if games_col:
                player_matches = self._extract_matches_from_games_column(
                    games_col, player_num, player_map
                )
                matches.extend(player_matches)
        
        return matches
    
    def _get_player_number_from_col1(self, col_1_div) -> Optional[int]:
        """Extract player number from a col-1 div"""
        row = col_1_div.find("div", class_="row")
        if not row or row.find("div", class_="row-header"):
            return None
        
        player_num_text = row.get_text(strip=True)
        try:
            return int(player_num_text) if player_num_text.isdigit() else None
        except (ValueError, AttributeError):
            return None
    
    def _find_games_column(self, col_1_div) -> Optional:
        """Find the games column (sibling of col-1)"""
        current = col_1_div.find_next_sibling()
        
        while current:
            if not self._is_valid_div(current):
                current = current.find_next_sibling()
                continue
            
            classes = current.get('class', [])
            
            if 'games' in classes and 'games-won' not in classes:
                return current
            
            if 'col-1' in classes:
                break  # Hit next player
            
            current = current.find_next_sibling()
        
        return None
    
    def _extract_matches_from_games_column(
        self, games_col, player_num: int, player_map: Dict
    ) -> List[Dict]:
        """Extract matches from a games column"""
        matches = []
        game_row = games_col.find("div", class_="row")
        
        if not game_row:
            return matches
        
        score_divs = game_row.find_all("div", class_="score")
        
        for opp_idx, score_div in enumerate(score_divs):
            if "empty" in score_div.get("class", []):
                continue
            
            nums = score_div.find_all("div", class_="num")
            if len(nums) < 2:
                continue
            
            p1_games_text = nums[0].get_text(strip=True)
            p2_games_text = nums[1].get_text(strip=True)
            
            # Skip if either is "+" or empty
            if p1_games_text == "+" or p2_games_text == "+" or not p1_games_text or not p2_games_text:
                continue
            
            try:
                p1_games = int(p1_games_text)
                p2_games = int(p2_games_text)
                opponent_num = opp_idx + 1
                
                # Only add match if opponent exists and player_num < opponent_num (avoid duplicates)
                if opponent_num in player_map and player_num < opponent_num:
                    matches.append({
                        'player1_number': player_num,
                        'player1_name': player_map[player_num]['name'],
                        'player2_number': opponent_num,
                        'player2_name': player_map[opponent_num]['name'],
                        'player1_score': p1_games,
                        'player2_score': p2_games
                    })
            except ValueError:
                continue
        
        return matches
    


def main():
    """Example usage"""
    parser = RoundRobinParser()
    url = "https://berkeleytabletennis.org/results/rr_results_2025nov07"
    
    print(f"Parsing round robin results from: {url}")
    results = parser.parse_url(url)
    
    print(f"\nTournament: {results['tournament'].get('name', 'Unknown')}")
    print(f"Date: {results['tournament'].get('date_string', 'Unknown')}")
    print(f"\nFound {len(results['groups'])} groups")
    
    for group in results['groups']:
        print(f"\n--- Group {group['group_number']} ---")
        print(f"Players: {len(group['players'])}")
        print(f"Matches: {len(group['matches'])}")
        
        print("\nPlayers:")
        for player in group['players']:
            print(f"  {player.get('player_number')}. {player.get('name')} - "
                  f"Rating: {player.get('rating_pre')} -> {player.get('rating_post')} "
                  f"({player.get('rating_change', 0):+d})")
        
        print("\nMatches:")
        for match in group['matches']:
            print(f"  {match['player1_name']} vs {match['player2_name']}: "
                  f"{match['player1_score']}-{match['player2_score']}")


if __name__ == "__main__":
    main()

