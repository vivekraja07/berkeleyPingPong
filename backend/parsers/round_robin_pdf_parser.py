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
                page_text = page.extract_text() or ""
                
                # If no text extracted, try OCR as fallback for image-based PDFs
                if not page_text or len(page_text.strip()) < 10:
                    try:
                        import pytesseract
                        from PIL import Image
                        # Convert PDF page to image and run OCR
                        img = page.to_image(resolution=200)
                        page_text = pytesseract.image_to_string(img.original)
                        if page_text and len(page_text.strip()) >= 10:
                            # Successfully extracted text via OCR
                            pass
                    except ImportError as e:
                        # OCR libraries not available
                        # This will be handled silently - the page_text will remain empty
                        pass
                    except Exception as ocr_error:
                        # OCR failed (e.g., tesseract not installed)
                        # This will be handled silently - the page_text will remain empty
                        pass
                
                self.text_content += page_text
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
        
        # Try multiple date formats
        date = None
        
        # Format 1: Try to extract date from filename/URL first (2024Jan05)
        date_match = re.search(r'(\d{4})([a-z]{3})(\d{2})', source, re.I)
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
            except ValueError:
                pass
        
        # Format 2: Try from PDF text - "January 13, 2022" or "October 28th, 2022" format
        if not date:
            # Look for full month name format: "January 13, 2023" or "October 28th, 2022" (with or without ordinal)
            date_match = re.search(r'([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})', self.text_content[:500])
            if date_match:
                month_str = date_match.group(1)
                day = int(date_match.group(2))
                year = int(date_match.group(3))
                
                month_map = {
                    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
                    'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                }
                month = month_map.get(month_str.lower(), None)
                
                if month:
                    try:
                        date = datetime(year, month, day)
                    except ValueError:
                        pass
        
        # Format 3: Try compact format from PDF text (2024Jan05)
        if not date:
            date_match = re.search(r'(\d{4})([a-z]{3})(\d{2})', self.text_content[:500], re.I)
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
                except ValueError:
                    pass
        
        # Format 4: Try "2023 Feb 03" format (YYYY Mon DD with spaces)
        if not date:
            date_match = re.search(r'(\d{4})\s+([A-Za-z]{3})\s+(\d{1,2})', self.text_content[:500])
            if date_match:
                year = int(date_match.group(1))
                month_str = date_match.group(2)
                day = int(date_match.group(3))
                
                month_map = {
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                }
                month = month_map.get(month_str.lower())
                
                if month:
                    try:
                        date = datetime(year, month, day)
                    except ValueError:
                        pass
        
        if date:
            tournament_info['date'] = date.isoformat()
            tournament_info['date_string'] = date.strftime('%Y %b %d')
            tournament_info['name'] = f"BTTC Round Robin results for {tournament_info['date_string']}"
        
        return tournament_info
    
    def _extract_all_groups(self) -> List[Dict]:
        """Extract all round robin groups from the PDF tables"""
        groups = []
        
        # If we have tables, use standard extraction
        if self.tables and len(self.tables) > 0:
            groups = self._extract_groups_from_tables()
            # Check if any groups are missing players (incomplete table extraction)
            groups_with_players = [g for g in groups if g.get('players') and len(g.get('players', [])) > 0]
            if len(groups_with_players) < len(groups):
                # Some groups have no players - try OCR fallback for those
                groups_without_players = [g for g in groups if not g.get('players') or len(g.get('players', [])) == 0]
                if groups_without_players and self.text_content and len(self.text_content.strip()) > 100:
                    # Try OCR extraction for groups without players
                    ocr_groups = self._extract_groups_from_ocr_text()
                    # Replace groups without players with OCR-extracted groups if available
                    ocr_group_nums = {g.get('group_number') for g in ocr_groups}
                    for i, g in enumerate(groups):
                        if (not g.get('players') or len(g.get('players', [])) == 0) and g.get('group_number') in ocr_group_nums:
                            # Replace with OCR-extracted group
                            ocr_group = next((og for og in ocr_groups if og.get('group_number') == g.get('group_number')), None)
                            if ocr_group and ocr_group.get('players') and len(ocr_group.get('players', [])) > 0:
                                groups[i] = ocr_group
            
            if groups:
                return groups
        
        # If no tables found but we have OCR text, try parsing OCR text
        if not groups and self.text_content and len(self.text_content.strip()) > 100:
            # Check if this looks like OCR text (has common OCR artifacts)
            if any(char in self.text_content for char in ['|', '}', '{', ']', '[']):
                groups = self._extract_groups_from_ocr_text()
                if groups:
                    return groups
        
        return groups
    
    def _extract_groups_from_tables(self) -> List[Dict]:
        """Extract groups from PDF tables (standard method)"""
        groups = []
        
        # Each table represents a group
        # Tables start with "#\n1", "#\n2", etc. in the first cell
        # OR they start with "Name" (alternative format without group numbers)
        group_counter = 1
        for table in self.tables:
            if not table or len(table) < 3:
                continue
            
            # Check if this is a group table
            first_row = table[0]
            if not first_row or len(first_row) < 1:
                continue
            
            first_cell = str(first_row[0] or '').strip()
            group_number = None
            
            # Check if this table has a group number in the first cell
            group_match = re.search(r'#\s*(\d+)', first_cell, re.I)
            if group_match:
                group_number = int(group_match.group(1))
            # Check if this is a table starting with "Name" (alternative format)
            elif first_cell.lower().startswith('name'):
                # Assign sequential group number for tables without explicit group numbers
                group_number = group_counter
                group_counter += 1
            else:
                # Not a recognized group table, skip it
                continue
            
            if group_number:
                group_data = self._extract_group_from_table(table, group_number)
                if group_data:
                    groups.append(group_data)
        
        return groups
    
    def _extract_groups_from_ocr_text(self) -> List[Dict]:
        """Extract groups from OCR text when table extraction fails"""
        groups = []
        
        if not self.text_content or len(self.text_content.strip()) < 100:
            return groups
        
        lines = [line.strip() for line in self.text_content.split('\n') if line.strip()]
        
        # Strategy: Look for patterns that indicate group boundaries
        # 1. Lines starting with "#" followed by number
        # 2. Lines starting with "1 |" (first player of a group)
        # 3. Header lines like "Name Rating Rating" followed by player lines
        
        group_sections = []
        current_group = None
        current_group_lines = []
        group_counter = 1
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Check for explicit group marker: "#1", "#2", etc.
            group_match = re.search(r'^#\s*(\d+)', line, re.I)
            if group_match:
                # Save previous group
                if current_group is not None and current_group_lines:
                    group_sections.append((current_group, current_group_lines))
                
                # Start new group
                current_group = int(group_match.group(1))
                current_group_lines = []
                i += 1
                continue
            
            # Check for player line starting with "1 |" - indicates start of a new group
            player_one_match = re.match(r'^1\s*[|]', line)
            if player_one_match:
                # If we already have a group with players, this might be a new group
                if current_group is not None and current_group_lines:
                    # Check if previous group had players
                    has_players = any(re.match(r'^\d+\s*[|]', l) for l in current_group_lines)
                    if has_players:
                        # Save previous group and start new one
                        group_sections.append((current_group, current_group_lines))
                        group_counter = max(group_counter, current_group) + 1
                        current_group = group_counter
                        current_group_lines = []
                
                # If no current group, start group 1
                if current_group is None:
                    current_group = 1
                    current_group_lines = []
            
            # Collect lines for current group
            if current_group is not None:
                current_group_lines.append(line)
            
            i += 1
        
        # Save last group
        if current_group is not None and current_group_lines:
            group_sections.append((current_group, current_group_lines))
        
        # Extract data from each group section
        for group_num, group_lines in group_sections:
            group_data = self._extract_group_from_ocr_lines(group_num, group_lines)
            if group_data and len(group_data.get('players', [])) > 0:
                groups.append(group_data)
        
        return groups
    
    def _extract_group_from_ocr_lines(self, group_number: int, lines: List[str]) -> Optional[Dict]:
        """Extract a single group from OCR text lines"""
        players = []
        matches = []
        
        # Extract players from OCR lines
        # Pattern variations:
        # 1. "1 |Player Name rating_pre rating_post" (with player number and pipe)
        # 2. "Player Name rating_pre rating_post" (without player number)
        # 3. "1Player Name rating_pre rating_post" (number attached to name)
        
        player_patterns = [
            # Pattern 1: "1 |Name 1601 1793" or "1|Name 1601 1793"
            re.compile(r'^(\d+)\s*[|]\s*([A-Z][a-zA-Z\s,\.]+?)\s+(\d{3,4})\s+(\d{3,4})'),
            # Pattern 2: "1Name 1601 1793" (number attached)
            re.compile(r'^(\d+)([A-Z][a-zA-Z\s,\.]+?)\s+(\d{3,4})\s+(\d{3,4})'),
            # Pattern 3: "Name 1601 1793" (no number)
            re.compile(r'^([A-Z][a-zA-Z\s,\.]+?)\s+(\d{3,4})\s+(\d{3,4})'),
        ]
        
        player_map = {}
        seen_names = set()
        used_player_numbers = set()
        
        for line in lines:
            # Skip header lines
            if any(keyword in line.lower() for keyword in ['name', 'rating', 'pre', 'post', 'games', 'won', 'lost']):
                if 'player' not in line.lower() or len(line) > 100:  # Skip long header lines
                    continue
            
            for pattern_idx, pattern in enumerate(player_patterns):
                match = pattern.match(line.strip())
                if match:
                    groups = match.groups()
                    
                    if len(groups) == 4:
                        # Has player number
                        player_num = int(groups[0])
                        name = groups[1].strip()
                        rating_pre = int(groups[2])
                        rating_post = int(groups[3])
                    elif len(groups) == 3:
                        # No player number
                        name = groups[0].strip()
                        rating_pre = int(groups[1])
                        rating_post = int(groups[2])
                        # Assign next available player number
                        player_num = len(players) + 1
                        while player_num in used_player_numbers:
                            player_num += 1
                    else:
                        continue
                    
                    # Clean name - remove special characters but keep spaces and commas
                    name = re.sub(r'[|{}\[\]\.]+', '', name).strip()
                    name = re.sub(r'\s+', ' ', name)  # Normalize whitespace
                    
                    # Validate name and ratings
                    if name and len(name) > 2 and name not in seen_names:
                        # Check if ratings are reasonable (typically 800-3000)
                        if 500 <= rating_pre <= 3500 and 500 <= rating_post <= 3500:
                            # Check if player number is already used
                            if player_num in used_player_numbers:
                                # Assign next available number
                                new_num = len(players) + 1
                                while new_num in used_player_numbers:
                                    new_num += 1
                                player_num = new_num
                            
                            player_data = {
                                'player_number': player_num,
                                'name': name,
                                'rating_pre': rating_pre,
                                'rating_post': rating_post,
                                'rating_change': rating_post - rating_pre
                            }
                            players.append(player_data)
                            player_map[player_num] = player_data
                            seen_names.add(name)
                            used_player_numbers.add(player_num)
                            break  # Found a match, move to next line
        
        if not players:
            return None
        
        # Extract matches from OCR text
        # This is challenging - scores might be embedded in player lines or separate
        # For now, we'll try to extract obvious score patterns
        matches = self._extract_matches_from_ocr_lines(lines, player_map)
        
        return {
            'group_number': group_number,
            'group_name': f"#{group_number}",
            'players': players,
            'matches': matches
        }
    
    def _extract_matches_from_ocr_lines(self, lines: List[str], player_map: Dict) -> List[Dict]:
        """Extract matches from OCR text lines - heuristic approach"""
        matches = []
        
        if not player_map or len(player_map) < 2:
            return matches
        
        # Strategy: Look for score patterns in player lines
        # Player lines often contain scores after the ratings
        # Format might be: "1 |Name rating1 rating2 score1 score2 score3 ..."
        # Or: "1 |Name rating1 rating2 + +/3 0/3 0/3 ..." (wins/losses format)
        
        player_list = sorted(player_map.items(), key=lambda x: x[0])
        extracted_matchups = set()
        
        for line in lines:
            # Skip header lines
            if any(keyword in line.lower() for keyword in ['name', 'rating', 'pre', 'post', 'games', 'won', 'lost', 'against']):
                continue
            
            # Look for player number at start of line
            player_match = re.match(r'^(\d+)\s*[|]', line)
            if not player_match:
                continue
            
            player1_num = int(player_match.group(1))
            if player1_num not in player_map:
                continue
            
            # Try format 1: "X/Y" format (e.g., "+ +/3 0/3 2/3")
            # This format shows wins/losses: "+ +" means both won, "X/Y" means player won X, opponent won Y
            score_pattern1 = re.findall(r'(\+|\d+)\s*[/]\s*(\+|\d+)', line)
            if score_pattern1:
                opponent_idx = 0
                for win_str, loss_str in score_pattern1:
                    # Skip "+ +" (both won - this might be a separator or invalid)
                    if win_str == '+' and loss_str == '+':
                        continue
                    
                    # Convert to scores
                    try:
                        if win_str == '+':
                            p1_score = 3  # Default win
                        else:
                            p1_score = int(win_str)
                        
                        if loss_str == '+':
                            p2_score = 3  # Default win
                        else:
                            p2_score = int(loss_str)
                        
                        # Scores should be 0-5
                        if not (0 <= p1_score <= 5 and 0 <= p2_score <= 5):
                            continue
                        
                        # Find opponent (assume scores appear in player order)
                        opponent_num = None
                        for opp_num, opp_data in player_list:
                            if opp_num != player1_num:
                                # Check if we already have this matchup
                                matchup = tuple(sorted([player1_num, opp_num]))
                                if matchup not in extracted_matchups:
                                    opponent_num = opp_num
                                    break
                        
                        if opponent_num and opponent_num in player_map:
                            matchup = tuple(sorted([player1_num, opponent_num]))
                            match = {
                                'player1_number': player1_num,
                                'player1_name': player_map[player1_num]['name'],
                                'player2_number': opponent_num,
                                'player2_name': player_map[opponent_num]['name'],
                                'player1_score': p1_score,
                                'player2_score': p2_score
                            }
                            matches.append(match)
                            extracted_matchups.add(matchup)
                            opponent_idx += 1
                            if opponent_idx >= len(player_list) - 1:
                                break
                    except (ValueError, TypeError):
                        continue
                
                # If we found matches in this format, continue to next line
                continue
            
            # Try format 2: Space-separated scores (e.g., "3 1", "0 3")
            # Extract all numbers from the line
            numbers = re.findall(r'\d+', line)
            if len(numbers) < 3:  # Need at least player_num, rating_pre, rating_post
                continue
            
            # Skip player number and ratings (first 3 numbers typically)
            # Look for score pairs in the remaining numbers
            score_pairs = []
            for i in range(3, len(numbers) - 1):
                n1, n2 = int(numbers[i]), int(numbers[i + 1])
                # Scores are typically 0-5
                if 0 <= n1 <= 5 and 0 <= n2 <= 5:
                    score_pairs.append((n1, n2))
            
            # Try to match scores to opponents
            # This is heuristic - we assume scores appear in player order
            if score_pairs:
                opponent_idx = 0
                for p1_score, p2_score in score_pairs[:len(player_list) - 1]:  # Max n-1 matches
                    # Find next opponent (skip self)
                    opponent_num = None
                    for opp_num, opp_data in player_list:
                        if opp_num != player1_num:
                            matchup = tuple(sorted([player1_num, opp_num]))
                            if matchup not in extracted_matchups:
                                opponent_num = opp_num
                                break
                    
                    if opponent_num and opponent_num in player_map:
                        matchup = tuple(sorted([player1_num, opponent_num]))
                        match = {
                            'player1_number': player1_num,
                            'player1_name': player_map[player1_num]['name'],
                            'player2_number': opponent_num,
                            'player2_name': player_map[opponent_num]['name'],
                            'player1_score': p1_score,
                            'player2_score': p2_score
                        }
                        matches.append(match)
                        extracted_matchups.add(matchup)
                        opponent_idx += 1
                        if opponent_idx >= len(player_list) - 1:
                            break
        
        return matches
    
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
        
        row2 = table[2] if len(table) > 2 else None
        
        # Strategy 1: Check if compact format (all data in column 0)
        # Format: "1Phan, Derrick 2147 2176\n2Situ, Jason 2054 2084\n..."
        # This is common in older PDFs, so try it first
        if row2 and len(row2) > 0:
            col0_cell = str(row2[0] or '').strip()
            if col0_cell and re.search(r'^\d{1,2}[A-Za-z]', col0_cell):
                players = self._extract_players_from_compact_column0(table)
                if players:
                    return players
        
        # Strategy 2: Row 2 contains the main player data (standard format)
        # Column 0: Player numbers (stacked: "1\n2\n3\n4\n5\n6")
        # Column 1: Names and ratings (stacked: "Name RatingPre RatingPost\n...")
        if row2 and len(row2) >= 2:
            players = self._extract_players_from_row2(row2)
            if players:
                return players
        
        # Strategy 3: Try extracting from multiple rows (each row might be a player)
        # This handles cases where players are in separate rows instead of stacked
        players = self._extract_players_from_multiple_rows(table)
        if players:
            return players
        
        # Strategy 4: Try extracting from column structure (each column might be a player)
        players = self._extract_players_from_columns(table)
        if players:
            return players
        
        # Strategy 5: Try extracting when column 0 has names+ratings but NO player numbers
        # Format: "Chen, Wei 2064 2102\nChao, Marco 1932 2027\n..." (no numbers)
        if row2 and len(row2) > 0:
            col0_cell = str(row2[0] or '').strip()
            if col0_cell and not re.search(r'^\d{1,2}[A-Za-z]', col0_cell):  # No number prefix
                # Check if it has name + rating pattern
                if re.search(r'[A-Za-z][A-Za-z\s,]+\s+\d{3,4}\s+\d{3,4}', col0_cell):
                    players = self._extract_players_from_nameless_column0(table)
                    if players:
                        return players
        
        return []
    
    def _extract_players_from_row2(self, row2: List) -> List[Dict]:
        """Extract players from row 2 (standard format)"""
        players = []
        
        # Extract player numbers from column 0
        player_nums_cell = str(row2[0] or '').strip()
        player_numbers = []
        for line in player_nums_cell.split('\n'):
            num = line.strip()
            if num.isdigit():
                player_numbers.append(int(num))
        
        if not player_numbers:
            return []
        
        # Extract names and ratings from column 1
        names_ratings_cell = str(row2[1] or '').strip()
        names_ratings_lines = [line.strip() for line in names_ratings_cell.split('\n') if line.strip()]
        
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
            
            if player_data.get('name'):
                players.append(player_data)
        
        return players
    
    def _extract_players_from_multiple_rows(self, table: List[List]) -> List[Dict]:
        """Extract players when each player is in a separate row"""
        players = []
        
        # Look for rows that contain player information
        # Typically starts after header rows (row 0 and 1)
        for row_idx in range(2, min(len(table), 20)):  # Check up to 20 rows
            row = table[row_idx]
            if not row or len(row) < 2:
                continue
            
            # Try to extract player number from first column
            player_num_cell = str(row[0] or '').strip()
            player_num_match = re.search(r'^(\d+)', player_num_cell)
            if not player_num_match:
                continue
            
            player_num = int(player_num_match.group(1))
            
            # Try to extract name and ratings from second column
            name_rating_cell = str(row[1] or '').strip()
            if not name_rating_cell:
                continue
            
            player_data = {'player_number': player_num}
            
            # Extract ratings
            rating_match = re.search(r'(\d{3,4})\s+(\d{3,4})', name_rating_cell)
            if rating_match:
                player_data['rating_pre'] = int(rating_match.group(1))
                player_data['rating_post'] = int(rating_match.group(2))
                player_data['rating_change'] = player_data['rating_post'] - player_data['rating_pre']
            
            # Extract name
            name_match = re.match(r'^([A-Za-z\s]+?)(?:\s+\d{3,4})', name_rating_cell)
            if name_match:
                player_data['name'] = name_match.group(1).strip()
            else:
                # Fallback: take everything before first number
                name_match = re.match(r'^([A-Za-z\s]+)', name_rating_cell)
                if name_match:
                    player_data['name'] = name_match.group(1).strip()
            
            if player_data.get('name'):
                players.append(player_data)
        
        return players
    
    def _extract_players_from_columns(self, table: List[List]) -> List[Dict]:
        """Extract players when structure is column-based"""
        players = []
        
        if len(table) < 3:
            return []
        
        # Look for a row that contains player numbers in the first column
        # Then extract names from subsequent columns or rows
        row2 = table[2] if len(table) > 2 else None
        if not row2 or len(row2) < 2:
            return []
        
        # Check if column 0 has player numbers
        col0_cell = str(row2[0] or '').strip()
        player_nums = []
        for line in col0_cell.split('\n'):
            num = line.strip()
            if num.isdigit() and 1 <= int(num) <= 20:  # Reasonable player number range
                player_nums.append(int(num))
        
        if not player_nums:
            return []
        
        # Try to find names in column 1 or other columns
        for col_idx in range(1, min(len(row2), 10)):
            col_cell = str(row2[col_idx] or '').strip()
            if not col_cell:
                continue
            
            # Check if this column contains names
            lines = [l.strip() for l in col_cell.split('\n') if l.strip()]
            if len(lines) >= len(player_nums):
                # This might be the names column
                for i, player_num in enumerate(player_nums):
                    if i >= len(lines):
                        break
                    
                    line = lines[i]
                    player_data = {'player_number': player_num}
                    
                    # Extract ratings
                    rating_match = re.search(r'(\d{3,4})\s+(\d{3,4})', line)
                    if rating_match:
                        player_data['rating_pre'] = int(rating_match.group(1))
                        player_data['rating_post'] = int(rating_match.group(2))
                        player_data['rating_change'] = player_data['rating_post'] - player_data['rating_pre']
                    
                    # Extract name
                    name_match = re.match(r'^([A-Za-z\s]+?)(?:\s+\d{3,4})', line)
                    if name_match:
                        player_data['name'] = name_match.group(1).strip()
                    else:
                        name_match = re.match(r'^([A-Za-z\s]+)', line)
                        if name_match:
                            player_data['name'] = name_match.group(1).strip()
                    
                    if player_data.get('name'):
                        players.append(player_data)
                
                if players:
                    return players
        
        return []
    
    def _extract_players_from_compact_column0(self, table: List[List]) -> List[Dict]:
        """Extract players when all data is in column 0 in compact format"""
        players = []
        
        if len(table) < 3:
            return []
        
        # Look for row 2 (index 2) which typically contains player data
        row2 = table[2] if len(table) > 2 else None
        if not row2 or len(row2) < 1:
            return []
        
        # Check column 0 for compact format: "1Phan, Derrick 2147 2176\n2Situ, Jason 2054 2084\n..."
        col0_cell = str(row2[0] or '').strip()
        if not col0_cell:
            return []
        
        # Split by newlines to get each player
        lines = [line.strip() for line in col0_cell.split('\n') if line.strip()]
        
        for line in lines:
            # Pattern variations:
            # Format A: "1Rogers, Greg 2317 2327" (no space between number and name)
            # Format B: "1 Rogers, Greg 2305 2317" (space between number and name)
            # Both can have special chars like "#" at the end: "5Lee, Bunny# 2048 2039"
            
            # Try Format A first: number directly attached to name (no space)
            match = re.match(r'^(\d{1,2})([A-Za-z][A-Za-z\s,#\-\.]+?)\s+(\d{3,4})\s+(\d{3,4})$', line)
            
            # If Format A doesn't match, try Format B: number with space before name
            if not match:
                match = re.match(r'^(\d{1,2})\s+([A-Za-z][A-Za-z\s,#\-\.]+?)\s+(\d{3,4})\s+(\d{3,4})$', line)
            
            if match:
                player_num = int(match.group(1))
                name = match.group(2).strip()
                rating_pre = int(match.group(3))
                rating_post = int(match.group(4))
                
                # Clean up name (remove trailing # or other special chars that might be formatting)
                name = re.sub(r'#+$', '', name).strip()
                
                # Skip if name is empty or just whitespace
                if not name or len(name.strip()) == 0:
                    continue
                
                player_data = {
                    'player_number': player_num,
                    'name': name,
                    'rating_pre': rating_pre,
                    'rating_post': rating_post,
                    'rating_change': rating_post - rating_pre
                }
                players.append(player_data)
        
        return players
    
    def _extract_players_from_nameless_column0(self, table: List[List]) -> List[Dict]:
        """Extract players when column 0 has names+ratings but NO player numbers"""
        players = []
        
        if len(table) < 3:
            return []
        
        row2 = table[2] if len(table) > 2 else None
        if not row2 or len(row2) < 1:
            return []
        
        col0_cell = str(row2[0] or '').strip()
        if not col0_cell:
            return []
        
        # Split by newlines to get each player
        lines = [line.strip() for line in col0_cell.split('\n') if line.strip()]
        
        for line_idx, line in enumerate(lines):
            # Pattern: "Chen, Wei 2064 2102" (name, rating_pre, rating_post)
            match = re.search(r'([A-Za-z][A-Za-z\s,]+?)\s+(\d{3,4})\s+(\d{3,4})', line)
            if match:
                name = match.group(1).strip()
                rating_pre = int(match.group(2))
                rating_post = int(match.group(3))
                
                # Player number is implicit (1, 2, 3, ... based on line order)
                player_num = line_idx + 1
                
                # Clean up name
                name = name.strip()
                if not name:
                    continue
                
                player_data = {
                    'player_number': player_num,
                    'name': name,
                    'rating_pre': rating_pre,
                    'rating_post': rating_post,
                    'rating_change': rating_post - rating_pre
                }
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
                # Only use valid opponent numbers (1 or higher, not 0)
                if opp_num > 0:
                    col_to_opponent[col_idx] = opp_num
        
        # Fallback: If no valid opponent numbers found in score columns, infer from column positions
        # Scores typically come in pairs: columns 4-5 = opponent 1, 6-7 = opponent 2, etc.
        # But some PDFs have scores only in even columns (4, 6, 8, 10)
        # Check if the FIRST few score columns (4, 6, 8, 10) have valid opponent numbers
        # We check the first 4 score columns (up to column 10) to determine if we need fallback
        first_score_cols = [score_start_col + i*2 for i in range(4)]  # [4, 6, 8, 10]
        score_cols_have_opponents = any(col_idx in col_to_opponent and col_to_opponent[col_idx] > 0 
                                       for col_idx in first_score_cols if col_idx < len(row1))
        
        if not score_cols_have_opponents:
            # First, check if scores are in row 5 (some PDFs have this structure)
            row5 = table[5] if len(table) > 5 else None
            if row5:
                # Check if row 5 has scores in even columns
                has_scores_in_even_cols = False
                for col_idx in range(score_start_col, min(len(row5), len(row1)), score_start_col + 20):
                    if col_idx % 2 == 0:  # Even column
                        cell = str(row5[col_idx] or '').strip()
                        if cell and cell != '0' and re.search(r'\d+\s+\d+', cell):
                            has_scores_in_even_cols = True
                            break
                
                if has_scores_in_even_cols:
                    # Scores are in even columns only: col 4 = opp 1, col 6 = opp 2, etc.
                    opponent_counter = 1
                    for col_idx in range(score_start_col, len(row1), 2):  # Step by 2 (even columns)
                        if col_idx < len(row1):
                            col_to_opponent[col_idx] = opponent_counter
                            opponent_counter += 1
                else:
                    # Standard pairs: columns 4-5 = opponent 1, 6-7 = opponent 2, etc.
                    opponent_counter = 1
                    for col_idx in range(score_start_col, len(row1), 2):  # Step by 2 (pairs)
                        if col_idx < len(row1):
                            col_to_opponent[col_idx] = opponent_counter
                            if col_idx + 1 < len(row1):
                                col_to_opponent[col_idx + 1] = opponent_counter
                            opponent_counter += 1
            else:
                # Standard pairs
                opponent_counter = 1
                for col_idx in range(score_start_col, len(row1), 2):  # Step by 2 (pairs)
                    if col_idx < len(row1):
                        col_to_opponent[col_idx] = opponent_counter
                        if col_idx + 1 < len(row1):
                            col_to_opponent[col_idx + 1] = opponent_counter
                        opponent_counter += 1
        
        # Extract matches from all data rows
        # Row 2 contains player 1's matches vs all opponents
        # Rows 3+ contain matches from other players' perspectives
        
        # Track which matchups we've already extracted
        extracted_matchups = set()
        
        # Check if this is the compact format (all player data in column 0)
        # In this format, row 2 has individual scores (not stacked)
        is_compact_format = False
        row2 = table[2] if len(table) > 2 else None
        if row2 and len(row2) > 0:
            col0_cell = str(row2[0] or '').strip()
            # Check if column 0 contains compact player format
            if col0_cell and re.search(r'^\d{1,2}[A-Za-z]', col0_cell):
                is_compact_format = True
        
        # First, extract matches from row 2
        # In compact format: row 2 has individual scores for player 1 vs each opponent
        # In standard format: row 2 has stacked scores for all players vs each opponent
        if row2:
            for col_idx in range(score_start_col, min(len(row2), len(row1))):
                if col_idx not in col_to_opponent:
                    continue
                
                opponent_num = col_to_opponent[col_idx]
                if opponent_num not in player_map or opponent_num == 0:
                    continue
                
                score_cell = str(row2[col_idx] or '').strip()
                if not score_cell or score_cell == '+' or score_cell == 'XXXXXX':
                    continue
                
                if is_compact_format:
                    # Compact format: individual score for player 1 vs this opponent
                    player1_num = 1
                    # Verify player 1 exists in player_map
                    if player1_num in player_map and player1_num < opponent_num:
                        matchup = (player1_num, opponent_num)
                        if matchup not in extracted_matchups:
                            match = self._extract_match_from_score(score_cell, player1_num, opponent_num, player_map)
                            if match:
                                matches.append(match)
                                extracted_matchups.add(matchup)
                else:
                    # Standard format: stacked scores - each line represents a different player vs this opponent
                    score_lines = [s.strip() for s in score_cell.split('\n') if s.strip()]
                    
                    # Each line represents player (line_index + 1) vs opponent_num
                    for line_idx, score_line in enumerate(score_lines):
                        player_num = line_idx + 1  # First line is player 1, second is player 2, etc.
                        
                        if player_num not in player_map or player_num >= opponent_num:
                            continue
                        
                        # Check if we already have this matchup
                        matchup = (player_num, opponent_num)
                        if matchup in extracted_matchups:
                            continue
                        
                        # Extract match from this score line
                        match = self._extract_match_from_score(score_line, player_num, opponent_num, player_map)
                        if match:
                            matches.append(match)
                            extracted_matchups.add(matchup)
        
        # Special case: Check if scores are in row 5 (some PDFs have this structure)
        # Row 1 has opponent numbers (may be all "0"), row 2 has player data, row 5 has stacked scores
        # This can happen in both compact and standard formats
        row5 = table[5] if len(table) > 5 else None
        if row5:
            # Check if row 5 has stacked scores in even columns
            for col_idx in range(score_start_col, min(len(row5), len(row1)), 2):  # Even columns only
                if col_idx not in col_to_opponent:
                    continue
                
                opponent_num = col_to_opponent[col_idx]
                if opponent_num not in player_map or opponent_num == 0:
                    continue
                
                score_cell = str(row5[col_idx] or '').strip()
                if not score_cell or score_cell == '+' or score_cell == 'XXXXXX' or score_cell == '0':
                    continue
                
                # Extract stacked scores (each line is a different player vs this opponent)
                score_lines = [s.strip() for s in score_cell.split('\n') if s.strip()]
                for line_idx, score_line in enumerate(score_lines):
                    player_num = line_idx + 1  # First line is player 1, second is player 2, etc.
                    
                    if player_num not in player_map or player_num >= opponent_num:
                        continue
                    
                    # Skip "0 0" scores only if they're clearly invalid (both players have 0)
                    # But still try to extract them as they might be valid forfeit scores
                    matchup = (player_num, opponent_num)
                    if matchup not in extracted_matchups:
                        match = self._extract_match_from_score(score_line, player_num, opponent_num, player_map)
                        if match:
                            matches.append(match)
                            extracted_matchups.add(matchup)
                
                # If we have fewer lines than expected players, check if there are more rows
                # Some PDFs might have scores spread across multiple rows
                if len(score_lines) < len(players) - 1:
                    # Check subsequent rows for additional scores in the same column
                    for check_row_idx in range(6, min(len(table), len(players) + 5)):
                        check_row = table[check_row_idx] if len(table) > check_row_idx else None
                        if check_row and len(check_row) > col_idx:
                            check_cell = str(check_row[col_idx] or '').strip()
                            if check_cell and check_cell != '0' and check_cell != 'XXXXXX':
                                # This might be additional scores for this opponent
                                additional_lines = [s.strip() for s in check_cell.split('\n') if s.strip()]
                                for line_idx, score_line in enumerate(additional_lines):
                                    # Continue player numbering from where row 5 left off
                                    player_num = len(score_lines) + line_idx + 1
                                    if player_num not in player_map or player_num >= opponent_num:
                                        continue
                                    matchup = (player_num, opponent_num)
                                    if matchup not in extracted_matchups:
                                        match = self._extract_match_from_score(score_line, player_num, opponent_num, player_map)
                                        if match:
                                            matches.append(match)
                                            extracted_matchups.add(matchup)
                                            score_lines.append(score_line)  # Track that we processed this
        
        # Then extract matches from rows 3+ (other players' matches)
        if is_compact_format:
            # Compact format: 
            # - Row 3, column 4 has stacked scores (all players vs opponent 1)
            # - Row 3+, other columns have individual scores for that row's player vs opponents
            # Row index corresponds to player number (row 3 = player 2, row 4 = player 3, etc.)
            
            # First, extract stacked scores from column 4 (all players vs opponent 1)
            if len(table) > 2:
                row3 = table[3] if len(table) > 3 else None
                if row3 and len(row3) > score_start_col:
                    col4_cell = str(row3[score_start_col] or '').strip()
                    if col4_cell and col4_cell != 'XXXXXX' and '\n' in col4_cell:
                        # Stacked scores: each line is a different player vs opponent 1
                        score_lines = [s.strip() for s in col4_cell.split('\n') if s.strip()]
                        opponent1_num = 1
                        if opponent1_num in player_map:
                            for line_idx, score_line in enumerate(score_lines):
                                player_num = line_idx + 2  # First line is player 2 (row 3), second is player 3, etc.
                                
                                if player_num not in player_map or player_num >= opponent1_num:
                                    continue
                                
                                matchup = (player_num, opponent1_num)
                                if matchup not in extracted_matchups:
                                    match = self._extract_match_from_score(score_line, player_num, opponent1_num, player_map)
                                    if match:
                                        matches.append(match)
                                        extracted_matchups.add(matchup)
            
            # Then extract individual scores from rows 3+ (each row is a different player)
            for row_idx in range(3, min(len(table), len(players) + 3)):
                row = table[row_idx]
                if not row or len(row) < score_start_col:
                    continue
                
                # Row index corresponds to player number
                player_num = row_idx - 1  # Row 3 = player 2, row 4 = player 3, etc.
                if player_num not in player_map:
                    continue
                
                # Extract scores from this row
                # Column 4 (index score_start_col) has stacked scores for all players vs opponent 1, skip it
                # Other columns have individual scores for this player vs opponents
                for col_idx in range(score_start_col, min(len(row), len(row1))):
                    # Skip column 4 (index score_start_col) which has stacked scores
                    if col_idx == score_start_col:
                        continue
                    if col_idx not in col_to_opponent:
                        continue
                    
                    opponent_num = col_to_opponent[col_idx]
                    if opponent_num not in player_map:
                        continue
                    
                    # Skip diagonal
                    if player_num == opponent_num:
                        continue
                    
                    score_cell = str(row[col_idx] or '').strip()
                    if not score_cell or score_cell == '+' or score_cell == 'XXXXXX':
                        continue
                    
                    # Determine matchup (always use lower number first)
                    if player_num < opponent_num:
                        matchup = (player_num, opponent_num)
                        swap_scores = False
                    else:
                        matchup = (opponent_num, player_num)
                        swap_scores = True
                    
                    if matchup in extracted_matchups:
                        continue
                    
                    # Extract match
                    if swap_scores:
                        match = self._extract_match_from_score(score_cell, opponent_num, player_num, player_map, swap=True)
                    else:
                        match = self._extract_match_from_score(score_cell, player_num, opponent_num, player_map)
                    
                    if match:
                        matches.append(match)
                        extracted_matchups.add(matchup)
        else:
            # Standard format: extract from rows 3+ (other players' matches)
            # These rows contain scores from different players' perspectives
            # Also check if scores are in later rows (some PDFs have scores in row 5+)
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
                    if opponent_num not in player_map or opponent_num == 0:
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
                        if score_cell and score_cell != '+' and score_cell != 'XXXXXX':
                            row_scores.append((check_col, opponent_num, score_cell))
                            break  # Found score in this pair, move on
                
                # If no scores found in this row but row has content, try to extract from all columns
                # This handles cases where scores are in unexpected positions
                if not row_scores:
                    for col_idx in range(score_start_col, min(len(row), len(row1))):
                        score_cell = str(row[col_idx] or '').strip()
                        if score_cell and score_cell != '+' and score_cell != 'XXXXXX' and score_cell != '0':
                            # Check if this looks like a score (contains "0 0", "3 1", etc.)
                            if re.search(r'\d+\s+\d+', score_cell):
                                # Try to infer opponent from column position
                                # Columns come in pairs, so col 4-5 = opp 1, 6-7 = opp 2, etc.
                                inferred_opp = ((col_idx - score_start_col) // 2) + 1
                                if inferred_opp <= len(players):
                                    row_scores.append((col_idx, inferred_opp, score_cell))
                
                # For each score in this row, try to match it to a player
                for col_idx, opponent_num, score_cell in row_scores:
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
                            if best_match is None:
                                best_match = match
                                best_matchup = matchup
                            else:
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
            
            # Try standard format: "3 2" or "3 1"
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
            
            # Try format with "D" (default/draw): "3 D" or "D 3"
            score_match_d = re.match(r'^(\d+|D)\s+(\d+|D)$', line, re.I)
            if score_match_d:
                p1_str = score_match_d.group(1).upper()
                p2_str = score_match_d.group(2).upper()
                
                # Convert "D" to 0 (default/draw)
                p1 = 0 if p1_str == 'D' else int(p1_str)
                p2 = 0 if p2_str == 'D' else int(p2_str)
                
                # Validate: game scores should be reasonable (0-3 typically)
                if 0 <= p1 <= 5 and 0 <= p2 <= 5:
                    if swap:
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
            
            # Verify both players exist in player_map before creating match
            if player1_num not in player_map:
                return None
            if player2_num not in player_map:
                return None
            
            return {
                'player1_number': player1_num,
                'player1_name': player_map[player1_num]['name'],
                'player2_number': player2_num,
                'player2_name': player_map[player2_num]['name'],
                'player1_score': p1_games,
                'player2_score': p2_games
            }
        
        return None
