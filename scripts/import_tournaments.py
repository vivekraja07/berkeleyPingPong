#!/usr/bin/env python3
"""
Clean Tournament Import Script
Imports tournaments from Berkeley website synchronously with strict validation.

Features:
- Synchronous processing (one tournament at a time)
- Strict validation before any database insertion
- Detailed error logging for debugging
- Resume capability from any tournament
- No partial data insertion on failure
"""
import argparse
import json
import os
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.parsers.round_robin_parser import RoundRobinParser
from backend.parsers.round_robin_pdf_parser import RoundRobinPDFParser
from backend.db.round_robin_client import RoundRobinClient


class TournamentImporter:
    """Clean tournament importer with strict validation"""
    
    def __init__(self, log_file: str = "import_errors.log"):
        self.parser = RoundRobinParser()
        self.pdf_parser = RoundRobinPDFParser()
        self.client = RoundRobinClient()
        self.base_url = "https://berkeleytabletennis.org"
        self.results_url = f"{self.base_url}/results"
        self.log_file = log_file
        
        # Statistics
        self.stats = {
            'total': 0,
            'imported': 0,
            'skipped': 0,
            'failed': 0,
            'validation_errors': 0,
            'parsing_errors': 0,
            'db_errors': 0
        }
    
    def log(self, message: str, level: str = "INFO"):
        """Log a message with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prefix = {
            "INFO": "ℹ️",
            "SUCCESS": "✅",
            "WARNING": "⚠️",
            "ERROR": "❌"
        }.get(level, "ℹ️")
        print(f"[{timestamp}] {prefix} {message}")
    
    def log_error(self, tournament: Dict, error_type: str, error_message: str, details: Optional[Dict] = None):
        """Log detailed error information to file for debugging"""
        # Get entry number
        entry_num = self._get_next_entry_number()
        
        error_entry = {
            'entry_number': entry_num,
            'timestamp': datetime.now().isoformat(),
            'tournament': {
                'url': tournament.get('url'),
                'date': tournament.get('date').isoformat() if tournament.get('date') else None,
                'format': tournament.get('format'),
                'display': tournament.get('display')
            },
            'error_type': error_type,
            'error_message': error_message,
            'details': details or {}
        }
        
        # Write to log file with clear separator
        with open(self.log_file, 'a') as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"ERROR ENTRY #{entry_num}\n")
            f.write(f"{'='*80}\n")
            f.write(json.dumps(error_entry, indent=2) + '\n')
            f.write(f"{'='*80}\n")
        
        # Also print summary
        self.log(f"Error #{entry_num} logged to {self.log_file} - {error_type}: {error_message}", "ERROR")
    
    def _get_next_entry_number(self) -> int:
        """Get the next entry number by counting existing entries"""
        if not os.path.exists(self.log_file):
            return 1
        
        try:
            with open(self.log_file, 'r') as f:
                content = f.read()
                # Count entries by looking for "ERROR ENTRY #" markers
                matches = re.findall(r'ERROR ENTRY #(\d+)', content)
                if matches:
                    return max(int(m) for m in matches) + 1
                return 1
        except Exception:
            return 1
    
    def scrape_tournament_links(self) -> List[Dict]:
        """Scrape all tournament links from the results page"""
        self.log(f"Scraping tournament links from: {self.results_url}")
        
        try:
            response = requests.get(self.results_url, timeout=60)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
        except Exception as e:
            raise Exception(f"Error fetching results page: {e}")
        
        tournaments = []
        links = soup.find_all("a", href=True)
        
        for link in links:
            href = link.get("href", "")
            
            # Skip if not a tournament link
            if not href or "results" not in href.lower():
                continue
            
            # Helper function to build full URL
            def build_url(href):
                if href.startswith("http"):
                    return href
                elif href.startswith("/"):
                    return f"{self.base_url}{href}"
                else:
                    return f"{self.base_url}/{href}"
            
            # Parse HTML format: /results/rr_results_YYYYmonDD
            html_match = re.search(r'/results/rr_results_(\d{4})([a-z]{3})(\d{2})', href, re.I)
            if html_match:
                year = int(html_match.group(1))
                month_str = html_match.group(2)
                day = int(html_match.group(3))
                
                month_map = {
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                }
                month = month_map.get(month_str.lower(), 1)
                
                try:
                    date = datetime(year, month, day)
                    tournaments.append({
                        'url': build_url(href),
                        'date': date,
                        'format': 'html',
                        'display': date.strftime('%Y %b %d')
                    })
                    continue
                except ValueError:
                    pass
            
            # Parse HTML format: results/RR_Results YYYYMonDD.html
            html_match2 = re.search(r'(?:^|/)results/RR[_\s%5F]?Results[_\s]+(\d{4})([a-z]{3})(\d{2})\.html', href, re.I)
            if html_match2:
                year = int(html_match2.group(1))
                month_str = html_match2.group(2)
                day = int(html_match2.group(3))
                
                month_map = {
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                }
                month = month_map.get(month_str.lower(), 1)
                
                try:
                    date = datetime(year, month, day)
                    tournaments.append({
                        'url': build_url(href),
                        'date': date,
                        'format': 'html',
                        'display': date.strftime('%Y %b %d')
                    })
                    continue
                except ValueError:
                    pass
            
            # Parse PDF format: results/RR_Results YYYYMonDD.pdf
            pdf_match = re.search(r'(?:^|/)results/RR[_\s%5F]?Results[_\s]+(\d{4})([a-z]{3})(\d{2})\.pdf', href, re.I)
            if pdf_match:
                year = int(pdf_match.group(1))
                month_str = pdf_match.group(2)
                day = int(pdf_match.group(3))
                
                month_map = {
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                }
                month = month_map.get(month_str.lower(), 1)
                
                try:
                    date = datetime(year, month, day)
                    tournaments.append({
                        'url': build_url(href),
                        'date': date,
                        'format': 'pdf',
                        'display': date.strftime('%Y %b %d')
                    })
                    continue
                except ValueError:
                    pass
            
            # Parse older PDF format: results/XXX.pdf
            old_pdf_match = re.search(r'(?:^|/)results/(\d+)\.pdf', href, re.I)
            if old_pdf_match:
                pdf_num = old_pdf_match.group(1)
                tournaments.append({
                    'url': build_url(href),
                    'date': None,  # Will be extracted from PDF
                    'format': 'pdf_old',
                    'display': f"PDF #{pdf_num}",
                    'pdf_number': pdf_num
                })
        
        # Remove duplicates by URL
        seen_urls = set()
        unique_tournaments = []
        
        for t in tournaments:
            url = t['url']
            if url not in seen_urls:
                seen_urls.add(url)
                unique_tournaments.append(t)
        
        # Sort by date, oldest first (None dates go to end)
        unique_tournaments.sort(key=lambda x: (x['date'] is None, x['date'] or datetime.min))
        
        # Count by format
        html_count = sum(1 for t in unique_tournaments if t.get('format') == 'html')
        pdf_count = sum(1 for t in unique_tournaments if t.get('format') == 'pdf')
        pdf_old_count = sum(1 for t in unique_tournaments if t.get('format') == 'pdf_old')
        
        self.log(f"Found {len(unique_tournaments)} unique tournaments:")
        self.log(f"  - HTML format: {html_count}")
        self.log(f"  - PDF format (with dates): {pdf_count}")
        self.log(f"  - Old PDF format (dates will be extracted): {pdf_old_count}")
        
        return unique_tournaments
    
    def _extract_date_from_old_pdf(self, tournament: Dict) -> Optional[datetime]:
        """Extract date from old PDF format"""
        try:
            response = requests.get(tournament['url'], timeout=120)
            response.raise_for_status()
            
            from io import BytesIO
            import pdfplumber
            
            pdf_bytes = BytesIO(response.content)
            with pdfplumber.open(pdf_bytes) as pdf:
                if len(pdf.pages) == 0:
                    return None
                
                first_page = pdf.pages[0]
                text = first_page.extract_text() or ""
                
                # If no text extracted, try OCR as fallback for image-based PDFs
                text_extracted_via_ocr = False
                if not text or len(text.strip()) < 10:
                    try:
                        import pytesseract
                        from PIL import Image
                        # Convert PDF page to image and run OCR
                        img = first_page.to_image(resolution=200)
                        ocr_text = pytesseract.image_to_string(img.original)
                        if ocr_text and len(ocr_text.strip()) >= 10:
                            text = ocr_text
                            text_extracted_via_ocr = True
                            self.log(f"Used OCR to extract text from image-based PDF", "INFO")
                        else:
                            self.log(f"OCR extracted text but it's too short or empty", "WARNING")
                    except ImportError:
                        # OCR libraries not available - log warning but continue to try other methods
                        self.log(f"OCR libraries (pytesseract/PIL) not installed. Cannot extract date from image-based PDF. Install with: pip install pytesseract pillow", "WARNING")
                        self.log(f"Then install tesseract system package. See scripts/install_tesseract_macos.sh for instructions", "WARNING")
                    except Exception as ocr_error:
                        # OCR failed - check if it's because tesseract isn't installed
                        error_str = str(ocr_error).lower()
                        if 'tesseract' in error_str and ('not found' in error_str or 'not installed' in error_str):
                            self.log(f"Tesseract OCR not installed. Cannot extract date from image-based PDF.", "WARNING")
                            self.log(f"Install tesseract system package. See scripts/install_tesseract_macos.sh for instructions", "WARNING")
                        else:
                            self.log(f"OCR extraction failed: {ocr_error}", "WARNING")
                
                # Try multiple date formats
                # Format 1: "October 28th, 2022" or "October 28, 2022" (with or without ordinal)
                date_match = re.search(r'([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})', text[:1000])
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
                    month = month_map.get(month_str.lower())
                    
                    if month:
                        try:
                            return datetime(year, month, day)
                        except ValueError:
                            pass
                
                # Format 2: "2024Jan05" compact format
                date_match = re.search(r'(\d{4})([a-z]{3})(\d{2})', text[:1000], re.I)
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
                        return datetime(year, month, day)
                    except ValueError:
                        pass
                
                # Format 3: "2023 Feb 03" format (YYYY Mon DD with spaces)
                date_match = re.search(r'(\d{4})\s+([A-Za-z]{3})\s+(\d{1,2})', text[:1000])
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
                            return datetime(year, month, day)
                        except ValueError:
                            pass
                        
        except Exception as e:
            self.log(f"Error extracting date from PDF: {e}", "WARNING")
        
        return None
    
    def validate_tournament_data(self, data: Dict) -> Tuple[bool, List[str]]:
        """
        STRICT validation of tournament data before insertion
        
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []
        
        # Validate tournament info
        tournament = data.get('tournament', {})
        if not tournament:
            errors.append("Missing tournament information")
            return False, errors
        
        tournament_name = tournament.get('name')
        tournament_date = tournament.get('date')
        
        if not tournament_name or not isinstance(tournament_name, str) or len(tournament_name.strip()) == 0:
            errors.append("Tournament name is missing or invalid")
        
        if not tournament_date:
            errors.append("Tournament date is missing")
        elif not isinstance(tournament_date, (str, datetime)):
            errors.append("Tournament date is invalid type")
        
        # Validate groups
        groups = data.get('groups', [])
        if not groups or not isinstance(groups, list):
            errors.append("No groups found or groups is not a list")
            return False, errors
        
        if len(groups) == 0:
            errors.append("Tournament has no groups")
            return False, errors
        
        # Validate each group
        for i, group in enumerate(groups):
            group_num = group.get('group_number')
            group_name = group.get('group_name', f"Group {group_num or i+1}")
            
            if group_num is None:
                errors.append(f"Group {i+1}: Missing group_number")
            
            # Validate players
            players = group.get('players', [])
            if not players or not isinstance(players, list):
                errors.append(f"Group {group_name}: No players found or players is not a list")
                continue
            
            if len(players) == 0:
                errors.append(f"Group {group_name}: Has no players")
                continue
            
            # Validate each player
            player_names = set()
            player_numbers = set()
            for j, player in enumerate(players):
                if not isinstance(player, dict):
                    errors.append(f"Group {group_name}, Player {j+1}: Not a dictionary")
                    continue
                
                player_name = player.get('name')
                player_number = player.get('player_number')
                
                if not player_name or not isinstance(player_name, str) or len(player_name.strip()) == 0:
                    errors.append(f"Group {group_name}, Player {j+1}: Missing or invalid name")
                
                if player_name and player_name.strip() in player_names:
                    errors.append(f"Group {group_name}: Duplicate player name '{player_name}'")
                
                if player_number is not None:
                    if not isinstance(player_number, (int, str)):
                        errors.append(f"Group {group_name}, Player {player_name}: Invalid player_number type")
                    elif player_number in player_numbers:
                        errors.append(f"Group {group_name}: Duplicate player_number {player_number}")
                    else:
                        player_numbers.add(player_number)
                
                if player_name:
                    player_names.add(player_name.strip())
            
            # Validate matches - CRITICAL: Must have matches in each group
            matches = group.get('matches', [])
            if not isinstance(matches, list):
                errors.append(f"Group {group_name}: Matches is not a list")
                continue
            
            # Expected number of matches for round robin: n*(n-1)/2
            expected_matches = len(players) * (len(players) - 1) // 2 if len(players) > 0 else 0
            actual_matches = len(matches)
            
            # STRICT: If no matches found, this is an error
            if actual_matches == 0 and expected_matches > 0:
                errors.append(f"Group {group_name}: Expected {expected_matches} matches for {len(players)} players, got 0 (no matches found)")
            
            # If we have significantly fewer matches than expected (>20% missing), error
            if actual_matches > 0 and expected_matches > 0:
                missing_pct = ((expected_matches - actual_matches) / expected_matches * 100)
                if missing_pct > 20:
                    errors.append(f"Group {group_name}: Expected {expected_matches} matches for {len(players)} players, got {actual_matches} (missing {expected_matches - actual_matches}, {missing_pct:.1f}%)")
            
            # If we have more matches than expected, error
            if actual_matches > expected_matches:
                errors.append(f"Group {group_name}: Got {actual_matches} matches but expected {expected_matches} for {len(players)} players (too many matches)")
            
            # Validate each match
            match_pairs = set()
            for j, match in enumerate(matches):
                if not isinstance(match, dict):
                    errors.append(f"Group {group_name}, Match {j+1}: Not a dictionary")
                    continue
                
                player1_name = match.get('player1_name')
                player2_name = match.get('player2_name')
                
                if not player1_name or not isinstance(player1_name, str):
                    errors.append(f"Group {group_name}, Match {j+1}: Missing or invalid player1_name")
                
                if not player2_name or not isinstance(player2_name, str):
                    errors.append(f"Group {group_name}, Match {j+1}: Missing or invalid player2_name")
                
                if player1_name and player2_name:
                    if player1_name == player2_name:
                        errors.append(f"Group {group_name}, Match {j+1}: Same player for both sides")
                    
                    # Check for duplicate matches
                    match_pair = tuple(sorted([player1_name.strip(), player2_name.strip()]))
                    if match_pair in match_pairs:
                        errors.append(f"Group {group_name}: Duplicate match between {player1_name} and {player2_name}")
                    else:
                        match_pairs.add(match_pair)
                    
                    # Verify players exist in group
                    if player1_name.strip() not in player_names:
                        errors.append(f"Group {group_name}, Match {j+1}: player1_name '{player1_name}' not found in group players")
                    
                    if player2_name.strip() not in player_names:
                        errors.append(f"Group {group_name}, Match {j+1}: player2_name '{player2_name}' not found in group players")
                
                # Validate scores
                player1_score = match.get('player1_score')
                player2_score = match.get('player2_score')
                
                if player1_score is not None:
                    try:
                        player1_score = int(player1_score)
                        if player1_score < 0:
                            errors.append(f"Group {group_name}, Match {j+1}: Invalid player1_score {player1_score}")
                    except (ValueError, TypeError):
                        errors.append(f"Group {group_name}, Match {j+1}: player1_score is not a valid number")
                
                if player2_score is not None:
                    try:
                        player2_score = int(player2_score)
                        if player2_score < 0:
                            errors.append(f"Group {group_name}, Match {j+1}: Invalid player2_score {player2_score}")
                    except (ValueError, TypeError):
                        errors.append(f"Group {group_name}, Match {j+1}: player2_score is not a valid number")
        
        return len(errors) == 0, errors
    
    def parse_tournament(self, tournament: Dict) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Parse a tournament with error handling
        
        Returns:
            Tuple of (parsed_data, error_message)
        """
        url = tournament['url']
        format_type = tournament['format']
        
        try:
            if format_type in ['pdf', 'pdf_old']:
                # Extract date for old PDFs
                if format_type == 'pdf_old' and not tournament.get('date'):
                    self.log(f"Extracting date from old PDF: {url}", "INFO")
                    date = self._extract_date_from_old_pdf(tournament)
                    if date:
                        tournament['date'] = date
                        tournament['display'] = date.strftime('%Y %b %d')
                        tournament['format'] = 'pdf'
                    else:
                        # If direct extraction failed, try parsing the PDF first
                        # The PDF parser has its own date extraction with OCR support
                        self.log(f"Direct date extraction failed, trying PDF parser extraction: {url}", "INFO")
                        try:
                            temp_results = self.pdf_parser.parse_url(url)
                            tournament_info = temp_results.get('tournament', {})
                            date_str = tournament_info.get('date')
                            if date_str:
                                try:
                                    date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                    tournament['date'] = date
                                    tournament['display'] = date.strftime('%Y %b %d')
                                    tournament['format'] = 'pdf'
                                    self.log(f"Successfully extracted date via PDF parser: {date.strftime('%Y %b %d')}", "SUCCESS")
                                except (ValueError, AttributeError):
                                    pass
                        except Exception as parse_error:
                            self.log(f"PDF parser date extraction also failed: {parse_error}", "WARNING")
                    
                    if not tournament.get('date'):
                        return None, "Could not extract date from PDF (image-based PDFs require OCR: pip install pytesseract pillow && brew install tesseract)"
                
                if not tournament.get('date'):
                    return None, "No date available for tournament"
                
                results = self.pdf_parser.parse_url(url)
            else:
                results = self.parser.parse_url(url)
            
            return results, None
            
        except Exception as e:
            error_msg = f"Error parsing tournament: {e}"
            return None, error_msg
    
    def is_tournament_imported(self, tournament: Dict) -> bool:
        """Check if tournament is already imported"""
        date_str = tournament.get('date')
        if not date_str:
            return False
        
        # Convert to string format
        if isinstance(date_str, datetime):
            date_str = date_str.isoformat()[:10]  # YYYY-MM-DD
        elif isinstance(date_str, str):
            # Try to parse and reformat
            try:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                date_str = dt.strftime('%Y-%m-%d')
            except:
                pass
        
        try:
            # Check database - access the Supabase client through RoundRobinClient
            supabase_client = self.client.client
            db_check = supabase_client.table('tournaments').select('id').eq('date', date_str).execute()
            if db_check.data and len(db_check.data) > 0:
                tournament_id = db_check.data[0]['id']
                # Check if it has groups (complete import)
                groups_check = supabase_client.table('round_robin_groups').select('id').eq('tournament_id', tournament_id).execute()
                if groups_check.data and len(groups_check.data) > 0:
                    return True
        except Exception as e:
            self.log(f"Error checking database: {e}", "WARNING")
        
        return False
    
    def import_tournament(self, tournament: Dict, index: int, total: int) -> Tuple[str, str]:
        """
        Import a single tournament with strict validation
        
        Returns:
            Tuple of (display_name, status)
        """
        url = tournament['url']
        display = tournament['display']
        date_str = tournament['date'].strftime('%Y-%m-%d') if tournament.get('date') else None
        
        self.log(f"[{index}/{total}] Processing: {display}")
        
        # Check if already imported
        if self.is_tournament_imported(tournament):
            self.log(f"[{index}/{total}] ⏭️  Already imported: {display}", "WARNING")
            self.stats['skipped'] += 1
            return (display, 'skipped')
        
        # Step 1: Parse tournament
        parsed_data, parse_error = self.parse_tournament(tournament)
        if parse_error or not parsed_data:
            error_msg = parse_error or "Failed to parse tournament"
            self.stats['parsing_errors'] += 1
            self.stats['failed'] += 1
            self.log(f"[{index}/{total}] ❌ Parsing failed: {display}", "ERROR")
            self.log_error(tournament, 'parsing_error', error_msg, {
                'traceback': traceback.format_exc()
            })
            
            # Still create tournament entry with URL and date, but mark as parsing_failed
            if date_str:
                try:
                    # Generate a default tournament name from date
                    if tournament.get('date'):
                        default_name = tournament['date'].strftime('BTTC Round Robin results for %Y %b %d')
                    else:
                        default_name = f"BTTC Round Robin results for {display}"
                    
                    # Create tournament entry with parsing status
                    tournament_id = self.client._get_or_create_tournament(
                        default_name,
                        date_str,
                        source_url=url,
                        parsing_status='parsing_failed',
                        parse_error=error_msg[:500]  # Limit error message length
                    )
                    if tournament_id:
                        self.log(f"[{index}/{total}] ⚠️  Created tournament entry (parsing failed): {display} (ID: {tournament_id})", "WARNING")
                        return (display, 'failed_but_created')
                except Exception as e:
                    self.log(f"[{index}/{total}] ⚠️  Failed to create tournament entry: {e}", "WARNING")
            
            return (display, 'failed')
        
        # Step 2: STRICT VALIDATION
        is_valid, validation_errors = self.validate_tournament_data(parsed_data)
        if not is_valid:
            self.stats['validation_errors'] += 1
            self.stats['failed'] += 1
            self.log(f"[{index}/{total}] ❌ Validation failed: {display} ({len(validation_errors)} errors)", "ERROR")
            
            # Log first 10 errors
            for err in validation_errors[:10]:
                self.log(f"     - {err}", "ERROR")
            if len(validation_errors) > 10:
                self.log(f"     ... and {len(validation_errors) - 10} more errors", "ERROR")
            
            self.log_error(tournament, 'validation_error', f"Validation failed with {len(validation_errors)} errors", {
                'errors': validation_errors,
                'parsed_data_summary': {
                    'tournament_name': parsed_data.get('tournament', {}).get('name'),
                    'num_groups': len(parsed_data.get('groups', [])),
                    'groups_summary': [
                        {
                            'group_number': g.get('group_number'),
                            'num_players': len(g.get('players', [])),
                            'num_matches': len(g.get('matches', []))
                        }
                        for g in parsed_data.get('groups', [])
                    ]
                }
            })
            
            # Still create tournament entry with URL and date, but mark as validation_failed
            if date_str:
                try:
                    tournament_info = parsed_data.get('tournament', {})
                    tournament_name = tournament_info.get('name') or f"BTTC Round Robin results for {display}"
                    error_summary = f"Validation failed: {validation_errors[0][:200]}" if validation_errors else "Validation failed"
                    
                    # Create tournament entry with parsing status
                    tournament_id = self.client._get_or_create_tournament(
                        tournament_name,
                        date_str,
                        source_url=url,
                        parsing_status='validation_failed',
                        parse_error=error_summary
                    )
                    if tournament_id:
                        self.log(f"[{index}/{total}] ⚠️  Created tournament entry (validation failed): {display} (ID: {tournament_id})", "WARNING")
                        return (display, 'failed_but_created')
                except Exception as e:
                    self.log(f"[{index}/{total}] ⚠️  Failed to create tournament entry: {e}", "WARNING")
            
            return (display, 'failed')
        
        # Step 3: Import into database
        try:
            import_result = self.client.insert_round_robin_data(parsed_data, source_url=url, parsing_status='success', parse_error=None)
            tournament_id = import_result.get('tournament_id')
            
            if not tournament_id:
                raise ValueError("Tournament ID not returned from import")
            
            # Explicitly update parsing_status to 'success' and clear parse_error
            # This ensures that tournaments that previously had parse errors are marked as successful
            try:
                supabase_client = self.client.client
                update_result = (
                    supabase_client.table('tournaments')
                    .update({'parsing_status': 'success', 'parse_error': None})
                    .eq('id', tournament_id)
                    .execute()
                )
                if update_result.data:
                    self.log(f"[{index}/{total}] Updated parsing_status to 'success' for tournament {tournament_id}", "INFO")
            except Exception as update_error:
                # Log but don't fail the import if status update fails
                self.log(f"[{index}/{total}] ⚠️  Warning: Could not update parsing_status: {update_error}", "WARNING")
            
            self.stats['imported'] += 1
            self.log(f"[{index}/{total}] ✅ Successfully imported: {display} (ID: {tournament_id})", "SUCCESS")
            return (display, 'imported')
            
        except Exception as e:
            self.stats['db_errors'] += 1
            self.stats['failed'] += 1
            error_msg = f"Database import failed: {e}"
            self.log(f"[{index}/{total}] ❌ {error_msg}: {display}", "ERROR")
            self.log_error(tournament, 'db_error', error_msg, {
                'traceback': traceback.format_exc(),
                'parsed_data_summary': {
                    'tournament_name': parsed_data.get('tournament', {}).get('name'),
                    'num_groups': len(parsed_data.get('groups', []))
                }
            })
            return (display, 'failed')
    
    def get_parse_error_tournaments_from_db(self) -> List[Dict]:
        """Get list of tournaments with parse errors from database"""
        self.log("Fetching tournaments with parse errors from database...", "INFO")
        
        try:
            # Query tournaments with parsing_status != 'success'
            # This includes: 'parsing_failed', 'validation_failed', 'db_error'
            supabase_client = self.client.client
            result = (
                supabase_client.table('tournaments')
                .select('id,date,source_url,parsing_status,parse_error')
                .neq('parsing_status', 'success')
                .order('date', desc=False)  # Oldest first
                .execute()
            )
            
            if not result.data:
                self.log("No tournaments with parse errors found in database", "WARNING")
                return []
            
            tournaments = []
            for row in result.data:
                date_str = row.get('date')
                source_url = row.get('source_url')
                parsing_status = row.get('parsing_status', 'unknown')
                parse_error = row.get('parse_error', '')
                
                if not source_url:
                    self.log(f"Skipping tournament {row.get('id')} - no source_url available", "WARNING")
                    continue
                
                # Parse date
                date = None
                if date_str:
                    try:
                        if isinstance(date_str, str):
                            date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        elif isinstance(date_str, datetime):
                            date = date_str
                    except (ValueError, AttributeError):
                        pass
                
                # Determine format from URL
                format_type = 'html'
                pdf_number = None
                if source_url.endswith('.pdf'):
                    format_type = 'pdf'
                    # Check if it's old PDF format (numeric filename)
                    import re
                    old_pdf_match = re.search(r'/(\d+)\.pdf', source_url)
                    if old_pdf_match:
                        format_type = 'pdf_old'
                        pdf_number = old_pdf_match.group(1)
                
                # Create display name
                if date:
                    display = date.strftime('%Y %b %d')
                else:
                    display = source_url.split('/')[-1] if source_url else f"Tournament {row.get('id')}"
                
                tournament_dict = {
                    'url': source_url,
                    'date': date,
                    'format': format_type,
                    'display': display,
                    'parsing_status': parsing_status,
                    'parse_error': parse_error
                }
                
                if pdf_number:
                    tournament_dict['pdf_number'] = pdf_number
                
                tournaments.append(tournament_dict)
            
            self.log(f"Found {len(tournaments)} tournaments with parse errors in database", "INFO")
            return tournaments
            
        except Exception as e:
            self.log(f"Error fetching parse error tournaments from database: {e}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
            return []
    
    def get_failed_tournaments_from_log(self, log_file: Optional[str] = None) -> List[Dict]:
        """Get list of failed tournaments from error log"""
        if log_file is None:
            log_file = self.log_file
        
        if not os.path.exists(log_file):
            self.log(f"Error log file not found: {log_file}", "WARNING")
            return []
        
        try:
            # Parse error log directly (avoid importing parse_errors to prevent circular imports)
            with open(log_file, 'r') as f:
                content = f.read()
            
            failed_tournaments = []
            seen_urls = set()
            
            # Extract all JSON objects from the file, handling both formats
            # Method: Find all JSON objects by matching balanced braces
            json_strs = []
            brace_count = 0
            start_pos = -1
            
            for i, char in enumerate(content):
                if char == '{':
                    if brace_count == 0:
                        start_pos = i
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0 and start_pos != -1:
                        json_strs.append(content[start_pos:i+1])
                        start_pos = -1
            
            # Parse each JSON object and extract tournament info
            for json_str in json_strs:
                try:
                    error_entry = json.loads(json_str)
                    # Check if this is an error entry with tournament info
                    if 'tournament' not in error_entry:
                        continue
                    
                    tournament = error_entry.get('tournament', {})
                    url = tournament.get('url')
                    if not url or url in seen_urls:
                        continue
                    
                    seen_urls.add(url)
                    date_str = tournament.get('date')
                    date = None
                    if date_str:
                        try:
                            # Handle both ISO format strings and None
                            if isinstance(date_str, str):
                                date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                            elif isinstance(date_str, datetime):
                                date = date_str
                        except (ValueError, AttributeError):
                            pass
                    
                    # Preserve pdf_number for old PDFs
                    tournament_dict = {
                        'url': url,
                        'date': date,
                        'format': tournament.get('format', 'html'),
                        'display': tournament.get('display', url)
                    }
                    if 'pdf_number' in tournament:
                        tournament_dict['pdf_number'] = tournament['pdf_number']
                    
                    failed_tournaments.append(tournament_dict)
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    # Skip malformed entries
                    continue
            
            self.log(f"Found {len(failed_tournaments)} unique failed tournaments in log", "INFO")
            return failed_tournaments
        except Exception as e:
            self.log(f"Error reading failed tournaments from log: {e}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
            return []
    
    def import_tournaments_from_list(self, tournaments: List[Dict]) -> Dict:
        """Import tournaments from a provided list"""
        self.log("="*70)
        self.log("Tournament Importer - Importing from List")
        self.log("="*70)
        
        if not tournaments:
            self.log("No tournaments provided!", "ERROR")
            return self.stats
        
        self.stats['total'] = len(tournaments)
        self.log(f"Will process {len(tournaments)} tournaments synchronously")
        self.log("")
        
        # Import one at a time
        for i, tournament in enumerate(tournaments, 1):
            display, status = self.import_tournament(tournament, i, len(tournaments))
            
            # Small delay to avoid overwhelming the server
            import time
            time.sleep(0.5)
        
        # Print summary
        self.log("")
        self.log("="*70)
        self.log("Import Summary")
        self.log("="*70)
        self.log(f"Total tournaments: {self.stats['total']}")
        self.log(f"✅ Successfully imported: {self.stats['imported']}")
        self.log(f"⏭️  Skipped: {self.stats['skipped']}")
        self.log(f"❌ Failed: {self.stats['failed']}")
        self.log(f"  - Parsing errors: {self.stats['parsing_errors']}")
        self.log(f"  - Validation errors: {self.stats['validation_errors']}")
        self.log(f"  - Database errors: {self.stats['db_errors']}")
        self.log("="*70)
        
        if self.stats['failed'] > 0:
            self.log(f"Check {self.log_file} for detailed error information", "WARNING")
        
        return self.stats
    
    def import_all(self, limit: Optional[int] = None, start_from: Optional[str] = None, 
                   failed_only: bool = False, failed_log_file: Optional[str] = None,
                   parse_errors_only: bool = False) -> Dict:
        """Import all tournaments synchronously"""
        self.log("="*70)
        self.log("Tournament Importer - Synchronous Mode")
        self.log("="*70)
        
        # If importing only tournaments with parse errors from database
        if parse_errors_only:
            self.log("Importing only tournaments with parse errors from database...")
            parse_error_tournaments = self.get_parse_error_tournaments_from_db()
            if not parse_error_tournaments:
                self.log("No tournaments with parse errors found in database!", "WARNING")
                return self.stats
            return self.import_tournaments_from_list(parse_error_tournaments)
        
        # If importing only failed tournaments
        if failed_only:
            self.log("Importing only failed tournaments from error log...")
            failed_tournaments = self.get_failed_tournaments_from_log(failed_log_file)
            if not failed_tournaments:
                self.log("No failed tournaments found in error log!", "WARNING")
                return self.stats
            return self.import_tournaments_from_list(failed_tournaments)
        
        # Scrape tournaments
        tournaments = self.scrape_tournament_links()
        
        if not tournaments:
            self.log("No tournaments found!", "ERROR")
            return self.stats
        
        # Filter by start_from (tournament display name or URL)
        if start_from:
            found = False
            for i, t in enumerate(tournaments):
                if start_from.lower() in t['display'].lower() or start_from in t['url']:
                    tournaments = tournaments[i:]
                    found = True
                    self.log(f"Starting from tournament: {t['display']}")
                    break
            if not found:
                self.log(f"Could not find tournament matching '{start_from}'", "WARNING")
        
        # Apply limit
        if limit:
            tournaments = tournaments[:limit]
            self.log(f"Limited to {limit} tournaments")
        
        self.stats['total'] = len(tournaments)
        self.log(f"Will process {len(tournaments)} tournaments synchronously")
        self.log("")
        
        # Import one at a time
        for i, tournament in enumerate(tournaments, 1):
            display, status = self.import_tournament(tournament, i, len(tournaments))
            
            # Small delay to avoid overwhelming the server
            import time
            time.sleep(0.5)
        
        # Print summary
        self.log("")
        self.log("="*70)
        self.log("Import Summary")
        self.log("="*70)
        self.log(f"Total tournaments: {self.stats['total']}")
        self.log(f"✅ Successfully imported: {self.stats['imported']}")
        self.log(f"⏭️  Skipped: {self.stats['skipped']}")
        self.log(f"❌ Failed: {self.stats['failed']}")
        self.log(f"  - Parsing errors: {self.stats['parsing_errors']}")
        self.log(f"  - Validation errors: {self.stats['validation_errors']}")
        self.log(f"  - Database errors: {self.stats['db_errors']}")
        self.log("="*70)
        
        if self.stats['failed'] > 0:
            self.log(f"Check {self.log_file} for detailed error information", "WARNING")
        
        return self.stats


def main():
    parser = argparse.ArgumentParser(
        description='Import tournaments from Berkeley website (synchronous)'
    )
    parser.add_argument('--limit', type=int,
                       help='Maximum number of tournaments to import')
    parser.add_argument('--start-from', type=str,
                       help='Start from this tournament (match by display name or URL)')
    parser.add_argument('--log-file', type=str, default='import_errors.log',
                       help='Log file for errors (default: import_errors.log)')
    parser.add_argument('--failed-only', action='store_true',
                       help='Import only tournaments that failed (from error log)')
    parser.add_argument('--failed-log-file', type=str,
                       help='Error log file to read failed tournaments from (default: same as --log-file)')
    parser.add_argument('--parse-errors-only', action='store_true',
                       help='Import only tournaments marked with parse errors in the database')
    
    args = parser.parse_args()
    
    importer = TournamentImporter(log_file=args.log_file)
    
    stats = importer.import_all(
        limit=args.limit,
        start_from=args.start_from,
        failed_only=args.failed_only,
        failed_log_file=args.failed_log_file or args.log_file,
        parse_errors_only=args.parse_errors_only
    )
    
    if stats['failed'] > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()

