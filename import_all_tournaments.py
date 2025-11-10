#!/usr/bin/env python3
"""
Import All Round Robin Tournaments
Scrapes the results page and imports all tournaments, starting from the latest
"""
import argparse
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Optional
from parsers.round_robin_parser import RoundRobinParser
from parsers.round_robin_pdf_parser import RoundRobinPDFParser
from db.round_robin_client import RoundRobinClient
import time


class TournamentImporter:
    """Imports all round robin tournaments from the results page"""
    
    def __init__(self):
        self.parser = RoundRobinParser()
        self.pdf_parser = RoundRobinPDFParser()
        self.client = RoundRobinClient()
        self.base_url = "https://berkeleytabletennis.org"
        self.results_url = f"{self.base_url}/results"
    
    def scrape_tournament_links(self) -> List[Dict]:
        """
        Scrape all tournament links from the results page
        
        Returns:
            List of tournament dictionaries with url, date, and format
        """
        print(f"Scraping tournament links from: {self.results_url}")
        
        try:
            response = requests.get(self.results_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
        except Exception as e:
            print(f"Error fetching results page: {e}")
            return []
        
        tournaments = []
        
        # Find all links in the results page
        # Links can be:
        # - /results/rr_results_YYYYmonDD (HTML format)
        # - /results/RR_Results YYYYMonDD.pdf (PDF format)
        # - results/XXX.pdf (older PDF format)
        
        links = soup.find_all("a", href=True)
        
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            
            # Skip if not a tournament link
            if not href or "results" not in href.lower():
                continue
            
            # Parse HTML format: /results/rr_results_YYYYmonDD
            html_match = re.search(r'/results/rr_results_(\d{4})([a-z]{3})(\d{2})', href, re.I)
            if html_match:
                year = int(html_match.group(1))
                month_str = html_match.group(2)
                day = int(html_match.group(3))
                
                # Convert month string to number
                month_map = {
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                }
                month = month_map.get(month_str.lower(), 1)
                
                try:
                    date = datetime(year, month, day)
                    full_url = href if href.startswith("http") else f"{self.base_url}{href}"
                    
                    tournaments.append({
                        'url': full_url,
                        'date': date,
                        'format': 'html',
                        'display': f"{year} {month_str.capitalize()} {day}"
                    })
                except ValueError:
                    continue
            
            # Parse PDF format: results/RR_Results YYYYMonDD.pdf or /results/RR_Results YYYYMonDD.pdf
            # Also handle URL encoded: RR%5FResults (where %5F is underscore)
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
                    # Handle relative URLs
                    if href.startswith("http"):
                        full_url = href
                    elif href.startswith("/"):
                        full_url = f"{self.base_url}{href}"
                    else:
                        full_url = f"{self.base_url}/{href}"
                    
                    tournaments.append({
                        'url': full_url,
                        'date': date,
                        'format': 'pdf',
                        'display': f"{year} {month_str.capitalize()} {day}"
                    })
                except ValueError:
                    continue
            
            # Parse older PDF format: results/XXX.pdf (need to check if we can extract date)
            # This is trickier - we might need to check the PDF content or skip these
        
        # Remove duplicates and sort by date (newest first)
        seen_urls = set()
        unique_tournaments = []
        for t in tournaments:
            if t['url'] not in seen_urls:
                seen_urls.add(t['url'])
                unique_tournaments.append(t)
        
        # Sort by date, newest first
        unique_tournaments.sort(key=lambda x: x['date'], reverse=True)
        
        print(f"Found {len(unique_tournaments)} unique tournaments")
        return unique_tournaments
    
    def get_imported_tournaments(self) -> set:
        """Get set of already imported tournament dates"""
        try:
            result = self.client.client.table('tournaments').select('date, name').execute()
            if result.data:
                # Return set of date strings for comparison
                return {t['date'] for t in result.data if t.get('date')}
        except Exception as e:
            print(f"Warning: Could not check imported tournaments: {e}")
        return set()
    
    def import_tournament(self, tournament: Dict, skip_existing: bool = True) -> str:
        """
        Import a single tournament
        
        Args:
            tournament: Tournament dictionary with url, date, format
            skip_existing: If True, skip tournaments that already exist
            
        Returns:
            'imported' if imported successfully, 'skipped' if skipped, 'failed' if failed
        """
        url = tournament['url']
        date_str = tournament['date'].strftime('%Y-%m-%d')
        format_type = tournament['format']
        display = tournament['display']
        
        print(f"\n{'='*60}")
        print(f"Processing: {display} ({format_type.upper()})")
        print(f"URL: {url}")
        print(f"{'='*60}")
        
        # Check if already imported
        if skip_existing:
            try:
                result = self.client.client.table('tournaments').select('id').eq('date', date_str).execute()
                if result.data:
                    print(f"⏭️  Already imported, skipping...")
                    return 'skipped'
            except Exception:
                pass
        
        # Parse the tournament
        try:
            print("Parsing tournament...")
            if format_type == 'pdf':
                results = self.pdf_parser.parse_url(url)
            else:
                results = self.parser.parse_url(url)
            
            tournament_info = results.get('tournament', {})
            groups = results.get('groups', [])
            
            if not groups:
                print(f"⚠️  No groups found, skipping...")
                return 'failed'
            
            print(f"Found {len(groups)} groups, {sum(len(g.get('players', [])) for g in groups)} players")
            
            # Import into database
            print("Importing into database...")
            import_result = self.client.insert_round_robin_data(results)
            
            print(f"✅ Successfully imported!")
            print(f"   Tournament ID: {import_result['tournament_id']}")
            print(f"   Groups: {len(import_result['groups'])}")
            
            return 'imported'
            
        except Exception as e:
            print(f"❌ Error importing tournament: {e}")
            import traceback
            traceback.print_exc()
            return 'failed'
    
    def import_all(self, 
                   limit: Optional[int] = None,
                   skip_existing: bool = True,
                   start_date: Optional[str] = None,
                   delay: float = 1.0) -> Dict:
        """
        Import all tournaments
        
        Args:
            limit: Maximum number of tournaments to import (None for all)
            skip_existing: Skip tournaments that already exist
            start_date: Start from this date (YYYY-MM-DD format), None for latest
            delay: Delay between imports in seconds (to be polite to server)
            
        Returns:
            Dictionary with import statistics
        """
        print("="*60)
        print("Round Robin Tournament Importer")
        print("="*60)
        print()
        
        # Scrape tournament links
        tournaments = self.scrape_tournament_links()
        
        if not tournaments:
            print("No tournaments found!")
            return {'total': 0, 'imported': 0, 'skipped': 0, 'failed': 0}
        
        # Filter by start date if provided
        if start_date:
            try:
                start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
                tournaments = [t for t in tournaments if t['date'] >= start_datetime]
                print(f"Filtered to tournaments from {start_date} onwards: {len(tournaments)} tournaments")
            except ValueError:
                print(f"Invalid start_date format: {start_date}. Use YYYY-MM-DD")
                return {'total': 0, 'imported': 0, 'skipped': 0, 'failed': 0}
        
        # Apply limit
        if limit:
            tournaments = tournaments[:limit]
            print(f"Limited to {limit} tournaments")
        
        print(f"\nWill process {len(tournaments)} tournaments")
        print(f"Starting from: {tournaments[0]['display'] if tournaments else 'N/A'}")
        print()
        
        stats = {
            'total': len(tournaments),
            'imported': 0,
            'skipped': 0,
            'failed': 0
        }
        
        # Import each tournament
        for i, tournament in enumerate(tournaments, 1):
            print(f"\n[{i}/{len(tournaments)}] {tournament['display']}")
            
            status = self.import_tournament(tournament, skip_existing=skip_existing)
            
            if status == 'imported':
                stats['imported'] += 1
            elif status == 'skipped':
                stats['skipped'] += 1
            else:
                stats['failed'] += 1
            
            # Delay between requests (except for last one)
            if i < len(tournaments) and delay > 0:
                time.sleep(delay)
        
        # Print summary
        print("\n" + "="*60)
        print("Import Summary")
        print("="*60)
        print(f"Total tournaments: {stats['total']}")
        print(f"✅ Successfully imported: {stats['imported']}")
        print(f"⏭️  Skipped (already exist): {stats['skipped']}")
        print(f"❌ Failed: {stats['failed']}")
        print("="*60)
        
        return stats


def main():
    parser = argparse.ArgumentParser(
        description='Import all round robin tournaments from berkeleytabletennis.org'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of tournaments to import (default: all)'
    )
    parser.add_argument(
        '--no-skip-existing',
        action='store_true',
        help='Re-import tournaments that already exist'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start from this date (YYYY-MM-DD format, default: latest)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=1.0,
        help='Delay between imports in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be imported without actually importing'
    )
    
    args = parser.parse_args()
    
    importer = TournamentImporter()
    
    if args.dry_run:
        print("DRY RUN MODE - No data will be imported")
        print()
        tournaments = importer.scrape_tournament_links()
        
        if args.limit:
            tournaments = tournaments[:args.limit]
        
        print(f"\nWould import {len(tournaments)} tournaments:")
        for i, t in enumerate(tournaments[:20], 1):  # Show first 20
            print(f"  {i}. {t['display']} ({t['format']}) - {t['url']}")
        
        if len(tournaments) > 20:
            print(f"  ... and {len(tournaments) - 20} more")
    else:
        stats = importer.import_all(
            limit=args.limit,
            skip_existing=not args.no_skip_existing,
            start_date=args.start_date,
            delay=args.delay
        )


if __name__ == '__main__':
    main()

