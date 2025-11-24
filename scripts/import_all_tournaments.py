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
import sys
import os

# Add parent directory to path to import backend modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.parsers.round_robin_parser import RoundRobinParser
from backend.parsers.round_robin_pdf_parser import RoundRobinPDFParser
from backend.db.round_robin_client import RoundRobinClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time


class TournamentImporter:
    """Imports all round robin tournaments from the results page"""
    
    def __init__(self):
        self.parser = RoundRobinParser()
        self.pdf_parser = RoundRobinPDFParser()
        self.client = RoundRobinClient()
        self.base_url = "https://berkeleytabletennis.org"
        self.results_url = f"{self.base_url}/results"
        self.print_lock = Lock()  # Lock for thread-safe printing
    
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
                    if href.startswith("http"):
                        full_url = href
                    elif href.startswith("/"):
                        full_url = f"{self.base_url}{href}"
                    else:
                        full_url = f"{self.base_url}/{href}"
                    
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
            
            # Parse older PDF format: results/XXX.pdf (numbered PDFs)
            # These don't have dates in the filename, so we'll need to extract from PDF content
            old_pdf_match = re.search(r'(?:^|/)results/(\d+)\.pdf', href, re.I)
            if old_pdf_match:
                # We'll parse the date from the PDF content later
                # For now, add it with a placeholder date that we'll update after parsing
                if href.startswith("http"):
                    full_url = href
                elif href.startswith("/"):
                    full_url = f"{self.base_url}{href}"
                else:
                    full_url = f"{self.base_url}/{href}"
                
                tournaments.append({
                    'url': full_url,
                    'date': None,  # Will be extracted from PDF
                    'format': 'pdf_old',
                    'display': f"PDF #{old_pdf_match.group(1)}",
                    'pdf_number': old_pdf_match.group(1)
                })
        
        # Remove duplicates
        seen_urls = set()
        unique_tournaments = []
        
        for t in tournaments:
            if t['url'] not in seen_urls:
                seen_urls.add(t['url'])
                unique_tournaments.append(t)
        
        # Sort by date, newest first (None dates go to end)
        # Old PDFs without dates will be sorted to the end
        unique_tournaments.sort(key=lambda x: (x['date'] is None, x['date'] or datetime.min), reverse=True)
        
        # Count by format
        html_count = sum(1 for t in unique_tournaments if t.get('format') == 'html')
        pdf_count = sum(1 for t in unique_tournaments if t.get('format') == 'pdf')
        pdf_old_count = sum(1 for t in unique_tournaments if t.get('format') == 'pdf_old')
        
        print(f"Found {len(unique_tournaments)} unique tournaments:")
        print(f"  - HTML format: {html_count}")
        print(f"  - PDF format (with dates): {pdf_count}")
        print(f"  - Old PDF format (dates will be extracted during import): {pdf_old_count}")
        print(f"\nNote: Old PDF dates will be extracted on-demand during import (faster!)")
        
        return unique_tournaments
    
    def _extract_date_from_old_pdf(self, tournament: Dict) -> Optional[datetime]:
        """Extract date from old PDF format (results/XXX.pdf) - optimized to only extract date, not full parse"""
        try:
            import requests
            import pdfplumber
            from io import BytesIO
            import re
            
            # Only download and parse first page to get date (much faster)
            response = requests.get(tournament['url'], timeout=10)
            response.raise_for_status()
            pdf_bytes = BytesIO(response.content)
            
            with pdfplumber.open(pdf_bytes) as pdf:
                if len(pdf.pages) == 0:
                    return None
                
                # Extract text from first page only
                first_page = pdf.pages[0]
                text = first_page.extract_text() or ""
                
                # Try multiple date formats
                # Format 1: "January 13, 2023" or "Jan 13, 2023"
                date_match = re.search(r'([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})', text[:500])
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
                date_match = re.search(r'(\d{4})([a-z]{3})(\d{2})', text[:500], re.I)
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
                        
        except Exception:
            pass
        return None
    
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
    
    def import_tournament(self, tournament: Dict, skip_existing: bool = True, index: Optional[int] = None, total: Optional[int] = None) -> tuple:
        """
        Import a single tournament
        
        Args:
            tournament: Tournament dictionary with url, date, format
            skip_existing: If True, skip tournaments that already exist
            index: Optional index for progress display
            total: Optional total count for progress display
            
        Returns:
            Tuple of (tournament display name, status) where status is 'imported', 'skipped', or 'failed'
        """
        url = tournament['url']
        format_type = tournament['format']
        display = tournament['display']
        
        # Create parsers for this thread (thread-safe)
        from backend.parsers.round_robin_parser import RoundRobinParser
        from backend.parsers.round_robin_pdf_parser import RoundRobinPDFParser
        from backend.db.round_robin_client import RoundRobinClient
        
        parser = RoundRobinParser()
        pdf_parser = RoundRobinPDFParser()
        client = RoundRobinClient()
        
        progress_prefix = f"[{index}/{total}] " if index and total else ""
        
        # Extract date for old PDFs on-demand (during import, not during scraping)
        if format_type == 'pdf_old' and not tournament.get('date'):
            with self.print_lock:
                print(f"{progress_prefix}Extracting date from old PDF format...")
            date = self._extract_date_from_old_pdf(tournament)
            if date:
                tournament['date'] = date
                tournament['display'] = date.strftime('%Y %b %d')
                tournament['format'] = 'pdf'  # Update format after successful extraction
            else:
                with self.print_lock:
                    print(f"{progress_prefix}⚠️  Could not extract date, skipping...")
                return (display, 'failed')
        
        # Get date string for database check
        if not tournament.get('date'):
            with self.print_lock:
                print(f"{progress_prefix}⚠️  No date available, skipping...")
            return (display, 'failed')
        
        date_str = tournament['date'].strftime('%Y-%m-%d')
        
        with self.print_lock:
            print(f"\n{'='*60}")
            print(f"{progress_prefix}Processing: {display} ({format_type.upper()})")
            print(f"URL: {url}")
            print(f"{'='*60}")
        
        # Check if already imported
        if skip_existing:
            try:
                result = client.client.table('tournaments').select('id').eq('date', date_str).execute()
                if result.data:
                    with self.print_lock:
                        print(f"{progress_prefix}⏭️  Already imported, skipping...")
                    return (display, 'skipped')
            except Exception:
                pass
        
        # Parse the tournament
        try:
            with self.print_lock:
                print(f"{progress_prefix}Parsing tournament...")
            
            if format_type in ['pdf', 'pdf_old']:
                results = pdf_parser.parse_url(url)
            else:
                results = parser.parse_url(url)
            
            tournament_info = results.get('tournament', {})
            groups = results.get('groups', [])
            
            if not groups:
                with self.print_lock:
                    print(f"{progress_prefix}⚠️  No groups found, skipping...")
                return (display, 'failed')
            
            with self.print_lock:
                print(f"{progress_prefix}Found {len(groups)} groups, {sum(len(g.get('players', [])) for g in groups)} players")
            
            # Import into database
            with self.print_lock:
                print(f"{progress_prefix}Importing into database...")
            
            import_result = client.insert_round_robin_data(results)
            
            with self.print_lock:
                print(f"{progress_prefix}✅ Successfully imported!")
                print(f"{progress_prefix}   Tournament ID: {import_result['tournament_id']}")
                print(f"{progress_prefix}   Groups: {len(import_result['groups'])}")
            
            return (display, 'imported')
            
        except Exception as e:
            with self.print_lock:
                print(f"{progress_prefix}❌ Error importing tournament: {e}")
                import traceback
                traceback.print_exc()
            return (display, 'failed')
    
    def import_all(self, 
                   limit: Optional[int] = None,
                   skip_existing: bool = True,
                   start_date: Optional[str] = None,
                   max_workers: int = 5) -> Dict:
        """
        Import all tournaments in parallel
        
        Args:
            limit: Maximum number of tournaments to import (None for all)
            skip_existing: Skip tournaments that already exist
            start_date: Start from this date (YYYY-MM-DD format), None for latest
            max_workers: Maximum number of parallel workers (default: 5)
            
        Returns:
            Dictionary with import statistics
        """
        print("="*60)
        print("Round Robin Tournament Importer (Parallel)")
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
                # Only include tournaments that have a date and are >= start_date
                # Tournaments with None dates (old PDFs) are excluded from date filtering
                tournaments = [t for t in tournaments if t['date'] is not None and t['date'] >= start_datetime]
                print(f"Filtered to tournaments from {start_date} onwards: {len(tournaments)} tournaments")
            except ValueError:
                print(f"Invalid start_date format: {start_date}. Use YYYY-MM-DD")
                return {'total': 0, 'imported': 0, 'skipped': 0, 'failed': 0}
        
        # Apply limit
        if limit:
            tournaments = tournaments[:limit]
            print(f"Limited to {limit} tournaments")
        
        print(f"\nWill process {len(tournaments)} tournaments in parallel (max {max_workers} workers)")
        print(f"Starting from: {tournaments[0]['display'] if tournaments else 'N/A'}")
        print()
        
        stats = {
            'total': len(tournaments),
            'imported': 0,
            'skipped': 0,
            'failed': 0
        }
        
        # Import tournaments in parallel
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_tournament = {
                executor.submit(
                    self.import_tournament, 
                    tournament, 
                    skip_existing, 
                    i + 1, 
                    len(tournaments)
                ): tournament 
                for i, tournament in enumerate(tournaments)
            }
            
            # Process completed tasks as they finish
            completed = 0
            for future in as_completed(future_to_tournament):
                completed += 1
                tournament = future_to_tournament[future]
                try:
                    display, status = future.result()
                    if status == 'imported':
                        stats['imported'] += 1
                    elif status == 'skipped':
                        stats['skipped'] += 1
                    else:
                        stats['failed'] += 1
                    
                    with self.print_lock:
                        print(f"\n[{completed}/{len(tournaments)}] Completed: {display} - {status}")
                except Exception as e:
                    stats['failed'] += 1
                    with self.print_lock:
                        print(f"\n[{completed}/{len(tournaments)}] Error processing {tournament.get('display', 'unknown')}: {e}")
        
        elapsed_time = time.time() - start_time
        
        # Print summary
        print("\n" + "="*60)
        print("Import Summary")
        print("="*60)
        print(f"Total tournaments: {stats['total']}")
        print(f"✅ Successfully imported: {stats['imported']}")
        print(f"⏭️  Skipped (already exist): {stats['skipped']}")
        print(f"❌ Failed: {stats['failed']}")
        print(f"⏱️  Total time: {elapsed_time:.2f} seconds")
        print(f"⚡ Average time per tournament: {elapsed_time / stats['total']:.2f} seconds")
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
        '--max-workers',
        type=int,
        default=5,
        help='Maximum number of parallel workers (default: 5)'
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
            max_workers=args.max_workers
        )


if __name__ == '__main__':
    main()

