#!/usr/bin/env python3
"""
Main script for parsing ping pong history from PDF and HTML sources
and storing them in Supabase
"""
import argparse
import sys
from pathlib import Path
from typing import List, Dict
from parsers.pdf_parser import PDFParser
from parsers.html_parser import HTMLParser
from db.supabase_client import SupabaseClient


def parse_pdf_files(file_paths: List[str]) -> List[Dict]:
    """Parse multiple PDF files"""
    parser = PDFParser()
    all_matches = []
    
    for file_path in file_paths:
        print(f"Parsing PDF: {file_path}")
        try:
            if file_path.startswith('http://') or file_path.startswith('https://'):
                matches = parser.parse_url(file_path)
            else:
                matches = parser.parse_file(file_path)
            
            # Add source information
            for match in matches:
                match['source'] = file_path
            
            all_matches.extend(matches)
            print(f"  Found {len(matches)} matches")
        except Exception as e:
            print(f"  Error parsing {file_path}: {e}")
    
    return all_matches


def parse_html_files(file_paths: List[str]) -> List[Dict]:
    """Parse multiple HTML files"""
    parser = HTMLParser()
    all_matches = []
    
    for file_path in file_paths:
        print(f"Parsing HTML: {file_path}")
        try:
            if file_path.startswith('http://') or file_path.startswith('https://'):
                matches = parser.parse_url(file_path)
            else:
                matches = parser.parse_file(file_path)
            
            # Add source information
            for match in matches:
                match['source'] = file_path
            
            all_matches.extend(matches)
            print(f"  Found {len(matches)} matches")
        except Exception as e:
            print(f"  Error parsing {file_path}: {e}")
    
    return all_matches


def main():
    parser = argparse.ArgumentParser(
        description='Parse ping pong history from PDF and HTML files and store in Supabase'
    )
    parser.add_argument(
        '--pdf',
        nargs='+',
        help='PDF file paths or URLs to parse'
    )
    parser.add_argument(
        '--html',
        nargs='+',
        help='HTML file paths or URLs to parse'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Parse files but do not insert into database'
    )
    
    args = parser.parse_args()
    
    if not args.pdf and not args.html:
        parser.print_help()
        sys.exit(1)
    
    all_matches = []
    
    # Parse PDF files
    if args.pdf:
        pdf_matches = parse_pdf_files(args.pdf)
        all_matches.extend(pdf_matches)
    
    # Parse HTML files
    if args.html:
        html_matches = parse_html_files(args.html)
        all_matches.extend(html_matches)
    
    print(f"\nTotal matches found: {len(all_matches)}")
    
    if not all_matches:
        print("No matches found. Exiting.")
        return
    
    # Display sample matches
    print("\nSample matches:")
    for i, match in enumerate(all_matches[:5], 1):
        print(f"  {i}. {match}")
    
    if args.dry_run:
        print("\nDry run mode - not inserting into database")
        return
    
    # Insert into Supabase
    print("\nConnecting to Supabase...")
    try:
        db_client = SupabaseClient()
        print("Inserting matches into database...")
        inserted = db_client.insert_matches(all_matches)
        print(f"Successfully inserted {len(inserted)} matches")
    except Exception as e:
        print(f"Error connecting to Supabase: {e}")
        print("Make sure SUPABASE_URL and SUPABASE_KEY are set in your .env file")
        sys.exit(1)


if __name__ == '__main__':
    main()

