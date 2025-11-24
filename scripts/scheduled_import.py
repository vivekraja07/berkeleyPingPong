#!/usr/bin/env python3
"""
Scheduled Tournament Import Script
Imports only new tournaments that haven't been parsed yet.
Designed to run weekly (e.g., every Friday at midnight PST).
"""
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.import_all_tournaments import TournamentImporter


def main():
    """
    Import new tournaments that haven't been parsed yet.
    Only processes tournaments from the last 2 weeks to avoid re-processing old data.
    """
    print("="*60)
    print("Scheduled Tournament Import")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    print()
    
    # Calculate date 2 weeks ago (to catch any tournaments we might have missed)
    # This ensures we don't miss tournaments if the script fails to run one week
    two_weeks_ago = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    
    print(f"Importing tournaments from {two_weeks_ago} onwards...")
    print("(This ensures we catch any tournaments from the past 2 weeks)")
    print()
    
    importer = TournamentImporter()
    
    # Import tournaments starting from 2 weeks ago
    # The import script will automatically skip tournaments that already exist
    stats = importer.import_all(
        limit=None,  # No limit - import all new tournaments
        skip_existing=True,  # Skip tournaments that are already imported
        start_date=two_weeks_ago,
        max_workers=5  # Process in parallel for speed
    )
    
    print()
    print("="*60)
    print("Import Complete")
    print("="*60)
    print(f"Total processed: {stats['total']}")
    print(f"✅ Newly imported: {stats['imported']}")
    print(f"⏭️  Already existed: {stats['skipped']}")
    print(f"❌ Failed: {stats['failed']}")
    print("="*60)
    
    # Exit with error code if there were failures (for monitoring)
    if stats['failed'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()

