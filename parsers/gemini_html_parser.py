import re
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from datetime import datetime

# 1. SETUP: Connect to your Supabase project
# Get these from your Supabase Project Settings > API
SUPABASE_URL = "https://aqqzmaknlbfsuagjbbdi.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFxcXptYWtubGJmc3VhZ2piYmRpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI3MTgzODQsImV4cCI6MjA3ODI5NDM4NH0.kwXHzFpRq0DnMTDmCZ-cPJdYueq-YEuQqG-ixESW9Jg"  # Use the secret one!
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- HELPER FUNCTIONS ---
# We use these to cache player/tournament lookups to speed things up.
player_cache = {}
def get_or_create_player(player_name: str) -> int:
    """Finds a player by name. If not found, creates them. Returns the player's ID."""
    player_name = player_name.strip()
    if player_name in player_cache:
        return player_cache[player_name]
    
    try:
        res = supabase.from_("players").select("id").eq("name", player_name).execute()
        if res.data:
            player_id = res.data[0]['id']
            player_cache[player_name] = player_id
            return player_id
        
        print(f"Creating new player: {player_name}")
        res = supabase.from_("players").insert({"name": player_name}).execute()
        player_id = res.data[0]['id']
        player_cache[player_name] = player_id
        return player_id
    except Exception as e:
        print(f"Error getting/creating player {player_name}: {e}")
        return None

def get_or_create_tournament(tournament_name: str, tournament_date: str) -> int:
    """Finds a tournament. If not found, creates it. Returns the tournament's ID."""
    try:
        res = supabase.from_("tournaments").select("id").eq("name", tournament_name).execute()
        if res.data:
            return res.data[0]['id']
        
        print(f"Creating new tournament: {tournament_name}")
        res = supabase.from_("tournaments").insert({
            "name": tournament_name,
            "date": tournament_date
        }).execute()
        return res.data[0]['id']
    except Exception as e:
        print(f"Error getting/creating tournament {tournament_name}: {e}")
        return None

# --- MAIN SCRIPT ---
# --- MAIN SCRIPT ---
def main():
    url = "https://berkeleytabletennis.org/results/rr_results_2025nov07"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    # --- 1. Get Tournament Info ---
    title_tag = soup.find("h1")
    if not title_tag:
        print("Could not find <h1> title tag. Exiting.")
        return
        
    tournament_name = title_tag.get_text(strip=True)
    
    date_str_match = re.search(r"for (\d{4} \w+ \d+)", tournament_name)
    if not date_str_match:
        print(f"Could not parse date from title: {tournament_name}")
        return
        
    date_str = date_str_match.group(1) # "2025 Nov 7"
    tournament_date = datetime.strptime(date_str, "%Y %b %d").isoformat()
    
    print(f"Processing Tournament: {tournament_name} ({tournament_date})")
    tournament_id = get_or_create_tournament(tournament_name, tournament_date)
    if not tournament_id:
        print("Could not create tournament. Exiting.")
        return

    # --- 2. Find All Group Tables ---
    
    # Debug: Check what H3 tags exist
    all_h3s = soup.find_all("h3")
    print(f"DEBUG: Found {len(all_h3s)} H3 tags total")
    if all_h3s:
        print("DEBUG: First few H3 tags:")
        for i, h3 in enumerate(all_h3s[:5]):
            print(f"  {i+1}. Text: '{h3.get_text(strip=True)}' | HTML: {str(h3)[:100]}")
    
    # Try multiple methods to find group headers
    group_headers = []
    
    # Method 1: Find H3 tags that start with "#"
    for h3 in all_h3s:
        text = h3.get_text(strip=True)
        if text.startswith("#"):
            group_headers.append(h3)
    
    # Method 2: If no H3 found, try finding by text pattern in any heading
    if not group_headers:
        print("DEBUG: Trying alternative method - searching all headings for # pattern...")
        all_headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        for heading in all_headings:
            text = heading.get_text(strip=True)
            if re.match(r'^#\d+', text):
                group_headers.append(heading)
                print(f"DEBUG: Found group header: '{text}' in {heading.name} tag")
    
    # Method 3: Try finding tables directly and look for preceding headers
    if not group_headers:
        print("DEBUG: Trying to find tables and their preceding elements...")
        all_tables = soup.find_all("table")
        print(f"DEBUG: Found {len(all_tables)} <table> tags")
        # Also check for div-based tables
        table_divs = soup.find_all("div", class_=re.compile(r"table|row", re.I))
        print(f"DEBUG: Found {len(table_divs)} divs with table/row classes")
        for table in all_tables:
            # Look for preceding sibling that might be a group header
            prev = table.find_previous_sibling()
            while prev and prev.name not in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'div']:
                prev = prev.find_previous_sibling()
            if prev:
                text = prev.get_text(strip=True)
                if re.match(r'^#\d+', text):
                    group_headers.append(prev)
                    print(f"DEBUG: Found group header before table: '{text}'")
    
    # Method 4: Search for any element containing # followed by a number
    if not group_headers:
        print("DEBUG: Searching for any element containing # pattern...")
        # Find all elements that contain text matching #\d+
        for element in soup.find_all(string=re.compile(r'^#\d+$')):
            parent = element.parent
            if parent and parent not in group_headers:
                group_headers.append(parent)
                print(f"DEBUG: Found group header via text search: '{element.strip()}' in {parent.name} tag")

    print(f"Found {len(group_headers)} groups to process.")

    if not group_headers:
        print("Warning: No group headers (e.g., '#1') were found. Check the HTML structure.")
        print("DEBUG: Saving HTML snippet for inspection...")
        # Save a snippet of the HTML for debugging
        with open("/tmp/debug_html_snippet.html", "w") as f:
            f.write(str(soup)[:5000])
        print("DEBUG: HTML snippet saved to /tmp/debug_html_snippet.html")
        
    for header in group_headers:
        group_name = header.get_text(strip=True)
        print(f"\n--- Processing Group: {group_name} ---")
        
        # Try multiple methods to find the table associated with this group header
        # Note: The page uses divs styled as tables, not actual <table> tags
        table = None
        
        # Method 1: Look for div with table-like structure after the header
        # The table might be in a div with class containing "table" or "row"
        next_elem = header.find_next_sibling()
        if next_elem and next_elem.name == "div":
            # Check if this div contains table-like structure (has rows)
            rows = next_elem.find_all("div", class_=re.compile(r"row", re.I))
            if rows:
                table = next_elem
        
        # Method 2: Find the bracket container (all columns are in a bracket div)
        if not table:
            # Find the bracket container - it's the parent of all columns
            bracket = soup.find("div", class_="bracket")
            if bracket:
                table = bracket
                print(f"DEBUG: Found bracket container")
            else:
                # Fallback: find any container with many column divs
                current = header.find_next()
                while current:
                    if current.name == "div":
                        column_divs = current.find_all("div", class_=re.compile(r"^col\s", re.I))
                        if not column_divs and current.parent:
                            column_divs = current.parent.find_all("div", class_=re.compile(r"^col\s", re.I))
                        if len(column_divs) >= 4:
                            if current.parent and current.parent.find_all("div", class_=re.compile(r"^col\s", re.I)):
                                table = current.parent
                            else:
                                table = current
                            print(f"DEBUG: Found table with {len(column_divs)} columns")
                            break
                    if current in group_headers and current != header:
                        break
                    current = current.find_next()
        
        # Method 3: Look for actual HTML tables (fallback)
        if not table:
            table = header.find_next("table")
        
        # Method 4: Find divs that look like table rows in the parent container
        if not table:
            # Find the parent container and look for row divs
            parent = header.parent
            if parent:
                # Look for divs with row classes that come after this header
                row_divs = parent.find_all("div", class_=re.compile(r"row", re.I))
                if row_divs:
                    # Find rows that come after this header
                    header_index = None
                    for i, elem in enumerate(parent.find_all("div")):
                        if elem == header:
                            header_index = i
                            break
                    if header_index is not None:
                        # Get the next div that contains rows
                        for i in range(header_index + 1, len(parent.find_all("div"))):
                            next_div = parent.find_all("div")[i]
                            if next_div.find_all("div", class_=re.compile(r"row", re.I)):
                                table = next_div
                                break
        
        if not table:
            print(f"Could not find table for {group_name}")
            # Debug: show what's around the header
            print(f"DEBUG: Header HTML: {str(header)[:200]}")
            print(f"DEBUG: Next sibling: {header.find_next_sibling()}")
            continue

        # Handle columnar div-based table structure
        # The table is organized as columns, not rows
        # Each column is a <div class="col ..."> containing header and data rows
        group_players = {} 
        ratings_to_insert = []
        
        if table.name == "table":
            # Traditional table structure
            rows = table.find_all("tr")
            for row in rows[1:]:  # Skip header
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue
                try:
                    player_num = int(cols[0].get_text(strip=True))
                    player_name = cols[1].get_text(strip=True)
                    rating_pre = int(cols[2].get_text(strip=True)) if cols[2].get_text(strip=True).isdigit() else 0
                    rating_post = int(cols[3].get_text(strip=True)) if cols[3].get_text(strip=True).isdigit() else 0
                    
                    player_id = get_or_create_player(player_name)
                    if not player_id:
                        continue
                    
                    group_players[player_num] = {"name": player_name, "id": player_id}
                    ratings_to_insert.append({
                        "player_id": player_id,
                        "tournament_id": tournament_id,
                        "rating": rating_post
                    })
                except Exception as e:
                    print(f"Error parsing player row: {e}")
        else:
            # Columnar div structure - find columns that belong to this group
            # Find the next group header to limit which rows we process
            next_header = None
            for h in group_headers:
                if h != header:
                    # Check if this header comes after our current header in document order
                    if header in h.find_all_previous():
                        next_header = h
                        break
            
            # Find all column divs in the bracket
            column_divs = table.find_all("div", class_=re.compile(r"^col\s", re.I))
            
            if not column_divs:
                print(f"DEBUG: Could not find column divs. Table structure: {str(table)[:200]}")
                continue
            
            # Extract data from each column
            # Column 0: Player number
            # Column 1: Player name  
            # Column 2: Rating pre
            # Column 3: Rating post
            # Columns 4+: Match scores
            
            num_col = None
            name_col = None
            rating_pre_col = None
            rating_post_col = None
            match_cols = []
            
            # Find the col-1 div that contains the current header
            # The header's parent should be the col-1 div
            group_col_1 = header.parent if header.parent and 'col-1' in ' '.join(header.parent.get('class', [])) else None
            
            # If not found, search for it
            if not group_col_1:
                for col in column_divs:
                    col_class = ' '.join(col.get('class', []))
                    if 'col-1' in col_class:
                        # Check if header is in this col
                        if header in col or header.parent == col:
                            group_col_1 = col
                            break
            
            if not group_col_1:
                print(f"DEBUG: Could not find col-1 for group {group_name}")
                continue
            
            num_col = group_col_1
            
            # Find other columns that come after this col-1 but before the next col-1
            # They should be siblings in the bracket
            current = group_col_1.find_next_sibling()
            while current:
                # Stop if we hit the next group's col-1
                if current.name == "div" and 'col-1' in ' '.join(current.get('class', [])):
                    break
                # Stop if we hit the next header
                if current == next_header or (next_header and next_header in current.find_all()):
                    break
                
                if current.name == "div":
                    col_class = ' '.join(current.get('class', []))
                    if 'names' in col_class:
                        name_col = current
                    elif 'rating-pre' in col_class:
                        rating_pre_col = current
                    elif 'rating-post' in col_class:
                        rating_post_col = current
                    elif 'games' in col_class:
                        match_cols.append(current)
                
                current = current.find_next_sibling()
            
            # If we couldn't find by class, use position
            if not num_col and len(column_divs) > 0:
                num_col = column_divs[0]
            if not name_col and len(column_divs) > 1:
                name_col = column_divs[1]
            if not rating_pre_col and len(column_divs) > 2:
                rating_pre_col = column_divs[2]
            if not rating_post_col and len(column_divs) > 3:
                rating_post_col = column_divs[3]
            if not match_cols:
                match_cols = column_divs[4:] if len(column_divs) > 4 else []
            
            # Extract rows from columns
            # Each col-1 div contains one row (header or player number)
            # Find all col-1 divs between this header and next header
            # Search in the entire soup, not just table, to make sure we find all col-1 divs
            all_col_1_divs = soup.find_all("div", class_=re.compile(r"col-1", re.I))
            print(f"DEBUG: Found {len(all_col_1_divs)} total col-1 divs in document")
            group_col_1_divs = []
            
            # Find the index of our header's col-1 in the list
            header_col_idx = None
            for i, col_div in enumerate(all_col_1_divs):
                if header.parent == col_div:
                    header_col_idx = i
                    break
            
            # Find the index of next header's col-1
            next_header_col_idx = None
            if next_header:
                for i, col_div in enumerate(all_col_1_divs):
                    if next_header.parent == col_div:
                        next_header_col_idx = i
                        break
            
            print(f"DEBUG: header_col_idx={header_col_idx}, next_header_col_idx={next_header_col_idx}, total col-1 divs={len(all_col_1_divs)}")
            
            # Get col-1 divs between our header and next header
            if header_col_idx is not None:
                end_idx = next_header_col_idx if next_header_col_idx is not None else len(all_col_1_divs)
                for i in range(header_col_idx + 1, end_idx):
                    col_div = all_col_1_divs[i]
                    # Get the row inside this col-1
                    row = col_div.find("div", class_=re.compile(r"row", re.I))
                    if row and not row.find("div", class_=re.compile(r"row-header", re.I)):
                        # This is a player number row
                        group_col_1_divs.append((col_div, row))
            
            print(f"DEBUG: Found {len(group_col_1_divs)} player rows for group {group_name}")
            
            # Use these rows as data_rows
            data_rows = [row for col_div, row in group_col_1_divs]
            
            for i, row_div in enumerate(data_rows):
                    try:
                        # Get corresponding data from each column
                        player_num_str = row_div.get_text(strip=True)
                        if not player_num_str.isdigit():
                            print(f"DEBUG: Skipping row {i}, not a digit: '{player_num_str}'")
                            continue
                        
                        player_num = int(player_num_str)
                        print(f"DEBUG: Processing player {player_num} in group {group_name}")
                        
                        # Get player name from name column
                        # The name column has rows aligned with col-1 rows
                        # Find the col-1 div that contains this row, then find the corresponding name row
                        if name_col:
                            # Get the col-1 div that contains this row
                            row_col_1 = row_div.find_parent("div", class_=re.compile(r"col-1", re.I))
                            if row_col_1:
                                # Find the index of this col-1 in all_col_1_divs
                                row_col_idx = None
                                for j, col_div in enumerate(all_col_1_divs):
                                    if col_div == row_col_1:
                                        row_col_idx = j
                                        break
                                
                                # Find all name rows in the name column
                                all_name_rows = name_col.find_all("div", class_=re.compile(r"row", re.I))
                                # Filter out header rows
                                name_data_rows = [r for r in all_name_rows if not r.find("div", class_=re.compile(r"row-header", re.I)) and r.get_text(strip=True).lower() != "name"]
                                
                                # The name rows should be aligned with col-1 rows
                                # Find the name row at the same relative position
                                if row_col_idx is not None and row_col_idx < len(name_data_rows):
                                    # Find the name row that corresponds to this col-1 row
                                    # Name rows are in the same order as col-1 rows
                                    name_row_idx = row_col_idx
                                    # Skip header rows in name column
                                    name_header_count = len([r for r in all_name_rows if r.find("div", class_=re.compile(r"row-header", re.I)) or r.get_text(strip=True).lower() == "name"])
                                    if name_row_idx >= name_header_count:
                                        player_name = name_data_rows[name_row_idx - name_header_count].get_text(strip=True)
                                        print(f"DEBUG: Found player name: '{player_name}'")
                                    else:
                                        print(f"DEBUG: Name row index {name_row_idx} is before header count {name_header_count}")
                                        continue
                                else:
                                    # Fallback: use index in data_rows
                                    if i < len(name_data_rows):
                                        player_name = name_data_rows[i].get_text(strip=True)
                                        print(f"DEBUG: Found player name (fallback): '{player_name}'")
                                    else:
                                        print(f"DEBUG: No name row found for index {i}, have {len(name_data_rows)} name rows")
                                        continue
                            else:
                                print(f"DEBUG: Could not find parent col-1 for row")
                                continue
                        else:
                            print(f"DEBUG: No name_col found")
                            continue
                        
                        # Get ratings - use same approach
                        rating_pre = 0
                        rating_post = 0
                        
                        if rating_pre_col:
                            all_pre_rows = rating_pre_col.find_all("div", class_=re.compile(r"row", re.I))
                            pre_data_rows = []
                            for pr in all_pre_rows:
                                if pr.find("div", class_=re.compile(r"row-header", re.I)):
                                    continue
                                if header in pr.find_all_previous():
                                    if not next_header or next_header not in pr.find_all_previous():
                                        pre_data_rows.append(pr)
                            if i < len(pre_data_rows):
                                pre_str = pre_data_rows[i].get_text(strip=True)
                                rating_pre = int(pre_str) if pre_str.isdigit() else 0
                        
                        if rating_post_col:
                            all_post_rows = rating_post_col.find_all("div", class_=re.compile(r"row", re.I))
                            post_data_rows = []
                            for pr in all_post_rows:
                                if pr.find("div", class_=re.compile(r"row-header", re.I)):
                                    continue
                                if header in pr.find_all_previous():
                                    if not next_header or next_header not in pr.find_all_previous():
                                        post_data_rows.append(pr)
                            if i < len(post_data_rows):
                                post_str = post_data_rows[i].get_text(strip=True)
                                rating_post = int(post_str) if post_str.isdigit() else 0
                        
                        player_id = get_or_create_player(player_name)
                        if not player_id:
                            continue
                        
                        group_players[player_num] = {"name": player_name, "id": player_id}
                        ratings_to_insert.append({
                            "player_id": player_id,
                            "tournament_id": tournament_id,
                            "rating": rating_post
                        })
                    except Exception as e:
                        print(f"Error parsing player row {i}: {e}")


        matches_to_insert = []
        
        # Parse matches from columnar structure
        if table.name == "table":
            # Traditional table - parse matches
            table_rows = table.find_all("tr")
            for row in table_rows[1:]:
                cols = row.find_all("td")
                num_players_in_group = len(group_players)
                if len(cols) < (4 + num_players_in_group):
                    continue
                try:
                    player_num_str = cols[0].get_text(strip=True)
                    if not player_num_str.isdigit():
                        continue
                    p1_num = int(player_num_str)
                    if p1_num not in group_players:
                        continue
                    p1_id = group_players[p1_num]["id"]
                    match_cols_cells = cols[4 : 4 + num_players_in_group]
                    for i, match_cell in enumerate(match_cols_cells):
                        opponent_num = i + 1
                        if p1_num >= opponent_num:
                            continue
                        if opponent_num not in group_players:
                            continue
                        p2_id = group_players[opponent_num]["id"]
                        score_text = match_cell.get_text(strip=True)
                        score_match = re.match(r"(\d+)\s*(\d+)", score_text)
                        if score_match:
                            p1_games = int(score_match.group(1))
                            p2_games = int(score_match.group(2))
                            matches_to_insert.append({
                                "tournament_id": tournament_id,
                                "player1_id": p1_id,
                                "player2_id": p2_id,
                                "player1_score": p1_games,
                                "player2_score": p2_games
                            })
                except Exception as e:
                    print(f"Error parsing match row: {e}")
        else:
            # Columnar structure - parse matches from match columns
            # Re-find columns if needed (they should already be found in player parsing)
            if 'column_divs' not in locals() or not column_divs:
                column_divs = table.find_all("div", class_=re.compile(r"^col\s", re.I))
                if not column_divs and table.parent:
                    column_divs = table.parent.find_all("div", class_=re.compile(r"^col\s", re.I))
            
            # Re-identify columns if needed
            if 'num_col' not in locals() or not num_col:
                for col in column_divs:
                    col_class = ' '.join(col.get('class', []))
                    if 'names' in col_class:
                        name_col = col
                    elif 'rating-pre' in col_class:
                        rating_pre_col = col
                    elif 'rating-post' in col_class:
                        rating_post_col = col
                    elif not num_col and len(col.find_all("div", class_=re.compile(r"row", re.I))) > 0:
                        num_col = col
                if not num_col and len(column_divs) > 0:
                    num_col = column_divs[0]
            
            # Get match columns (columns after rating columns)
            match_cols = column_divs[4:] if len(column_divs) > 4 else []
            
            # Get player numbers and their corresponding match scores
            if num_col and match_cols:
                num_rows = num_col.find_all("div", class_=re.compile(r"row", re.I))
                num_data_rows = [r for r in num_rows if not r.find("div", class_=re.compile(r"row-header", re.I))]
                
                for i, num_row in enumerate(num_data_rows):
                    try:
                        player_num_str = num_row.get_text(strip=True)
                        if not player_num_str.isdigit():
                            continue
                        p1_num = int(player_num_str)
                        if p1_num not in group_players:
                            continue
                        p1_id = group_players[p1_num]["id"]
                        
                        # Parse matches from each match column
                        for col_idx, match_col in enumerate(match_cols):
                            opponent_num = col_idx + 1
                            
                            # Only process when p1_num < opponent_num (avoid duplicates)
                            if p1_num >= opponent_num:
                                continue
                            
                            if opponent_num not in group_players:
                                continue
                            
                            p2_id = group_players[opponent_num]["id"]
                            
                            # Get score from this column, row i
                            match_rows = match_col.find_all("div", class_=re.compile(r"row", re.I))
                            match_data_rows = [r for r in match_rows if not r.find("div", class_=re.compile(r"row-header", re.I))]
                            
                            if i < len(match_data_rows):
                                score_text = match_data_rows[i].get_text(strip=True)
                                score_match = re.match(r"(\d+)\s*(\d+)", score_text)
                                
                                if score_match:
                                    p1_games = int(score_match.group(1))
                                    p2_games = int(score_match.group(2))
                                    
                                    matches_to_insert.append({
                                        "tournament_id": tournament_id,
                                        "player1_id": p1_id,
                                        "player2_id": p2_id,
                                        "player1_score": p1_games,
                                        "player2_score": p2_games
                                    })
                    except Exception as e:
                        print(f"Error parsing match for row {i}: {e}")

        try:
            if ratings_to_insert:
                print(f"Inserting {len(ratings_to_insert)} rating entries...")
                supabase.from_("player_rating_history").insert(ratings_to_insert).execute()
            
            if matches_to_insert:
                print(f"Inserting {len(matches_to_insert)} matches...")
                supabase.from_("matches").insert(matches_to_insert).execute()
                
        except Exception as e:
            print(f"DATABASE ERROR for group {group_name}: {e}")

    print("\n--- HTML Import Complete ---")

if __name__ == "__main__":
    main()