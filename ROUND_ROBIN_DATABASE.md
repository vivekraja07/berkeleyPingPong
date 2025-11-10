# Round Robin Tournament Database

This document explains how to set up and use the database for storing round robin tournament data and querying player statistics for charts.

## Database Schema

The database consists of the following tables:

### Core Tables

1. **players** - Stores unique player information
2. **tournaments** - Stores tournament information with dates
3. **round_robin_groups** - Stores round robin group information
4. **player_tournament_stats** - Stores comprehensive player statistics per tournament/group
5. **player_rating_history** - Tracks player rating changes over time (for charting)
6. **matches** - Stores individual match results

### Views

1. **player_stats_view** - Player statistics with tournament details
2. **match_results_view** - Match results with player names
3. **player_rating_chart_view** - Player rating history formatted for charting

### Functions

1. **get_player_rating_history(player_name)** - Get player rating over time
2. **get_player_match_stats(player_name)** - Get player match statistics

## Setup

### 1. Create Database Tables

Run the SQL schema file in your Supabase SQL Editor:

```bash
# Copy the contents of round_robin_schema.sql and run in Supabase SQL Editor
```

Or use the Supabase CLI:

```bash
supabase db push
```

### 2. Configure Environment Variables

Make sure your `.env` file has:

```bash
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_anon_key
```

## Usage

### Import Round Robin Data

Import data from a URL:

```bash
python import_round_robin.py --url https://berkeleytabletennis.org/results/rr_results_2025nov07
```

Import from a local HTML file:

```bash
python import_round_robin.py --file path/to/results.html
```

Dry run (parse without inserting):

```bash
python import_round_robin.py --url https://berkeleytabletennis.org/results/rr_results_2025nov07 --dry-run
```

### Query Player Statistics

Query statistics for a specific player:

```bash
python import_round_robin.py --query-player "GuangPeng Chen"
```

List all players:

```bash
python import_round_robin.py --list-players
```

## Querying Data for Charts

### Player Rating History (for line charts)

```python
from db.round_robin_client import RoundRobinClient

client = RoundRobinClient()
rating_history = client.get_player_rating_history("GuangPeng Chen")

# Returns list of dicts with:
# - tournament_date: Date of tournament
# - rating_pre: Rating before tournament
# - rating_post: Rating after tournament
# - rating_change: Change in rating
# - tournament_name: Name of tournament
```

### Player Match Statistics

```python
match_stats = client.get_player_match_stats("GuangPeng Chen")

# Returns dict with:
# - total_matches: Total number of matches
# - wins: Number of wins
# - losses: Number of losses
# - draws: Number of draws
# - win_percentage: Win percentage
```

### Player Statistics by Tournament

```python
tournament_stats = client.get_player_stats_by_tournament("GuangPeng Chen")

# Returns list of dicts with all player statistics per tournament
```

### Direct SQL Queries

You can also query directly using SQL in Supabase:

#### Get player rating history for charting:

```sql
SELECT 
    tournament_date,
    rating_pre,
    rating_post,
    rating_change,
    tournament_name
FROM player_rating_chart_view
WHERE player_name = 'GuangPeng Chen'
ORDER BY tournament_date ASC;
```

#### Get player match win/loss over time:

```sql
SELECT 
    t.date AS tournament_date,
    t.name AS tournament_name,
    COUNT(*) FILTER (WHERE m.winner_id = p.id) AS wins,
    COUNT(*) FILTER (WHERE m.winner_id != p.id AND m.winner_id IS NOT NULL) AS losses
FROM players p
JOIN matches m ON (m.player1_id = p.id OR m.player2_id = p.id)
JOIN tournaments t ON m.tournament_id = t.id
WHERE p.name = 'GuangPeng Chen'
GROUP BY t.id, t.date, t.name
ORDER BY t.date ASC;
```

#### Get top players by rating:

```sql
SELECT 
    p.name,
    MAX(prh.rating_post) AS current_rating,
    COUNT(DISTINCT prh.tournament_id) AS tournaments_played
FROM players p
JOIN player_rating_history prh ON p.id = prh.player_id
GROUP BY p.id, p.name
ORDER BY current_rating DESC
LIMIT 10;
```

## Example: Creating Charts

### Python Example with Matplotlib

```python
from db.round_robin_client import RoundRobinClient
import matplotlib.pyplot as plt
from datetime import datetime

client = RoundRobinClient()
rating_history = client.get_player_rating_history("GuangPeng Chen")

# Extract data for charting
dates = [datetime.strptime(r['tournament_date'], '%Y-%m-%d') for r in rating_history]
ratings = [r['rating_post'] for r in rating_history]

# Create line chart
plt.figure(figsize=(12, 6))
plt.plot(dates, ratings, marker='o', linewidth=2, markersize=8)
plt.title('Rating Over Time - GuangPeng Chen')
plt.xlabel('Date')
plt.ylabel('Rating')
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

### JavaScript/React Example

```javascript
// Fetch player rating history
const fetchPlayerRatingHistory = async (playerName) => {
  const response = await fetch(
    `https://your-project.supabase.co/rest/v1/rpc/get_player_rating_history`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'apikey': 'your-anon-key'
      },
      body: JSON.stringify({ player_name_param: playerName })
    }
  );
  return await response.json();
};

// Use with Chart.js
import { Line } from 'react-chartjs-2';

const RatingChart = ({ playerName }) => {
  const [data, setData] = useState(null);
  
  useEffect(() => {
    fetchPlayerRatingHistory(playerName).then(setData);
  }, [playerName]);
  
  if (!data) return <div>Loading...</div>;
  
  const chartData = {
    labels: data.map(d => d.tournament_date),
    datasets: [{
      label: 'Rating',
      data: data.map(d => d.rating_post),
      borderColor: 'rgb(75, 192, 192)',
      tension: 0.1
    }]
  };
  
  return <Line data={chartData} />;
};
```

## Database Indexes

The schema includes indexes for optimal query performance:

- `idx_players_name` - Fast player lookups
- `idx_tournaments_date` - Fast tournament date queries
- `idx_rating_history_player_date` - Fast rating history queries by player and date
- `idx_matches_players` - Fast match queries by players

## Notes

- The database uses foreign key constraints to maintain data integrity
- Automatic triggers set `winner_id` in matches based on scores
- The `player_rating_history` table is specifically designed for time-series charting
- Use the views for easier querying without complex joins

