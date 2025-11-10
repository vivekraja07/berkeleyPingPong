# Round Robin Statistics UI

A web application for visualizing round robin tournament statistics with interactive charts.

## Features

- **Player List**: Browse all players in the database
- **Player Statistics**: View detailed statistics for each player
- **Rating Charts**: Line chart showing rating changes over time
- **Match Statistics**: Doughnut chart showing win/loss/draw breakdown
- **Recent Matches**: Table of recent match results
- **Search**: Search for players by name

## Running the Application

### Start the Flask Server

```bash
python3 app.py
```

The application will start on `http://localhost:5000`

### Access the UI

1. **Home Page**: `http://localhost:5000`
   - Lists all players
   - Search functionality
   - Click on any player to view their statistics

2. **Player Page**: `http://localhost:5000/player/<player_name>`
   - Rating over time chart
   - Match statistics chart
   - Recent matches table
   - Overall statistics cards

## API Endpoints

The application provides REST API endpoints:

- `GET /api/players` - Get all players
- `GET /api/player/<player_name>/rating-history` - Get player rating history
- `GET /api/player/<player_name>/match-stats` - Get player match statistics
- `GET /api/player/<player_name>/tournament-stats` - Get player tournament statistics
- `GET /api/player/<player_name>/matches?limit=N` - Get recent matches

## Technologies Used

- **Backend**: Flask (Python)
- **Frontend**: HTML, CSS, JavaScript
- **Charts**: Chart.js
- **Database**: Supabase (via round_robin_client)

## Development

The application structure:

```
berkeleyPingPong/
├── app.py                 # Flask application
├── templates/
│   ├── index.html         # Home page
│   └── player.html        # Player detail page
├── static/
│   ├── css/               # Custom styles (if any)
│   └── js/                # Custom JavaScript (if any)
└── db/
    └── round_robin_client.py  # Database client
```

## Troubleshooting

### Port Already in Use

If port 5000 is already in use, you can change it:

```bash
PORT=5001 python3 app.py
```

### Database Connection Issues

Make sure your `.env` file has the correct Supabase credentials:
- `SUPABASE_URL`
- `SUPABASE_KEY`

### No Data Showing

- Verify that tournaments have been imported
- Check that the database tables exist
- Run `python3 check_tables.py` to verify setup

