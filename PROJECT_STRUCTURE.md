# Project Structure

This document describes the organized folder structure of the Berkeley Ping Pong Round Robin Tournament application.

## Infrastructure (production)

The live setup is intentionally **low-cost**:

- **GitHub Actions** — runs **weekly** (Saturdays 09:00 UTC) to **scrape** the latest tournaments from berkeleytabletennis.org and **import** them into the database (`scripts/scheduled_import.py`, workflow under `.github/workflows/`).
- **Supabase** — database on the **free** tier.
- **Render** — Flask web app on the **free** tier (cold starts after idle).
- **FastCron** — optional external HTTP cron to mitigate **free-tier limits** (e.g. keep Render from always sleeping right before traffic).

Full detail: **[docs/INFRASTRUCTURE.md](docs/INFRASTRUCTURE.md)**.

## Directory Structure

```
berkeleyPingPong/
├── backend/                 # Backend application code
│   ├── app.py              # Flask application entry point
│   ├── api/                # API routes
│   │   ├── __init__.py
│   │   └── routes.py       # All API endpoint definitions
│   ├── db/                 # Database clients
│   │   ├── __init__.py
│   │   └── round_robin_client.py
│   └── parsers/            # Data parsers
│       ├── __init__.py
│       ├── round_robin_parser.py
│       └── round_robin_pdf_parser.py
│
├── frontend/                # Frontend application code
│   ├── templates/          # HTML templates
│   │   ├── index.html      # Home page
│   │   └── player.html    # Player detail page
│   └── static/             # Static assets
│       ├── css/            # Stylesheets
│       └── js/             # JavaScript files
│
├── scripts/                 # Utility and setup scripts
│   ├── import_round_robin.py        # Import single tournament
│   ├── import_all_tournaments.py    # Import all tournaments
│   ├── check_setup.py               # Verify environment setup
│   ├── check_tables.py               # Verify database tables
│   ├── setup_and_run.sh              # Setup and run script
│   └── start_ui.sh                   # Start UI script
│
├── sql/                    # SQL schema files
│   ├── round_robin_schema.sql       # Main database schema
│   ├── schema_fix.sql               # Schema fixes
│   └── create_missing_tables.sql    # Missing table creation
│
├── docs/                   # Documentation
│   ├── README.md
│   ├── INFRASTRUCTURE.md   # GitHub Actions, Supabase, Render, FastCron
│   ├── QUICK_START.md
│   ├── UI_README.md
│   ├── DEPLOYMENT.md
│   ├── QUICK_DEPLOY.md
│   ├── ROUND_ROBIN_DATABASE.md
│   └── ...
├── .github/workflows/      # Weekly scheduled import (see INFRASTRUCTURE.md)
│
├── Procfile                # Heroku/Render deployment config
├── render.yaml             # Render deployment config
├── requirements.txt        # Python dependencies
└── runtime.txt             # Python runtime version
```

## Key Components

### Backend (`backend/`)
- **app.py**: Main Flask application that initializes the app and registers routes
- **api/routes.py**: Contains all API endpoint definitions
- **db/**: Database client for interacting with Supabase
- **parsers/**: Parsers for extracting data from HTML and PDF sources

### Frontend (`frontend/`)
- **templates/**: Jinja2 HTML templates
- **static/**: CSS and JavaScript files served by Flask

### Scripts (`scripts/`)
- Import and data management scripts
- Setup and verification utilities

### SQL (`sql/`)
- Database schema definitions
- Migration and fix scripts

### Documentation (`docs/`)
- All project documentation and guides; start with **INFRASTRUCTURE.md** for hosting and automation

## Running the Application

### Start the Web UI
```bash
python3 backend/app.py
```

Or use the convenience script:
```bash
./scripts/start_ui.sh
```

### Import Tournament Data
```bash
python3 scripts/import_round_robin.py --url <url>
```

### Check Setup
```bash
python3 scripts/check_setup.py
python3 scripts/check_tables.py
```

## Import Paths

All scripts in the `scripts/` folder automatically add the project root to `sys.path`, allowing them to import from `backend.*` modules.

The Flask app in `backend/app.py` also adds the project root to the path to enable imports.

