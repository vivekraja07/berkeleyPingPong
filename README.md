# Berkeley Ping Pong — round robin stats

Flask app + importer for Berkeley Table Tennis round robin results, backed by **Supabase**.

## How this repo is run in production (free tier)

| Component | What it does |
|-----------|----------------|
| **GitHub Actions** | **Weekly** import: scrape results and refresh the database. |
| **Supabase** | Database (free tier). |
| **Render** | Web UI hosting (free tier; the service **spins down** when idle). |
| **FastCron** | Optional external HTTP cron to **work around free-tier limits**—e.g. ping the Render URL so the app is less likely to be cold when someone visits. |

Details, scheduling, and secrets: **[docs/INFRASTRUCTURE.md](docs/INFRASTRUCTURE.md)**.

## Quick links

- [Quick start (local)](docs/QUICK_START.md)
- [Project structure](PROJECT_STRUCTURE.md)
- [Deployment](docs/DEPLOYMENT.md)
- [GitHub Actions scheduled import](docs/GITHUB_ACTIONS_SETUP.md)
