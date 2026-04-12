# Infrastructure and automation

This project is built to run on **free tiers** and a small amount of external scheduling. Here is how the pieces fit together.

## Stack overview

| Piece | Role | Tier |
|--------|------|------|
| **GitHub Actions** | Weekly job that runs `scripts/scheduled_import.py`: scrapes [Berkeley Table Tennis results](https://berkeleytabletennis.org/results), imports new tournaments into the database | Free for public repos (within [usage limits](https://docs.github.com/en/billing/managing-billing-for-github-actions/about-billing-for-github-actions)) |
| **Supabase** | PostgreSQL + API for tournament and player data | [Free tier](https://supabase.com/pricing) (see [Keeping Supabase active](#keeping-supabase-active-free-tier)) |
| **Render** | Hosts the Flask web UI (`gunicorn backend.app:app`; see `render.yaml`) | [Free tier](https://render.com/pricing) (web service spins down after idle; cold start on first request) |
| **FastCron** (optional) | External HTTP cron to work around platform limits—see below | Depends on [FastCron](https://www.fastcron.com/) plan |

## Weekly data refresh (GitHub Actions)

- **Workflow:** `.github/workflows/scheduled-import.yml`
- **Schedule:** Cron **`0 9 * * 6`** (Saturdays **09:00 UTC**). GitHub Actions only supports **UTC**, so this targets **1:00 AM Pacific when PST** is in effect; during **PDT** the same run is **2:00 AM local** (a one-hour shift when clocks change—expected for a fixed UTC schedule). You can also run it manually (**Actions → Scheduled Tournament Import → Run workflow**).
- **What it does:** Executes the scheduled import: discovers tournament links, pulls HTML/PDFs, validates, and writes to Supabase. Failures can open a GitHub issue (see the workflow).
- **Secrets:** `SUPABASE_URL` and `SUPABASE_KEY` must be set (repository secrets and/or the `supabase` GitHub Environment as configured in the workflow).

Detailed setup: [GITHUB_ACTIONS_SETUP.md](./GITHUB_ACTIONS_SETUP.md). Behavior of the import script: [SCHEDULED_IMPORT_GUIDE.md](./SCHEDULED_IMPORT_GUIDE.md).

## Keeping Supabase active (free tier)

On the **free** plan, Supabase may **pause** a project after it sees **no activity** for an extended period (often described as on the order of **a week**—check [current pricing and platform behavior](https://supabase.com/pricing) and dashboard notices). Pausing is separate from Render sleeping: it is about **your database/API project** on Supabase’s side.

**What counts:** Successful use of the Supabase API against your project (for example reads or writes through the client or PostgREST) typically counts as activity.

**Why you might still get paused**

- The **weekly import** did not run for more than ~7 days (workflow disabled, failed secrets, environment waiting for approval, fork without Actions, etc.).
- Imports succeeded but something else left the project idle—less common if imports touch the DB every week.

**What to do**

1. **Verify the weekly workflow** — In GitHub: **Actions → Scheduled Tournament Import** and confirm recent runs are **green**. Fix secrets or environment rules if runs are skipped or failing.
2. **Optional redundant activity** — If you use **FastCron** (or similar), point it at an **API route that hits the database**, e.g. `https://<your-app>.onrender.com/api/players`, not only the HTML homepage (`/` loads stats via JavaScript and does not run a DB query on the first request). That helps both Render (wake) and Supabase (API activity) if the import ever misses a week.
3. **Guaranteed fix:** **Upgrade** the Supabase project to a **paid** plan if you need the platform’s guarantee against inactivity-based pausing (confirm current policy in Supabase docs for your plan).

## Why FastCron?

Free tiers impose practical limits:

- **Render (free):** The web service **sleeps** after a period of inactivity. The first request after sleep pays a **cold start** (often tens of seconds).
- **GitHub Actions:** Scheduled workflows are reliable for weekly jobs but are not meant to keep a web app warm 24/7.

**FastCron** is used as a lightweight **external scheduler** (HTTP GET to a URL on a cadence you choose) to mitigate those limits—for example:

- **Wake Render:** Pinging the homepage (`/`) reduces cold starts for visitors.
- **Touch Supabase:** Pinging an API route such as **`/api/players`** runs a server-side DB read (the homepage alone does not; see [Keeping Supabase active](#keeping-supabase-active-free-tier)).

Configure FastCron in their dashboard (this repo does not store FastCron credentials).

## Local development

Use a `.env` file with `SUPABASE_URL` and `SUPABASE_KEY` (see [QUICK_START.md](./QUICK_START.md)). The weekly job does not run locally unless you trigger the script yourself or use a local cron.

## Related docs

- [DEPLOYMENT.md](./DEPLOYMENT.md) — deploying the Flask app (Render and alternatives)
- [PROJECT_STRUCTURE.md](../PROJECT_STRUCTURE.md) — repository layout
