#!/usr/bin/env bash
# Run the same steps as .github/workflows/scheduled-import.yml locally (no push needed).
#
# Usage (from anywhere):
#   bash scripts/run_github_actions_import_locally.sh smoke          # fast: deps + db + dry-run scrape
#   bash scripts/run_github_actions_import_locally.sh full           # exact CI: deps + scheduled_import.py
#   bash scripts/run_github_actions_import_locally.sh smoke --skip-pip
#
# Requires: repo-root .env with SUPABASE_URL and SUPABASE_KEY (see docs/GITHUB_ACTIONS_SETUP.md).
#
# Optional — run the real workflow in Docker (slower, closest to Ubuntu runner):
#   brew install act   # https://github.com/nektos/act
#   act -j import-tournaments -W .github/workflows/scheduled-import.yml --secret-file .env
# (You must pass secrets; never commit .env.)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="smoke"
SKIP_PIP=0
for arg in "$@"; do
  case "$arg" in
    smoke|full) MODE="$arg" ;;
    --skip-pip) SKIP_PIP=1 ;;
    *)
      echo "Unknown argument: $arg"
      echo "Usage: $0 [smoke|full] [--skip-pip]"
      exit 1
      ;;
  esac
done

echo "==> Repo root: $ROOT"
echo "==> Mode: $MODE"

if [[ "$SKIP_PIP" -eq 0 ]]; then
  echo "==> Installing dependencies (same as CI: requirements.txt)"
  python3 -m pip install --upgrade pip
  pip install -r requirements.txt
else
  echo "==> Skipping pip install (--skip-pip)"
fi

case "$MODE" in
  smoke)
    echo "==> DB connectivity (fast)"
    python3 scripts/test_scheduled_import.py --mode db-test
    echo "==> Dry-run scrape + date window (no writes)"
    python3 scripts/test_scheduled_import.py --mode dry-run
    echo "==> Smoke OK — run '$0 full' before relying on CI, or push and use Run workflow."
    ;;
  full)
    echo "==> scheduled_import.py (same command as GitHub Actions)"
    python3 scripts/scheduled_import.py
    ;;
esac
