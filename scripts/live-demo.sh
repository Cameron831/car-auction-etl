#!/usr/bin/env bash
set -euo pipefail

RESET_DB=0
CLEANUP=0
CLEANUP_ALL=0
DATABASE_URL_VALUE="postgresql://auction_user:localdevpassword@127.0.0.1:5433/auction_etl"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PYTHON="${REPO_ROOT}/.venv/bin/python"

usage() {
  cat <<'USAGE'
Usage: scripts/live-demo.sh [--reset-db] [--cleanup] [--cleanup-all]

Options:
  --reset-db      Remove the local Docker Compose database volume before running.
  --cleanup       Stop the Postgres container after the demo.
  --cleanup-all   Stop Postgres, remove the database volume, and remove .venv.
  -h, --help      Show this help text.
USAGE
}

for arg in "$@"; do
  case "$arg" in
    --reset-db)
      RESET_DB=1
      ;;
    --cleanup)
      CLEANUP=1
      ;;
    --cleanup-all)
      CLEANUP_ALL=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      usage >&2
      exit 2
      ;;
  esac
done

step() {
  printf '\n==> %s\n' "$1"
}

find_python() {
  local candidate
  for candidate in python3.11 python3 python; do
    if command -v "$candidate" >/dev/null 2>&1; then
      if "$candidate" -c 'import sys; raise SystemExit(sys.version_info < (3, 11))'; then
        printf '%s\n' "$candidate"
        return 0
      fi
    fi
  done

  return 1
}

cleanup() {
  local status=$?
  set +e

  cd "$REPO_ROOT"
  if [[ "$CLEANUP_ALL" -eq 1 ]]; then
    step "Removing demo database volume and virtual environment"
    docker compose down -v
    rm -rf .venv
  elif [[ "$CLEANUP" -eq 1 ]]; then
    step "Stopping Postgres container"
    docker compose stop postgres
  fi

  exit "$status"
}

trap cleanup EXIT

cd "$REPO_ROOT"

step "Checking prerequisites"
PYTHON="$(find_python)" || {
  echo "Python 3.11 or newer was not found." >&2
  exit 1
}
docker compose version >/dev/null

if [[ "$RESET_DB" -eq 1 ]]; then
  step "Resetting local demo database volume"
  docker compose down -v
fi

if [[ ! -x "$VENV_PYTHON" ]]; then
  step "Creating .venv"
  "$PYTHON" -m venv .venv
fi

step "Installing Python dependencies"
"$VENV_PYTHON" -m pip install -r requirements.txt

step "Installing Playwright Chromium"
"$VENV_PYTHON" -m playwright install chromium

step "Starting Postgres"
docker compose up -d postgres

export DATABASE_URL="$DATABASE_URL_VALUE"

step "Waiting for Postgres"
"$VENV_PYTHON" - <<'PY'
import os
import sys
import time

import psycopg

deadline = time.time() + 60
last_error = None

while time.time() < deadline:
    try:
        with psycopg.connect(os.environ["DATABASE_URL"]):
            print("Postgres is ready.")
            sys.exit(0)
    except Exception as exc:
        last_error = exc
        time.sleep(2)

print(f"Postgres did not become ready: {last_error}", file=sys.stderr)
sys.exit(1)
PY

step "Running tests"
"$VENV_PYTHON" -m pytest -q

step "Running Bring a Trailer discovery batch"
"$VENV_PYTHON" -m app.cli bat discover --max-candidates 5
"$VENV_PYTHON" -m app.cli bat ingest-discovered --batch-size 5
"$VENV_PYTHON" -m app.cli bat transform-discovered --batch-size 5

step "Running Cars & Bids discovery batch"
"$VENV_PYTHON" -m app.cli cab discover --max-candidates 5
"$VENV_PYTHON" -m app.cli cab ingest-discovered --batch-size 5
"$VENV_PYTHON" -m app.cli cab transform-discovered --batch-size 5

step "Live demo completed"
