#!/bin/bash
set -e
poetry run alembic upgrade head

# Use environment variables for Uvicorn configuration with sensible defaults
WORKERS=${UVICORN_WORKERS:-4}
BACKLOG=${UVICORN_BACKLOG:-2048}
LIMIT_CONCURRENCY=${UVICORN_LIMIT_CONCURRENCY:-1000}
TIMEOUT_KEEP_ALIVE=${UVICORN_TIMEOUT_KEEP_ALIVE:-5}

poetry run uvicorn app.api:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers $WORKERS \
  --backlog $BACKLOG \
  --limit-concurrency $LIMIT_CONCURRENCY \
  --timeout-keep-alive $TIMEOUT_KEEP_ALIVE
