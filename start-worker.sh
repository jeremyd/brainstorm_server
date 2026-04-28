#!/bin/bash
set -e

# Worker-only mode startup script
# This runs the FastAPI app in worker-only mode (no API endpoints served)

echo "Starting in WORKER-ONLY mode..."
echo "Worker concurrency: ${WORKER_CONCURRENCY:-1}"
echo "TA publish batch size: ${TA_PUBLISH_BATCH_SIZE:-50}"

# Run with single worker since we're not serving HTTP traffic
poetry run uvicorn app.api:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers 1
