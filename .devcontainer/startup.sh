#!/bin/bash
set -e

# Start Postgres service using docker compose
echo "Starting Postgres service with docker compose..."
docker compose up -d postgres

# Wait for Postgres to be ready
echo "Waiting for Postgres to be ready..."
until docker exec $(docker compose ps -q postgres) pg_isready -U dagster > /dev/null 2>&1; do
    sleep 1
done

echo "Postgres is ready."

# Start Dagster dev in the background
echo "Starting Dagster dev server..."
dagster dev &

echo "Startup complete."