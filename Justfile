set shell := ["bash", "-euo", "pipefail", "-c"]

default: setup

# Install the ingestion dependencies.
setup:
    uv sync --locked

# Start Postgres (with pgvector) and RabbitMQ, and wait until both are healthy.
up:
    docker compose up -d --wait

# Initialize the schema after Postgres is ready.
schema:
    docker compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d companies < db/schema.sql

# Consume registry events and write them to Postgres.
consume:
    uv run --locked python -m consumer

# Stop everything and delete the volumes.
down:
    docker compose down -v

# Replay the registry feed onto the company-events queue.
publish:
    uv run python -m producer.publish

# Run ingestion tests against the local Postgres service.
test: up
    uv run --locked python -m unittest discover -s tests -v
