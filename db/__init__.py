"""Postgres connection settings shared by the consumer and API."""

import os


def database_url() -> str:
    port = os.environ.get("POSTGRES_PORT", "5432")
    return os.environ.get(
        "DATABASE_URL", f"postgresql://postgres:postgres@localhost:{port}/companies"
    )
