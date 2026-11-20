"""Read similar-company cards from Postgres."""

from typing import Annotated

import psycopg
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row
from pydantic import BaseModel

from db import database_url

app = FastAPI(title="Company recommendations")

# Matching dimensions alone cannot make vectors from different models comparable.
SIMILAR_COMPANIES = """
    SELECT candidate.registration_number, candidate.name, candidate.industry, candidate.state
    FROM companies AS source
    LEFT JOIN LATERAL (
        SELECT registration_number, company_name AS name, industry, state,
               embedding <=> source.embedding AS distance
        FROM companies
        WHERE registration_number <> source.registration_number
          AND is_active
          AND embedding_model = source.embedding_model
          AND embedding IS NOT NULL AND vector_norm(embedding) > 0
          AND source.embedding IS NOT NULL AND vector_norm(source.embedding) > 0
        ORDER BY distance, registration_number COLLATE "C"
        LIMIT %s
    ) AS candidate ON true
    WHERE source.registration_number = %s AND source.is_active
    ORDER BY candidate.distance, candidate.registration_number COLLATE "C"
"""


class CompanyCard(BaseModel):
    registration_number: str
    name: str | None
    industry: str | None
    state: str | None


class Recommendations(BaseModel):
    recommendations: list[CompanyCard]


@app.get("/companies/{registration_number}/similar")
def similar_companies(
    registration_number: str,
    limit: Annotated[int, Query(ge=1, le=20)] = 5,
) -> Recommendations:
    """Return the nearest active company cards for an active source."""
    with psycopg.connect(database_url(), row_factory=dict_row) as db:
        rows = db.execute(SIMILAR_COMPANIES, (limit, registration_number)).fetchall()

    # The left join keeps an active source visible even when it has no matches.
    if not rows:
        raise HTTPException(status_code=404, detail="Active company not found")

    return Recommendations(
        recommendations=[CompanyCard(**row) for row in rows if row["registration_number"] is not None]
    )
