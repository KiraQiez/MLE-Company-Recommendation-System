"""Rebuild older vectors from stored text without replaying registry messages."""

import psycopg

from db import database_url
from embeddings import MODEL_NAME, CompanyEmbedder, embedding_text


def main() -> None:
    model = CompanyEmbedder()
    with psycopg.connect(database_url()) as db:
        # Keep the backfill atomic and prevent company text from changing during it.
        db.execute("LOCK TABLE companies IN SHARE ROW EXCLUSIVE MODE")
        # The model label lets a repeat run skip rows already migrated.
        rows = db.execute("""
            SELECT registration_number, nature_of_business, industry
            FROM companies WHERE embedding_model <> %s
        """, (MODEL_NAME,)).fetchall()
        inputs = [
            (registration, embedding_text(description, industry))
            for registration, description, industry in rows
        ]
        if inputs:
            vectors = model.embed_many([text for _, text in inputs if text is not None])
            with db.cursor() as cursor:
                cursor.executemany("""
                    UPDATE companies SET embedding = %s::vector, embedding_model = %s
                    WHERE registration_number = %s
                """, (
                    (next(vectors) if text is not None else None, MODEL_NAME, registration)
                    for registration, text in inputs
                ))
    print(f"Rebuilt {len(inputs)} company embeddings using {MODEL_NAME}")


if __name__ == "__main__":
    main()
