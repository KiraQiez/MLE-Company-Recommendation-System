"""Store registry events from RabbitMQ in Postgres."""

import asyncio
import hashlib
import json
import logging
import os
import re
from collections.abc import Callable

import aio_pika
import psycopg
from aio_pika.exceptions import ChannelInvalidStateError

from db import database_url
from embeddings import MODEL_NAME, CompanyEmbedder, embedding_text
from producer.publish import QUEUE
from scripts.dataset import parse_timestamp

LOGGER = logging.getLogger(__name__)

# A conflict returns no row, so only the transaction that claims the ID can apply the event.
CLAIM_MESSAGE = """
    INSERT INTO processed_messages (message_id)
    VALUES (%s)
    ON CONFLICT DO NOTHING
    RETURNING message_id
"""

# Postgres compares the tuple left to right. Source time wins first, then inactive
# status wins ties, then the greater message ID wins using a fixed sort order.
UPSERT_COMPANY = """
    INSERT INTO companies (
        registration_number, company_name, nature_of_business, state, industry,
        source_updated_at, source_message_id, is_active, embedding, embedding_model
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::vector, %s)
    ON CONFLICT (registration_number) DO UPDATE SET
        company_name = EXCLUDED.company_name,
        nature_of_business = EXCLUDED.nature_of_business,
        state = EXCLUDED.state,
        industry = EXCLUDED.industry,
        source_updated_at = EXCLUDED.source_updated_at,
        source_message_id = EXCLUDED.source_message_id,
        is_active = EXCLUDED.is_active,
        embedding = EXCLUDED.embedding,
        embedding_model = EXCLUDED.embedding_model
    WHERE (EXCLUDED.source_updated_at, NOT EXCLUDED.is_active, EXCLUDED.source_message_id COLLATE "C")
        > (companies.source_updated_at, NOT companies.is_active, companies.source_message_id COLLATE "C")
"""

# The schema deduplicates failed deliveries by message ID and body hash.
RECORD_FAILURE = """
    INSERT INTO failed_messages (message_id, body_sha256, body, reason)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""


class InvalidEvent(ValueError):
    """An input error that can be recorded and acknowledged."""


def optional_text(value: object) -> str | None:
    """Keep nonblank strings that can be stored as UTF-8 text in Postgres."""
    if isinstance(value, str) and value.strip() and not re.search(r"[\x00\ud800-\udfff]", value):
        return value
    return None


async def persist(
    db: psycopg.AsyncConnection, message_id: str | None, body: bytes,
    embed: Callable[[str], str],
) -> str:
    """Validate one event and record its ID with any resulting company change."""
    try:
        event = json.loads(body)
    except (ValueError, RecursionError) as error:
        raise InvalidEvent("invalid JSON body") from error
    if not isinstance(event, dict):
        raise InvalidEvent("expected a JSON object")
    if optional_text(message_id) is None:
        raise InvalidEvent("missing or invalid message ID")

    event_type = event.get("type")
    registration = optional_text(event.get("registrationNumber"))
    updated_at = parse_timestamp(event.get("updatedAt"))
    if event_type not in ("company-upserted", "company-struck-off"):
        raise InvalidEvent("unsupported event type")
    if registration is None:
        raise InvalidEvent("missing or invalid registration number")
    if updated_at is None:
        raise InvalidEvent("invalid updatedAt or missing time zone")

    # Keep an inactive row and its source time so a late upsert cannot undo a strike-off.
    is_active = event_type == "company-upserted"
    data = event.get("data") if is_active else {}
    if not isinstance(data, dict):
        raise InvalidEvent("expected an upsert data object")

    name = optional_text(data.get("companyName"))
    description = optional_text(data.get("natureOfBusiness"))
    state = optional_text(data.get("state"))
    industry = optional_text(data.get("industry"))

    # If the company write fails, the message claim rolls back with it so a replay can retry.
    async with db.transaction():
        claim = await db.execute(CLAIM_MESSAGE, (message_id,))
        if await claim.fetchone() is None:
            return "duplicate"
        text = embedding_text(description, industry)
        embedding = await asyncio.to_thread(embed, text) if text is not None else None
        result = await db.execute(
            UPSERT_COMPANY,
            (
                registration, name, description, state, industry, updated_at,
                message_id, is_active, embedding, MODEL_NAME,
            ),
        )
    # A stale event commits its message ID even though the SQL skips the company update.
    return "applied" if result.rowcount else "stale"


async def handle_message(
    db: psycopg.AsyncConnection, message: aio_pika.IncomingMessage, embed: Callable[[str], str],
) -> str:
    """Acknowledge this delivery after its database work succeeds."""
    try:
        outcome = await persist(db, message.message_id, message.body, embed)
    except InvalidEvent as error:
        # Preserve the original bytes for review, including bodies that failed JSON decoding.
        async with db.transaction():
            await db.execute(
                RECORD_FAILURE,
                (optional_text(message.message_id), hashlib.sha256(message.body).hexdigest(),
                 message.body, str(error)),
            )
        outcome = "rejected"

    # This also acknowledges duplicates whose original write already committed.
    await message.ack()
    return outcome


async def consume() -> None:
    rabbitmq_port = os.environ.get("RABBITMQ_PORT", "5672")
    rabbitmq_url = os.environ.get(
        "RABBITMQ_URL", f"amqp://guest:guest@localhost:{rabbitmq_port}/"
    )

    # Reuse the loaded model across database and RabbitMQ reconnects.
    embed = await asyncio.to_thread(CompanyEmbedder)
    delay = 1
    while True:
        try:
            async with await psycopg.AsyncConnection.connect(database_url()) as db:
                async with await aio_pika.connect(rabbitmq_url) as connection:
                    channel = await connection.channel()
                    # Limit this consumer to one unacknowledged delivery at a time.
                    await channel.set_qos(prefetch_count=1)
                    queue = await channel.declare_queue(QUEUE, durable=True)
                    LOGGER.info("Consuming from %s", QUEUE)
                    async with queue.iterator(no_ack=False) as messages:
                        async for message in messages:
                            outcome = await handle_message(db, message, embed)
                            LOGGER.info("outcome=%s message_id=%r", outcome, message.message_id)
                            delay = 1
                    raise ConnectionError("RabbitMQ stopped delivering messages")
        except (psycopg.OperationalError, OSError, ChannelInvalidStateError) as error:
            # Leaving the connection context closes any open channel and lets RabbitMQ
            # redeliver unacknowledged messages.
            LOGGER.warning("Retrying ingestion in %ss after %s", delay, error)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        pass
