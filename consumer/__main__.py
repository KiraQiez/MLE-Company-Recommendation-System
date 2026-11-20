"""Store registry events from RabbitMQ in Postgres."""

import asyncio
import hashlib
import json
import logging
import math
import os
import re

import aio_pika
import psycopg
from aio_pika.exceptions import ChannelInvalidStateError

from producer.publish import QUEUE
from scripts.dataset import parse_timestamp

DIMENSION = 384
LOGGER = logging.getLogger(__name__)

CLAIM_MESSAGE = """
    INSERT INTO processed_messages (message_id)
    VALUES (%s)
    ON CONFLICT DO NOTHING
    RETURNING message_id
"""

UPSERT_COMPANY = """
    INSERT INTO companies (
        registration_number, company_name, nature_of_business, state, industry,
        source_updated_at, source_message_id, is_active, embedding
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::vector)
    ON CONFLICT (registration_number) DO UPDATE SET
        company_name = EXCLUDED.company_name,
        nature_of_business = EXCLUDED.nature_of_business,
        state = EXCLUDED.state,
        industry = EXCLUDED.industry,
        source_updated_at = EXCLUDED.source_updated_at,
        source_message_id = EXCLUDED.source_message_id,
        is_active = EXCLUDED.is_active,
        embedding = EXCLUDED.embedding
    WHERE (EXCLUDED.source_updated_at, NOT EXCLUDED.is_active, EXCLUDED.source_message_id COLLATE "C")
        > (companies.source_updated_at, NOT companies.is_active, companies.source_message_id COLLATE "C")
"""

RECORD_FAILURE = """
    INSERT INTO failed_messages (message_id, body_sha256, body, reason)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""


class InvalidEvent(ValueError):
    pass


def optional_text(value: object) -> str | None:
    if isinstance(value, str) and value.strip() and not re.search(r"[\x00\ud800-\udfff]", value):
        return value
    return None


def embed(description: str | None) -> str | None:
    counts = [0] * DIMENSION
    for token in re.findall(r"[^\W_]+", (description or "").casefold()):
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        counts[int.from_bytes(digest, "big") % DIMENSION] += 1

    norm = math.sqrt(sum(count * count for count in counts))
    if not norm:
        return None
    return "[" + ",".join(str(count / norm) for count in counts) + "]"


async def persist(db: psycopg.AsyncConnection, message_id: str | None, body: bytes) -> str:
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

    is_active = event_type == "company-upserted"
    data = event.get("data") if is_active else {}
    if not isinstance(data, dict):
        raise InvalidEvent("expected an upsert data object")

    name = optional_text(data.get("companyName"))
    description = optional_text(data.get("natureOfBusiness"))
    state = optional_text(data.get("state"))
    industry = optional_text(data.get("industry"))

    async with db.transaction():
        claim = await db.execute(CLAIM_MESSAGE, (message_id,))
        if await claim.fetchone() is None:
            return "duplicate"
        result = await db.execute(
            UPSERT_COMPANY,
            (
                registration, name, description, state, industry, updated_at,
                message_id, is_active, embed(description) or embed(industry),
            ),
        )
    return "applied" if result.rowcount else "stale"


async def handle_message(db: psycopg.AsyncConnection, message: aio_pika.IncomingMessage) -> str:
    try:
        outcome = await persist(db, message.message_id, message.body)
    except InvalidEvent as error:
        async with db.transaction():
            await db.execute(
                RECORD_FAILURE,
                (optional_text(message.message_id), hashlib.sha256(message.body).hexdigest(),
                 message.body, str(error)),
            )
        outcome = "rejected"

    await message.ack()
    return outcome


async def consume() -> None:
    postgres_port = os.environ.get("POSTGRES_PORT", "5432")
    rabbitmq_port = os.environ.get("RABBITMQ_PORT", "5672")
    database_url = os.environ.get(
        "DATABASE_URL", f"postgresql://postgres:postgres@localhost:{postgres_port}/companies"
    )
    rabbitmq_url = os.environ.get(
        "RABBITMQ_URL", f"amqp://guest:guest@localhost:{rabbitmq_port}/"
    )

    delay = 1
    while True:
        try:
            async with await psycopg.AsyncConnection.connect(database_url) as db:
                async with await aio_pika.connect(rabbitmq_url) as connection:
                    channel = await connection.channel()
                    await channel.set_qos(prefetch_count=1)
                    queue = await channel.declare_queue(QUEUE, durable=True)
                    LOGGER.info("Consuming from %s", QUEUE)
                    async with queue.iterator(no_ack=False) as messages:
                        async for message in messages:
                            outcome = await handle_message(db, message)
                            LOGGER.info("outcome=%s message_id=%r", outcome, message.message_id)
                            delay = 1
                    raise ConnectionError("RabbitMQ stopped delivering messages")
        except (psycopg.OperationalError, OSError, ChannelInvalidStateError) as error:
            LOGGER.warning("Retrying ingestion in %ss after %s", delay, error)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        pass
