"""Check ingestion using real Postgres transactions and simulated RabbitMQ deliveries."""

import asyncio
import json
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import psycopg
from psycopg import sql

from consumer.__main__ import consume, handle_message

OLD = "2026-08-14T08:00:00+08:00"
NEW = "2026-08-14T09:00:00+08:00"


def event(timestamp=NEW, kind="company-upserted", registration="TEST-001", **data):
    return json.dumps({
        "type": kind,
        "registrationNumber": registration,
        "updatedAt": timestamp,
        "data": data,
    }).encode()


class Delivery:
    """Capture acknowledgement calls without sending them to RabbitMQ."""

    def __init__(self, message_id, body):
        self.message_id = message_id
        self.body = body
        self.ack = AsyncMock()


class IngestionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        port = os.environ.get("POSTGRES_PORT", "5432")
        url = os.environ.get(
            "DATABASE_URL", f"postgresql://postgres:postgres@localhost:{port}/companies"
        )
        self.db = await psycopg.AsyncConnection.connect(url, autocommit=True)
        self.addAsyncCleanup(self.db.close)
        # A second session can only see committed writes from the consumer session.
        self.observer = await psycopg.AsyncConnection.connect(url, autocommit=True)
        self.addAsyncCleanup(self.observer.close)
        schema = sql.Identifier(f"ingestion_test_{uuid4().hex}")
        await self.db.execute(sql.SQL("CREATE SCHEMA {}").format(schema))
        self.addAsyncCleanup(
            self.db.execute, sql.SQL("DROP SCHEMA {} CASCADE").format(schema)
        )
        for connection in (self.db, self.observer):
            await connection.execute(sql.SQL("SET search_path TO {}, public").format(schema))
        await self.db.execute(Path("db/schema.sql").read_text())

    async def deliver(self, message_id, body, expected):
        delivery = Delivery(message_id, body)
        self.assertEqual(await handle_message(self.db, delivery), expected)
        delivery.ack.assert_awaited_once()

    async def query(self, query):
        return await (await self.observer.execute(query)).fetchall()

    async def test_duplicate_is_acknowledged_without_rewriting_company(self):
        body = event(companyName="Sample", natureOfBusiness="Manufacture of gloves")

        async def check_committed():
            self.assertEqual(await self.query(
                "SELECT count(*) FROM companies JOIN processed_messages "
                "ON source_message_id = message_id"
            ), [(1,)])

        # Query Postgres when ack is called to prove the writes committed first.
        first = Delivery("one", body)
        first.ack.side_effect = check_committed
        self.assertEqual(await handle_message(self.db, first), "applied")
        first.ack.assert_awaited_once()
        # xmin identifies the transaction that created this row version.
        query = "SELECT xmin::text, company_name, vector_dims(embedding) FROM companies"
        original = await self.query(query)

        await self.deliver("one", body, "duplicate")
        self.assertEqual(await self.query(query), original)
        self.assertEqual([row[1:] for row in original], [("Sample", 384)])
        self.assertEqual(await self.query("SELECT count(*) FROM processed_messages"), [(1,)])

    async def test_failed_company_write_rolls_back_message_claim(self):
        # Force the company write to fail after the message ID has been claimed.
        await self.db.execute("ALTER TABLE companies ADD CHECK (company_name <> 'blocked')")
        delivery = Delivery("retry", event(companyName="blocked"))
        with self.assertRaises(psycopg.errors.CheckViolation):
            await handle_message(self.db, delivery)
        delivery.ack.assert_not_awaited()
        self.assertEqual(await self.query("SELECT count(*) FROM processed_messages"), [(0,)])
        self.assertEqual(await self.query("SELECT count(*) FROM companies"), [(0,)])

    async def test_source_order_ties_strike_off_and_reactivation(self):
        # Start with a strike-off for an unseen company. The same-time upsert loses
        # even though its message ID sorts later.
        await self.deliver("b", event(OLD, "company-struck-off"), "applied")
        await self.deliver("z", event(OLD), "stale")
        await self.deliver("a", event("2026-08-13T08:00:00+08:00"), "stale")
        self.assertEqual(await self.query("SELECT is_active FROM companies"), [(False,)])

        # A newer upsert reactivates the company. Equal-time upserts compare message IDs.
        await self.deliver("c", event(NEW, companyName="Reopened"), "applied")
        await self.deliver("d", event(NEW, companyName="Latest tie"), "applied")
        await self.deliver("aa", event(NEW), "stale")
        await self.deliver("older", event(OLD, companyName="Arrived late"), "stale")
        self.assertEqual(
            await self.query("SELECT is_active, company_name FROM companies"),
            [(True, "Latest tie")],
        )
        await self.deliver("e", event(NEW, "company-struck-off"), "applied")
        self.assertEqual(
            await self.query("SELECT is_active, source_message_id FROM companies"),
            [(False, "e")],
        )

    async def test_bad_input_is_durable_before_ack_and_keeps_raw_bytes(self):
        cases = [
            ("json", b"\xff", "invalid JSON body"),
            ("type", event(kind="unknown"), "unsupported event type"),
            ("time", event("2026-08-14T09:00:00"), "invalid updatedAt or missing time zone"),
            ("reg", event(registration=" "), "missing or invalid registration number"),
            (None, event(), "missing or invalid message ID"),
        ]
        for message_id, body, reason in cases:
            with self.subTest(message_id=message_id):
                delivery = Delivery(message_id, body)

                async def check_committed():
                    rows = await self.query("SELECT message_id, body, reason FROM failed_messages")
                    self.assertIn((message_id, body, reason), rows)

                delivery.ack.side_effect = check_committed
                self.assertEqual(await handle_message(self.db, delivery), "rejected")
                delivery.ack.assert_awaited_once()
                await self.deliver(message_id, body, "rejected")

        # Each bad delivery was replayed. Its failure record should exist only once.
        self.assertEqual(await self.query("SELECT count(*) FROM failed_messages"), [(len(cases),)])
        await self.deliver("good", event(), "applied")

    async def test_incomplete_text_falls_back_to_industry_or_stays_unscored(self):
        await self.deliver("fallback", event(industry="Manufacturing"), "applied")
        await self.deliver("empty", event(registration="TEST-002"), "applied")
        self.assertEqual(await self.query(
            "SELECT company_name, nature_of_business, state, industry, vector_dims(embedding) "
            "FROM companies ORDER BY registration_number"
        ), [(None, None, None, "Manufacturing", 384), (None, None, None, None, None)])


class RetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_connection_errors_wait_with_exponential_backoff(self):
        # Capture the requested delays without waiting, then cancel the retry loop.
        with (
            patch("consumer.__main__.psycopg.AsyncConnection.connect",
                  side_effect=psycopg.OperationalError("database unavailable")) as connect,
            patch("consumer.__main__.asyncio.sleep",
                  side_effect=[None, None, asyncio.CancelledError]) as sleep,
            self.assertLogs("consumer.__main__", level="WARNING"),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await consume()

        self.assertEqual(connect.await_count, 3)
        self.assertEqual([call.args for call in sleep.await_args_list], [(1,), (2,), (4,)])
