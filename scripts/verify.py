"""Verify the complete local setup in an isolated Compose project."""

import argparse
from collections import Counter
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import signal
import socket
from statistics import median
import subprocess
from tempfile import TemporaryDirectory
import time
from uuid import uuid4

import httpx
import psycopg
from psycopg.conninfo import make_conninfo

from api.main import SIMILAR_COMPANIES

ROOT = Path(__file__).resolve().parents[1]
CAFE = "SYN-001550"
FEED_SIZE = 2518


def verification_environment():
    sockets = [socket.socket() for _ in range(4)]
    try:
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        postgres, broker, management, api = (listener.getsockname()[1] for listener in sockets)
    finally:
        for listener in sockets:
            listener.close()

    env = os.environ.copy()
    # Child recipes use the project's environment even when this script has Playwright.
    env.pop("VIRTUAL_ENV", None)
    return {
        **env,
        "COMPOSE_FILE": str(ROOT / "docker-compose.yml"),
        "COMPOSE_PROJECT_NAME": f"company-verify-{uuid4().hex[:10]}",
        "POSTGRES_PORT": str(postgres),
        "RABBITMQ_PORT": str(broker),
        "RABBITMQ_UI_PORT": str(management),
        "API_PORT": str(api),
        "DATABASE_URL": f"postgresql://postgres:postgres@127.0.0.1:{postgres}/companies",
        "RABBITMQ_URL": f"amqp://guest:guest@127.0.0.1:{broker}/",
    }


def run(*command, env):
    subprocess.run(command, cwd=ROOT, env=env, check=True, timeout=180)


@contextmanager
def worker(recipe, log, env):
    with log.open("w") as output:
        process = subprocess.Popen(
            ["just", recipe], cwd=ROOT, env=env, stdout=output, stderr=output,
            start_new_session=True,
        )
        try:
            yield process
        except Exception:
            print(f"\nLast lines from just {recipe}:", flush=True)
            print("\n".join(log.read_text().splitlines()[-15:]), flush=True)
            raise
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)


def wait_until(check, process, description, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Worker exited before {description}")
        result = check()
        if result:
            return result
        time.sleep(0.2)
    raise TimeoutError(f"Timed out waiting for {description}")


def outcomes(log, process, total):
    def read():
        found = re.findall(r"outcome=(\w+)", log.read_text())
        return found if len(found) >= total else None

    lines = wait_until(read, process, f"{total} delivery outcomes")
    assert len(lines) == total, f"Expected {total} outcomes, got {len(lines)}"
    return lines


def snapshot(db):
    companies = db.execute("""
        SELECT registration_number, xmin::text, embedding_model
        FROM companies ORDER BY registration_number
    """).fetchall()
    failures = db.execute("""
        SELECT message_id, body_sha256, reason, xmin::text
        FROM failed_messages ORDER BY message_id, body_sha256
    """).fetchall()
    return companies, failures


def check_api(client, process):
    def ready():
        try:
            return client.get("/").is_success
        except httpx.TransportError:
            return False

    wait_until(ready, process, "the portal to respond")
    assert client.get("/assets/styles.css").is_success
    assert client.get("/assets/portal.js").is_success
    assert "Sinar Teguh Bakery" in client.get(f"/companies/{CAFE}").json()["name"]
    for limit in (5, 100):
        response = client.get(f"/companies/{CAFE}/similar", params={"limit": limit})
        assert response.status_code == 200
        cards = response.json()["recommendations"]
        assert len(cards) == limit
        scores = [card["similarity_percent"] for card in cards]
        assert scores == sorted(scores, reverse=True)
        assert all(0 <= score <= 100 for score in scores)
        assert all(card["registration_number"] != CAFE for card in cards)
    for registration in ("UNKNOWN", "SYN-000186"):
        assert client.get(f"/companies/{registration}/similar").status_code == 404
    assert client.get("/companies/SYN-000048/similar").json() == {"recommendations": []}
    assert client.get(f"/companies/{CAFE}/similar?limit=101").status_code == 422
    print("Live portal and API checks passed.", flush=True)


def check_ranking(db):
    registration_numbers = (CAFE, "SYN-000082", "SYN-001777")
    for registration in registration_numbers:
        rows = db.execute(SIMILAR_COMPANIES, (5, registration)).fetchall()
        print(f"{registration} top matches: {', '.join(row[0] for row in rows)}")

    query = """
        SELECT nature_of_business FROM companies WHERE registration_number = %s
    """
    furniture = db.execute(query, ("SYN-000082",)).fetchone()[0]
    matches = db.execute(SIMILAR_COMPANIES, (5, "SYN-000082")).fetchall()
    assert len(matches) == 5
    assert all(db.execute(query, (row[0],)).fetchone()[0] == furniture for row in matches)

    explain = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + SIMILAR_COMPANIES
    runs = [db.execute(explain, (5, CAFE)).fetchone()[0][0] for _ in range(31)]
    assert "<=>" in json.dumps(runs[0]["Plan"])
    times = [plan["Execution Time"] for plan in runs[1:]]
    print(
        f"SQL ranking, 30 warm runs: median {median(times):.3f} ms "
        f"(min {min(times):.3f}, max {max(times):.3f} ms).",
        flush=True,
    )


def check_retry(db, directory, consumer, log, env):
    pid = db.execute("""
        SELECT pid FROM pg_stat_activity WHERE application_name = 'handoff-verification'
    """).fetchone()[0]
    assert db.execute("SELECT pg_terminate_backend(%s, 1000)", (pid,)).fetchone() == (True,)
    with (ROOT / "data/feed.jsonl").open() as feed:
        sample = json.loads(next(feed))
    sample["registrationNumber"] = "VERIFY-RECOVERY"
    path = directory / "recovery.jsonl"
    path.write_text(json.dumps(sample) + "\n")
    run("uv", "run", "--locked", "python", "-m", "producer.publish", "--file", str(path), env=env)
    assert outcomes(log, consumer, 2 * FEED_SIZE + 1)[-1] == "applied"
    assert "Retrying ingestion in 1s" in log.read_text()
    assert db.execute("""
        SELECT count(*) FROM companies WHERE registration_number = 'VERIFY-RECOVERY'
    """).fetchone() == (1,)
    print("Live retry passed: the consumer reconnected and applied the delivery.", flush=True)


def check_browser(base_url):
    from playwright.sync_api import expect, sync_playwright

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        for width, columns in ((1440, 2), (820, 2), (390, 1), (320, 1)):
            page = browser.new_page(viewport={"width": width, "height": 900})
            page.goto(base_url)
            cards = page.locator(".result-card")
            expect(cards).to_have_count(5)
            expect(cards.first.locator("strong")).to_have_text("100%")
            assert page.locator("#results").evaluate(
                "(grid) => getComputedStyle(grid).gridTemplateColumns.split(' ').length"
            ) == columns
            page.get_by_role("combobox").select_option("100")
            expect(cards).to_have_count(100)
            assert page.evaluate("document.documentElement.scrollWidth === innerWidth")
            cards.first.click()
            expect(page.locator("#profile-name")).to_contain_text("Seri Sejahtera")
            page.close()
        browser.close()
    print("Chromium checks passed at four screen widths.", flush=True)


def verify(env, directory, browser):
    run("just", "up", env=env)
    run("just", "schema", env=env)
    run("just", "test", env=env)
    log = directory / "consumer.log"
    consumer_env = {
        **env,
        "DATABASE_URL": make_conninfo(env["DATABASE_URL"], application_name="handoff-verification"),
    }
    base_url = f'http://127.0.0.1:{env["API_PORT"]}'
    with (
        worker("consume", log, consumer_env) as consumer,
        psycopg.connect(env["DATABASE_URL"], autocommit=True) as db,
    ):
        assert snapshot(db) == ([], [])
        wait_until(lambda: "Consuming from company-events" in log.read_text(), consumer, "consumer readiness")
        run("just", "publish", env=env)
        first = Counter(outcomes(log, consumer, FEED_SIZE))
        assert first == {"applied": 2308, "duplicate": 63, "stale": 66, "rejected": 81}, first
        print("First publish:", dict(first), flush=True)
        before = snapshot(db)
        assert (len(before[0]), len(before[1])) == (2049, 76)
        assert {row[2] for row in before[0]} == {"BAAI/bge-small-en-v1.5"}

        with worker("api", directory / "api.log", env) as api, httpx.Client(base_url=base_url) as client:
            check_api(client, api)
            check_ranking(db)
            if browser:
                check_browser(base_url)

        run("just", "publish", env=env)
        replay = Counter(outcomes(log, consumer, 2 * FEED_SIZE)[FEED_SIZE:])
        assert replay == {"duplicate": 2437, "rejected": 81}, replay
        assert snapshot(db) == before, "Replay rewrote company rows or failure records"
        queue = subprocess.check_output([
            "docker", "compose", "exec", "-T", "rabbitmq", "rabbitmqctl", "list_queues",
            "name", "messages_ready", "messages_unacknowledged",
        ], cwd=ROOT, env=env, text=True, timeout=30)
        assert "company-events\t0\t0" in queue, queue
        print("Replay:", dict(replay), "with unchanged rows and an empty queue.", flush=True)
        check_retry(db, directory, consumer, log, env)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--browser", action="store_true", help="Include Chromium layout and navigation checks")
    args = parser.parse_args()
    env = verification_environment()
    with TemporaryDirectory(prefix="company-verify-") as temp:
        try:
            verify(env, Path(temp), args.browser)
        finally:
            run("docker", "compose", "down", "-v", env=env)
    print("Verification passed.", flush=True)


if __name__ == "__main__":
    main()
