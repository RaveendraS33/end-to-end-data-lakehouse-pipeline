"""End-to-end pipeline tests against the running Docker stack.

These publish events through the FastAPI producer and assert they flow through
Kafka -> Spark -> Iceberg and are queryable in Trino. They cover the three
behaviours that matter: clean routing, bad routing, and MERGE idempotency.

Run them with the stack up:

    docker compose up --build -d kafka minio minio-init iceberg-postgres \
        iceberg-rest trino api spark
    pip install -r requirements-dev.txt
    RUN_INTEGRATION_TESTS=1 python -m pytest tests -m integration
"""
import os
import time

import pytest

# Both are only needed for integration runs. Import defensively so the unit
# suite still collects cleanly if requirements-dev is not installed.
try:  # pragma: no cover
    import requests
except ImportError:  # pragma: no cover
    requests = None

try:  # pragma: no cover
    import trino
except ImportError:  # pragma: no cover
    trino = None

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
TRINO_HOST = os.getenv("TRINO_HOST_LOCAL", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT_LOCAL", "8080"))
INGEST_TIMEOUT_SECONDS = int(os.getenv("INGEST_TIMEOUT_SECONDS", "120"))

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def cursor():
    if trino is None or requests is None:
        pytest.skip("integration deps missing; run pip install -r requirements-dev.txt")
    connection = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="integration-test",
        catalog="iceberg",
        schema="quality",
    )
    try:
        yield connection.cursor()
    finally:
        connection.close()


def _scalar(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchone()[0]


def _wait_until(predicate, timeout=INGEST_TIMEOUT_SECONDS, interval=3):
    """Poll until predicate() is truthy or the timeout elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _publish_sample():
    response = requests.post(f"{API_BASE_URL}/transactions/sample", timeout=10)
    response.raise_for_status()
    return response.json()["transaction_id"]


def test_clean_events_are_ingested(cursor):
    before = _scalar(cursor, "SELECT count(*) FROM iceberg.quality.transactions_clean")

    published_ids = [_publish_sample() for _ in range(3)]
    id_list = ", ".join(f"'{tid}'" for tid in published_ids)

    arrived = _wait_until(
        lambda: _scalar(
            cursor,
            f"SELECT count(*) FROM iceberg.quality.transactions_clean WHERE transaction_id IN ({id_list})",
        )
        == len(published_ids)
    )
    assert arrived, "published clean events did not reach the clean table in time"
    assert _scalar(cursor, "SELECT count(*) FROM iceberg.quality.transactions_clean") >= before + 3


def test_bad_events_are_routed_to_bad_table(cursor):
    before = _scalar(cursor, "SELECT count(*) FROM iceberg.quality.transactions_bad")

    response = requests.post(f"{API_BASE_URL}/transactions/sample-bad", timeout=10)
    response.raise_for_status()

    grew = _wait_until(
        lambda: _scalar(cursor, "SELECT count(*) FROM iceberg.quality.transactions_bad") > before
    )
    assert grew, "bad event did not reach the bad table in time"
    assert (
        _scalar(
            cursor,
            "SELECT count(*) FROM iceberg.quality.transactions_bad WHERE error_reason = 'invalid_email'",
        )
        >= 1
    )


def test_duplicate_transaction_id_is_idempotent(cursor):
    transaction_id = "it-dup-001"
    payload = {
        "transaction_id": transaction_id,
        "user_id": 4242,
        "email": "dup@example.com",
        "amount": 99.0,
        "currency": "USD",
        "status": "SUCCESS",
        "event_time": "2026-06-10T10:00:00+00:00",
    }

    for _ in range(2):
        response = requests.post(f"{API_BASE_URL}/transactions", json=payload, timeout=10)
        response.raise_for_status()

    def _single_row():
        count = _scalar(
            cursor,
            f"SELECT count(*) FROM iceberg.quality.transactions_clean WHERE transaction_id = '{transaction_id}'",
        )
        return count == 1

    assert _wait_until(_single_row), "MERGE upsert did not converge to exactly one row"
