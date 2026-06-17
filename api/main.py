import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from random import choice, randint, uniform
from uuid import uuid4

from fastapi import FastAPI
from kafka import KafkaProducer
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_transactions")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=3,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One producer per process, reused across requests, rather than opening and
    # closing a TCP connection to Kafka on every call.
    app.state.producer = build_producer()
    logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP_SERVERS)
    try:
        yield
    finally:
        app.state.producer.flush()
        app.state.producer.close()
        logger.info("Kafka producer closed")


app = FastAPI(title="Lakehouse Transaction API", lifespan=lifespan)


class TransactionEvent(BaseModel):
    transaction_id: str = Field(default_factory=lambda: f"txn-{uuid4()}")
    user_id: int | None = None
    email: str | None = None
    amount: float | None = None
    currency: str = "USD"
    status: str = "SUCCESS"
    event_time: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@app.get("/")
def root():
    return {
        "service": "Lakehouse Transaction API",
        "status": "running",
        "docs": "/docs",
        "health": "/health",
        "publish_sample": "POST /transactions/sample",
        "publish_bad_sample": "POST /transactions/sample-bad",
        "note": (
            "Opening /transactions/sample in a browser uses GET, so use the docs "
            "page or PowerShell curl to publish."
        ),
    }


@app.get("/health")
def health():
    return {"status": "ok", "topic": KAFKA_TOPIC}


@app.post("/transactions")
def publish_transaction(event: TransactionEvent):
    producer = app.state.producer
    payload = event.model_dump()
    producer.send(KAFKA_TOPIC, key=payload["transaction_id"], value=payload)
    producer.flush()
    logger.info("Published transaction %s", payload["transaction_id"])
    return {"published": True, "transaction_id": payload["transaction_id"]}


@app.post("/transactions/sample")
def publish_sample_transaction():
    event = TransactionEvent(
        user_id=randint(1000, 9999),
        email=f"user{randint(1, 9999)}@example.com",
        amount=round(uniform(1, 500), 2),
        status=choice(["SUCCESS", "FAILED", "PENDING"]),
    )
    return publish_transaction(event)


@app.post("/transactions/sample-bad")
def publish_bad_sample_transaction():
    event = TransactionEvent(
        user_id=randint(1000, 9999),
        email="invalid-email",
        amount=-25.0,
        status="FAILED",
    )
    return publish_transaction(event)


@app.get("/transactions/sample")
def sample_transaction_help():
    return {
        "message": "This endpoint publishes only with POST.",
        "try_docs": "http://localhost:8000/docs",
        "powershell": "Invoke-RestMethod -Method Post -Uri http://localhost:8000/transactions/sample",
    }


@app.get("/transactions/sample-bad")
def bad_sample_transaction_help():
    return {
        "message": "This endpoint publishes an invalid transaction only with POST.",
        "try_docs": "http://localhost:8000/docs",
        "powershell": "Invoke-RestMethod -Method Post -Uri http://localhost:8000/transactions/sample-bad",
    }
