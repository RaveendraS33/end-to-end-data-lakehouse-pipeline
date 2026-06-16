import json
import logging
import os
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

app = FastAPI(title="Lakehouse Transaction API")


class TransactionEvent(BaseModel):
    transaction_id: str = Field(default_factory=lambda: f"txn-{uuid4()}")
    user_id: int | None = None
    email: str | None = None
    amount: float | None = None
    currency: str = "USD"
    status: str = "SUCCESS"
    event_time: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=3,
    )


@app.get("/health")
def health():
    return {"status": "ok", "topic": KAFKA_TOPIC}


@app.post("/transactions")
def publish_transaction(event: TransactionEvent):
    producer = kafka_producer()
    payload = event.model_dump()
    producer.send(KAFKA_TOPIC, key=payload["transaction_id"], value=payload)
    producer.flush()
    producer.close()
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
