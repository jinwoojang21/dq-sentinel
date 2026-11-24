"""Kafka (Redpanda) producer/consumer. Imported lazily so the core runs without confluent-kafka."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from .producer import generate_batch

log = logging.getLogger(__name__)


def produce(bootstrap: str, topic: str, scenario: str, healthy_batches: int, batch_size: int,
            interval_s: float, seed: int) -> None:
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    plan = ["healthy"] * healthy_batches + ([scenario] if scenario != "healthy" else [])
    for i, sc in enumerate(plan):
        for rec in generate_batch(i, batch_size, sc, seed, now=datetime.now(UTC)):
            p.produce(topic, key=str(rec.get("order_id", "")), value=json.dumps(rec))
            p.poll(0)
        p.flush()
        log.info("produced batch %d (%s)", i, sc)
        print(f"produced batch {i} scenario={sc}", flush=True)
        time.sleep(interval_s)


def consume_batches(bootstrap: str, topic: str, batch_size: int, max_wait_s: float = 5.0,
                    group: str = "dq-sentinel") -> Iterator[tuple[str, list[dict[str, Any]]]]:
    """Micro-batch by count or time window; commit offsets only after the caller processes a batch."""
    from confluent_kafka import Consumer

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group,
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    try:
        while True:
            batch: list[dict[str, Any]] = []
            deadline = time.monotonic() + max_wait_s
            while len(batch) < batch_size and time.monotonic() < deadline:
                msg = c.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    log.warning("kafka error: %s", msg.error())
                    continue
                try:
                    batch.append(json.loads(msg.value()))
                except json.JSONDecodeError:
                    batch.append({"_unparseable": msg.value().decode(errors="replace")})
            if batch:
                yield datetime.now(UTC).strftime("k%Y%m%dT%H%M%S%f"), batch
                c.commit(asynchronous=False)
    finally:
        c.close()
