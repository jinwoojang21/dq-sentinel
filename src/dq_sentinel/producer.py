"""Synthetic `orders` events with injectable failure modes ("chaos scenarios").

Each scenario mimics a real-world upstream incident so the pipeline and agent can be
exercised deterministically (seeded RNG) without production data.
"""

from __future__ import annotations

import copy
import random
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

SCENARIOS: dict[str, str] = {
    "healthy": "No failure injected.",
    "schema_drift_rename": "Upstream deploy renames `amount` to `order_amount` in every event.",
    "type_change": "`amount` starts arriving as a formatted string like '$12.50'.",
    "null_spike": "~45% of events lose the optional `channel` attribute.",
    "new_category": "A new market launch introduces currency 'JPY' in ~25% of events.",
    "duplicates": "A producer retry bug re-sends ~15% of events with the same order_id.",
    "volume_drop": "A partition stops producing; batch volume falls to ~4% of normal.",
    "out_of_range": "Refunds start being encoded as negative amounts (~20% of events).",
    # --- hard scenarios: the surface symptom points at the wrong cause ---
    "unit_change": "Upstream switches `amount` to cents (x100). Only the largest orders breach the max; "
                   "the rest pass validation but are silently 100x too big.",
    "enum_case_change": "A serializer change lower-cases `currency` ('usd', 'eur'). Looks like new "
                        "categories, but it's a formatting bug.",
    "mobile_null_customer": "A mobile app release drops `customer_id` on ~70% of mobile orders; web and "
                            "store are unaffected.",
}

# Scenarios whose obvious symptom is misleading; the rule-based baseline is expected to fail these.
HARD_SCENARIOS = {"unit_change", "enum_case_change", "mobile_null_customer"}

# What a correct investigation should conclude — used by the eval suite.
EXPECTED_ROOT_CAUSE = {
    "schema_drift_rename": "schema_drift",
    "type_change": "type_change",
    "null_spike": "null_spike",
    "new_category": "new_category",
    "duplicates": "duplicates",
    "volume_drop": "volume_anomaly",
    "out_of_range": "out_of_range",
    "unit_change": "unit_change",
    "enum_case_change": "format_change",
    "mobile_null_customer": "null_spike",
}


def _healthy_record(rng: random.Random, now: datetime) -> dict[str, Any]:
    amount = round(min(rng.lognormvariate(3.6, 0.9), 9_999.0), 2)
    channel = rng.choices(["web", "mobile", "store"], weights=[55, 35, 10])[0]
    if rng.random() < 0.01:
        channel = None
    return {
        "order_id": f"ord-{rng.getrandbits(48):012x}",
        "customer_id": f"cust-{rng.randint(1, 5000):05d}",
        "amount": amount,
        "currency": rng.choices(["USD", "EUR", "GBP"], weights=[70, 20, 10])[0],
        "status": rng.choices(["placed", "shipped", "cancelled", "refunded"], weights=[60, 30, 6, 4])[0],
        "created_at": (now - timedelta(seconds=rng.randint(0, 600))).isoformat(),
        "channel": channel,
    }


def generate_batch(
    batch_index: int,
    size: int,
    scenario: str = "healthy",
    seed: int = 7,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario {scenario!r}; choose from {sorted(SCENARIOS)}")
    rng = random.Random(f"{seed}-{batch_index}-{scenario}")
    now = now or datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=5 * batch_index)

    if scenario == "volume_drop":
        size = max(5, int(size * 0.04))

    records = [_healthy_record(rng, now) for _ in range(size)]

    if scenario == "schema_drift_rename":
        for r in records:
            r["order_amount"] = r.pop("amount")
    elif scenario == "type_change":
        for r in records:
            r["amount"] = f"${r['amount']:,.2f}"
    elif scenario == "null_spike":
        for r in records:
            if rng.random() < 0.45:
                r["channel"] = None
    elif scenario == "new_category":
        for r in records:
            if rng.random() < 0.25:
                r["currency"] = "JPY"
                r["amount"] = round(min(r["amount"] * 150, 9_999.0), 0)
    elif scenario == "duplicates":
        dupes = [copy.deepcopy(r) for r in records if rng.random() < 0.15]
        records.extend(dupes)
        rng.shuffle(records)
    elif scenario == "out_of_range":
        for r in records:
            if rng.random() < 0.20:
                r["amount"] = -r["amount"]
                r["status"] = "refunded"
    elif scenario == "unit_change":
        for r in records:
            r["amount"] = round(r["amount"] * 100, 0)
    elif scenario == "enum_case_change":
        for r in records:
            r["currency"] = r["currency"].lower()
    elif scenario == "mobile_null_customer":
        for r in records:
            if r["channel"] == "mobile" and rng.random() < 0.7:
                r["customer_id"] = None
    return records


def scenario_stream(
    scenario: str, healthy_batches: int = 8, batch_size: int = 500, seed: int = 7
) -> Iterator[tuple[str, list[dict[str, Any]]]]:
    """`healthy_batches` of normal traffic, then one batch of the chosen scenario."""
    for i in range(healthy_batches):
        yield f"b{i:05d}", generate_batch(i, batch_size, "healthy", seed)
    if scenario != "healthy":
        i = healthy_batches
        yield f"b{i:05d}", generate_batch(i, batch_size, scenario, seed)
