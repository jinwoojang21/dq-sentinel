"""Record-level validation (quarantine) and batch-level SLO checks.

These are deliberately deterministic: the LLM never decides *whether* data is bad,
only *why* it is bad and what to do about it.
"""

from __future__ import annotations

import json
import statistics
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from .contract import Contract, FieldSpec


@dataclass
class ValidationResult:
    total: int
    valid: list[dict[str, Any]]
    quarantined: list[tuple[dict[str, Any], list[str]]]
    null_counts: Counter = field(default_factory=Counter)
    missing_fields: Counter = field(default_factory=Counter)
    unexpected_fields: Counter = field(default_factory=Counter)
    duplicates: int = 0
    reason_counts: Counter = field(default_factory=Counter)


@dataclass
class BatchMetrics:
    batch_id: str
    total: int
    valid: int
    quarantined: int
    quarantine_rate: float
    duplicate_rate: float
    null_rates: dict[str, float]
    missing_field_rates: dict[str, float]
    unexpected_field_rates: dict[str, float]
    top_quarantine_reasons: dict[str, int]

    def to_row(self) -> dict[str, Any]:
        row = asdict(self)
        for k in ("null_rates", "missing_field_rates", "unexpected_field_rates", "top_quarantine_reasons"):
            row[k] = json.dumps(row[k])
        return row


@dataclass
class CheckResult:
    check: str
    status: str  # "pass" | "fail"
    observed: float
    threshold: float
    detail: str
    field: str | None = None


# ----------------------------------------------------------------------------- record level


def _coerce(spec: FieldSpec, value: Any) -> tuple[Any, str | None]:
    t = spec.type
    if t == "string":
        return (value, None) if isinstance(value, str) else (None, f"type:{spec.name}:expected string")
    if t in ("double", "long"):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None, f"type:{spec.name}:expected {t}, got {type(value).__name__}"
        return (float(value) if t == "double" else int(value)), None
    if t == "boolean":
        return (value, None) if isinstance(value, bool) else (None, f"type:{spec.name}:expected boolean")
    if t == "timestamp":
        if not isinstance(value, str):
            return None, f"type:{spec.name}:expected ISO-8601 string"
        try:
            ts = datetime.fromisoformat(value)
        except ValueError:
            return None, f"type:{spec.name}:unparseable timestamp"
        if ts.tzinfo is None:
            return None, f"type:{spec.name}:timestamp missing timezone"
        return ts, None
    return None, f"type:{spec.name}:unsupported type {t}"


def validate_batch(records: list[dict[str, Any]], contract: Contract) -> ValidationResult:
    res = ValidationResult(total=len(records), valid=[], quarantined=[])
    seen: dict[str, set[Any]] = {f: set() for f in contract.unique_fields}
    known = set(contract.fields)

    for rec in records:
        reasons: list[str] = []
        clean: dict[str, Any] = {}
        for extra in rec.keys() - known:
            res.unexpected_fields[extra] += 1

        for name, spec in contract.fields.items():
            if name not in rec:
                res.missing_fields[name] += 1
            value = rec.get(name)
            if value is None:
                res.null_counts[name] += 1
                if spec.required:
                    reasons.append(f"required:{name}:{'missing' if name not in rec else 'null'}")
                clean[name] = None
                continue
            coerced, err = _coerce(spec, value)
            if err:
                reasons.append(err)
                continue
            if spec.allowed is not None and coerced not in spec.allowed:
                reasons.append(f"allowed:{name}:value not in contract")
            if spec.min is not None and coerced < spec.min:
                reasons.append(f"range:{name}:below min {spec.min}")
            if spec.max is not None and coerced > spec.max:
                reasons.append(f"range:{name}:above max {spec.max}")
            clean[name] = coerced

        if not reasons:
            for ukey, values in seen.items():
                if clean.get(ukey) in values:
                    reasons.append(f"duplicate:{ukey}:repeated within batch")
                    res.duplicates += 1
                    break
        if reasons:
            res.quarantined.append((rec, reasons))
            for r in reasons:
                res.reason_counts[r] += 1
        else:
            for ukey, values in seen.items():
                values.add(clean[ukey])
            res.valid.append(clean)
    return res


def compute_metrics(batch_id: str, vr: ValidationResult, contract: Contract) -> BatchMetrics:
    n = max(vr.total, 1)
    return BatchMetrics(
        batch_id=batch_id,
        total=vr.total,
        valid=len(vr.valid),
        quarantined=len(vr.quarantined),
        quarantine_rate=round(len(vr.quarantined) / n, 4),
        duplicate_rate=round(vr.duplicates / n, 4),
        null_rates={f: round(vr.null_counts[f] / n, 4) for f in contract.fields},
        missing_field_rates={f: round(c / n, 4) for f, c in vr.missing_fields.items()},
        unexpected_field_rates={f: round(c / n, 4) for f, c in vr.unexpected_fields.items()},
        top_quarantine_reasons=dict(vr.reason_counts.most_common(8)),
    )


# ------------------------------------------------------------------------------ batch level


def run_checks(m: BatchMetrics, contract: Contract, history_totals: list[int]) -> list[CheckResult]:
    out: list[CheckResult] = []

    def add(check: str, observed: float, threshold: float, failed: bool, detail: str, field_: str | None = None):
        out.append(CheckResult(check, "fail" if failed else "pass", observed, threshold, detail, field_))

    q = contract.slo("max_quarantine_rate", 0.02)
    add("quarantine_rate", m.quarantine_rate, q, m.quarantine_rate > q,
        f"{m.quarantined}/{m.total} records failed record-level validation")

    d = contract.slo("max_duplicate_rate", 0.01)
    add("duplicate_rate", m.duplicate_rate, d, m.duplicate_rate > d,
        "share of records whose unique key repeats within the batch")

    for name, spec in contract.fields.items():
        if spec.max_null_rate is not None:
            r = m.null_rates.get(name, 0.0)
            add("null_rate", r, spec.max_null_rate, r > spec.max_null_rate,
                f"null rate of `{name}`", name)

    drift = contract.slo("schema_drift_threshold", 0.5)
    for name, r in m.unexpected_field_rates.items():
        add("schema_unexpected_field", r, drift, r >= drift,
            f"field `{name}` is not in the contract", name)
    for name, r in m.missing_field_rates.items():
        add("schema_missing_field", r, drift, r >= drift,
            f"contract field `{name}` absent from events", name)

    min_hist = int(contract.slo("volume_min_history", 5))
    if len(history_totals) >= min_hist:
        med = statistics.median(history_totals)
        ratio = round(m.total / med, 4) if med else 0.0
        lo, hi = contract.slo("volume_min_ratio", 0.5), contract.slo("volume_max_ratio", 3.0)
        add("volume", ratio, lo if ratio < 1 else hi, ratio < lo or ratio > hi,
            f"batch size {m.total} vs trailing median {med:.0f}")
    return out
