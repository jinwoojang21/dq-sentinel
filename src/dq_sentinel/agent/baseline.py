"""Rule-based baseline investigator.

It uses the exact same Toolbox as the Claude agent, so it:
  * lets the full pipeline + CI run with no API key, and
  * gives the eval suite a baseline to compare the LLM against.
It only knows the failure modes it was written for; the LLM agent has to generalise.
"""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from .tools import Toolbox


def _call(tb: Toolbox, name: str, **args: Any) -> dict[str, Any]:
    text, _ = tb.call(name, args)
    return json.loads(text)


def run_baseline(toolbox: Toolbox, failed_checks: list[dict[str, Any]]) -> dict[str, Any]:
    info = _call(toolbox, "get_failed_checks")
    failed = {(c["check"], c.get("field")) for c in info["failed_checks"]}
    checks = {c for c, _ in failed}
    m = info["metrics"]
    reasons = Counter({k.split(":")[0]: v for k, v in (m.get("top_quarantine_reasons") or {}).items()})
    evidence: list[str] = [f"{m['quarantined']}/{m['total']} records quarantined ({m['quarantine_rate']:.1%})"]
    upstream_bug, affected = True, []

    unexpected = [f for c, f in failed if c == "schema_unexpected_field"]
    missing = [f for c, f in failed if c == "schema_missing_field"]

    if unexpected and missing:
        cause, affected, upstream_bug = "schema_drift", missing + unexpected, False
        evidence.append(f"contract field(s) {missing} missing while new field(s) {unexpected} appeared")
        if len(missing) == 1 and len(unexpected) == 1:
            _call(toolbox, "propose_contract_patch",
                  operations=[{"op": "rename_field", "from": missing[0], "to": unexpected[0]}],
                  rationale=f"1:1 rename {missing[0]} -> {unexpected[0]} observed in all events")
    elif "volume" in checks:
        cause = "volume_anomaly"
        hist = _call(toolbox, "get_batch_history", limit=6)["batches"]
        evidence.append(f"batch sizes (latest first): {[b['total'] for b in hist]}")
    elif "duplicate_rate" in checks:
        cause, affected = "duplicates", ["order_id"]
        evidence.append(f"duplicate rate {m['duplicate_rate']:.1%}")
    elif "null_rate" in checks:
        cause = "null_spike"
        affected = [f for c, f in failed if c == "null_rate"]
        evidence.append(f"null rates: { {f: m['null_rates'][f] for f in affected} }")
    elif reasons:
        top = reasons.most_common(1)[0][0]
        cause = {"type": "type_change", "allowed": "new_category", "range": "out_of_range",
                 "required": "schema_drift"}.get(top, "unknown")
        sample = _call(toolbox, "sample_quarantine", limit=3, reason_contains=f"{top}:")["samples"]
        if sample:
            field = sample[0]["reasons"][0].split(":")[1]
            affected = [field]
            values = [s["record"].get(field) for s in sample]
            evidence.append(f"sample `{field}` values: {values}")
            if cause == "new_category":
                upstream_bug = False
                new_vals = sorted({str(v) for v in values})
                _call(toolbox, "propose_contract_patch",
                      operations=[{"op": "add_allowed_value", "field": field, "value": v} for v in new_vals],
                      rationale=f"new value(s) {new_vals} for `{field}`")
    else:
        cause = "unknown"

    report = {
        "severity": "sev2" if m["quarantine_rate"] > 0.1 or cause == "volume_anomaly" else "sev3",
        "root_cause": cause,
        "summary": f"Rule-based baseline classified this as {cause}.",
        "evidence": evidence,
        "upstream_bug": upstream_bug,
        "recommended_actions": ["Review the evidence and confirm with the producing team."],
        "affected_fields": affected,
    }
    _call(toolbox, "submit_incident_report", **report)
    return toolbox.report or report
