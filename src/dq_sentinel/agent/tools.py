"""Tools the investigation agent can call.

Design rules:
  * Every tool is read-only except `propose_contract_patch`, which writes a *proposal*
    file for human review — the live contract is never modified by the agent.
  * SQL runs in an isolated in-memory DuckDB over snapshots of the lake and state,
    with a single-statement SELECT/WITH guard and a row cap.
  * `submit_incident_report` is the terminal tool; its schema is the report contract.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import duckdb
import yaml

from ..contract import PATCH_OPS, apply_patch, yaml_diff

ROOT_CAUSES = [
    "schema_drift", "type_change", "null_spike", "new_category",
    "duplicates", "volume_anomaly", "out_of_range", "unit_change", "format_change", "unknown",
]
ROOT_CAUSE_HELP = (
    "schema_drift: fields renamed/added/removed; type_change: values arrive as a different type; "
    "null_spike: a field goes missing/null for a share of events; new_category: a genuinely new valid "
    "enum value; duplicates: repeated unique keys; volume_anomaly: batch size far from normal; "
    "out_of_range: values outside bounds because their meaning/sign changed; unit_change: same field, "
    "different unit or scale (e.g. cents vs dollars) — check distributions vs history; format_change: "
    "same values in a different textual format (case, padding, separators); unknown: none fit."
)
SEVERITIES = ["sev1", "sev2", "sev3"]
MAX_SQL_ROWS = 50

TOOL_SCHEMAS: list[dict[str, Any]] = [
    {
        "name": "get_failed_checks",
        "description": "Failed batch-level checks for the batch under investigation, plus the batch's metrics "
                       "(null rates, missing/unexpected field rates, top quarantine reasons).",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_contract",
        "description": "The current data contract (YAML) the stream is validated against.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_batch_history",
        "description": "Recent batches with size, quarantine rate, duplicate rate and health. Use it to tell a "
                       "sudden change from a gradual trend.",
        "input_schema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 10}},
        },
    },
    {
        "name": "sample_quarantine",
        "description": "Raw quarantined records from the batch with the validation reasons for each. "
                       "Optionally filter to records whose reasons contain `reason_contains`.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "minimum": 1, "maximum": 25, "default": 5},
                "reason_contains": {"type": "string"},
            },
        },
    },
    {
        "name": "run_sql",
        "description": (
            "Run ONE read-only DuckDB SELECT (or WITH ... SELECT). Tables: "
            "`orders` (validated rows, columns from the contract plus _batch_id, _ingested_at), "
            "`orders_quarantine` (_batch_id, raw_json, reasons JSON array as text), "
            "`batches` (per-batch metrics), `checks` (per-batch check results). "
            f"Results are capped at {MAX_SQL_ROWS} rows. Use json_extract_string(raw_json, '$.field') "
            "to inspect quarantined payloads."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    },
    {
        "name": "propose_contract_patch",
        "description": (
            "Propose a change to the data contract when the evidence shows a LEGITIMATE upstream change "
            "(e.g. a deliberate field rename or a new valid category). Do NOT use it to paper over a bug "
            "(e.g. duplicates or broken values). Writes a proposal for human review and returns the diff. "
            "Operations: "
            "rename_field{from,to}; add_allowed_value{field,value}; "
            "set_field_property{field,key,value} with key in required|min|max|max_null_rate|type|unique; "
            "set_slo{key,value}; add_field{field,spec}."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "operations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"op": {"type": "string", "enum": sorted(PATCH_OPS)}},
                        "required": ["op"],
                    },
                    "minItems": 1,
                },
                "rationale": {"type": "string"},
            },
            "required": ["operations", "rationale"],
        },
    },
    {
        "name": "submit_incident_report",
        "description": "Finish the investigation. Call exactly once, after gathering evidence.",
        "input_schema": {
            "type": "object",
            "properties": {
                "severity": {"type": "string", "enum": SEVERITIES,
                             "description": "sev1: data loss/corruption reaching consumers; "
                                            "sev2: large share quarantined or misleading metrics; "
                                            "sev3: minor or contained."},
                "root_cause": {"type": "string", "enum": ROOT_CAUSES, "description": ROOT_CAUSE_HELP},
                "summary": {"type": "string", "description": "2-4 sentences for an on-call engineer."},
                "evidence": {"type": "array", "items": {"type": "string"}, "minItems": 1,
                             "description": "Concrete observations (numbers, field names, sample values)."},
                "upstream_bug": {"type": "boolean",
                                 "description": "true if the producer is wrong and must be fixed; "
                                                "false if the contract should evolve."},
                "recommended_actions": {"type": "array", "items": {"type": "string"}, "minItems": 1},
                "affected_fields": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["severity", "root_cause", "summary", "evidence", "upstream_bug",
                         "recommended_actions"],
        },
    },
]

_SQL_ALLOWED = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
_SQL_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|create|drop|alter|attach|detach|copy|export|import|install|load|pragma|"
    r"call|set|read_csv\w*|read_parquet|read_json\w*|glob|httpfs)\b",
    re.IGNORECASE,
)


@dataclass
class ToolCall:
    name: str
    input: dict[str, Any]
    ok: bool
    ms: int
    preview: str


@dataclass
class Toolbox:
    """Executes tool calls against one incident's context and keeps an audit trail."""

    incident_id: str
    batch_id: str
    contract: Any
    lake: Any
    state: Any
    proposals_dir: Any
    trace: list[ToolCall] = field(default_factory=list)
    proposal: dict[str, Any] | None = None
    report: dict[str, Any] | None = None
    _sql: duckdb.DuckDBPyConnection | None = None

    # ---------------------------------------------------------------- dispatch
    def call(self, name: str, args: dict[str, Any] | None) -> tuple[str, bool]:
        """Return (json_result, is_error). Never raises."""
        args = args or {}
        fn: Callable[..., Any] | None = getattr(self, f"t_{name}", None)
        start = time.perf_counter()
        if fn is None:
            out, ok = {"error": f"unknown tool {name!r}"}, False
        else:
            try:
                out, ok = fn(**args), True
            except Exception as exc:  # noqa: BLE001 — tool errors go back to the model, never crash the loop
                out, ok = {"error": f"{type(exc).__name__}: {exc}"}, False
        text = json.dumps(out, default=str)
        self.trace.append(ToolCall(name, args, ok, int((time.perf_counter() - start) * 1000), text[:160]))
        return text, not ok

    # ---------------------------------------------------------------- tools
    def t_get_failed_checks(self) -> dict[str, Any]:
        b = self.state.batch(self.batch_id) or {}
        return {"batch_id": self.batch_id, "failed_checks": self.state.failed_checks(self.batch_id),
                "metrics": {k: b.get(k) for k in (
                    "total", "valid", "quarantined", "quarantine_rate", "duplicate_rate", "null_rates",
                    "missing_field_rates", "unexpected_field_rates", "top_quarantine_reasons")}}

    def t_get_contract(self) -> dict[str, Any]:
        return {"contract_yaml": self.contract.to_yaml()}

    def t_get_batch_history(self, limit: int = 10) -> dict[str, Any]:
        return {"batches": self.state.history(min(int(limit), 50))}

    def t_sample_quarantine(self, limit: int = 5, reason_contains: str | None = None) -> dict[str, Any]:
        q = "SELECT raw_json, reasons FROM orders_quarantine WHERE _batch_id = ?"
        params: list[Any] = [self.batch_id]
        if reason_contains:
            q += " AND reasons ILIKE ?"
            params.append(f"%{reason_contains}%")
        q += " LIMIT ?"
        params.append(min(int(limit), 25))
        rows = self._conn().execute(q, params).fetchall()
        return {"samples": [{"record": json.loads(r), "reasons": json.loads(s)} for r, s in rows]}

    def t_run_sql(self, query: str) -> dict[str, Any]:
        stripped = query.strip().rstrip(";")
        if ";" in stripped:
            raise ValueError("only a single statement is allowed")
        if not _SQL_ALLOWED.match(stripped) or _SQL_FORBIDDEN.search(stripped):
            raise ValueError("only read-only SELECT/WITH queries over the provided tables are allowed")
        cur = self._conn().execute(f"SELECT * FROM ({stripped}) AS q LIMIT {MAX_SQL_ROWS + 1}")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return {"columns": cols, "rows": [list(r) for r in rows[:MAX_SQL_ROWS]],
                "truncated": len(rows) > MAX_SQL_ROWS}

    def t_propose_contract_patch(self, operations: list[dict[str, Any]], rationale: str) -> dict[str, Any]:
        new = apply_patch(self.contract.raw, operations)
        diff = yaml_diff(self.contract.raw, new, self.contract.name)
        self.proposals_dir.mkdir(parents=True, exist_ok=True)
        path = self.proposals_dir / f"{self.incident_id}-{self.contract.name}.yaml"
        header = f"# Proposed by dq-sentinel for {self.incident_id}. Review before merging.\n# {rationale}\n"
        path.write_text(header + yaml.safe_dump(new, sort_keys=False))
        self.proposal = {"operations": operations, "rationale": rationale, "diff": diff, "path": str(path)}
        return {"written_to": str(path), "new_version": new["version"], "diff": diff}

    def t_submit_incident_report(self, **report: Any) -> dict[str, Any]:
        if report.get("root_cause") not in ROOT_CAUSES:
            raise ValueError(f"root_cause must be one of {ROOT_CAUSES}")
        if report.get("severity") not in SEVERITIES:
            raise ValueError(f"severity must be one of {SEVERITIES}")
        if not report.get("evidence"):
            raise ValueError("evidence must not be empty")
        self.report = report
        return {"accepted": True, "incident_id": self.incident_id}

    # ---------------------------------------------------------------- sql sandbox
    def _conn(self) -> duckdb.DuckDBPyConnection:
        if self._sql is None:
            con = duckdb.connect(":memory:")
            snapshots = {
                "orders": self.lake.scan(self.lake.table_name),
                "orders_quarantine": self.lake.scan(self.lake.quarantine_name),
                "batches": self.state.table_arrow("batches"),
                "checks": self.state.table_arrow("checks"),
            }
            for name, tbl in snapshots.items():
                con.register(f"_{name}_arrow", tbl)
                con.execute(f"CREATE TABLE {name} AS SELECT * FROM _{name}_arrow")
                con.unregister(f"_{name}_arrow")
            con.execute("SET enable_external_access = false")
            self._sql = con
        return self._sql
