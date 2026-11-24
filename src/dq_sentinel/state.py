"""Operational state (batch metrics, check results, incidents) in a DuckDB file."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

import duckdb

from .checks import BatchMetrics, CheckResult

DDL = """
CREATE TABLE IF NOT EXISTS batches (
  batch_id VARCHAR PRIMARY KEY, recorded_at TIMESTAMPTZ, total INTEGER, valid INTEGER,
  quarantined INTEGER, quarantine_rate DOUBLE, duplicate_rate DOUBLE, null_rates JSON,
  missing_field_rates JSON, unexpected_field_rates JSON, top_quarantine_reasons JSON, healthy BOOLEAN
);
CREATE TABLE IF NOT EXISTS checks (
  batch_id VARCHAR, "check" VARCHAR, field VARCHAR, status VARCHAR,
  observed DOUBLE, threshold DOUBLE, detail VARCHAR
);
CREATE TABLE IF NOT EXISTS incidents (
  incident_id VARCHAR PRIMARY KEY, batch_id VARCHAR, opened_at TIMESTAMPTZ, status VARCHAR,
  agent VARCHAR, severity VARCHAR, root_cause VARCHAR, summary VARCHAR, report JSON
);
"""


class State:
    def __init__(self, path: str):
        self.con = duckdb.connect(path)
        self.con.execute(DDL)

    def record_batch(self, m: BatchMetrics, checks: list[CheckResult]) -> None:
        row = m.to_row()
        healthy = all(c.status == "pass" for c in checks)
        self.con.execute(
            "INSERT OR REPLACE INTO batches VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [row["batch_id"], datetime.now(UTC), row["total"], row["valid"], row["quarantined"],
             row["quarantine_rate"], row["duplicate_rate"], row["null_rates"], row["missing_field_rates"],
             row["unexpected_field_rates"], row["top_quarantine_reasons"], healthy],
        )
        self.con.execute("DELETE FROM checks WHERE batch_id = ?", [m.batch_id])
        for c in checks:
            self.con.execute(
                "INSERT INTO checks VALUES (?,?,?,?,?,?,?)",
                [m.batch_id, c.check, c.field, c.status, c.observed, c.threshold, c.detail],
            )

    def healthy_totals(self, limit: int = 20) -> list[int]:
        rows = self.con.execute(
            "SELECT total FROM batches WHERE healthy ORDER BY recorded_at DESC LIMIT ?", [limit]
        ).fetchall()
        return [r[0] for r in rows]

    def failed_checks(self, batch_id: str) -> list[dict[str, Any]]:
        cur = self.con.execute(
            'SELECT "check", field, observed, threshold, detail FROM checks '
            "WHERE batch_id = ? AND status = 'fail'", [batch_id]
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def batch(self, batch_id: str) -> dict[str, Any] | None:
        cur = self.con.execute("SELECT * FROM batches WHERE batch_id = ?", [batch_id])
        r = cur.fetchone()
        if not r:
            return None
        d = dict(zip([c[0] for c in cur.description], r))
        for k in ("null_rates", "missing_field_rates", "unexpected_field_rates", "top_quarantine_reasons"):
            d[k] = json.loads(d[k]) if d[k] else {}
        d["recorded_at"] = str(d["recorded_at"])
        return d

    def history(self, limit: int = 10) -> list[dict[str, Any]]:
        cur = self.con.execute(
            "SELECT batch_id, total, quarantine_rate, duplicate_rate, healthy FROM batches "
            "ORDER BY recorded_at DESC LIMIT ?", [limit]
        )
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def next_incident_id(self) -> str:
        (n,) = self.con.execute("SELECT count(*) FROM incidents").fetchone()
        return f"INC-{n + 1:04d}"

    def save_incident(self, incident_id: str, batch_id: str, agent: str, report: dict[str, Any] | None) -> None:
        report = report or {}
        self.con.execute(
            "INSERT OR REPLACE INTO incidents VALUES (?,?,?,?,?,?,?,?,?)",
            [incident_id, batch_id, datetime.now(UTC), "open", agent,
             report.get("severity"), report.get("root_cause"), report.get("summary"),
             json.dumps(report, default=str)],
        )

    def table_arrow(self, name: str):
        rel = self.con.sql(f"SELECT * FROM {name}")
        # DuckDB renamed this across versions: arrow() -> to_arrow_table()
        for meth in ("to_arrow_table", "arrow"):
            if hasattr(rel, meth):
                return getattr(rel, meth)()
        raise RuntimeError("unsupported duckdb version: no arrow fetch method")

    @staticmethod
    def checks_as_dicts(checks: list[CheckResult]) -> list[dict[str, Any]]:
        return [asdict(c) for c in checks]
