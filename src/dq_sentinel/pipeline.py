"""Glue: batch -> validate -> write to Iceberg -> checks -> (on failure) agent investigation."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .agent import Toolbox, run_baseline, run_claude_agent
from .checks import CheckResult, compute_metrics, run_checks, validate_batch
from .config import Settings
from .contract import Contract, load_contract
from .lake import Lake
from .report import render_markdown
from .state import State

log = logging.getLogger(__name__)


@dataclass
class BatchOutcome:
    batch_id: str
    total: int
    quarantined: int
    failed: list[CheckResult]
    incident_id: str | None = None
    report: dict[str, Any] | None = None
    report_path: Path | None = None
    proposal_path: str | None = None
    tool_calls: int = 0

    @property
    def healthy(self) -> bool:
        return not self.failed


class Pipeline:
    def __init__(self, settings: Settings, contract: Contract | None = None, agent: str = "claude",
                 client: Any | None = None):
        if agent not in ("claude", "baseline", "none"):
            raise ValueError("agent must be claude|baseline|none")
        self.settings = settings
        settings.ensure_dirs()
        self.contract = contract or load_contract(settings.contracts_dir / "orders.yaml")
        self.lake = Lake(settings, self.contract)
        self.state = State(str(settings.state_db))
        self.agent = agent
        self.client = client

    def process(self, batch_id: str, records: list[dict[str, Any]]) -> BatchOutcome:
        vr = validate_batch(records, self.contract)
        self.lake.append_valid(vr.valid, batch_id)
        self.lake.append_quarantine(vr.quarantined, batch_id)
        metrics = compute_metrics(batch_id, vr, self.contract)
        checks = run_checks(metrics, self.contract, self.state.healthy_totals())
        self.state.record_batch(metrics, checks)
        failed = [c for c in checks if c.status == "fail"]
        out = BatchOutcome(batch_id, vr.total, len(vr.quarantined), failed)
        if failed and self.agent != "none":
            self._investigate(out)
        return out

    def run(self, batches: Iterable[tuple[str, list[dict[str, Any]]]]) -> list[BatchOutcome]:
        return [self.process(bid, recs) for bid, recs in batches]

    def _investigate(self, out: BatchOutcome) -> None:
        incident_id = self.state.next_incident_id()
        tb = Toolbox(incident_id, out.batch_id, self.contract, self.lake, self.state,
                     self.settings.proposals_dir)
        failed = self.state.failed_checks(out.batch_id)
        if self.agent == "claude":
            report = run_claude_agent(tb, failed, self.settings.model, self.settings.max_agent_turns,
                                      client=self.client)
            agent_name = f"claude ({self.settings.model})"
        else:
            report = run_baseline(tb, failed)
            agent_name = "rule-based baseline"
        self.state.save_incident(incident_id, out.batch_id, agent_name, report)
        md = render_markdown(tb, report, failed, agent_name)
        path = self.settings.incidents_dir / f"{incident_id}.md"
        path.write_text(md)
        out.incident_id, out.report, out.report_path = incident_id, report, path
        out.proposal_path = tb.proposal["path"] if tb.proposal else None
        out.tool_calls = len(tb.trace)
