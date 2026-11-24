"""Scenario-based evaluation of the investigator.

For every failure scenario: fresh lake, N healthy batches to build a baseline, one bad batch,
then grade the incident report:
  * root_cause matches the injected failure
  * upstream_bug matches expectation (contract change vs producer fix)
  * a contract patch is proposed iff the change is legitimate
  * evidence is non-empty
"""

from __future__ import annotations

import json
import shutil
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .config import Settings
from .pipeline import Pipeline
from .producer import EXPECTED_ROOT_CAUSE, scenario_stream

# Scenarios where the right answer is "evolve the contract", not "fix the producer".
LEGITIMATE_CHANGE = {"schema_drift_rename", "new_category"}


@dataclass
class EvalResult:
    scenario: str
    expected: str
    got: str
    root_cause_ok: bool
    upstream_bug_ok: bool
    proposal_ok: bool
    evidence_ok: bool
    tool_calls: int

    @property
    def passed(self) -> bool:
        return self.root_cause_ok and self.upstream_bug_ok and self.proposal_ok and self.evidence_ok


def run_scenario(scenario: str, agent: str, contracts_dir: Path, client: Any | None = None,
                 batch_size: int = 300, healthy_batches: int = 6, model: str | None = None) -> EvalResult:
    tmp = Path(tempfile.mkdtemp(prefix=f"dq-eval-{scenario}-"))
    try:
        cdir = tmp / "contracts"
        cdir.mkdir()
        shutil.copy(contracts_dir / "orders.yaml", cdir / "orders.yaml")
        settings = Settings(home=tmp / "home", contracts_dir=cdir)
        if model:
            settings.model = model
        pipe = Pipeline(settings, agent=agent, client=client)
        outcomes = pipe.run(scenario_stream(scenario, healthy_batches, batch_size))
        last = outcomes[-1]
        report = last.report or {}
        expected = EXPECTED_ROOT_CAUSE[scenario]
        legit = scenario in LEGITIMATE_CHANGE
        return EvalResult(
            scenario=scenario,
            expected=expected,
            got=report.get("root_cause", "no-incident"),
            root_cause_ok=report.get("root_cause") == expected,
            upstream_bug_ok=report.get("upstream_bug") == (not legit),
            proposal_ok=(last.proposal_path is not None) == legit,
            evidence_ok=bool(report.get("evidence")),
            tool_calls=last.tool_calls,
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def run_all(agent: str, contracts_dir: Path, scenarios: list[str] | None = None, **kw: Any) -> list[EvalResult]:
    return [run_scenario(s, agent, contracts_dir, **kw) for s in (scenarios or list(EXPECTED_ROOT_CAUSE))]


def format_table(results: list[EvalResult]) -> str:
    rows = ["| scenario | expected | got | cause | bug? | patch | evidence | result |",
            "|---|---|---|---|---|---|---|---|"]
    mark = lambda b: "✓" if b else "✗"
    for r in results:
        rows.append(f"| {r.scenario} | {r.expected} | {r.got} | {mark(r.root_cause_ok)} | "
                    f"{mark(r.upstream_bug_ok)} | {mark(r.proposal_ok)} | {mark(r.evidence_ok)} | "
                    f"{'PASS' if r.passed else 'FAIL'} |")
    passed = sum(r.passed for r in results)
    rows.append(f"\n**{passed}/{len(results)} scenarios passed**")
    return "\n".join(rows)


def to_json(results: list[EvalResult]) -> str:
    return json.dumps([asdict(r) | {"passed": r.passed} for r in results], indent=2)
