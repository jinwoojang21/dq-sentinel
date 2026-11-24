"""Scenario-based evaluation of the investigator.

For every failure scenario: fresh lake, N healthy batches to build a baseline, one bad batch,
then grade the incident report:
  * root_cause matches the injected failure
  * upstream_bug matches expectation (contract change vs producer fix)
  * a contract patch is proposed iff the change is legitimate
  * evidence is non-empty

Suites:
  core  the seven textbook failure modes
  hard  scenarios whose obvious symptom points at the wrong cause
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
from .producer import EXPECTED_ROOT_CAUSE, HARD_SCENARIOS, scenario_stream

# Scenarios where the right answer is "evolve the contract", not "fix the producer".
LEGITIMATE_CHANGE = {"schema_drift_rename", "new_category"}

SUITES = {
    "core": [s for s in EXPECTED_ROOT_CAUSE if s not in HARD_SCENARIOS],
    "hard": [s for s in EXPECTED_ROOT_CAUSE if s in HARD_SCENARIOS],
    "all": list(EXPECTED_ROOT_CAUSE),
}


@dataclass
class EvalResult:
    scenario: str
    suite: str
    expected: str
    got: str
    root_cause_ok: bool
    upstream_bug_ok: bool
    proposal_ok: bool
    evidence_ok: bool
    tool_calls: int
    input_tokens: int = 0
    output_tokens: int = 0

    @property
    def passed(self) -> bool:
        return self.root_cause_ok and self.upstream_bug_ok and self.proposal_ok and self.evidence_ok


def run_scenario(scenario: str, agent: str, contracts_dir: Path, client: Any | None = None,
                 batch_size: int = 300, healthy_batches: int = 6, model: str | None = None,
                 save_reports: Path | None = None) -> EvalResult:
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
        if save_reports and last.report_path:
            save_reports.mkdir(parents=True, exist_ok=True)
            shutil.copy(last.report_path, save_reports / f"{agent}-{scenario}.md")
        usage = report.get("_usage") or {}
        expected = EXPECTED_ROOT_CAUSE[scenario]
        legit = scenario in LEGITIMATE_CHANGE
        return EvalResult(
            scenario=scenario,
            suite="hard" if scenario in HARD_SCENARIOS else "core",
            expected=expected,
            got=report.get("root_cause", "no-incident"),
            root_cause_ok=report.get("root_cause") == expected,
            upstream_bug_ok=report.get("upstream_bug") == (not legit),
            proposal_ok=(last.proposal_path is not None) == legit,
            evidence_ok=bool(report.get("evidence")),
            tool_calls=last.tool_calls,
            input_tokens=usage.get("input_tokens", 0),
            output_tokens=usage.get("output_tokens", 0),
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def run_all(agent: str, contracts_dir: Path, scenarios: list[str] | None = None, suite: str = "all",
            **kw: Any) -> list[EvalResult]:
    return [run_scenario(s, agent, contracts_dir, **kw) for s in (scenarios or SUITES[suite])]


def mark(ok: bool) -> str:
    return "✓" if ok else "✗"


def format_table(results: list[EvalResult]) -> str:
    rows = [
        "| suite | scenario | expected | got | cause | bug? | patch | evidence | tools | tokens in/out | result |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        tokens = f"{r.input_tokens:,}/{r.output_tokens:,}" if r.input_tokens else "—"
        rows.append(f"| {r.suite} | {r.scenario} | {r.expected} | {r.got} | {mark(r.root_cause_ok)} | "
                    f"{mark(r.upstream_bug_ok)} | {mark(r.proposal_ok)} | {mark(r.evidence_ok)} | "
                    f"{r.tool_calls} | {tokens} | {'PASS' if r.passed else 'FAIL'} |")
    for suite in ("core", "hard"):
        sub = [r for r in results if r.suite == suite]
        if sub:
            rows.append(f"\n**{suite}: {sum(r.passed for r in sub)}/{len(sub)} passed**")
    return "\n".join(rows)


def to_json(results: list[EvalResult]) -> str:
    return json.dumps([asdict(r) | {"passed": r.passed} for r in results], indent=2)
