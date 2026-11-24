"""Agent plumbing tests. The Claude loop is driven by a scripted fake client — no API key needed."""

import json
from types import SimpleNamespace

import pytest

from dq_sentinel.agent import Toolbox
from dq_sentinel.evals import SUITES, run_scenario
from dq_sentinel.pipeline import Pipeline
from dq_sentinel.producer import scenario_stream


@pytest.fixture
def drift_pipeline(settings):
    p = Pipeline(settings, agent="none")
    p.run(scenario_stream("schema_drift_rename", 6, 100))
    return p


def toolbox(p, batch="b00006"):
    return Toolbox("INC-T", batch, p.contract, p.lake, p.state, p.settings.proposals_dir)


@pytest.mark.parametrize("query", [
    "DROP TABLE orders",
    "select 1; drop table orders",
    "COPY orders TO '/tmp/x.csv'",
    "select * from read_csv('/etc/passwd')",
    "ATTACH 'x.db'",
])
def test_sql_guard_blocks_writes_and_file_access(drift_pipeline, query):
    _, is_error = toolbox(drift_pipeline).call("run_sql", {"query": query})
    assert is_error


def test_sql_reads_and_caps_rows(drift_pipeline):
    out, err = toolbox(drift_pipeline).call("run_sql", {"query": "select * from orders"})
    res = json.loads(out)
    assert not err and len(res["rows"]) == 50 and res["truncated"]


def test_quarantine_sample_shows_raw_payload(drift_pipeline):
    out, _ = toolbox(drift_pipeline).call("sample_quarantine", {"limit": 2})
    s = json.loads(out)["samples"]
    assert len(s) == 2 and "order_amount" in s[0]["record"]


def test_unknown_tool_and_bad_args_are_errors_not_crashes(drift_pipeline):
    tb = toolbox(drift_pipeline)
    assert tb.call("rm_rf", {})[1]
    assert tb.call("get_batch_history", {"bogus": 1})[1]
    assert tb.call("submit_incident_report", {"root_cause": "aliens"})[1]


# ------------------------------------------------------------------ fake Anthropic client
def _block(**kw):
    return SimpleNamespace(**kw)


class FakeClient:
    """Replays a fixed sequence of assistant turns and records what it was sent."""

    def __init__(self, turns):
        self.turns, self.calls = list(turns), []
        self.messages = self

    def create(self, **kw):
        self.calls.append(kw)
        content = self.turns.pop(0)
        return SimpleNamespace(content=content, usage=SimpleNamespace(input_tokens=100, output_tokens=20))


def test_claude_loop_executes_tools_and_collects_report(settings):
    turns = [
        [_block(type="text", text="Looking at the checks."),
         _block(type="tool_use", id="t1", name="get_failed_checks", input={})],
        [_block(type="tool_use", id="t2", name="sample_quarantine", input={"limit": 2}),
         _block(type="tool_use", id="t3", name="propose_contract_patch", input={
             "operations": [{"op": "rename_field", "from": "amount", "to": "order_amount"}],
             "rationale": "clean rename"})],
        [_block(type="tool_use", id="t4", name="submit_incident_report", input={
            "severity": "sev2", "root_cause": "schema_drift", "summary": "amount renamed",
            "evidence": ["100% of events carry order_amount"], "upstream_bug": False,
            "recommended_actions": ["merge proposal"]})],
    ]
    fake = FakeClient(turns)
    p = Pipeline(settings, agent="claude", client=fake)
    out = p.run(scenario_stream("schema_drift_rename", 6, 100))[-1]

    assert out.report["root_cause"] == "schema_drift"
    assert out.proposal_path and out.tool_calls == 4
    assert out.report["_usage"] == {"input_tokens": 300, "output_tokens": 60, "turns": 3}
    # tool results were fed back with matching ids
    tool_result_turns = [
        {r["tool_use_id"] for r in m["content"]}
        for m in fake.calls[-1]["messages"] if m["role"] == "user" and isinstance(m["content"], list)
    ]
    assert tool_result_turns == [{"t1"}, {"t2", "t3"}, {"t4"}]
    assert "Proposed contract change" in out.report_path.read_text()


def test_claude_loop_falls_back_when_agent_never_submits(settings):
    chatter = [[_block(type="text", text="hmm")]] * 2
    p = Pipeline(settings, agent="claude", client=FakeClient(chatter))
    out = p.run(scenario_stream("duplicates", 6, 100))[-1]
    assert out.report["root_cause"] == "unknown"


@pytest.mark.parametrize("scenario", SUITES["core"])
def test_baseline_passes_core_suite(settings, scenario):
    r = run_scenario(scenario, "baseline", settings.contracts_dir, batch_size=200)
    assert r.passed, r


@pytest.mark.parametrize("scenario", SUITES["hard"])
def test_hard_scenarios_open_incidents_that_fool_the_baseline(settings, scenario):
    r = run_scenario(scenario, "baseline", settings.contracts_dir, batch_size=300)
    assert r.got != "no-incident"          # checks fired
    assert not r.root_cause_ok, r          # but the surface symptom misleads the rules
