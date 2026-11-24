from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
from pathlib import Path

from .config import Settings
from .producer import SCENARIOS, scenario_stream


def _agent_default() -> str:
    return "claude" if os.getenv("ANTHROPIC_API_KEY") else "baseline"


def cmd_scenarios(_: argparse.Namespace) -> int:
    for name, desc in SCENARIOS.items():
        print(f"{name:<22} {desc}")
    return 0


def cmd_demo(a: argparse.Namespace) -> int:
    from .pipeline import Pipeline

    s = Settings()
    if not a.keep and s.home.exists():
        shutil.rmtree(s.home)
    pipe = Pipeline(s, agent=a.agent)
    print(f"agent={a.agent}  scenario={a.scenario}  lake={s.home}\n")
    for out in pipe.run(scenario_stream(a.scenario, a.healthy_batches, a.batch_size, a.seed)):
        status = "OK  " if out.healthy else "FAIL"
        print(f"[{status}] {out.batch_id}  rows={out.total:<5} quarantined={out.quarantined:<5}"
              + (f"  failed={[c.check + (':' + c.field if c.field else '') for c in out.failed]}"
                 if out.failed else ""))
        if out.report:
            r = out.report
            print(f"\n  -> {out.incident_id}: {r['root_cause']} ({r['severity']}, "
                  f"upstream_bug={r['upstream_bug']})")
            print(f"     {r['summary']}")
            print(f"     report:   {out.report_path}")
            if out.proposal_path:
                print(f"     proposal: {out.proposal_path}")
    return 0


def cmd_produce(a: argparse.Namespace) -> int:
    from .kafka_io import produce

    s = Settings()
    produce(a.bootstrap or s.kafka_bootstrap, s.kafka_topic, a.scenario, a.healthy_batches,
            a.batch_size, a.interval, a.seed)
    return 0


def cmd_consume(a: argparse.Namespace) -> int:
    from .kafka_io import consume_batches
    from .pipeline import Pipeline

    s = Settings()
    pipe = Pipeline(s, agent=a.agent)
    for n, (bid, recs) in enumerate(
        consume_batches(a.bootstrap or s.kafka_bootstrap, s.kafka_topic, a.batch_size), 1
    ):
        out = pipe.process(bid, recs)
        print(f"[{'OK  ' if out.healthy else 'FAIL'}] {bid} rows={out.total} q={out.quarantined}"
              + (f" -> {out.incident_id}: {out.report['root_cause']}" if out.report else ""), flush=True)
        if a.max_batches and n >= a.max_batches:
            break
    return 0


def cmd_evals(a: argparse.Namespace) -> int:
    from .evals import format_table, run_all, to_json

    s = Settings()
    results = run_all(a.agent, s.contracts_dir, a.scenario or None, suite=a.suite,
                      save_reports=Path(a.save_reports) if a.save_reports else None)
    print(to_json(results) if a.json else format_table(results))
    return 0 if all(r.passed for r in results) or not a.strict else 1


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=os.getenv("DQ_LOG_LEVEL", "WARNING"))
    p = argparse.ArgumentParser(prog="dq-sentinel", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("scenarios", help="list failure scenarios").set_defaults(fn=cmd_scenarios)

    def common(sp: argparse.ArgumentParser, agent: bool = True) -> None:
        sp.add_argument("--batch-size", type=int, default=500)
        sp.add_argument("--seed", type=int, default=7)
        if agent:
            sp.add_argument("--agent", choices=["claude", "baseline", "none"], default=_agent_default())

    d = sub.add_parser("demo", help="run healthy batches then one failure, in-process (no Kafka)")
    d.add_argument("--scenario", choices=list(SCENARIOS), default="schema_drift_rename")
    d.add_argument("--healthy-batches", type=int, default=8)
    d.add_argument("--keep", action="store_true", help="keep existing lake instead of starting fresh")
    common(d)
    d.set_defaults(fn=cmd_demo)

    pr = sub.add_parser("produce", help="publish synthetic events to Kafka/Redpanda")
    pr.add_argument("--scenario", choices=list(SCENARIOS), default="healthy")
    pr.add_argument("--healthy-batches", type=int, default=8)
    pr.add_argument("--interval", type=float, default=3.0)
    pr.add_argument("--bootstrap")
    common(pr, agent=False)
    pr.set_defaults(fn=cmd_produce)

    c = sub.add_parser("consume", help="consume from Kafka/Redpanda and run the pipeline")
    c.add_argument("--bootstrap")
    c.add_argument("--max-batches", type=int, default=0, help="stop after N batches (0 = run forever)")
    common(c)
    c.set_defaults(fn=cmd_consume)

    e = sub.add_parser("evals", help="grade the investigator on every failure scenario")
    e.add_argument("--scenario", action="append")
    e.add_argument("--suite", choices=["core", "hard", "all"], default="all")
    e.add_argument("--save-reports", metavar="DIR", help="copy each incident report into DIR")
    e.add_argument("--json", action="store_true")
    e.add_argument("--strict", action="store_true", help="exit 1 if any scenario fails")
    e.add_argument("--agent", choices=["claude", "baseline"], default=_agent_default())
    e.set_defaults(fn=cmd_evals)

    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
