"""Render incident reports as Markdown."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .agent.tools import Toolbox


def render_markdown(
    toolbox: Toolbox, report: dict[str, Any], failed_checks: list[dict[str, Any]], agent: str
) -> str:
    lines = [
        f"# {toolbox.incident_id} — {report['root_cause'].replace('_', ' ')} on `{toolbox.contract.name}`",
        "",
        "| Batch | Severity | Upstream bug | Investigator |",
        "|---|---|---|---|",
        f"| `{toolbox.batch_id}` | {report['severity']} | {'yes' if report['upstream_bug'] else 'no — contract change'} | {agent} |",
        "",
        "## Summary",
        report["summary"],
        "",
        "## Failed checks",
        *[f"- **{c['check']}**{' `' + c['field'] + '`' if c.get('field') else ''}: {c['observed']} "
          f"(threshold {c['threshold']}) — {c['detail']}" for c in failed_checks],
        "",
        "## Evidence",
        *[f"- {e}" for e in report["evidence"]],
        "",
        "## Recommended actions",
        *[f"1. {a}" for a in report["recommended_actions"]],
    ]
    if toolbox.proposal:
        lines += ["", "## Proposed contract change",
                  f"_{toolbox.proposal['rationale']}_ — written to `{Path(toolbox.proposal['path']).name}`",
                  "", "```diff", toolbox.proposal["diff"].rstrip(), "```"]
    lines += ["", "## Investigation trace", "| # | Tool | Input | ok | ms |", "|---|---|---|---|---|"]
    for i, c in enumerate(toolbox.trace, 1):
        inp = str(c.input).replace("|", "\\|")
        lines.append(f"| {i} | `{c.name}` | `{inp[:90]}` | {'✓' if c.ok else '✗'} | {c.ms} |")
    usage = report.get("_usage")
    if usage:
        lines += ["", (f"_Model turns: {usage['turns']}, tokens in/out: "
                       f"{usage['input_tokens']}/{usage['output_tokens']}_")]
    return "\n".join(lines) + "\n"
