"""Claude tool-use loop for incident investigation."""

from __future__ import annotations

import logging
from typing import Any

from .prompts import SYSTEM_PROMPT, briefing
from .tools import TOOL_SCHEMAS, Toolbox

log = logging.getLogger(__name__)


def run_claude_agent(
    toolbox: Toolbox,
    failed_checks: list[dict[str, Any]],
    model: str,
    max_turns: int = 12,
    client: Any | None = None,
) -> dict[str, Any]:
    """Run the agent until it submits a report or runs out of turns.

    `client` is injectable so tests can drive the loop with a scripted fake.
    Returns the report dict (or a fallback 'unknown' report if the agent never submits).
    """
    if client is None:
        import anthropic

        client = anthropic.Anthropic()

    messages: list[dict[str, Any]] = [
        {"role": "user", "content": briefing(toolbox.incident_id, toolbox.batch_id,
                                             toolbox.contract.name, failed_checks)}
    ]
    usage = {"input_tokens": 0, "output_tokens": 0, "turns": 0}
    nudged = False

    for turn in range(max_turns):
        resp = client.messages.create(
            model=model,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOL_SCHEMAS,
            messages=messages,
        )
        usage["turns"] = turn + 1
        if getattr(resp, "usage", None):
            usage["input_tokens"] += getattr(resp.usage, "input_tokens", 0) or 0
            usage["output_tokens"] += getattr(resp.usage, "output_tokens", 0) or 0
        messages.append({"role": "assistant", "content": resp.content})

        tool_uses = [b for b in resp.content if getattr(b, "type", None) == "tool_use"]
        if not tool_uses:
            if toolbox.report is not None or nudged:
                break
            nudged = True
            messages.append({"role": "user", "content": "Please finish by calling submit_incident_report."})
            continue

        results = []
        for block in tool_uses:
            text, is_error = toolbox.call(block.name, block.input)
            log.info("tool %s -> %s", block.name, "error" if is_error else "ok")
            results.append({"type": "tool_result", "tool_use_id": block.id, "content": text,
                            "is_error": is_error})
        messages.append({"role": "user", "content": results})

        if toolbox.report is not None:
            break

    report = toolbox.report or {
        "severity": "sev2", "root_cause": "unknown", "upstream_bug": True,
        "summary": f"Agent did not submit a report within {max_turns} turns; manual triage needed.",
        "evidence": ["agent turn budget exhausted"], "recommended_actions": ["Triage manually."],
    }
    report["_usage"] = usage
    return report
