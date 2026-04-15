"""Nightly agent — automated pipeline monitoring for DataPipelineAgent."""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

REPO_ROOT = Path(__file__).parent.parent


def run_mock_monitor() -> None:
    """Run a mock pipeline monitor and save the output."""
    from dpagent.core.config import AgentConfig
    from dpagent.agents.schema import mock_schema
    from dpagent.agents.orchestrator import run_pipeline_monitor

    config = AgentConfig()
    pipelines = [
        ("payments-etl", "payments-db"),
        ("fx-rates-etl", "fx-source"),
        ("audit-etl", "audit-db"),
    ]

    results = []
    for pipeline, source in pipelines:
        current = mock_schema(pipeline, source, version=2)
        previous = mock_schema(pipeline, source, version=1)
        status = run_pipeline_monitor(
            pipeline=pipeline, source=source,
            current_schema=current, previous_schema=previous,
            row_count=9900, expected_row_count=10000,
            null_rates={"amount": 0.001}, duplicate_count=0,
            config=config,
        )
        results.append({
            "pipeline": pipeline,
            "status": status.status,
            "quality_score": status.quality.quality_score if status.quality else 0,
            "drift_severity": status.schema_drift.severity if status.schema_drift else "none",
            "heal_actions": len(status.heal_actions),
        })

    out = REPO_ROOT / "docs" / "nightly-monitor-report.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "date": date.today().isoformat(),
        "pipelines_monitored": len(results),
        "results": results,
    }, indent=2))
    print(f"[agent] Monitored {len(results)} pipelines -> {out}")


def refresh_changelog() -> None:
    changelog = REPO_ROOT / "CHANGELOG.md"
    if not changelog.exists():
        return
    today = date.today().isoformat()
    content = changelog.read_text()
    if today not in content:
        content = content.replace("## [Unreleased]", f"## [Unreleased]\n\n_Last checked: {today}_", 1)
        changelog.write_text(content)
    print("[agent] Refreshed CHANGELOG timestamp")


if __name__ == "__main__":
    print(f"[agent] Starting nightly agent - {date.today().isoformat()}")
    run_mock_monitor()
    refresh_changelog()
    print("[agent] Done.")