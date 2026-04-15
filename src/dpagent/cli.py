"""DataPipelineAgent CLI entry point."""

from __future__ import annotations

import json
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from dpagent.core.config import AgentConfig
from dpagent.agents.schema import mock_schema
from dpagent.agents.quality import mock_quality
from dpagent.agents.orchestrator import run_pipeline_monitor

app = typer.Typer(name="dp-agent", help="Agentic ETL pipeline orchestrator.")
console = Console()


@app.command("monitor")
def monitor(
    pipeline: str = typer.Option(..., "--pipeline", "-p", help="Pipeline name"),
    source: str = typer.Option("default-source", "--source", "-s", help="Data source"),
    output_json: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """Run the full pipeline monitoring workflow."""
    config = AgentConfig.from_env()
    current = mock_schema(pipeline, source, version=2)
    previous = mock_schema(pipeline, source, version=1)

    status = run_pipeline_monitor(
        pipeline=pipeline, source=source,
        current_schema=current, previous_schema=previous,
        row_count=9800, expected_row_count=10000,
        null_rates={"amount": 0.002, "currency": 0.0},
        duplicate_count=0, config=config,
    )

    if output_json:
        print(json.dumps(status.model_dump(), indent=2))
        return

    color = {"healthy": "green", "degraded": "yellow", "failed": "red", "healing": "blue"}.get(status.status, "white")
    console.print(Panel.fit(
        f"Status: [{color}]{status.status.upper()}[/{color}]\n"
        f"Quality score: {status.quality.quality_score if status.quality else 'N/A'}/100\n"
        f"Schema drift: {status.schema_drift.severity if status.schema_drift else 'none'}\n"
        f"Heal actions: {len(status.heal_actions)}\n"
        f"Notifications: {len(status.notifications_sent)}",
        title=f"Pipeline: {pipeline}",
    ))

    if status.notifications_sent:
        console.print("\n[bold]Notifications sent:[/bold]")
        for n in status.notifications_sent:
            console.print(f"  [dim]{n}[/dim]")


@app.command("check-schema")
def check_schema(
    pipeline: str = typer.Option(..., "--pipeline", "-p"),
    version_a: int = typer.Option(1, "--from"),
    version_b: int = typer.Option(2, "--to"),
) -> None:
    """Compare two schema versions for drift."""
    from dpagent.agents.schema import detect_schema_drift
    prev = mock_schema(pipeline, "source", version=version_a)
    curr = mock_schema(pipeline, "source", version=version_b)
    drift = detect_schema_drift(curr, prev)

    color = {"critical": "red", "warning": "yellow", "info": "blue", "none": "green"}.get(drift.severity, "white")
    console.print(Panel.fit(
        f"Drift detected: [{color}]{drift.severity.upper()}[/{color}]\n"
        f"Added: {drift.added_columns or 'none'}\n"
        f"Removed: {drift.removed_columns or 'none'}\n"
        f"Type changes: {drift.type_changes or 'none'}",
        title=f"Schema diff: v{version_a} -> v{version_b}",
    ))


def main() -> None:
    app()


if __name__ == "__main__":
    main()