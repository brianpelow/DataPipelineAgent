"""Orchestrator agent — coordinates the full pipeline monitoring loop."""

from __future__ import annotations

from dpagent.models.pipeline import PipelineSchema, PipelineStatus
from dpagent.agents.schema import detect_schema_drift
from dpagent.agents.quality import assess_quality
from dpagent.agents.heal import determine_heal_actions
from dpagent.agents.notify import build_notifications
from dpagent.core.config import AgentConfig


def run_pipeline_monitor(
    pipeline: str,
    source: str,
    current_schema: PipelineSchema,
    previous_schema: PipelineSchema | None,
    row_count: int,
    expected_row_count: int,
    null_rates: dict[str, float],
    duplicate_count: int,
    config: AgentConfig,
) -> PipelineStatus:
    """Run the full pipeline monitoring and remediation workflow."""
    from datetime import datetime, timezone
    status = PipelineStatus(
        pipeline=pipeline,
        source=source,
        last_run=datetime.now(timezone.utc).isoformat(),
    )

    drift = None
    if previous_schema:
        drift = detect_schema_drift(current_schema, previous_schema)
        status.schema_drift = drift

    quality = assess_quality(
        pipeline=pipeline,
        row_count=row_count,
        expected_row_count=expected_row_count,
        null_rates=null_rates,
        duplicate_count=duplicate_count,
        config=config,
    )
    status.quality = quality

    heal_actions = determine_heal_actions(drift, quality, config)
    status.heal_actions = heal_actions

    notifications = build_notifications(pipeline, drift, quality, heal_actions, config)
    status.notifications_sent = notifications

    if drift and drift.severity == "critical":
        status.status = "failed"
    elif quality.quality_score < 70:
        status.status = "degraded"
    elif heal_actions:
        status.status = "healing"
    else:
        status.status = "healthy"

    if config.industry == "fintech":
        status.compliance_notes = (
            "Pipeline events logged per FFIEC data governance requirements. "
            "Schema changes require change management ticket within 1 hour."
        )
    else:
        status.compliance_notes = (
            "Pipeline events logged per ISO 9001 data quality requirements."
        )

    return status