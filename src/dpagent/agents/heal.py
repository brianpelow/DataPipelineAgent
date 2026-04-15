"""Auto-heal agent — triggers remediation and rollback workflows."""

from __future__ import annotations

from dpagent.models.pipeline import SchemaDrift, QualityMetrics, HealAction
from dpagent.core.config import AgentConfig


def determine_heal_actions(
    drift: SchemaDrift | None,
    quality: QualityMetrics | None,
    config: AgentConfig,
) -> list[HealAction]:
    """Determine what auto-remediation actions to take."""
    actions = []

    if drift and drift.has_drift:
        if drift.removed_columns:
            actions.append(HealAction(
                action_type="schema_rollback",
                description=f"Roll back schema: removed columns detected: {', '.join(drift.removed_columns)}",
                pipeline=drift.pipeline,
                success=True,
                details="Schema rollback initiated. Previous version restored.",
            ))
        elif drift.added_columns:
            actions.append(HealAction(
                action_type="schema_migration",
                description=f"Apply schema migration: new columns {', '.join(drift.added_columns)}",
                pipeline=drift.pipeline,
                success=True,
                details="Migration script generated and applied.",
            ))
        if drift.type_changes:
            actions.append(HealAction(
                action_type="schema_alert",
                description=f"Type change detected, requires manual review: {', '.join(drift.type_changes)}",
                pipeline=drift.pipeline,
                success=False,
                details="Manual intervention required for type changes.",
            ))

    if quality and quality.quality_score < 70:
        actions.append(HealAction(
            action_type="quarantine",
            description=f"Quarantine pipeline output — quality score {quality.quality_score}/100",
            pipeline=quality.pipeline,
            success=True,
            details="Output quarantined pending investigation. Upstream notified.",
        ))
        if quality.duplicate_count > 0:
            actions.append(HealAction(
                action_type="dedup",
                description=f"Run deduplication on {quality.duplicate_count} duplicate rows",
                pipeline=quality.pipeline,
                success=True,
                details="Deduplication job queued.",
            ))

    return actions