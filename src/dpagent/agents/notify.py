"""Notification agent — routes alerts to the right stakeholders."""

from __future__ import annotations

from dpagent.models.pipeline import SchemaDrift, QualityMetrics, HealAction
from dpagent.core.config import AgentConfig


STAKEHOLDER_MAP = {
    "fintech": {
        "critical": ["data-engineering", "platform-oncall", "data-governance", "compliance"],
        "warning": ["data-engineering", "data-governance"],
        "info": ["data-engineering"],
    },
    "manufacturing": {
        "critical": ["data-engineering", "operations", "quality-control", "compliance"],
        "warning": ["data-engineering", "quality-control"],
        "info": ["data-engineering"],
    },
}

DEFAULT_STAKEHOLDERS = {
    "critical": ["data-engineering", "platform-oncall"],
    "warning": ["data-engineering"],
    "info": ["data-engineering"],
}


def build_notifications(
    pipeline: str,
    drift: SchemaDrift | None,
    quality: QualityMetrics | None,
    heal_actions: list[HealAction],
    config: AgentConfig,
) -> list[str]:
    """Build list of notifications to send."""
    notifications = []
    stakeholders_map = STAKEHOLDER_MAP.get(config.industry, DEFAULT_STAKEHOLDERS)

    if drift and drift.has_drift:
        level = "critical" if drift.severity == "critical" else "warning"
        teams = stakeholders_map.get(level, ["data-engineering"])
        for team in teams:
            notifications.append(
                f"[{level.upper()}] Schema drift on {pipeline} — "
                f"removed: {drift.removed_columns}, added: {drift.added_columns} "
                f"-> @{team}"
            )

    if quality and quality.quality_score < 70:
        teams = stakeholders_map.get("critical", ["data-engineering"])
        for team in teams:
            notifications.append(
                f"[CRITICAL] Quality degradation on {pipeline} — "
                f"score: {quality.quality_score}/100, issues: {len(quality.issues)} "
                f"-> @{team}"
            )
    elif quality and quality.quality_score < 85:
        teams = stakeholders_map.get("warning", ["data-engineering"])
        for team in teams:
            notifications.append(
                f"[WARNING] Quality below threshold on {pipeline} — "
                f"score: {quality.quality_score}/100 -> @{team}"
            )

    failed_heals = [a for a in heal_actions if not a.success]
    if failed_heals:
        notifications.append(
            f"[ACTION REQUIRED] {len(failed_heals)} heal action(s) require manual intervention "
            f"on {pipeline} -> @data-engineering @platform-oncall"
        )

    return notifications