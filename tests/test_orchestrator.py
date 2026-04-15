"""Tests for the orchestrator agent."""

from dpagent.core.config import AgentConfig
from dpagent.agents.schema import mock_schema
from dpagent.agents.orchestrator import run_pipeline_monitor


def make_config() -> AgentConfig:
    return AgentConfig()


def test_healthy_pipeline() -> None:
    config = make_config()
    current = mock_schema("test", "db", version=1)
    status = run_pipeline_monitor(
        "test", "db", current, None,
        10000, 10000, {}, 0, config,
    )
    assert status.status == "healthy"
    assert status.quality is not None
    assert status.quality.quality_score == 100


def test_schema_drift_detected() -> None:
    config = make_config()
    current = mock_schema("test", "db", version=3)
    previous = mock_schema("test", "db", version=1)
    status = run_pipeline_monitor(
        "test", "db", current, previous,
        10000, 10000, {}, 0, config,
    )
    assert status.schema_drift is not None
    assert status.schema_drift.has_drift is True
    assert status.status == "failed"


def test_quality_degradation() -> None:
    config = make_config()
    current = mock_schema("test", "db", version=1)
    status = run_pipeline_monitor(
        "test", "db", current, None,
        3000, 10000, {"amount": 0.30}, 200, config,
    )
    assert status.status == "degraded"


def test_compliance_notes_set() -> None:
    config = make_config()
    current = mock_schema("test", "db", version=1)
    status = run_pipeline_monitor(
        "test", "db", current, None,
        10000, 10000, {}, 0, config,
    )
    assert len(status.compliance_notes) > 0


def test_heal_actions_on_drift() -> None:
    config = make_config()
    current = mock_schema("test", "db", version=2)
    previous = mock_schema("test", "db", version=1)
    status = run_pipeline_monitor(
        "test", "db", current, previous,
        10000, 10000, {}, 0, config,
    )
    assert len(status.heal_actions) > 0