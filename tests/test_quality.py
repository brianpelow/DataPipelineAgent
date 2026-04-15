"""Tests for quality assessment agent."""

from dpagent.agents.quality import assess_quality, mock_quality
from dpagent.core.config import AgentConfig


def test_perfect_quality_score() -> None:
    config = AgentConfig()
    q = assess_quality("test", 10000, 10000, {}, 0, config)
    assert q.quality_score == 100
    assert q.issues == []


def test_quality_penalizes_row_variance() -> None:
    config = AgentConfig()
    q = assess_quality("test", 5000, 10000, {}, 0, config)
    assert q.quality_score < 100
    assert len(q.issues) > 0


def test_quality_penalizes_high_nulls() -> None:
    config = AgentConfig()
    q = assess_quality("test", 10000, 10000, {"amount": 0.20}, 0, config)
    assert q.quality_score < 100
    assert any("null" in i.lower() for i in q.issues)


def test_quality_penalizes_duplicates() -> None:
    config = AgentConfig()
    q = assess_quality("test", 10000, 10000, {}, 500, config)
    assert q.quality_score < 100


def test_mock_quality_healthy() -> None:
    q = mock_quality("test", healthy=True)
    assert q.quality_score == 100


def test_mock_quality_unhealthy() -> None:
    q = mock_quality("test", healthy=False)
    assert q.quality_score < 100