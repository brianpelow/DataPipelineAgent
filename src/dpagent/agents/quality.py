"""Data quality monitoring agent."""

from __future__ import annotations

import uuid
from dpagent.models.pipeline import QualityMetrics, PipelineSchema
from dpagent.core.config import AgentConfig


def assess_quality(
    pipeline: str,
    row_count: int,
    expected_row_count: int,
    null_rates: dict[str, float],
    duplicate_count: int,
    config: AgentConfig,
) -> QualityMetrics:
    """Assess data quality for a pipeline run."""
    issues = []
    score = 100

    variance = abs(row_count - expected_row_count) / max(expected_row_count, 1)
    if variance > config.row_count_variance:
        issues.append(f"Row count variance {variance:.1%} exceeds threshold {config.row_count_variance:.1%}")
        score -= 25

    high_null_cols = {col: rate for col, rate in null_rates.items() if rate > config.null_rate_threshold}
    if high_null_cols:
        for col, rate in high_null_cols.items():
            issues.append(f"High null rate in {col}: {rate:.1%}")
            score -= 10

    if duplicate_count > 0:
        dup_rate = duplicate_count / max(row_count, 1)
        if dup_rate > 0.01:
            issues.append(f"Duplicate rows detected: {duplicate_count} ({dup_rate:.2%})")
            score -= 15

    return QualityMetrics(
        pipeline=pipeline,
        run_id=str(uuid.uuid4())[:8],
        row_count=row_count,
        expected_row_count=expected_row_count,
        null_rates=null_rates,
        duplicate_count=duplicate_count,
        quality_score=max(0, score),
        issues=issues,
    )


def mock_quality(pipeline: str, healthy: bool = True) -> QualityMetrics:
    """Generate mock quality metrics."""
    from dpagent.core.config import AgentConfig
    config = AgentConfig()
    if healthy:
        return assess_quality(pipeline, 10000, 10000, {"amount": 0.001, "currency": 0.0}, 0, config)
    return assess_quality(pipeline, 6500, 10000, {"amount": 0.15, "currency": 0.08}, 120, config)