"""Tests for pipeline data models."""

from dpagent.models.pipeline import ColumnSchema, PipelineSchema, SchemaDrift, QualityMetrics, HealAction


def test_column_schema_defaults() -> None:
    col = ColumnSchema(name="id", dtype="bigint")
    assert col.nullable is True
    assert col.description == ""


def test_pipeline_schema_column_names() -> None:
    schema = PipelineSchema(pipeline="test", source="db", columns=[
        ColumnSchema(name="id", dtype="bigint"),
        ColumnSchema(name="amount", dtype="decimal"),
    ])
    assert schema.column_names == {"id", "amount"}


def test_schema_drift_defaults() -> None:
    drift = SchemaDrift(pipeline="test")
    assert drift.has_drift is False
    assert drift.severity == "none"


def test_quality_metrics_defaults() -> None:
    q = QualityMetrics(pipeline="test")
    assert q.quality_score == 100
    assert q.issues == []


def test_heal_action_defaults() -> None:
    action = HealAction(action_type="rollback", description="test", pipeline="test")
    assert action.success is False