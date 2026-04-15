"""Tests for schema drift detection."""

from dpagent.agents.schema import detect_schema_drift, mock_schema
from dpagent.models.pipeline import ColumnSchema, PipelineSchema


def test_no_drift_same_schema() -> None:
    s1 = mock_schema("test", "db", version=1)
    s2 = mock_schema("test", "db", version=1)
    drift = detect_schema_drift(s1, s2)
    assert drift.has_drift is False
    assert drift.severity == "none"


def test_detects_added_column() -> None:
    s1 = mock_schema("test", "db", version=1)
    s2 = mock_schema("test", "db", version=2)
    drift = detect_schema_drift(s2, s1)
    assert drift.has_drift is True
    assert "region" in drift.added_columns
    assert drift.severity == "warning"


def test_detects_removed_column() -> None:
    s1 = mock_schema("test", "db", version=1)
    s3 = mock_schema("test", "db", version=3)
    drift = detect_schema_drift(s3, s1)
    assert drift.has_drift is True
    assert drift.severity == "critical"
    assert "status" in drift.removed_columns


def test_detects_type_change() -> None:
    prev = PipelineSchema(pipeline="t", source="db", columns=[
        ColumnSchema(name="amount", dtype="decimal")
    ])
    curr = PipelineSchema(pipeline="t", source="db", columns=[
        ColumnSchema(name="amount", dtype="float")
    ])
    drift = detect_schema_drift(curr, prev)
    assert len(drift.type_changes) == 1
    assert "amount" in drift.type_changes[0]