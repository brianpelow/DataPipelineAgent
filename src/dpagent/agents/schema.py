"""Schema drift detection agent."""

from __future__ import annotations

from dpagent.models.pipeline import PipelineSchema, SchemaDrift, ColumnSchema


def detect_schema_drift(current: PipelineSchema, previous: PipelineSchema) -> SchemaDrift:
    """Compare two schema versions and identify drift."""
    added = list(current.column_names - previous.column_names)
    removed = list(previous.column_names - current.column_names)

    type_changes = []
    nullable_changes = []

    prev_cols = {c.name: c for c in previous.columns}
    curr_cols = {c.name: c for c in current.columns}

    for name in current.column_names & previous.column_names:
        prev = prev_cols[name]
        curr = curr_cols[name]
        if prev.dtype != curr.dtype:
            type_changes.append(f"{name}: {prev.dtype} -> {curr.dtype}")
        if prev.nullable != curr.nullable:
            nullable_changes.append(f"{name}: nullable={prev.nullable} -> {curr.nullable}")

    has_drift = bool(added or removed or type_changes or nullable_changes)

    if removed or type_changes:
        severity = "critical"
    elif added or nullable_changes:
        severity = "warning"
    elif has_drift:
        severity = "info"
    else:
        severity = "none"

    return SchemaDrift(
        pipeline=current.pipeline,
        added_columns=added,
        removed_columns=removed,
        type_changes=type_changes,
        nullable_changes=nullable_changes,
        has_drift=has_drift,
        severity=severity,
    )


def mock_schema(pipeline: str, source: str, version: int = 1) -> PipelineSchema:
    """Generate a mock schema for testing."""
    base_columns = [
        ColumnSchema(name="id", dtype="bigint", nullable=False),
        ColumnSchema(name="created_at", dtype="timestamp", nullable=False),
        ColumnSchema(name="updated_at", dtype="timestamp", nullable=True),
        ColumnSchema(name="amount", dtype="decimal(18,4)", nullable=False),
        ColumnSchema(name="currency", dtype="varchar(3)", nullable=False),
        ColumnSchema(name="status", dtype="varchar(20)", nullable=False),
    ]
    if version == 2:
        base_columns.append(ColumnSchema(name="region", dtype="varchar(10)", nullable=True))
    if version == 3:
        base_columns = [c for c in base_columns if c.name != "status"]
        base_columns.append(ColumnSchema(name="state", dtype="varchar(20)", nullable=False))

    from datetime import datetime, timezone
    return PipelineSchema(
        pipeline=pipeline,
        source=source,
        columns=base_columns,
        version=version,
        captured_at=datetime.now(timezone.utc).isoformat(),
    )