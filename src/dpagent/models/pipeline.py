"""Data pipeline models."""

from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Optional


class ColumnSchema(BaseModel):
    """Schema definition for a single column."""

    name: str
    dtype: str
    nullable: bool = True
    description: str = ""


class PipelineSchema(BaseModel):
    """Schema definition for a pipeline source or sink."""

    pipeline: str
    source: str
    columns: list[ColumnSchema] = Field(default_factory=list)
    version: int = 1
    captured_at: str = ""

    @property
    def column_names(self) -> set[str]:
        return {c.name for c in self.columns}


class SchemaDrift(BaseModel):
    """Detected schema drift between two versions."""

    pipeline: str
    added_columns: list[str] = Field(default_factory=list)
    removed_columns: list[str] = Field(default_factory=list)
    type_changes: list[str] = Field(default_factory=list)
    nullable_changes: list[str] = Field(default_factory=list)
    has_drift: bool = False
    severity: str = "none"


class QualityMetrics(BaseModel):
    """Data quality metrics for a pipeline run."""

    pipeline: str
    run_id: str = ""
    row_count: int = 0
    expected_row_count: int = 0
    null_rates: dict[str, float] = Field(default_factory=dict)
    duplicate_count: int = 0
    quality_score: int = Field(100, description="0-100 quality score")
    issues: list[str] = Field(default_factory=list)


class HealAction(BaseModel):
    """A remediation action taken by the heal agent."""

    action_type: str
    description: str
    pipeline: str
    success: bool = False
    details: str = ""


class PipelineStatus(BaseModel):
    """Current status of a monitored pipeline."""

    pipeline: str
    source: str
    status: str = Field("healthy", description="healthy/degraded/failed/healing")
    last_run: Optional[str] = None
    schema_drift: Optional[SchemaDrift] = None
    quality: Optional[QualityMetrics] = None
    heal_actions: list[HealAction] = Field(default_factory=list)
    notifications_sent: list[str] = Field(default_factory=list)
    compliance_notes: str = ""