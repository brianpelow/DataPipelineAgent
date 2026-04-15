"""Configuration for DataPipelineAgent."""

from __future__ import annotations

import os
from pydantic import BaseModel, Field


class AgentConfig(BaseModel):
    """Runtime configuration for DataPipelineAgent."""

    anthropic_api_key: str = Field("", description="Anthropic API key")
    industry: str = Field("fintech", description="Industry context")
    model: str = Field("claude-sonnet-4-20250514", description="Claude model")
    null_rate_threshold: float = Field(0.05, description="Max acceptable null rate")
    row_count_variance: float = Field(0.2, description="Max acceptable row count variance")
    schema_strict: bool = Field(True, description="Fail on any schema change")

    @classmethod
    def from_env(cls) -> "AgentConfig":
        return cls(
            anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
            industry=os.environ.get("DP_INDUSTRY", "fintech"),
        )

    @property
    def has_api_key(self) -> bool:
        return bool(self.anthropic_api_key)