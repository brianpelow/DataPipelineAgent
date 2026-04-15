# DataPipelineAgent

> Agentic ETL orchestrator — monitors data sources, detects schema drift, and auto-heals pipelines.

![CI](https://github.com/brianpelow/DataPipelineAgent/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.12+-green.svg)

## Overview

DataPipelineAgent is an agentic ETL orchestration system that continuously
monitors data pipelines, detects schema drift and quality degradation, and
autonomously triggers remediation workflows. It notifies stakeholders and
maintains an audit trail of all pipeline events.

Built for data engineering teams in regulated financial services and
manufacturing where data lineage, schema governance, and pipeline SLAs
are compliance requirements.

## Agents

- SchemaAgent - detects schema drift and column-level changes
- QualityAgent - monitors row counts, null rates, and value distributions
- HealAgent - triggers auto-remediation and rollback workflows
- NotifyAgent - routes alerts to the right stakeholders
- OrchestratorAgent - coordinates the full pipeline monitoring loop

## Quick start

```bash
pip install DataPipelineAgent
export ANTHROPIC_API_KEY=your_key
dp-agent monitor --pipeline payments-etl --source payments-db
dp-agent check-schema --pipeline payments-etl
dp-agent heal --pipeline payments-etl --issue schema-drift
```

## Configuration

| Variable | Description | Required |
|----------|-------------|----------|
| ANTHROPIC_API_KEY | Claude API key | No |
| DP_INDUSTRY | Industry context | No |

## License

Apache 2.0