**Databricks Data Pipeline Project**
## Overview
This project demonstrates a hands-on Databricks data pipeline built to apply Lakehouse concepts in practice.  
The goal is to move beyond theory and understand how ingestion, transformations, schema handling, and overall pipeline behavior work in real-world scenarios.

The pipeline follows a structured **raw → processed → curated** data flow and is designed with a focus on clarity, maintainability, and learning.

This project also emphasizes understanding real-world data pipeline challenges, including errors, schema changes, and troubleshooting — not just successful executions.

![Databricks Medallion Pipeline](databricks-medallion-pipeline-overview.png)

## Objectives
- Build an end-to-end data pipeline in Databricks
- Apply Lakehouse architecture principles
- Understand real-world data behavior and errors
- Document issues and learnings

## High-Level Architecture
Raw → Processed → Curated data layers implemented using Databricks notebooks.

Detailed architecture is documented in `/docs/architecture.md`.

## Repository Structure

/notebooks
  ├─ ingestion       # Raw data ingestion
  ├─ processing      # Transformations and cleaning
  ├─ curated         # Final curated tables
/docs
  ├─ architecture.md
  ├─ troubleshooting.md
README.md

## How to Run
1. Import notebooks into a Databricks workspace
2. Configure data source paths
3. Run ingestion notebooks
4. Run processing notebooks
5. Run curated notebooks

## Key Learnings
- Schema handling is critical in data pipelines
- Early validation prevents downstream failures
- Pipeline structure impacts performance and maintainability

## Documentation
- Architecture & design decisions: `/docs/architecture.md`
- Issues & troubleshooting: `/docs/troubleshooting.md`

## What’s Next
- Improve data validation
- Explore performance optimizations
- Expand pipeline complexity
Learning:
Early validation prevents downstream failures.
