# Architecture Overview
This document describes the detailed design of the Databricks data pipeline and the reasoning behind key decisions.

## Ingestion Layer (Raw)
- Source handling
- Minimal transformations
- Schema capture
- Traceability goals

## Processing Layer (Processed)
- Data cleaning logic
- Schema enforcement
- Transformation patterns

## Curated Layer
- Final table structure
- Analytics-ready format
- Naming conventions

## Design Decisions
- Why Lakehouse architecture
- Why layered notebooks
- Why separation of concerns

## Trade-offs & Considerations
- Simplicity vs flexibility
- Validation cost vs reliability
- Performance vs readability
