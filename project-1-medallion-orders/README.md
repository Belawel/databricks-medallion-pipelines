Project 1 – Medallion Orders Pipeline (Databricks SQL)

  Overview
End-to-end Databricks SQL pipeline implementing the Medallion architecture:
Bronze → Silver → Gold.

  Architecture
- **Bronze**: Raw CSV ingestion from Unity Catalog volume
- **Silver**: Data quality validation, quarantine, and deduplication
- **Gold**: Analytics-ready aggregates

  Features
- Folder-based ingestion (no hard-coded filenames)
- Data quality rules with quarantine table
- Deduplication using window functions
- Revenue consistency checks
- Runnable pipeline via Databricks SQL job

  Technologies
- Databricks SQL
- Delta Lake
- Unity Catalog
