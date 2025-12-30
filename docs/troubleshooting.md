# **Challenges & Troubleshooting**
This document tracks problems encountered during development and how they were resolved.


## Issue 1 – Schema Mismatch During Ingestion

### Problem
Pipeline failed due to unexpected schema changes in source data.

### Cause
Source data structure was inconsistent between loads.

### Solution
Implemented schema validation and handling before transformations.

### Learning
Schema management is critical for stable data pipelines.


**Issue 2: Transformation Performance Issues**

Problem:
Transformations became slow as data volume increased.

Cause:
Inefficient transformations and lack of structure between layers.

Solution:
Refactored transformations and separated logic by pipeline layer.

Learning:
Pipeline structure directly impacts performance and maintainability.

**Issue 3: Data Quality Errors**

Problem:
Null values and malformed records caused incorrect outputs.

Cause:
Insufficient validation at ingestion stage.

Solution:
Added data quality checks and filtering logic.