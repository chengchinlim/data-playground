## Data Pipeline Architecture

```
PostgreSQL (source) → dlt (raw) → dbt (staging) → dbt (analytics)
                        ↓              ↓               ↓
                   DuckDB.raw   DuckDB.staging   DuckDB.analytics
```

### Pipeline Layers

- **Raw Layer**: dlt extracts data from PostgreSQL and loads into DuckDB.raw with minimal transformation
- **Staging Layer**: dbt transforms raw data into clean, typed, and standardized tables in DuckDB.staging  
- **Analytics Layer**: dbt creates business logic models, aggregations, and analytics-ready datasets in DuckDB.analytics

### Orchestration
- Dagster for pipeline orchestration and scheduling
