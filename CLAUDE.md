# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Database Configuration

The project uses PostgreSQL with configuration managed through the `DatabaseConfig` class in `config/database.py`:

- Database connection parameters are loaded from `.env.local` file (not tracked in git)
- Environment variables: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- Defaults to localhost:5433 with postgres/postgres credentials if no env file exists

## Running the Application  

```bash
# Main data extraction script
python3 scripts/main.py

# Alternative execution from project root
python3 -m scripts.main
```
### Download Dependencies
```bash
pip3 install -r requirements.txt
```

## Project Architecture

This is a health data extraction and processing pipeline built with **dlt (data load tool)** for modern data pipeline architecture.

### Data Pipeline Flow
```
PostgreSQL (source) → dlt (raw) → dbt (staging) → dbt (analytics)
                        ↓              ↓               ↓
                   DuckDB.raw   DuckDB.staging   DuckDB.analytics
```

### Core Components

- **`config/database.py`**: Database configuration management with environment variable support
- **`dlt_extraction/health_data_pipeline.py`**: Main dlt-based pipeline orchestration
- **`dlt_extraction/pipeline_config.py`**: Configurable pipeline settings and extraction parameters
- **`dlt_extraction/postgres_source.py`**: PostgreSQL data sources for dlt with incremental loading
- **`scripts/main.py`**: Main execution script demonstrating the pipeline workflow

### Health Data Schema

The pipeline extracts from these health tracking tables:
- `users` - User profiles and demographics
- `medical_checkups` - Medical examination records  
- `blood_pressure_readings` - Blood pressure measurements
- `blood_sugar_readings` - Glucose level readings
- `cholesterol_readings` - Cholesterol test results
- `sodium_readings` - Sodium level measurements
- `medications` - Medication tracking
- `health_goals` - User health objectives

### Data Flow

1. **Raw Layer**: Database configuration loaded from environment or defaults
2. **Raw Layer**: `HealthDataPipeline` initializes dlt pipeline with DuckDB destination
3. **Raw Layer**: `PipelineConfig` defines extraction parameters and table configurations
4. **Raw Layer**: PostgreSQL sources extract data with incremental loading support to `DuckDB.raw`
5. **Staging Layer**: dbt transforms raw data into clean, typed tables in `DuckDB.staging`
6. **Analytics Layer**: dbt creates business logic models and aggregations in `DuckDB.analytics`
7. Pipeline supports multiple extraction modes: basic (users only), full (all tables), or file export

### Key Classes

- **`DatabaseConfig`**: Handles connection parameters and environment loading with dlt compatibility
- **`HealthDataPipeline`**: Main pipeline orchestration with configurable destinations
- **`PipelineConfig`**: Pipeline configuration including extraction parameters and output settings
- **`PostgresSource`**: dlt-compatible data sources with incremental loading capabilities

## dbt Staging Layer

The project includes a comprehensive dbt staging layer in `dbt_health_data/` that transforms raw dlt data into clean, typed staging tables.

### dbt Commands

```bash
# Navigate to dbt project directory
cd dbt_health_data

# Install dbt packages
dbt deps

# Run all staging models
dbt run

# Run tests
dbt test

# Run specific model
dbt run --models stg_customers

# Test specific model
dbt test --models stg_customers

# Generate and serve documentation
dbt docs generate
dbt docs serve

# Debug connection
dbt debug
```

### Staging Models

- **`stg_customers`**: Customer demographics with age calculations and categorization
- **`stg_medical_checkups`**: Medical records with BMI calculations and health risk assessment
- **`stg_blood_pressure_readings`**: BP measurements with AHA guideline categorization
- **`stg_blood_sugar_readings`**: Glucose readings with diabetes risk indicators
- **`stg_cholesterol_readings`**: Lipid panels with cardiovascular risk assessment
- **`stg_sodium_readings`**: Sodium levels with clinical significance indicators

Each model includes:
- Data cleaning and type casting
- Clinical categorization based on medical guidelines
- Risk level assessments
- Data quality flags
- Comprehensive testing

### MCP Integration

The project includes MCP (Model Context Protocol) configuration:
- **Filesystem server**: File operations and directory access
- **PostgreSQL server**: Database queries via `postgresql://localhost:5433/data_playground`
- **Claude Code permissions**: Git operations, bash commands, and database access