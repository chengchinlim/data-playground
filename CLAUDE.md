# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Database Configuration

The project uses PostgreSQL with configuration managed through the `DatabaseConfig` class in `config/database.py`:

- Database connection parameters are loaded from `.env.local` file (not tracked in git)
- Environment variables: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- Defaults to localhost:5432 with postgres/postgres credentials if no env file exists

## Running the Application  

```bash
# Main data extraction script
python scripts/main.py

# Alternative execution from project root
python -m scripts.main
```

## Project Architecture

### Core Components

- **`config/database.py`**: Database configuration management with environment variable support
- **`extraction/postgres_extractor.py`**: PostgreSQL data extraction class with pandas integration
- **`extraction/table_configs.py`**: Configurable table extraction with query building (columns, WHERE clauses, ORDER BY, LIMIT)
- **`scripts/main.py`**: Main execution script that demonstrates the extraction workflow

### Data Flow

1. Database configuration loaded from environment or defaults
2. `PostgresExtractor` establishes connection using `psycopg2`
3. `TableConfig` objects define extraction parameters for each table
4. Data extracted as pandas DataFrames with automatic query generation
5. Table metadata retrieved via information_schema queries

### Key Classes

- **`DatabaseConfig`**: Handles connection parameters and environment loading
- **`PostgresExtractor`**: Core extraction engine with connection management
- **`TableConfig`**: Individual table extraction configuration 
- **`TableConfigs`**: Collection manager for multiple table configurations

The architecture supports flexible data extraction with configurable table-specific parameters while maintaining clean separation between database connection, extraction logic, and table configuration.