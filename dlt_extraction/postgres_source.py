"""PostgreSQL data source for dlt extraction - Fixed Version."""

from typing import Dict, Optional, List

import dlt
from dlt.sources import DltResource
from dlt.sources.sql_database import sql_table

from config.database import DatabaseConfig


@dlt.source
def postgres_health_data(
        database_config: DatabaseConfig,
        table_names: Optional[List[str]] = None,
        table_limits: Optional[Dict[str, int]] = None,
        table_filters: Optional[Dict[str, str]] = None,
        table_ordering: Optional[Dict[str, str]] = None
) -> List[DltResource]:
    """
    PostgreSQL source for health tracking data using dlt's sql_database source.

    Args:
        database_config: Database configuration object
        table_names: Optional list of specific tables to extract
        table_limits: Optional dictionary of row limits per table
        table_filters: Optional dictionary of WHERE clauses per table
        table_ordering: Optional dictionary of ORDER BY clauses per table

    Returns:
        List of dlt resources for each table
    """
    connection_string = database_config.connection_string()

    # Define all available tables with their configurations
    all_tables = {
        'customers': {
            'primary_key': 'customer_id',
            'incremental_key': 'updated_at'
        },
        'medical_checkups': {
            'primary_key': 'checkup_id',
            'incremental_key': 'checkup_date'
        },
        'blood_pressure_readings': {
            'primary_key': 'reading_id',
            'incremental_key': 'created_at'
        },
        'blood_sugar_readings': {
            'primary_key': 'reading_id',
            'incremental_key': 'created_at'
        },
        'cholesterol_readings': {
            'primary_key': 'reading_id',
            'incremental_key': 'created_at'
        },
        'sodium_readings': {
            'primary_key': 'reading_id',
            'incremental_key': 'created_at'
        }
    }

    # Filter tables if specific ones are requested
    tables_to_extract = table_names if table_names else list(all_tables.keys())

    print(f"Tables to extract: {tables_to_extract}")

    resources = []

    for table_name in tables_to_extract:
        if table_name not in all_tables:
            print(f"Warning: Table '{table_name}' not found in available tables. Skipping.")
            continue

        try:
            print(f"Creating resource for table: {table_name}")

            table_config = all_tables[table_name]

            # Build the SQL query for this table
            query = f"SELECT * FROM public.{table_name}"

            # Add WHERE clause if provided
            if table_filters and table_name in table_filters:
                query += f" WHERE {table_filters[table_name]}"

            # Add ORDER BY if provided
            if table_ordering and table_name in table_ordering:
                query += f" ORDER BY {table_ordering[table_name]}"
            elif table_config.get('incremental_key'):
                query += f" ORDER BY {table_config['incremental_key']}"

            # Add LIMIT if provided
            if table_limits and table_name in table_limits:
                query += f" LIMIT {table_limits[table_name]}"

            print(f"Query for {table_name}: {query}")

            # Create the resource using dlt's sql_table
            resource = sql_table(
                credentials=connection_string,
                table=table_name,
                schema="public",
                primary_key=table_config['primary_key'],
                write_disposition="merge"
            )

            resources.append(resource)
            print(f"✅ Successfully created resource for {table_name}")

        except Exception as e:
            print(f"❌ Error creating resource for table '{table_name}': {e}")
            # Continue with other tables instead of failing completely
            continue

    print(f"Total resources created: {len(resources)}")
    return resources


