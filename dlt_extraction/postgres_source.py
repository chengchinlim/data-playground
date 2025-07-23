"""PostgreSQL data source for dlt extraction."""

import dlt
from typing import Iterator, Dict, Any, Optional, List
from dlt.sources import DltResource
from config.database import DatabaseConfig


@dlt.source
def postgres_health_data(
    database_config: DatabaseConfig,
    table_names: Optional[List[str]] = None
) -> List[DltResource]:
    """
    PostgreSQL source for health tracking data.
    
    Args:
        database_config: Database configuration object
        table_names: Optional list of specific tables to extract
    
    Returns:
        List of dlt resources for each table
    """
    connection_string = database_config.connection_string()
    
    # Define all available tables with their incremental strategies
    all_tables = {
        'users': {
            'primary_key': 'id',
            'incremental_key': 'updated_at',
            'columns': ['id', 'first_name', 'last_name', 'email', 'date_of_birth', 
                       'gender', 'height_cm', 'weight_kg', 'created_at', 'updated_at']
        },
        'medical_checkups': {
            'primary_key': 'id', 
            'incremental_key': 'checkup_date',
            'columns': ['id', 'user_id', 'checkup_date', 'doctor_name', 'notes', 
                       'created_at', 'updated_at']
        },
        'blood_pressure_readings': {
            'primary_key': 'id',
            'incremental_key': 'created_at', 
            'columns': ['id', 'checkup_id', 'systolic', 'diastolic', 'pulse', 
                       'measurement_time', 'created_at', 'updated_at']
        },
        'blood_sugar_readings': {
            'primary_key': 'id',
            'incremental_key': 'created_at',
            'columns': ['id', 'checkup_id', 'glucose_level', 'measurement_type', 
                       'measurement_time', 'created_at', 'updated_at']
        },
        'cholesterol_readings': {
            'primary_key': 'id',
            'incremental_key': 'created_at',
            'columns': ['id', 'checkup_id', 'total_cholesterol', 'hdl_cholesterol', 
                       'ldl_cholesterol', 'triglycerides', 'measurement_time', 
                       'created_at', 'updated_at']
        },
        'sodium_readings': {
            'primary_key': 'id', 
            'incremental_key': 'created_at',
            'columns': ['id', 'checkup_id', 'sodium_level', 'measurement_time', 
                       'created_at', 'updated_at']
        },
        'medications': {
            'primary_key': 'id',
            'incremental_key': 'updated_at',
            'columns': ['id', 'user_id', 'medication_name', 'dosage', 'frequency', 
                       'start_date', 'end_date', 'created_at', 'updated_at']
        },
        'health_goals': {
            'primary_key': 'id',
            'incremental_key': 'updated_at', 
            'columns': ['id', 'user_id', 'goal_type', 'target_value', 'current_value',
                       'target_date', 'status', 'created_at', 'updated_at']
        }
    }
    
    # Filter tables if specific ones are requested
    tables_to_extract = table_names if table_names else list(all_tables.keys())
    
    resources = []
    for table_name in tables_to_extract:
        if table_name in all_tables:
            table_config = all_tables[table_name]
            resource = create_table_resource(
                connection_string=connection_string,
                table_name=table_name,
                **table_config
            )
            resources.append(resource)
    
    return resources


def create_table_resource(
    connection_string: str,
    table_name: str,
    primary_key: str,
    incremental_key: str,
    columns: List[str],
    limit: Optional[int] = None,
    where_clause: Optional[str] = None,
    order_by: Optional[str] = None
) -> DltResource:
    """
    Create a dlt resource for a specific table.
    
    Args:
        connection_string: PostgreSQL connection string
        table_name: Name of the table to extract
        primary_key: Primary key column name
        incremental_key: Column for incremental loading
        columns: List of columns to select
        limit: Optional row limit
        where_clause: Optional WHERE clause
        order_by: Optional ORDER BY clause
    
    Returns:
        dlt resource for the table
    """
    
    @dlt.resource(
        name=table_name,
        primary_key=primary_key,
        write_disposition="merge"
    )
    def table_resource() -> Iterator[Dict[str, Any]]:
        """Extract data from PostgreSQL table."""
        
        # Build SQL query
        columns_str = ", ".join(columns)
        query = f"SELECT {columns_str} FROM public.{table_name}"
        
        # Add WHERE clause if provided
        if where_clause:
            query += f" WHERE {where_clause}"
            
        # Add ORDER BY if provided, otherwise order by incremental key
        if order_by:
            query += f" ORDER BY {order_by}"
        elif incremental_key:
            query += f" ORDER BY {incremental_key}"
            
        # Add LIMIT if provided
        if limit:
            query += f" LIMIT {limit}"
        
        # Use dlt's sql_database helper for incremental loading
        from dlt.sources.sql_database import sql_table
        
        yield from sql_table(
            credentials=connection_string,
            table=table_name,
            schema="public",
            incremental=dlt.sources.incremental(incremental_key) if incremental_key else None
        )
    
    return table_resource


@dlt.resource
def users_filtered(
    database_config: DatabaseConfig,
) -> Iterator[Dict[str, Any]]:
    """
    Filtered users resource matching the original extraction logic.
    
    Args:
        database_config: Database configuration
        limit: Maximum number of rows to return
        order_by: ORDER BY clause
    
    Yields:
        User records as dictionaries
    """
    connection_string = database_config.connection_string()
    
    from dlt.sources.sql_database import sql_table
    
    # Create incremental loading on updated_at
    yield from sql_table(
        credentials=connection_string,
        table="users",
        schema="public",
        incremental=dlt.sources.incremental("updated_at")
    )