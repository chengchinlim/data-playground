"""Pipeline configuration for dlt health data extraction."""

import os
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class PipelineConfig:
    """Configuration for the health data pipeline."""
    
    # Pipeline settings
    pipeline_name: str = "health_data_extraction"
    destination: str = "duckdb"  # Can be changed to postgres, filesystem, etc.
    destination_config: Optional[Dict[str, Any]] = None
    
    # Data extraction settings
    tables_to_extract: Optional[List[str]] = None  # None means extract all tables
    
    # Table-specific configurations
    table_limits: Dict[str, int] = None
    table_filters: Dict[str, str] = None  # WHERE clauses per table
    table_ordering: Dict[str, str] = None  # ORDER BY clauses per table
    
    # Incremental loading settings
    enable_incremental: bool = True
    full_refresh: bool = False
    
    # Output settings
    output_format: str = "parquet"  # parquet, jsonl, csv
    output_directory: str = None
    
    def __post_init__(self):
        """Initialize default configurations."""
        # Set default output directory to project root/data
        if self.output_directory is None:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.output_directory = os.path.join(project_root, "data")
            
        if self.table_limits is None:
            self.table_limits = {
                'users': 1000,
                'medical_checkups': 5000,
                'blood_pressure_readings': 10000,
                'blood_sugar_readings': 10000,
                'cholesterol_readings': 10000,
                'sodium_readings': 10000,
                'medications': 5000,
                'health_goals': 2000
            }
            
        if self.table_filters is None:
            self.table_filters = {
                # Example filters - can be customized
                'users': "created_at >= '2020-01-01'",
                'medical_checkups': "checkup_date >= '2020-01-01'",
            }
            
        if self.table_ordering is None:
            self.table_ordering = {
                'users': 'created_at DESC',
                'medical_checkups': 'checkup_date DESC',
                'blood_pressure_readings': 'created_at DESC',
                'blood_sugar_readings': 'created_at DESC',
                'cholesterol_readings': 'created_at DESC',
                'sodium_readings': 'created_at DESC',
                'medications': 'start_date DESC',
                'health_goals': 'created_at DESC'
            }
            
        if self.destination_config is None:
            if self.destination == "duckdb":
                self.destination_config = {
                    "credentials": f"{self.output_directory}/health_data.duckdb"
                }
            elif self.destination == "filesystem":
                self.destination_config = {
                    "bucket_url": self.output_directory,
                    "layout": "{table_name}/{load_id}.{file_format}"
                }


def get_default_config() -> PipelineConfig:
    """Get default pipeline configuration."""
    return PipelineConfig()


def get_users_only_config() -> PipelineConfig:
    """Get configuration for extracting only users table (matching original logic)."""
    return PipelineConfig(
        tables_to_extract=['users'],
        table_limits={'users': 1000},
        table_ordering={'users': 'created_at DESC'}
    )


def get_full_extraction_config() -> PipelineConfig:
    """Get configuration for extracting all health tracking tables."""
    return PipelineConfig(
        tables_to_extract=None,  # All tables
        enable_incremental=True
    )


def get_filesystem_config(output_dir: str = "./health_data_export") -> PipelineConfig:
    """Get configuration for exporting to filesystem."""
    return PipelineConfig(
        destination="filesystem",
        output_directory=output_dir,
        destination_config={
            "bucket_url": output_dir,
            "layout": "{table_name}/{load_id}.{file_format}"
        }
    )