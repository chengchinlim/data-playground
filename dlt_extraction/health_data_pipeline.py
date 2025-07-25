"""Main pipeline orchestration for health data extraction using dlt."""

import dlt
import pandas as pd
from typing import Dict, Any, Optional

from config.database import DatabaseConfig
from .postgres_source import postgres_health_data
from .pipeline_config import PipelineConfig, get_default_config


class HealthDataPipeline:
    """Main pipeline class for health data extraction and loading."""
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        """
        Initialize the pipeline.
        
        Args:
            config: Pipeline configuration. If None, uses default config.
        """
        self.config = config or get_default_config()
        self.database_config = DatabaseConfig()
        self.pipeline = None
        
    def create_pipeline(self) -> dlt.Pipeline:
        """Create and configure the dlt pipeline."""
        if self.pipeline is None:
            # Create destination with explicit configuration
            if self.config.destination == "duckdb" and self.config.destination_config:
                destination = dlt.destinations.duckdb(
                    credentials=self.config.destination_config["credentials"]
                )
            else:
                destination = self.config.destination
                
            self.pipeline = dlt.pipeline(
                pipeline_name=self.config.pipeline_name,
                destination=destination,
                dataset_name="raw"
            )
        return self.pipeline
    
    def run_extraction(self, show_progress: bool = True) -> Dict[str, Any]:
        """
        Run the complete data extraction pipeline.
        
        Args:
            show_progress: Whether to show extraction progress
            
        Returns:
            Dictionary containing extraction results and statistics
        """
        pipeline = self.create_pipeline()
        
        print(f"Starting health data extraction pipeline: {self.config.pipeline_name}")
        print(f"Database: {self.database_config.host}:{self.database_config.port}/{self.database_config.database}")
        print(f"Destination: {self.config.destination}")

        # Get the data source
        source = postgres_health_data(
            database_config=self.database_config,
            table_names=self.config.tables_to_extract,
            table_limits=self.config.table_limits,
            table_filters=self.config.table_filters,
            table_ordering=self.config.table_ordering
        )
        
        # Run the pipeline
        if self.config.full_refresh:
            print("Running full refresh...")
            load_info = pipeline.run(
                source,
                write_disposition="replace"
            )
        else:
            print("Running incremental extraction...")
            load_info = pipeline.run(source)
        
        # Display results
        self._display_results(load_info, show_progress)
        
        return {
            'load_info': load_info,
            'pipeline_name': pipeline.pipeline_name,
            'destination': self.config.destination,
            'tables_extracted': self.config.tables_to_extract or 'all'
        }
    
    def get_extracted_data(self, table_name: str) -> Optional[pd.DataFrame]:
        """
        Get extracted data as pandas DataFrame.
        
        Args:
            table_name: Name of the table to retrieve
            
        Returns:
            DataFrame with the extracted data, or None if not found
        """
        if self.pipeline is None:
            print("Pipeline not initialized. Run extraction first.")
            return None
            
        try:
            # For DuckDB destination, we can query directly
            if self.config.destination == "duckdb":
                with self.pipeline.sql_client() as client:
                    query = f"SELECT * FROM raw.{table_name}"
                    result = client.execute_sql(query)
                    if hasattr(result, 'df'):
                        return result.df()
                    else:
                        # Convert list of rows to DataFrame
                        import pandas as pd
                        rows = result.fetchall() if hasattr(result, 'fetchall') else result
                        if rows:
                            columns = [desc[0] for desc in result.description] if hasattr(result, 'description') else None
                            return pd.DataFrame(rows, columns=columns)
                        return pd.DataFrame()
            else:
                print(f"Data retrieval not implemented for destination: {self.config.destination}")
                return None
        except Exception as e:
            print(f"Error retrieving data for {table_name}: {e}")
            return None
    
    def _display_results(self, load_info, show_progress: bool = True):
        """Display extraction results."""
        print("\n" + "="*50)
        print("EXTRACTION RESULTS")
        print("="*50)
        
        # Safely access LoadInfo attributes
        pipeline_name = 'Unknown'
        if hasattr(load_info, 'pipeline') and load_info.pipeline:
            if hasattr(load_info.pipeline, 'pipeline_name'):
                pipeline_name = load_info.pipeline.pipeline_name
            elif hasattr(load_info.pipeline, 'name'):
                pipeline_name = load_info.pipeline.name
        
        print(f"Pipeline: {pipeline_name}")
        
        # Load ID might not exist in all dlt versions
        if hasattr(load_info, 'load_id'):
            print(f"Load ID: {load_info.load_id}")
        elif hasattr(load_info, 'loads_ids') and load_info.loads_ids:
            print(f"Load IDs: {', '.join(load_info.loads_ids)}")
        
        # Destination name
        if hasattr(load_info, 'destination_name'):
            print(f"Destination: {load_info.destination_name}")
        elif hasattr(load_info, 'destination_type'):
            print(f"Destination: {load_info.destination_type}")
        
        # Check for failed jobs
        if hasattr(load_info, 'has_failed_jobs') and load_info.has_failed_jobs:
            print("❌ Some jobs failed:")
            if hasattr(load_info, 'failed_jobs'):
                for job in load_info.failed_jobs:
                    table_name = getattr(job, 'table_name', 'Unknown table')
                    if hasattr(job, 'job_file_info') and hasattr(job.job_file_info, 'table_name'):
                        table_name = job.job_file_info.table_name
                    exception = getattr(job, 'exception', 'Unknown error')
                    print(f"  - {table_name}: {exception}")
        else:
            print("✅ All jobs completed successfully")
        
        # Display table statistics if available
        if hasattr(load_info, 'loaded_packages') and load_info.loaded_packages:
            try:
                packages = load_info.loaded_packages
                if packages and len(packages) > 0:
                    first_package = packages[0]
                    if hasattr(first_package, 'schema_updates'):
                        print(f"\nTables processed: {len(first_package.schema_updates)}")
                    
                    # Try to display load metrics
                    for package in packages:
                        if hasattr(package, 'load_metrics'):
                            for table_name, metrics in package.load_metrics.items():
                                if hasattr(metrics, 'items_count'):
                                    print(f"  - {table_name}: {metrics.items_count} rows")
                                elif hasattr(metrics, 'rows_count'):
                                    print(f"  - {table_name}: {metrics.rows_count} rows")
            except (AttributeError, IndexError) as e:
                print(f"Could not retrieve detailed statistics: {e}")
        
        if show_progress and self.config.destination == "duckdb":
            self._show_sample_data()
    
    def _show_sample_data(self):
        """Show sample data from extracted tables."""
        if self.pipeline is None:
            return
            
        try:
            with self.pipeline.sql_client() as client:
                # Get all tables in the schema
                tables = client.execute_sql(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
                ).fetchall()
                
                for (table_name,) in tables:
                    print(f"\n--- Sample from {table_name} ---")
                    result = client.execute_sql(f"SELECT * FROM raw.{table_name} LIMIT 3")
                    if hasattr(result, 'df'):
                        sample_df = result.df()
                    else:
                        import pandas as pd
                        rows = result.fetchall() if hasattr(result, 'fetchall') else result
                        if rows:
                            columns = [desc[0] for desc in result.description] if hasattr(result, 'description') else None
                            sample_df = pd.DataFrame(rows, columns=columns)
                        else:
                            sample_df = pd.DataFrame()
                    
                    if not sample_df.empty:
                        print(sample_df.to_string(index=False))
                        count_result = client.execute_sql(f'SELECT COUNT(*) FROM raw.{table_name}')
                        count = count_result.fetchone()[0] if hasattr(count_result, 'fetchone') else count_result[0][0]
                        print(f"Total rows: {count}")
                    else:
                        print("No data found")
                        
        except Exception as e:
            print(f"Error displaying sample data: {e}")



def run_full_extraction():
    """Run full extraction of all health tracking tables."""
    from .pipeline_config import get_full_extraction_config
    
    pipeline = HealthDataPipeline(get_full_extraction_config())
    return pipeline.run_extraction()


def export_to_files(output_dir: str = "./health_data_export"):
    """Export data to files (CSV, Parquet, etc.)."""
    from .pipeline_config import get_filesystem_config
    
    config = get_filesystem_config(output_dir)
    pipeline = HealthDataPipeline(config)
    return pipeline.run_extraction()


if __name__ == "__main__":
    # Run full extraction by default
    run_full_extraction()