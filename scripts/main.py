#!/usr/bin/env python3

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database import DatabaseConfig
from extraction.postgres_extractor import PostgresExtractor
from extraction.table_configs import get_default_table_configs


def main():
    print("Starting data extraction...")

    db_config = DatabaseConfig.from_env()
    extractor = PostgresExtractor(db_config)

    try:
        extractor.connect()

        table_configs = get_default_table_configs()

        for table_name, config in table_configs.get_all_tables().items():
            print(f"\n--- Extracting data from {table_name} ---")

            try:
                query = config.get_query()
                print(f"Query: {query}")

                df = extractor.execute_query(query)

                if not df.empty:
                    print(f"\nFirst 5 rows of {table_name}:")
                    print(df.head().to_string(index=False))
                    print(f"\nDataset shape: {df.shape}")
                    print(f"Columns: {list(df.columns)}")
                else:
                    print(f"No data found in {table_name}")

            except Exception as e:
                print(f"Failed to extract from {table_name}: {e}")
                continue

        print("\n--- Getting table information ---")
        for table_name in table_configs.get_all_tables().keys():
            try:
                table_info = extractor.get_table_info(table_name)
                if table_info:
                    print(f"\nTable: {table_name}")
                    for col in table_info:
                        print(
                            f"  - {col['column_name']}: {col['data_type']} ({'nullable' if col['is_nullable'] == 'YES' else 'not null'})")
                else:
                    print(f"Table {table_name} not found or no access")
            except Exception as e:
                print(f"Could not get info for {table_name}: {e}")

    except Exception as e:
        print(f"Database connection failed: {e}")
        print("Make sure your database is running and connection parameters are correct")

    finally:
        extractor.disconnect()
        print("\nData extraction completed!")


if __name__ == "__main__":
    main()
