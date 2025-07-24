#!/usr/bin/env python3

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dlt_extraction import run_full_extraction, export_to_files


def main():
    """
    Main extraction script using dlt pipeline.
    
    This replaces the old extraction logic with dlt-based extraction.
    Available options:
    - Full extraction (all health tracking tables)
    - Export to files
    """
    print("Health Data Extraction using dlt")
    print("=" * 40)
    
    # Option 2: Uncomment to run full extraction of all tables
    print("\n🔄 Running full extraction (all health tables)...")
    try:
        result = run_full_extraction()
        print("✅ Full extraction completed successfully")
    except Exception as e:
        print(f"❌ Full extraction failed: {e}")
    
    # Option 3: Uncomment to export data to files
    # print("\n🔄 Exporting data to files...")
    # try:
    #     result = export_to_files("./health_data_export")
    #     print("✅ Data export completed successfully")
    # except Exception as e:
    #     print(f"❌ Data export failed: {e}")
    
    print("\n🎉 Data extraction pipeline completed!")


if __name__ == "__main__":
    main()
