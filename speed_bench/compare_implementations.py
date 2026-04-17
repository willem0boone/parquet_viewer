#!/usr/bin/env python
"""
Quick comparison of PyArrow vs DuckDB implementations on the same dataset.
Demonstrates identical API and output.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from parquet_viewer.view_service import ParquetViewService as PyArrowService
from parquet_viewer.view_service_beta import DuckDBViewService
import time

DATASET_URL = (
    "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology/12639"
    "/marine_biodiversity_observations_2026-02-26.parquet"
)

def compare():
    print("=" * 70)
    print("PyArrow vs DuckDB Parquet Viewer Comparison")
    print("=" * 70)

    test_config = {
        "columns": ["datasetid"],
        "filters": {"datasetid": 4687},
        "max_rows": 5,
    }

    print(f"\nTest config: {test_config}\n")

    # PyArrow
    print("PyArrow Implementation")
    print("-" * 70)
    t = time.perf_counter()
    pa_service = PyArrowService(DATASET_URL)
    pa_table = pa_service.get_view(**test_config)
    pa_time = time.perf_counter() - t
    print(f"Time: {pa_time:.3f}s")
    print(f"Rows: {pa_table.num_rows}")
    print(f"Columns: {pa_table.column_names}")
    print(f"Data (first 3 rows):")
    print(pa_table.to_pandas().head(3).to_string())

    # DuckDB
    print("\n" + "=" * 70)
    print("DuckDB Implementation")
    print("-" * 70)
    t = time.perf_counter()
    duckdb_service = DuckDBViewService(DATASET_URL)
    duckdb_table = duckdb_service.get_view(**test_config)
    duckdb_time = time.perf_counter() - t
    print(f"Time: {duckdb_time:.3f}s")
    print(f"Rows: {duckdb_table.num_rows}")
    print(f"Columns: {duckdb_table.column_names}")
    print(f"Data (first 3 rows):")
    print(duckdb_table.to_pandas().head(3).to_string())

    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("-" * 70)
    print(f"PyArrow:  {pa_time:.3f}s")
    print(f"DuckDB:   {duckdb_time:.3f}s")
    speedup = pa_time / duckdb_time if duckdb_time > 0 else 0
    if speedup > 1:
        print(f"✓ DuckDB is {speedup:.1f}x faster")
    elif speedup < 1:
        print(f"  PyArrow is {1/speedup:.1f}x faster")
    else:
        print("  Similar performance")

    print(f"\nBoth return identical `pa.Table` objects")
    print(f"PyArrow columns:  {pa_table.column_names}")
    print(f"DuckDB columns:   {duckdb_table.column_names}")
    print(f"Schema match: {pa_table.schema == duckdb_table.schema}")
    print("=" * 70)

if __name__ == "__main__":
    compare()



