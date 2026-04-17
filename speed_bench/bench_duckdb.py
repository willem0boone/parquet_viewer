import sys
from pathlib import Path
import timeit

sys.path.insert(0, str(Path(__file__).parent.parent))
from parquet_viewer.view_service_beta import DuckDBViewService
DATASET_PATH = Path(__file__).parent / "data" / "marine_biodiversity_observations_2026-02-26.parquet"
def bench_init():
    print("run bench init")
    DuckDBViewService(DATASET_PATH)
def bench0():
    print("run bench0")
    service = DuckDBViewService(DATASET_PATH)
    service.get_view(max_rows=25)
def bench1():
    print("run bench1")
    service = DuckDBViewService(DATASET_PATH)
    service.get_view(filters={"datasetid": 4687}, max_rows=25)
def bench2():
    print("run bench2")
    service = DuckDBViewService(DATASET_PATH)
    service.get_view(filters={"datasetid": 4687}, max_rows=25, columns=["datasetid"])
if __name__ == "__main__":
    runs = 10
    tests = {
        "test_init": bench_init,
        "test0": bench0,
        "test1": bench1,
        "test2": bench2,
    }
    print(f"Running DuckDB benchmarks ({runs} runs each)...\n")
    for name, func in tests.items():
        total_seconds = timeit.timeit(func, number=runs)
        avg_ms = (total_seconds / runs) * 1000
        print(f"{name:>9} | total: {total_seconds:8.3f} s | avg: {avg_ms:8.2f} ms")
