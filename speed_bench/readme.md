# Benchmark Comparison: PyArrow vs DuckDB

| Test    | PyArrow Total (s) | PyArrow Avg (ms) | DuckDB Total (s) | DuckDB Avg (ms) | Speedup (≈x) |
|---------|------------------|------------------|------------------|------------------|--------------|
| init    | 0.367            | 36.74            | 0.128            | 12.84            | 2.9x         |
| test0   | 35.329           | 3532.89          | 2.443            | 244.25           | 14.5x        |
| test1   | 43.175           | 4317.54          | 1.944            | 194.40           | 22.2x        |
| test2   | 3.937            | 393.68           | 0.970            | 96.98            | 4.1x         |

## Summary
- DuckDB outperforms PyArrow in all benchmarks.
- The most significant gains are in **test1** and **test0**.
- Even for initialization, DuckDB is ~3× faster.