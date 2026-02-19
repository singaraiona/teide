#!/usr/bin/env python3

#   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
#   All rights reserved.
#
#   Permission is hereby granted, free of charge, to any person obtaining a copy
#   of this software and associated documentation files (the "Software"), to deal
#   in the Software without restriction, including without limitation the rights
#   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#   copies of the Software, and to permit persons to whom the Software is
#   furnished to do so, subject to the following conditions:
#
#   The above copyright notice and this permission notice shall be included in all
#   copies or substantial portions of the Software.
#
#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#   SOFTWARE.

"""Baseline window function benchmark for comparison with Teide on 10M dataset.

Uses CREATE OR REPLACE TABLE to materialize results,
avoiding Python fetchall() overhead for fair comparison.
"""

import duckdb
import time
import os

CSV_PATH = os.path.join(os.path.dirname(__file__),
                        "..", "rayforce-bench", "datasets",
                        "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv")

N_ITER = 5

QUERIES = {
    "w1": "SELECT id1, v1, ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY v1) as rn FROM df",
    "w2": "SELECT id1, id4, RANK() OVER (PARTITION BY id1 ORDER BY id4) as rnk FROM df",
    "w3": "SELECT id3, v1, SUM(v1) OVER (PARTITION BY id3 ORDER BY v1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum FROM df",
    "w4": "SELECT id1, v1, LAG(v1, 1) OVER (PARTITION BY id1 ORDER BY v1) as lag_v1 FROM df",
    "w5": "SELECT id1, v1, AVG(v1) OVER (PARTITION BY id1) as avg_v1 FROM df",
    "w6": "SELECT id1, id2, v1, ROW_NUMBER() OVER (PARTITION BY id1, id2 ORDER BY v1) as rn FROM df",
}


def run_query(con, label, sql):
    # Warmup
    for _ in range(2):
        con.execute(f"CREATE OR REPLACE TABLE _result AS {sql}")

    times = []
    for _ in range(N_ITER):
        t0 = time.perf_counter()
        con.execute(f"CREATE OR REPLACE TABLE _result AS {sql}")
        times.append(time.perf_counter() - t0)

    elapsed = sorted(times)[len(times) // 2]
    nrows = con.execute("SELECT COUNT(*) FROM _result").fetchone()[0]
    ncols = len(con.execute("SELECT * FROM _result LIMIT 1").description)
    print(f"  {label:12s}  {elapsed*1000:8.1f} ms   {nrows:>10,} rows x {ncols} cols")
    con.execute("DROP TABLE IF EXISTS _result")


def main():
    csv_path = os.path.abspath(CSV_PATH)
    con = duckdb.connect()

    print(f"Loading {csv_path} ...")
    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE df AS SELECT * FROM read_csv_auto('{csv_path}')")
    load_time = time.perf_counter() - t0

    row_count = con.execute("SELECT COUNT(*) FROM df").fetchone()[0]
    print(f"Loaded: {row_count:,} rows in {load_time*1000:.0f} ms\n")

    print("Baseline window benchmarks (multi-threaded):")
    nthreads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    print(f"  Threads: {nthreads}")
    print(f"  {'Query':12s}  {'Time':>8s}       Result")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    for label, sql in QUERIES.items():
        run_query(con, label, sql)

    print("\nDone.")
    con.close()


if __name__ == "__main__":
    main()
