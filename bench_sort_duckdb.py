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

"""DuckDB sort benchmark for comparison with Teide on H2OAI 10M dataset.

Uses CREATE OR REPLACE TABLE to materialize results inside DuckDB,
avoiding Python fetchall() overhead for fair comparison with Teide's
in-C materialization.
"""

import duckdb
import time
import os

CSV_PATH = os.path.join(os.path.dirname(__file__),
                        "..", "rayforce-bench", "datasets",
                        "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv")

N_ITER = 7

QUERIES = {
    "s1": "SELECT * FROM df ORDER BY id1",
    "s2": "SELECT * FROM df ORDER BY id3",
    "s3": "SELECT * FROM df ORDER BY id4",
    "s4": "SELECT * FROM df ORDER BY v3 DESC",
    "s5": "SELECT * FROM df ORDER BY id1, id2",
    "s6": "SELECT * FROM df ORDER BY id1, id2, id3",
}


def run_query(con, label, sql):
    # Warmup
    for _ in range(3):
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

    con.execute("SET threads TO 1")

    print(f"Loading {csv_path} ...")
    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE df AS SELECT * FROM read_csv_auto('{csv_path}')")
    load_time = time.perf_counter() - t0

    row_count = con.execute("SELECT COUNT(*) FROM df").fetchone()[0]
    print(f"Loaded: {row_count:,} rows in {load_time*1000:.0f} ms\n")

    print("DuckDB sort benchmarks (single-threaded):")
    print(f"  {'Query':12s}  {'Time':>8s}       Result")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    for label, sql in QUERIES.items():
        run_query(con, label, sql)

    con.execute("RESET threads")
    nthreads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    print(f"\nDuckDB sort benchmarks (multi-threaded, {nthreads} threads):")
    print(f"  {'Query':12s}  {'Time':>8s}       Result")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    for label, sql in QUERIES.items():
        run_query(con, label, sql)

    print("\nDone.")
    con.close()


if __name__ == "__main__":
    main()
