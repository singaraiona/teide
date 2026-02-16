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

"""DuckDB benchmark for comparison with Teide on H2OAI 10M groupby dataset."""

import duckdb
import time
import os

CSV_PATH = os.path.join(os.path.dirname(__file__),
                        "..", "rayforce-bench", "datasets",
                        "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv")

N_ITER = 7  # median of 7 runs

GROUPBY_QUERIES = {
    "q1": "SELECT id1, SUM(v1) AS v1 FROM df GROUP BY id1",
    "q2": "SELECT id1, id2, SUM(v1) AS v1 FROM df GROUP BY id1, id2",
    "q3": "SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM df GROUP BY id3",
    "q4": "SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM df GROUP BY id4",
    "q5": "SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM df GROUP BY id6",
    "q6": "SELECT id3, MAX(v1) AS v1, MIN(v2) AS v2 FROM df GROUP BY id3",
    "q7": "SELECT id1, id2, id3, id4, id5, id6, SUM(v3) AS v3, COUNT(*) AS cnt FROM df GROUP BY id1, id2, id3, id4, id5, id6",
}

SORT_QUERIES = {
    "s1": "SELECT * FROM df ORDER BY id1",
    "s2": "SELECT * FROM df ORDER BY id3",
    "s3": "SELECT * FROM df ORDER BY id4",
    "s4": "SELECT * FROM df ORDER BY v3 DESC",
    "s5": "SELECT * FROM df ORDER BY id1, id2",
    "s6": "SELECT * FROM df ORDER BY id1, id2, id3",
}

WINDOW_QUERIES = {
    "w1": "SELECT id1, v1, ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY v1) AS rn FROM df",
    "w2": "SELECT id1, id4, RANK() OVER (PARTITION BY id1 ORDER BY id4) AS rnk FROM df",
    "w3": "SELECT id3, v1, SUM(v1) OVER (PARTITION BY id3 ORDER BY v1) AS csum FROM df",
    "w4": "SELECT id1, v1, LAG(v1, 1) OVER (PARTITION BY id1 ORDER BY v1) AS lag_v1 FROM df",
    "w5": "SELECT id1, v1, AVG(v1) OVER (PARTITION BY id1) AS avg_v1 FROM df",
    "w6": "SELECT id1, id2, v1, ROW_NUMBER() OVER (PARTITION BY id1, id2 ORDER BY v1) AS rn FROM df",
}


def run_query(con, label, sql):
    """Run a query using CREATE OR REPLACE TABLE for fair in-engine timing."""
    for _ in range(3):
        con.execute(f"CREATE OR REPLACE TABLE _result AS {sql}")

    times = []
    for _ in range(N_ITER):
        t0 = time.perf_counter()
        con.execute(f"CREATE OR REPLACE TABLE _result AS {sql}")
        times.append(time.perf_counter() - t0)

    elapsed = sorted(times)[len(times) // 2]  # median
    nrows = con.execute("SELECT COUNT(*) FROM _result").fetchone()[0]
    ncols = len(con.execute("SELECT * FROM _result LIMIT 1").description)
    print(f"  {label:12s}  {elapsed*1000:8.1f} ms   {nrows:>10,} rows x {ncols} cols")
    con.execute("DROP TABLE IF EXISTS _result")


def run_section(con, title, queries):
    print(f"\n{title}:")
    print(f"  {'Query':12s}  {'Time':>8s}       Result")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")
    for label, sql in queries.items():
        run_query(con, label, sql)


def main():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                        "..", "rayforce-bench", "datasets"))
    csv_groupby = os.path.join(base, "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv")
    csv_big = os.path.join(base, "h2oai_join_1e7", "J1_1e7_NA_0_0.csv")
    csv_small = os.path.join(base, "h2oai_join_1e7", "J1_1e7_1e7_0_0.csv")

    con = duckdb.connect()
    con.execute("RESET threads")
    nthreads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    print(f"DuckDB benchmarks ({nthreads} threads)")

    print(f"\nLoading groupby CSV ...")
    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE df AS SELECT * FROM read_csv_auto('{csv_groupby}')")
    load_time = time.perf_counter() - t0
    row_count = con.execute("SELECT COUNT(*) FROM df").fetchone()[0]
    print(f"Loaded: {row_count:,} rows in {load_time*1000:.0f} ms")

    run_section(con, "=== Groupby (10M rows)", GROUPBY_QUERIES)
    run_section(con, "=== Sort (10M rows)", SORT_QUERIES)
    run_section(con, "=== Window (10M rows)", WINDOW_QUERIES)

    # Join tables
    print(f"\nLoading join CSVs ...")
    con.execute(f"CREATE TABLE x AS SELECT * FROM read_csv_auto('{csv_big}')")
    con.execute(f"CREATE TABLE small AS SELECT * FROM read_csv_auto('{csv_small}')")

    JOIN_QUERIES = {
        "j1-inner": "SELECT x.id1, x.id2, x.id3, x.v1, small.v2 FROM x INNER JOIN small ON x.id1 = small.id1 AND x.id2 = small.id2 AND x.id3 = small.id3",
        "j2-left": "SELECT x.id1, x.id2, x.id3, x.v1, small.v2 FROM x LEFT JOIN small ON x.id1 = small.id1 AND x.id2 = small.id2 AND x.id3 = small.id3",
    }
    run_section(con, "=== Join (10M rows)", JOIN_QUERIES)

    print("\nDone.")
    con.close()


if __name__ == "__main__":
    main()
