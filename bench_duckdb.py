#!/usr/bin/env python3
"""DuckDB benchmark for comparison with Teide on H2OAI 10M groupby dataset."""

import duckdb
import time
import os

CSV_PATH = os.path.join(os.path.dirname(__file__),
                        "..", "rayforce-bench", "datasets",
                        "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv")

QUERIES = {
    "q1": "SELECT id1, SUM(v1) AS v1 FROM df GROUP BY id1",
    "q2": "SELECT id1, id2, SUM(v1) AS v1 FROM df GROUP BY id1, id2",
    "q3": "SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM df GROUP BY id3",
    "q4": "SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM df GROUP BY id4",
    "q5": "SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM df GROUP BY id6",
    "q6": "SELECT id3, MAX(v1) AS v1, MIN(v2) AS v2 FROM df GROUP BY id3",
    "q7": "SELECT id1, id2, id3, id4, id5, id6, SUM(v3) AS v3, COUNT(*) AS cnt FROM df GROUP BY id1, id2, id3, id4, id5, id6",
}


def main():
    csv_path = os.path.abspath(CSV_PATH)
    con = duckdb.connect()

    # Set single-threaded for fair comparison
    con.execute("SET threads TO 1")

    print(f"Loading {csv_path} ...")
    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE df AS SELECT * FROM read_csv_auto('{csv_path}')")
    load_time = time.perf_counter() - t0

    row_count = con.execute("SELECT COUNT(*) FROM df").fetchone()[0]
    print(f"Loaded: {row_count:,} rows in {load_time*1000:.0f} ms\n")

    print("DuckDB groupby benchmarks (single-threaded):")
    print(f"  {'Query':12s}  {'Time':>8s}       Result")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    for label, sql in QUERIES.items():
        # Warm up: run once to compile
        con.execute(sql).fetchall()

        t0 = time.perf_counter()
        result = con.execute(sql).fetchall()
        elapsed = time.perf_counter() - t0
        nrows = len(result)
        ncols = len(result[0]) if result else 0
        print(f"  {label:12s}  {elapsed*1000:8.1f} ms   {nrows:>10,} rows x {ncols} cols")

    # Now multi-threaded for reference
    con.execute("RESET threads")
    nthreads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    print(f"\nDuckDB groupby benchmarks (multi-threaded, {nthreads} threads):")
    print(f"  {'Query':12s}  {'Time':>8s}       Result")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    for label, sql in QUERIES.items():
        con.execute(sql).fetchall()  # warm

        t0 = time.perf_counter()
        result = con.execute(sql).fetchall()
        elapsed = time.perf_counter() - t0
        nrows = len(result)
        ncols = len(result[0]) if result else 0
        print(f"  {label:12s}  {elapsed*1000:8.1f} ms   {nrows:>10,} rows x {ncols} cols")

    print("\nDone.")
    con.close()


if __name__ == "__main__":
    main()
