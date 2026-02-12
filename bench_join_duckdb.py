#!/usr/bin/env python3
"""DuckDB join benchmark for comparison with Teide on H2OAI 10M join dataset.

Uses CREATE OR REPLACE TABLE to materialize results inside DuckDB,
avoiding Python fetchall() overhead for fair comparison.
"""

import duckdb
import time
import os

JOIN_DIR = os.path.join(os.path.dirname(__file__),
                        "..", "rayforce-bench", "datasets", "h2oai_join_1e7")
X_CSV = os.path.join(JOIN_DIR, "J1_1e7_NA_0_0.csv")
Y_CSV = os.path.join(JOIN_DIR, "J1_1e7_1e7_0_0.csv")

N_ITER = 5

QUERIES = {
    "j1-inner": """SELECT * FROM x INNER JOIN y
                    ON x.id1 = y.id1 AND x.id2 = y.id2 AND x.id3 = y.id3""",
    "j2-left":  """SELECT * FROM x LEFT JOIN y
                    ON x.id1 = y.id1 AND x.id2 = y.id2 AND x.id3 = y.id3""",
}


def run_query(con, label, sql):
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
    for path in [X_CSV, Y_CSV]:
        if not os.path.exists(os.path.abspath(path)):
            print(f"CSV not found: {os.path.abspath(path)}")
            return

    con = duckdb.connect()
    con.execute("SET threads TO 1")

    print(f"Loading x: {os.path.abspath(X_CSV)} ...")
    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE x AS SELECT * FROM read_csv_auto('{os.path.abspath(X_CSV)}')")
    print(f"  {con.execute('SELECT COUNT(*) FROM x').fetchone()[0]:,} rows in "
          f"{(time.perf_counter()-t0)*1000:.0f} ms")

    print(f"Loading y: {os.path.abspath(Y_CSV)} ...")
    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE y AS SELECT * FROM read_csv_auto('{os.path.abspath(Y_CSV)}')")
    print(f"  {con.execute('SELECT COUNT(*) FROM y').fetchone()[0]:,} rows in "
          f"{(time.perf_counter()-t0)*1000:.0f} ms\n")

    print("DuckDB join benchmarks (single-threaded):")
    print(f"  {'Query':12s}  {'Time':>8s}       Result")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    for label, sql in QUERIES.items():
        run_query(con, label, sql)

    con.execute("RESET threads")
    nthreads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    print(f"\nDuckDB join benchmarks (multi-threaded, {nthreads} threads):")
    print(f"  {'Query':12s}  {'Time':>8s}       Result")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    for label, sql in QUERIES.items():
        run_query(con, label, sql)

    print("\nDone.")
    con.close()


if __name__ == "__main__":
    main()
