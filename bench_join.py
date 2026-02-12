#!/usr/bin/env python3
"""Join benchmark runner for Teide on H2OAI 10M join dataset."""

import ctypes
import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "bindings", "python"))

from teide import TeideLib

N_ITER = 5  # fewer iterations since joins are expensive

JOIN_DIR = os.path.join(os.path.dirname(__file__),
                        "..", "rayforce-bench", "datasets", "h2oai_join_1e7")
X_CSV = os.path.join(JOIN_DIR, "J1_1e7_NA_0_0.csv")
Y_CSV = os.path.join(JOIN_DIR, "J1_1e7_1e7_0_0.csv")


def run_join(lib, left_df, right_df, label, key_names, join_type):
    """Run a join benchmark.
    join_type: 0=INNER, 1=LEFT
    """
    g = lib.graph_new(left_df)
    try:
        left_node = lib.const_df(g, left_df)
        right_node = lib.const_df(g, right_df)

        left_keys = [lib.scan(g, k) for k in key_names]

        # Right keys: use const_vec for each right key column
        right_keys = []
        for k in key_names:
            name_id = lib.sym_intern(k)
            col_vec = lib._lib.td_table_get_col(right_df, name_id)
            right_keys.append(lib.const_vec(g, col_vec))

        root = lib.join(g, left_node, left_keys, right_node, right_keys, join_type)
        root = lib.optimize(g, root)

        times = []
        nrows = ncols = 0
        for _ in range(N_ITER):
            t0 = time.perf_counter()
            result = lib.execute(g, root)
            times.append(time.perf_counter() - t0)

            if not result or result < 32:
                print(f"  {label:12s}  FAILED")
                return

            nrows = lib.df_nrows(result)
            ncols = lib.df_ncols(result)
            lib.release(result)

        elapsed = sorted(times)[len(times) // 2]
        print(f"  {label:12s}  {elapsed*1000:8.1f} ms   {nrows:>10,} rows x {ncols} cols")
    finally:
        lib.graph_free(g)


def main():
    for path in [X_CSV, Y_CSV]:
        if not os.path.exists(os.path.abspath(path)):
            print(f"CSV not found: {os.path.abspath(path)}")
            sys.exit(1)

    lib = TeideLib()
    lib.arena_init()
    lib.sym_init()

    print(f"Loading x: {os.path.abspath(X_CSV)} ...")
    t0 = time.perf_counter()
    x = lib.csv_read(os.path.abspath(X_CSV))
    print(f"  {lib.df_nrows(x):,} rows in {(time.perf_counter()-t0)*1000:.0f} ms")

    print(f"Loading y: {os.path.abspath(Y_CSV)} ...")
    t0 = time.perf_counter()
    y = lib.csv_read(os.path.abspath(Y_CSV))
    print(f"  {lib.df_nrows(y):,} rows in {(time.perf_counter()-t0)*1000:.0f} ms\n")

    print("Join benchmarks (execution time only, excludes build/optimize):")
    print(f"  {'Query':12s}  {'Time':>8s}       Result")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    # j1: INNER JOIN on (id1,id2,id3) — nearly 1:1, ~10M result rows
    run_join(lib, x, y, "j1-inner", ["id1", "id2", "id3"], 0)
    # j2: LEFT JOIN on (id1,id2,id3) — 10M result rows
    run_join(lib, x, y, "j2-left", ["id1", "id2", "id3"], 1)

    print("\nDone.")
    lib.release(y)
    lib.release(x)


if __name__ == "__main__":
    main()
