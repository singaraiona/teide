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

"""Sort benchmark runner for Teide on 10M dataset."""

import ctypes
import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "bindings", "python"))

from teide import TeideLib

N_ITER = 7

CSV_PATH = os.path.join(os.path.dirname(__file__),
                        "..", "rayforce-bench", "datasets",
                        "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv")


def run_sort(lib, tbl, label, col_names, descs):
    g = lib.graph_new(tbl)
    try:
        table_node = lib.const_table(g, tbl)
        keys = [lib.scan(g, c) for c in col_names]

        root = lib.sort_op(g, table_node, keys, descs)
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

            nrows = lib.table_nrows(result)
            ncols = lib.table_ncols(result)
            lib.release(result)

        elapsed = sorted(times)[len(times) // 2]
        print(f"  {label:12s}  {elapsed*1000:8.1f} ms   {nrows:>10,} rows x {ncols} cols")
    finally:
        lib.graph_free(g)


def main():
    csv_path = os.path.abspath(CSV_PATH)
    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}")
        sys.exit(1)

    lib = TeideLib()
    lib.sym_init()

    print(f"Loading {csv_path} ...")
    t0 = time.perf_counter()
    tbl = lib.read_csv(csv_path)
    load_time = time.perf_counter() - t0

    if not tbl or tbl < 32:
        print("CSV load failed!")
        sys.exit(1)

    nrows = lib.table_nrows(tbl)
    ncols = lib.table_ncols(tbl)
    print(f"Loaded: {nrows:,} rows x {ncols} cols in {load_time*1000:.0f} ms\n")

    print("Sort benchmarks (execution time only, excludes build/optimize):")
    print(f"  {'Query':12s}  {'Time':>8s}       Result")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    # s1: single low-cardinality key ASC
    run_sort(lib, tbl, "s1", ["id1"], [0])
    # s2: single high-cardinality key ASC
    run_sort(lib, tbl, "s2", ["id3"], [0])
    # s3: integer column ASC
    run_sort(lib, tbl, "s3", ["id4"], [0])
    # s4: float column DESC
    run_sort(lib, tbl, "s4", ["v3"], [1])
    # s5: 2-key composite ASC
    run_sort(lib, tbl, "s5", ["id1", "id2"], [0, 0])
    # s6: 3-key composite ASC
    run_sort(lib, tbl, "s6", ["id1", "id2", "id3"], [0, 0, 0])

    print("\nDone.")
    lib.release(tbl)


if __name__ == "__main__":
    main()
