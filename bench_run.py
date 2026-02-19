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

"""Quick benchmark runner for Teide groupby queries on 10M dataset."""

import ctypes
import time
import sys
import os

# Ensure we can import teide
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "bindings", "python"))

from teide import TeideLib, OP_SUM, OP_AVG, OP_MIN, OP_MAX, OP_COUNT

N_ITER = 7  # median of 7 runs

CSV_PATH = os.path.join(os.path.dirname(__file__),
                        "..", "rayforce-bench", "datasets",
                        "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv")


def const_col(lib, g, df_ptr, name):
    name_id = lib.sym_intern(name)
    col_vec = lib._lib.td_table_get_col(df_ptr, name_id)
    if not col_vec:
        raise ValueError(f"Column '{name}' not found")
    return lib.const_vec(g, col_vec)


def run_groupby(lib, tbl, label, key_names, agg_ops, agg_col_names):
    g = lib.graph_new(tbl)
    try:
        keys = [lib.scan(g, k) for k in key_names]
        agg_ins = [lib.scan(g, c) for c in agg_col_names]

        nk = len(keys)
        na = len(agg_ops)
        keys_arr = (ctypes.c_void_p * nk)(*keys)
        ops_arr = (ctypes.c_uint16 * na)(*agg_ops)
        ins_arr = (ctypes.c_void_p * na)(*agg_ins)

        root = lib._lib.td_group(g, keys_arr, nk, ops_arr, ins_arr, na)
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

        elapsed = sorted(times)[len(times) // 2]  # median
        print(f"  {label:12s}  {elapsed*1000:8.1f} ms   {nrows:>10,} rows x {ncols} cols")
    finally:
        lib.graph_free(g)


def main():
    csv_path = os.path.abspath(CSV_PATH)
    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}")
        sys.exit(1)

    lib = TeideLib()
    lib.arena_init()
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

    print("Groupby benchmarks (execution time only, excludes build/optimize):")
    print(f"  {'Query':12s}  {'Time':>8s}       Result")
    print(f"  {'-'*12}  {'-'*8}  {'-'*20}")

    run_groupby(lib, tbl, "q1", ["id1"], [OP_SUM], ["v1"])
    run_groupby(lib, tbl, "q2", ["id1", "id2"], [OP_SUM], ["v1"])
    run_groupby(lib, tbl, "q3", ["id3"], [OP_SUM, OP_AVG], ["v1", "v3"])
    run_groupby(lib, tbl, "q4", ["id4"], [OP_AVG, OP_AVG, OP_AVG], ["v1", "v2", "v3"])
    run_groupby(lib, tbl, "q5", ["id6"], [OP_SUM, OP_SUM, OP_SUM], ["v1", "v2", "v3"])
    run_groupby(lib, tbl, "q6", ["id3"], [OP_MAX, OP_MIN], ["v1", "v2"])
    run_groupby(lib, tbl, "q7",
                ["id1", "id2", "id3", "id4", "id5", "id6"],
                [OP_SUM, OP_COUNT], ["v3", "v1"])

    print("\nDone.")
    lib.release(tbl)
    lib.sym_destroy()
    lib.arena_destroy_all()


if __name__ == "__main__":
    main()
