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

"""Mount a partitioned table from disk and run groupby benchmarks.

Opens each partition via td_splay_open (zero-copy mmap), then runs
queries per-partition. The on-disk column format IS the in-memory format,
so mmap'd vectors are used directly by the executor — no copies.

Usage:
    TEIDE_LIB=build_release/libteide.so python bench_query_parted.py [--db /tmp/teide_db]
"""

import ctypes
import time
import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "py"))
from teide import TeideLib, OP_SUM, OP_AVG, OP_MIN, OP_MAX, OP_COUNT

N_ITER = 7  # median of 7 runs

TABLE_NAME = "quotes"


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


def discover_partitions(db_root, table_name):
    """Find partition directories (YYYY.MM.DD format), sorted."""
    parts = []
    for name in os.listdir(db_root):
        if name == "sym" or name.startswith("."):
            continue
        path = os.path.join(db_root, name, table_name)
        if os.path.isdir(path):
            parts.append((name, path))
    parts.sort()
    return parts


def main():
    parser = argparse.ArgumentParser(description="Query partitioned table with groupby benchmarks")
    parser.add_argument("--db", type=str, default="/tmp/teide_db", help="Database root directory")
    parser.add_argument("--mode", choices=["parted", "per-partition", "both"], default="both",
                        help="Query mode: parted (td_part_open), per-partition, or both")
    args = parser.parse_args()

    db_root = os.path.abspath(args.db)
    sym_path = os.path.join(db_root, "sym")

    if not os.path.exists(db_root):
        print(f"Database not found: {db_root}")
        print("Run bench_gen_parted.py first to generate the partitioned table.")
        sys.exit(1)

    if not os.path.exists(sym_path):
        print(f"Symfile not found: {sym_path}")
        sys.exit(1)

    lib = TeideLib()
    lib.arena_init()
    lib.sym_init()

    # === td_part_open: unified parted table ===
    if args.mode in ("parted", "both"):
        print(f"=== td_part_open (zero-copy parted table) ===")
        t0 = time.perf_counter()
        parted_tbl = lib.part_open(db_root, TABLE_NAME)
        open_ms = (time.perf_counter() - t0) * 1000

        if not parted_tbl or parted_tbl < 32:
            print(f"td_part_open FAILED")
        else:
            nrows = lib.table_nrows(parted_tbl)
            ncols = lib.table_ncols(parted_tbl)
            print(f"Opened: {nrows:,} total rows x {ncols} cols (parted) in {open_ms:.1f} ms")
            print(f"  (includes {ncols - 1} data cols + 1 MAPCOMMON virtual partition col)")

            # Note: running queries on parted tables requires Phase 3
            # (partition-aware executor). For now, just verify the table is
            # structurally correct.
            print(f"  td_table_nrows = {nrows:,}")

            lib.release(parted_tbl)
        print()

    # === Per-partition queries (existing path) ===
    if args.mode in ("per-partition", "both"):
        # Load shared symfile
        print(f"=== Per-partition queries ===")
        print(f"Loading symfile: {sym_path}")
        t0 = time.perf_counter()
        err = lib.sym_load(sym_path)
        sym_ms = (time.perf_counter() - t0) * 1000
        if err != 0:
            print(f"sym_load failed (err={err})")
            sys.exit(1)
        print(f"Loaded {lib._lib.td_sym_count()} symbols in {sym_ms:.1f} ms")

        # Discover partitions
        parts = discover_partitions(db_root, TABLE_NAME)
        print(f"\nFound {len(parts)} partitions")

        # Open each partition via mmap (zero-copy)
        print(f"Opening partitions via td_splay_open (zero-copy mmap) ...")
        t0 = time.perf_counter()
        tables = []
        total_rows = 0
        for date_str, path in parts:
            tbl = lib.splay_open(path)
            if not tbl or tbl < 32:
                print(f"  {date_str}: FAILED")
                continue
            nr = lib.table_nrows(tbl)
            nc = lib.table_ncols(tbl)
            total_rows += nr
            tables.append((date_str, tbl, nr, nc))
            print(f"  {date_str}: {nr:,} rows x {nc} cols (mmap'd)")

        open_ms = (time.perf_counter() - t0) * 1000
        print(f"Opened {len(tables)} partitions ({total_rows:,} total rows) in {open_ms:.1f} ms")

        # Run groupby benchmarks per partition
        for date_str, tbl, nr, nc in tables:
            print(f"\n--- Partition {date_str} ({nr:,} rows) ---")
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

        for _, tbl, _, _ in tables:
            lib.release(tbl)

    print("\nDone.")
    lib.sym_destroy()
    lib.arena_destroy_all()


if __name__ == "__main__":
    main()
