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

"""Generate a 500M-row partitioned table from H2OAI 10M CSV.

Replicates the full 10M-row CSV into 50 date-partitioned splayed tables,
producing 500M total rows under /tmp/db:

    /tmp/db/
      sym                          <- shared symbol intern table
      2024.01.01/
        quotes/                    <- splayed table (partition 1, 10M rows)
      2024.01.02/
        quotes/                    <- splayed table (partition 2, 10M rows)
      ...
      2024.02.19/
        quotes/                    <- splayed table (partition 50, 10M rows)

Usage:
    TEIDE_LIB=build_release/libteide.so python gen_500m.py
"""

import ctypes
import time
import sys
import os
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "py"))
from teide import TeideLib

CSV_PATH = os.path.join(os.path.dirname(__file__),
                        "..", "rayforce-bench", "datasets",
                        "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv")

DB_ROOT = "/tmp/db"
TABLE_NAME = "quotes"
N_PARTS = 50


def main():
    csv_path = os.path.abspath(CSV_PATH)
    db_root = os.path.abspath(DB_ROOT)

    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}")
        sys.exit(1)

    lib = TeideLib()
    lib.arena_init()
    lib.sym_init()

    # Load CSV once
    print(f"Loading {csv_path} ...")
    t0 = time.perf_counter()
    tbl = lib.read_csv(csv_path)
    load_ms = (time.perf_counter() - t0) * 1000

    if not tbl or tbl < 32:
        print("CSV load failed!")
        sys.exit(1)

    nrows = lib.table_nrows(tbl)
    ncols = lib.table_ncols(tbl)
    print(f"Loaded: {nrows:,} rows x {ncols} cols in {load_ms:.0f} ms")
    print(f"\nGenerating {N_PARTS} partitions x {nrows:,} rows = {N_PARTS * nrows:,} total rows")

    # Clean and create db_root
    if os.path.exists(db_root):
        shutil.rmtree(db_root)
    os.makedirs(db_root, exist_ok=True)

    t0 = time.perf_counter()

    for p in range(N_PARTS):
        # Date: 2024.01.01 through 2024.02.19 (50 days starting Jan 1)
        day = p + 1
        if day <= 31:
            date_str = f"2024.01.{day:02d}"
        else:
            date_str = f"2024.02.{day - 31:02d}"

        part_dir = os.path.join(db_root, date_str, TABLE_NAME)
        os.makedirs(part_dir, exist_ok=True)

        # Build sub-table: full copy of all columns via slice(0, nrows)
        sub_tbl = lib.table_new(ncols)
        for c in range(ncols):
            col = lib.table_get_col_idx(tbl, c)
            name_id = lib.table_col_name(tbl, c)
            sliced = lib._lib.td_vec_slice(col, 0, nrows)
            if sliced and sliced > 32:
                sub_tbl = lib._lib.td_table_add_col(sub_tbl, name_id, sliced)
                lib.release(sliced)

        # Save as splayed table
        err = lib.splay_save(sub_tbl, part_dir)
        if err != 0:
            print(f"  ERROR: splay_save failed for partition {p} (err={err})")
            sys.exit(1)

        lib.release(sub_tbl)

        elapsed = time.perf_counter() - t0
        rate = (p + 1) / elapsed
        eta = (N_PARTS - p - 1) / rate if rate > 0 else 0
        print(f"  [{p+1:2d}/{N_PARTS}] {date_str}/{TABLE_NAME}: {nrows:,} rows  "
              f"({elapsed:.1f}s elapsed, ETA {eta:.0f}s)")

    # Save shared symfile
    sym_path = os.path.join(db_root, "sym")
    err = lib.sym_save(sym_path)
    if err != 0:
        print(f"ERROR: sym_save failed (err={err})")
        sys.exit(1)

    save_ms = (time.perf_counter() - t0) * 1000

    # Report sizes
    total_size = 0
    for root, dirs, files in os.walk(db_root):
        for f in files:
            total_size += os.path.getsize(os.path.join(root, f))

    print(f"\nDone in {save_ms / 1000:.1f} s")
    print(f"Database: {db_root}")
    print(f"Total rows: {N_PARTS * nrows:,}")
    print(f"Total size: {total_size / 1024 / 1024 / 1024:.1f} GB ({N_PARTS} partitions)")
    print(f"Sym count: {lib._lib.td_sym_count()}")

    lib.release(tbl)
    lib.sym_destroy()
    lib.arena_destroy_all()


if __name__ == "__main__":
    main()
