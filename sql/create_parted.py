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

"""Create a partitioned table from the H2OAI 10M-row benchmark CSV.

Loads the CSV, splits into N date-partitioned splayed tables on disk:

    /tmp/teide_db/
      sym                       <- shared symbol file
      2024.01.01/quotes/        <- partition 1 (splayed columns)
      2024.01.02/quotes/        <- partition 2
      ...

Usage:
    TEIDE_LIB=build_release/libteide.so python3 sql/create_parted.py [--parts 5] [--db /tmp/teide_db]
"""

import time
import sys
import os
import shutil
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "py"))
from teide import TeideLib

CSV_PATH = os.path.join(os.path.dirname(__file__),
                        "..", "..", "rayforce-bench", "datasets",
                        "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv")

TABLE_NAME = "quotes"


def main():
    parser = argparse.ArgumentParser(description="Create partitioned table from H2OAI CSV")
    parser.add_argument("--parts", type=int, default=5, help="Number of partitions")
    parser.add_argument("--db", type=str, default="/tmp/teide_db", help="Database root")
    args = parser.parse_args()

    n_parts = args.parts
    db_root = os.path.abspath(args.db)
    csv_path = os.path.abspath(CSV_PATH)

    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}")
        sys.exit(1)

    lib = TeideLib()
    lib.arena_init()
    lib.sym_init()

    # Load CSV
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

    # Clean and create db_root
    if os.path.exists(db_root):
        shutil.rmtree(db_root)
    os.makedirs(db_root, exist_ok=True)

    # Split into N partitions
    rows_per_part = nrows // n_parts
    print(f"\nPartitioning into {n_parts} partitions (~{rows_per_part:,} rows each) ...")

    t0 = time.perf_counter()

    for p in range(n_parts):
        start = p * rows_per_part
        end = nrows if p == n_parts - 1 else (p + 1) * rows_per_part
        part_rows = end - start

        date_str = f"2024.01.{p + 1:02d}"
        part_dir = os.path.join(db_root, date_str, TABLE_NAME)
        os.makedirs(part_dir, exist_ok=True)

        sub_tbl = lib.table_new(ncols)
        for c in range(ncols):
            col = lib.table_get_col_idx(tbl, c)
            name_id = lib.table_col_name(tbl, c)
            sliced = lib._lib.td_vec_slice(col, start, part_rows)
            if sliced and sliced > 32:
                sub_tbl = lib._lib.td_table_add_col(sub_tbl, name_id, sliced)
                lib.release(sliced)

        err = lib.splay_save(sub_tbl, part_dir)
        if err != 0:
            print(f"  ERROR: splay_save failed for partition {p} (err={err})")
            sys.exit(1)

        lib.release(sub_tbl)
        print(f"  {date_str}/{TABLE_NAME}: {part_rows:,} rows")

    # Save shared symfile
    sym_path = os.path.join(db_root, "sym")
    err = lib.sym_save(sym_path)
    if err != 0:
        print(f"ERROR: sym_save failed (err={err})")
        sys.exit(1)

    save_ms = (time.perf_counter() - t0) * 1000

    total_size = 0
    for root, dirs, files in os.walk(db_root):
        for f in files:
            total_size += os.path.getsize(os.path.join(root, f))

    print(f"\nDone in {save_ms:.0f} ms")
    print(f"Database: {db_root}")
    print(f"Total size: {total_size / 1024 / 1024:.1f} MB ({n_parts} partitions)")

    lib.release(tbl)
    lib.sym_destroy()
    lib.arena_destroy_all()


if __name__ == "__main__":
    main()
