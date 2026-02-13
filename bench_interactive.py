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

"""Interactive benchmarking REPL for Teide.

Usage:
    TEIDE_LIB=build_release/libteide.so python3 -i bench_interactive.py

Then in the REPL:
    timeit(q1)           # single run
    timeit(q1, n=5)      # 5 runs, prints min/mean/max
    timeit_all()         # all queries, 3 runs each
    timeit_all(n=5)      # all queries, 5 runs each
    compare()            # side-by-side Teide vs DuckDB
"""

import ctypes
import time
import sys
import os
import statistics

_here = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()
sys.path.insert(0, os.path.join(_here, "bindings", "python"))
from teide import TeideLib, OP_SUM, OP_AVG, OP_MIN, OP_MAX, OP_COUNT

CSV_PATH = os.path.abspath(os.path.join(
    _here, "..", "rayforce-bench", "datasets",
    "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv"))

# --------------------------------------------------------------------------
# Init
# --------------------------------------------------------------------------

lib = TeideLib()
lib.arena_init()
lib.sym_init()

print(f"Loading {CSV_PATH} ...")
t0 = time.perf_counter()
tbl = lib.csv_read(CSV_PATH)
load_ms = (time.perf_counter() - t0) * 1000
nrows = lib.table_nrows(tbl)
ncols = lib.table_ncols(tbl)
print(f"Loaded: {nrows:,} rows x {ncols} cols in {load_ms:.0f} ms\n")

# --------------------------------------------------------------------------
# Query definitions
# --------------------------------------------------------------------------

QUERIES = {}

def _def_query(name, key_names, agg_ops, agg_col_names):
    """Register a named query."""
    QUERIES[name] = (key_names, agg_ops, agg_col_names)

_def_query("q1", ["id1"], [OP_SUM], ["v1"])
_def_query("q2", ["id1", "id2"], [OP_SUM], ["v1"])
_def_query("q3", ["id3"], [OP_SUM, OP_AVG], ["v1", "v3"])
_def_query("q4", ["id4"], [OP_AVG, OP_AVG, OP_AVG], ["v1", "v2", "v3"])
_def_query("q5", ["id6"], [OP_SUM, OP_SUM, OP_SUM], ["v1", "v2", "v3"])
_def_query("q6", ["id3"], [OP_MAX, OP_MIN], ["v1", "v2"])
_def_query("q7", ["id1", "id2", "id3", "id4", "id5", "id6"],
           [OP_SUM, OP_COUNT], ["v3", "v1"])

# Convenience globals
q1, q2, q3, q4, q5, q6, q7 = "q1", "q2", "q3", "q4", "q5", "q6", "q7"

# --------------------------------------------------------------------------
# Core: run a single query, return (elapsed_ms, nrows, ncols)
# --------------------------------------------------------------------------

def run(name):
    """Run a named query once. Returns (elapsed_ms, nrows, ncols)."""
    key_names, agg_ops, agg_col_names = QUERIES[name]
    g = lib.graph_new(tbl)
    try:
        keys = [lib.scan(g, k) for k in key_names]
        agg_ins = [lib.scan(g, c) for c in agg_col_names]
        nk, na = len(keys), len(agg_ops)
        keys_arr = (ctypes.c_void_p * nk)(*keys)
        ops_arr = (ctypes.c_uint16 * na)(*agg_ops)
        ins_arr = (ctypes.c_void_p * na)(*agg_ins)
        root = lib._lib.td_group(g, keys_arr, nk, ops_arr, ins_arr, na)
        root = lib.optimize(g, root)

        t0 = time.perf_counter()
        result = lib.execute(g, root)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        if not result or result < 32:
            return (elapsed_ms, 0, 0)
        nr = lib.table_nrows(result)
        nc = lib.table_ncols(result)
        lib.release(result)
        return (elapsed_ms, nr, nc)
    finally:
        lib.graph_free(g)

# --------------------------------------------------------------------------
# timeit: run a query n times, print stats
# --------------------------------------------------------------------------

def timeit(name, n=3, warmup=1):
    """Time a query with warmup and n measured runs.

    Examples:
        timeit(q1)         # 3 runs
        timeit(q3, n=10)   # 10 runs
        timeit("q5", n=5)
    """
    key_names, agg_ops, agg_col_names = QUERIES[name]
    desc = f"group_by({','.join(key_names)}), agg({','.join(agg_col_names)})"

    # Warmup
    for _ in range(warmup):
        run(name)

    # Measured runs
    times = []
    result_info = None
    for _ in range(n):
        ms, nr, nc = run(name)
        times.append(ms)
        result_info = (nr, nc)

    nr, nc = result_info
    mn = min(times)
    avg = statistics.mean(times)
    mx = max(times)

    print(f"  {name}: {desc}")
    print(f"    {n} runs: min={mn:.1f} ms  mean={avg:.1f} ms  max={mx:.1f} ms")
    print(f"    result: {nr:,} rows x {nc} cols")
    if n > 1:
        stdev = statistics.stdev(times) if n > 1 else 0
        print(f"    stdev={stdev:.1f} ms  runs={[f'{t:.1f}' for t in times]}")
    print()
    return times

# --------------------------------------------------------------------------
# timeit_all: run all queries
# --------------------------------------------------------------------------

def timeit_all(n=3, warmup=1):
    """Time all queries. Returns dict of {name: [times]}."""
    print(f"{'Query':6s}  {'min':>8s}  {'mean':>8s}  {'max':>8s}  {'rows':>12s}")
    print(f"{'─'*6}  {'─'*8}  {'─'*8}  {'─'*8}  {'─'*12}")
    results = {}
    for name in ["q1", "q2", "q3", "q4", "q5", "q6", "q7"]:
        for _ in range(warmup):
            run(name)
        times = []
        nr = 0
        for _ in range(n):
            ms, nr_, nc_ = run(name)
            times.append(ms)
            nr = nr_
        results[name] = times
        mn, avg, mx = min(times), statistics.mean(times), max(times)
        print(f"{name:6s}  {mn:7.1f}ms  {avg:7.1f}ms  {mx:7.1f}ms  {nr:>10,} rows")
    print()
    return results

# --------------------------------------------------------------------------
# compare: Teide vs DuckDB side-by-side
# --------------------------------------------------------------------------

def compare(n=3):
    """Run Teide and DuckDB benchmarks side-by-side."""
    try:
        import duckdb
    except ImportError:
        print("pip install duckdb to enable comparison")
        return

    con = duckdb.connect()
    con.execute(f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{CSV_PATH}')")

    duckdb_queries = {
        "q1": "SELECT id1, SUM(v1) FROM data GROUP BY id1",
        "q2": "SELECT id1, id2, SUM(v1) FROM data GROUP BY id1, id2",
        "q3": "SELECT id3, SUM(v1), AVG(v3) FROM data GROUP BY id3",
        "q4": "SELECT id4, AVG(v1), AVG(v2), AVG(v3) FROM data GROUP BY id4",
        "q5": "SELECT id6, SUM(v1), SUM(v2), SUM(v3) FROM data GROUP BY id6",
        "q6": "SELECT id3, MAX(v1), MIN(v2) FROM data GROUP BY id3",
        "q7": "SELECT id1,id2,id3,id4,id5,id6, SUM(v3), COUNT(v1) FROM data GROUP BY id1,id2,id3,id4,id5,id6",
    }

    # DuckDB single-thread
    con.execute("SET threads=1")

    print(f"{'Query':6s}  {'Teide':>10s}  {'DuckDB/1':>10s}  {'DuckDB/N':>10s}  {'ratio':>8s}")
    print(f"{'─'*6}  {'─'*10}  {'─'*10}  {'─'*10}  {'─'*8}")

    for name in ["q1", "q2", "q3", "q4", "q5", "q6", "q7"]:
        # Teide
        for _ in range(1):
            run(name)
        teide_times = [run(name)[0] for _ in range(n)]
        t_ms = min(teide_times)

        # DuckDB single-thread
        con.execute("SET threads=1")
        for _ in range(1):
            con.execute(duckdb_queries[name]).fetchall()
        dk1_times = []
        for _ in range(n):
            t0 = time.perf_counter()
            con.execute(duckdb_queries[name]).fetchall()
            dk1_times.append((time.perf_counter() - t0) * 1000)
        d1_ms = min(dk1_times)

        # DuckDB multi-thread
        con.execute("RESET threads")
        for _ in range(1):
            con.execute(duckdb_queries[name]).fetchall()
        dkn_times = []
        for _ in range(n):
            t0 = time.perf_counter()
            con.execute(duckdb_queries[name]).fetchall()
            dkn_times.append((time.perf_counter() - t0) * 1000)
        dn_ms = min(dkn_times)

        ratio = t_ms / d1_ms if d1_ms > 0 else float('inf')
        print(f"{name:6s}  {t_ms:8.1f}ms  {d1_ms:8.1f}ms  {dn_ms:8.1f}ms  {ratio:7.2f}x")

    con.close()
    print()

# --------------------------------------------------------------------------
# Help
# --------------------------------------------------------------------------

def help():
    print("""
Interactive Teide Benchmark
===========================
  timeit(q1)           - time query q1 (3 runs)
  timeit(q3, n=10)     - time q3 with 10 runs
  timeit_all()         - time all queries (3 runs each)
  timeit_all(n=5)      - time all queries (5 runs each)
  compare()            - Teide vs DuckDB side-by-side
  compare(n=5)         - ... with 5 runs each
  run(q1)              - single run, returns (ms, nrows, ncols)

Available queries: q1, q2, q3, q4, q5, q6, q7
""")

print("Ready. Type help() for usage, or timeit(q1) to start.")
print("Queries: q1..q7  |  timeit(q1)  timeit_all()  compare()")
