#!/usr/bin/env python3
"""
Teide Interactive Examples
==========================

Run all examples:
    PYTHONPATH=bindings/python TEIDE_LIB=build_release/libteide.so python3 bindings/python/examples.py

Or pick one:
    ... python3 bindings/python/examples.py 3    # just aggregations
"""

import os
import sys
import time
import shutil
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from teide.api import Context, col, lit

CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "rayforce-bench",
    "datasets", "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv",
)

def _banner(title):
    w = max(len(title) + 4, 50)
    print("\n" + "=" * w)
    print(f"  {title}")
    print("=" * w)

def _timed(label, fn):
    t0 = time.perf_counter()
    result = fn()
    ms = (time.perf_counter() - t0) * 1000
    print(f"  [{label}: {ms:.1f} ms]")
    return result


# ===================================================================
#  1. Loading data
# ===================================================================

def example_load(ctx):
    """Read a CSV and inspect the table."""
    _banner("1. Loading Data")

    t = _timed("read_csv 10M rows", lambda: ctx.read_csv(CSV_PATH))
    print(f"\nShape: {t.shape[0]:,} rows x {t.shape[1]} columns")
    print(f"Columns: {t.columns}\n")
    print(t)
    return t


# ===================================================================
#  2. Filtering
# ===================================================================

def example_slicing(ctx, t):
    """Slice and sample tables with head() and sorting."""
    _banner("2. Slicing & Sampling")

    # head() — grab first N rows from a result
    print("\n--- head(5) on original table ---")
    print(t.head(5))

    # Sort + head for top-N queries
    print("\n--- Top 5 id1 groups by v1_sum ---")
    agg = t.group_by("id1").agg(col("v1").sum()).collect()
    sorted_ = agg.sort("v1_sum", descending=True).collect()
    print(sorted_.head(5))

    # Column access (Series)
    print("\n--- Column access: t['v1'] ---")
    s = t["v1"]
    print(f"  Series: {s}")
    print(f"  First 5 values: {s.to_list()[:5]}")


# ===================================================================
#  3. Aggregations
# ===================================================================

def example_aggregations(ctx, t):
    """Group-by with various key cardinalities."""
    _banner("3. Aggregations — low, medium, and high cardinality")

    # Low cardinality (100 groups) — direct-array path, very fast
    print("\n--- group_by(id1).agg(sum(v1), count(v1)) ---")
    r = _timed("q1 (100 groups)", lambda:
        t.group_by("id1").agg(col("v1").sum(), col("v1").count()).collect())
    print(r)

    # Medium cardinality (100K groups)
    print("\n--- group_by(id3).agg(sum(v1), mean(v3), min(v2), max(v2)) ---")
    r = _timed("q3 (100K groups)", lambda:
        t.group_by("id3")
         .agg(col("v1").sum(), col("v3").mean(),
              col("v2").min(), col("v2").max())
         .collect())
    print(r)

    # Multi-key groupby (10K groups)
    print("\n--- group_by(id1, id2).agg(sum(v1)) ---")
    r = _timed("q2 (10K groups)", lambda:
        t.group_by("id1", "id2").agg(col("v1").sum()).collect())
    print(r)

    # High cardinality: 6-key groupby (radix hash table path)
    print("\n--- group_by(id1..id6).agg(sum(v3), count(v1)) ---")
    r = _timed("q7 (10M groups)", lambda:
        t.group_by("id1", "id2", "id3", "id4", "id5", "id6")
         .agg(col("v3").sum(), col("v1").count())
         .collect())
    print(f"  Result: {r.shape[0]:,} groups x {r.shape[1]} columns")


# ===================================================================
#  4. Sorting
# ===================================================================

def example_sort(ctx, t):
    """Sort tables by one or multiple columns."""
    _banner("4. Sorting")

    # Sort a small result set
    agg = t.group_by("id1").agg(col("v1").sum()).collect()

    print("\n--- Sort by v1_sum ascending ---")
    r = agg.sort("v1_sum").collect()
    print(r)

    print("\n--- Sort by v1_sum descending ---")
    r = agg.sort("v1_sum", descending=True).collect()
    print(r)


# ===================================================================
#  5. Computed columns (expressions)
# ===================================================================

def example_multi_agg(ctx, t):
    """Multiple aggregation types in a single group-by."""
    _banner("5. Multi-Aggregation — combine sum, mean, min, max, count")

    # All agg types in one call
    print("\n--- group_by(id4).agg(sum, mean, min, max, count) ---")
    r = _timed("5-way agg", lambda:
        t.group_by("id4")
         .agg(col("v1").sum(), col("v3").mean(),
              col("v2").min(), col("v2").max(), col("v1").count())
         .collect())
    print(r)

    # first + last
    print("\n--- group_by(id4).agg(first(v1), last(v1)) ---")
    r = t.group_by("id4").agg(col("v1").first(), col("v1").last()).collect()
    print(r)


# ===================================================================
#  6. Joins
# ===================================================================

def example_join(ctx, t):
    """Inner join two tables on a shared key."""
    _banner("6. Joins")

    sums = t.group_by("id4").agg(col("v1").sum()).collect()
    avgs = t.group_by("id4").agg(col("v3").mean()).collect()

    print("Left table (sums):")
    print(sums.head(5))
    print("\nRight table (avgs):")
    print(avgs.head(5))

    joined = _timed("inner join on id4", lambda: sums.join(avgs, on="id4"))
    print(f"\nJoined: {joined.shape[0]} rows x {joined.shape[1]} columns")
    print(joined)


# ===================================================================
#  7. Splayed tables (column-per-file storage)
# ===================================================================

def example_splay(ctx, t):
    """Save and load a splayed table."""
    _banner("7. Splayed Tables — column-per-file storage")

    summary = t.group_by("id4").agg(
        col("v1").sum(), col("v2").min(), col("v3").mean()
    ).collect()

    splay_dir = os.path.join(tempfile.gettempdir(), "teide_splay_demo")
    if os.path.exists(splay_dir):
        shutil.rmtree(splay_dir)

    ctx.splay_save(summary, splay_dir)
    files = sorted(os.listdir(splay_dir))
    print(f"\nSaved to {splay_dir}/")
    for f in files:
        sz = os.path.getsize(os.path.join(splay_dir, f))
        print(f"  {f:12s}  {sz:>6,} bytes")

    loaded = ctx.splay_load(splay_dir)
    print(f"\nLoaded back: {loaded.shape[0]} rows x {loaded.shape[1]} columns")
    print(loaded)
    shutil.rmtree(splay_dir)


# ===================================================================
#  8. Partitioned tables
# ===================================================================

def example_partitioned(ctx, t):
    """Create and load a date-partitioned table."""
    _banner("8. Partitioned Tables — date directories of splayed tables")

    summary = t.group_by("id4").agg(col("v1").sum()).collect()

    db_root = os.path.join(tempfile.gettempdir(), "teide_part_demo")
    if os.path.exists(db_root):
        shutil.rmtree(db_root)
    os.makedirs(db_root)

    for date in ["2025.01.01", "2025.01.02", "2025.01.03"]:
        part_dir = os.path.join(db_root, date, "trades")
        os.makedirs(part_dir)
        ctx.splay_save(summary, part_dir)

    print(f"\nPartitioned DB at {db_root}/")
    for date in sorted(os.listdir(db_root)):
        if date.startswith("."):
            continue
        print(f"  {date}/trades/")

    loaded = ctx.part_load(db_root, "trades")
    print(f"\nLoaded: {loaded.shape[0]:,} rows x {loaded.shape[1]} columns")
    print(f"(3 partitions x {summary.shape[0]} rows = {3 * summary.shape[0]} rows)")
    print(loaded)
    shutil.rmtree(db_root)


# ===================================================================
#  9. Chained pipeline
# ===================================================================

def example_pipeline(ctx, t):
    """Full pipeline: group → sort → head."""
    _banner("9. Chained Pipeline — group → sort → head")

    print("\n--- Top 20 id1 groups by v1_sum (descending) ---")
    def run_pipeline():
        grouped = (t.group_by("id1")
                    .agg(col("v1").sum(), col("v3").mean(), col("v1").count())
                    .collect())
        sorted_ = grouped.sort("v1_sum", descending=True).collect()
        return sorted_.head(20)
    r = _timed("pipeline", run_pipeline)
    print(r)


# ===================================================================
#  10. Data export
# ===================================================================

def example_export(ctx, t):
    """Export to Python dicts, lists, and optionally numpy/pandas."""
    _banner("10. Data Export — to_dict, to_list, to_numpy, to_pandas")

    small = t.group_by("id4").agg(col("v1").sum()).collect().head(5)

    d = small.to_dict()
    print("\nto_dict():")
    for k, v in d.items():
        print(f"  {k}: {v}")

    s = small["v1_sum"]
    print(f"\nSeries: {s}")
    print(f"  .to_list(): {s.to_list()}")

    try:
        import numpy
        arr = small["v1_sum"].to_numpy()
        print(f"  .to_numpy(): {arr}  (zero-copy!)")
    except ImportError:
        print("  (numpy not installed — skipping to_numpy)")

    try:
        import pandas
        pdf = small.to_pandas()
        print(f"\nto_pandas():\n{pdf}")
    except ImportError:
        print("\n  (pandas not installed — skipping to_pandas)")


# ===================================================================
#  Main
# ===================================================================

EXAMPLES = {
    1: ("Loading Data",       example_load),
    2: ("Slicing & Sampling", example_slicing),
    3: ("Aggregations",       example_aggregations),
    4: ("Sorting",            example_sort),
    5: ("Multi-Aggregation",  example_multi_agg),
    6: ("Joins",              example_join),
    7: ("Splayed Tables",     example_splay),
    8: ("Partitioned Tables", example_partitioned),
    9: ("Chained Pipeline",   example_pipeline),
    10: ("Data Export",       example_export),
}

def main():
    if not os.path.exists(CSV_PATH):
        print(f"Dataset not found: {CSV_PATH}")
        print("Place H2OAI groupby CSV at: ../rayforce-bench/datasets/G1_1e7_1e2_0_0/")
        sys.exit(1)

    # Pick specific example or run all
    which = None
    if len(sys.argv) > 1:
        which = int(sys.argv[1])
        if which not in EXAMPLES:
            print(f"Example {which} not found. Available: {list(EXAMPLES.keys())}")
            sys.exit(1)

    with Context() as ctx:
        t = example_load(ctx)

        if which:
            if which != 1:
                EXAMPLES[which][1](ctx, t)
        else:
            for num, (name, fn) in EXAMPLES.items():
                if num == 1:
                    continue  # already ran
                fn(ctx, t)

        _banner("Done!")
        print("  All examples completed successfully.\n")


if __name__ == "__main__":
    main()
