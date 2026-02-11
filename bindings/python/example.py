import time

from teide.api import Context, col

# Context manages the C library lifecycle (arena + symbol table)
with Context() as ctx:
    # Load CSV
    start = time.perf_counter()
    table = ctx.read_csv(
        "../../../rayforce-bench/datasets/G1_1e7_1e2_0_0/G1_1e7_1e2_0_0.csv"
    )
    end = time.perf_counter()
    print(table)  # Table(10000000 rows x 9 cols: ['id1', 'id2', ...])
    print(f"Time of csv load: {(end - start) * 1000:.3f} ms")

    # Group by id1, compute sum of v1 and mean of v3
    start = time.perf_counter()
    result = (
        table.group_by("id1", "id2", "id3", "id4", "id5", "id6")
        .agg(col("v1").sum())
        .collect()
    )
    end = time.perf_counter()
    print(f"Time of q1: {(end - start) * 1000:.3f} ms")

    # Print results
    print(result)
