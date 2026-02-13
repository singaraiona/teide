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
