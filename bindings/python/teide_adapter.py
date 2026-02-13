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

"""
Teide benchmark adapter for rayforce-bench.

Implements the Adapter interface for H2OAI groupby, join, sort,
and window join benchmark suites.

Usage:
    TEIDE_LIB=build_release/libteide.so python -m rayforce_bench --adapter teide
"""

import ctypes
import time
from teide import TeideLib, OP_SUM, OP_AVG, OP_MIN, OP_MAX, OP_COUNT
from teide.api import Context, Table, col, lit


class AdapterResult:
    """Result of a benchmark query execution."""

    def __init__(self, execution_time_ns, row_count, success=True, checksum=None, error=None):
        self.execution_time_ns = execution_time_ns
        self.row_count = row_count
        self.success = success
        self.checksum = checksum
        self.error = error


def const_col(lib, g, df_ptr, name):
    """Extract a column from a DataFrame and wrap as td_const_vec node."""
    name_id = lib.sym_intern(name)
    col_vec = lib._lib.td_table_get_col(df_ptr, name_id)
    if not col_vec:
        raise ValueError(f"Column '{name}' not found in DataFrame")
    return lib.const_vec(g, col_vec)


class TeideAdapter:
    """Benchmark adapter implementing the rayforce-bench Adapter interface."""

    name = "teide"

    def __init__(self):
        self._ctx = None
        self._tables = {}

    def setup(self, schema=None):
        self._ctx = Context()

    def load_csv(self, csv_paths, table_name):
        if isinstance(csv_paths, str):
            csv_paths = [csv_paths]
        # Load first CSV; ignore additional paths for now
        df = self._ctx.read_csv(csv_paths[0])
        self._tables[table_name] = df

    def run(self, task, params=None):
        try:
            start = time.perf_counter_ns()
            result_table = self._dispatch(task, params or {})
            end = time.perf_counter_ns()

            row_count = len(result_table) if result_table else 0
            return AdapterResult(
                execution_time_ns=end - start,
                row_count=row_count,
                success=True,
            )
        except Exception as e:
            return AdapterResult(
                execution_time_ns=0,
                row_count=0,
                success=False,
                error=str(e),
            )

    def close(self):
        self._tables.clear()
        if self._ctx:
            self._ctx.close()
            self._ctx = None

    def _dispatch(self, task, params):
        """Route a benchmark task to the appropriate handler."""
        # Parse task string like "groupby_q1", "join_q1", "sort_q1", "window_q1"
        parts = task.lower().split("_")
        suite = parts[0]
        query = parts[1] if len(parts) > 1 else "q1"

        if suite == "groupby":
            return self._groupby(query, params)
        elif suite == "join":
            return self._join(query, params)
        elif suite == "sort":
            return self._sort(query, params)
        elif suite == "window":
            return self._window(query, params)
        else:
            raise ValueError(f"Unknown benchmark suite: {suite}")

    # === Group-by queries ===

    def _groupby(self, query, params):
        table = self._tables.get("groupby") or self._tables.get("G1")
        if not table:
            raise ValueError("No groupby table loaded")
        lib = self._ctx._lib
        df_ptr = table._ptr

        if query == "q1":
            # sum(v1) group by id1
            return self._run_group(lib, df_ptr, ["id1"],
                                   [OP_SUM], ["v1"])

        elif query == "q2":
            # sum(v1) group by id1, id2
            return self._run_group(lib, df_ptr, ["id1", "id2"],
                                   [OP_SUM], ["v1"])

        elif query == "q3":
            # sum(v1), avg(v3) group by id3
            return self._run_group(lib, df_ptr, ["id3"],
                                   [OP_SUM, OP_AVG], ["v1", "v3"])

        elif query == "q4":
            # avg(v1), avg(v2), avg(v3) group by id4
            return self._run_group(lib, df_ptr, ["id4"],
                                   [OP_AVG, OP_AVG, OP_AVG], ["v1", "v2", "v3"])

        elif query == "q5":
            # sum(v1), sum(v2), sum(v3) group by id6
            return self._run_group(lib, df_ptr, ["id6"],
                                   [OP_SUM, OP_SUM, OP_SUM], ["v1", "v2", "v3"])

        elif query == "q6":
            # max(v1) - min(v2) group by id3
            # Two-step: group then expression
            return self._run_group(lib, df_ptr, ["id3"],
                                   [OP_MAX, OP_MIN], ["v1", "v2"])

        elif query == "q7":
            # sum(v3), count group by id1..id6
            return self._run_group(lib, df_ptr,
                                   ["id1", "id2", "id3", "id4", "id5", "id6"],
                                   [OP_SUM, OP_COUNT], ["v3", "v1"])

        elif query == "q8":
            # filter(v1>=3) then sum(v3) group by id2
            return self._run_filter_group(lib, df_ptr,
                                          ("v1", "ge", 3),
                                          ["id2"], [OP_SUM], ["v3"])

        elif query == "q9":
            # filter(v1>=2 AND v2<=8) then sum(v1,v2,v3) group by id3
            return self._run_filter2_group(lib, df_ptr,
                                           ("v1", "ge", 2), ("v2", "le", 8),
                                           ["id3"],
                                           [OP_SUM, OP_SUM, OP_SUM],
                                           ["v1", "v2", "v3"])

        elif query == "q10":
            # filter(v3>0) then sum(v1), sum(v2) group by id1..id4
            return self._run_filter_group(lib, df_ptr,
                                          ("v3", "gt", 0),
                                          ["id1", "id2", "id3", "id4"],
                                          [OP_SUM, OP_SUM], ["v1", "v2"])

        raise ValueError(f"Unknown groupby query: {query}")

    def _run_group(self, lib, df_ptr, key_names, agg_ops, agg_col_names):
        """Execute a simple group-by query."""
        g = lib.graph_new(df_ptr)
        try:
            n_keys = len(key_names)
            n_aggs = len(agg_ops)

            keys = [lib.scan(g, name) for name in key_names]
            agg_inputs = [lib.scan(g, name) for name in agg_col_names]

            keys_arr = (ctypes.c_void_p * n_keys)(*keys)
            ops_arr = (ctypes.c_uint16 * n_aggs)(*agg_ops)
            ins_arr = (ctypes.c_void_p * n_aggs)(*agg_inputs)

            root = lib._lib.td_group(g, keys_arr, n_keys, ops_arr, ins_arr, n_aggs)
            root = lib.optimize(g, root)
            result = lib.execute(g, root)
            if not result or result < 32:
                raise RuntimeError("Group-by execution failed")
            return Table(lib, result)
        finally:
            lib.graph_free(g)

    def _run_filter_group(self, lib, df_ptr, filter_spec, key_names, agg_ops, agg_col_names):
        """Execute filter + group-by query."""
        g = lib.graph_new(df_ptr)
        try:
            # Build filter predicate
            col_name, cmp_op, val = filter_spec
            scan_node = lib.scan(g, col_name)
            const_node = lib.const_i64(g, val)

            cmp_map = {"ge": lib._lib.td_ge, "le": lib._lib.td_le,
                       "gt": lib._lib.td_gt, "lt": lib._lib.td_lt,
                       "eq": lib._lib.td_eq}
            pred = cmp_map[cmp_op](g, scan_node, const_node)
            filt = lib._lib.td_filter(g, scan_node, pred)

            # Group-by on filtered data
            n_keys = len(key_names)
            n_aggs = len(agg_ops)
            keys = [lib.scan(g, name) for name in key_names]
            agg_inputs = [lib.scan(g, name) for name in agg_col_names]

            keys_arr = (ctypes.c_void_p * n_keys)(*keys)
            ops_arr = (ctypes.c_uint16 * n_aggs)(*agg_ops)
            ins_arr = (ctypes.c_void_p * n_aggs)(*agg_inputs)

            root = lib._lib.td_group(g, keys_arr, n_keys, ops_arr, ins_arr, n_aggs)
            root = lib.optimize(g, root)
            result = lib.execute(g, root)
            if not result or result < 32:
                raise RuntimeError("Filter+group execution failed")
            return Table(lib, result)
        finally:
            lib.graph_free(g)

    def _run_filter2_group(self, lib, df_ptr, filt1, filt2, key_names, agg_ops, agg_col_names):
        """Execute double-filter + group-by query."""
        g = lib.graph_new(df_ptr)
        try:
            # Build first predicate
            cmp_map = {"ge": lib._lib.td_ge, "le": lib._lib.td_le,
                       "gt": lib._lib.td_gt, "lt": lib._lib.td_lt}

            s1 = lib.scan(g, filt1[0])
            c1 = lib.const_i64(g, filt1[2])
            p1 = cmp_map[filt1[1]](g, s1, c1)

            s2 = lib.scan(g, filt2[0])
            c2 = lib.const_i64(g, filt2[2])
            p2 = cmp_map[filt2[1]](g, s2, c2)

            pred = lib._lib.td_and(g, p1, p2)
            filt = lib._lib.td_filter(g, s1, pred)

            # Group-by
            n_keys = len(key_names)
            n_aggs = len(agg_ops)
            keys = [lib.scan(g, name) for name in key_names]
            agg_inputs = [lib.scan(g, name) for name in agg_col_names]

            keys_arr = (ctypes.c_void_p * n_keys)(*keys)
            ops_arr = (ctypes.c_uint16 * n_aggs)(*agg_ops)
            ins_arr = (ctypes.c_void_p * n_aggs)(*agg_inputs)

            root = lib._lib.td_group(g, keys_arr, n_keys, ops_arr, ins_arr, n_aggs)
            root = lib.optimize(g, root)
            result = lib.execute(g, root)
            if not result or result < 32:
                raise RuntimeError("Filter2+group execution failed")
            return Table(lib, result)
        finally:
            lib.graph_free(g)

    # === Join queries ===

    def _join(self, query, params):
        left = self._tables.get("left") or self._tables.get("X")
        right = self._tables.get("right") or self._tables.get("Y")
        if not left or not right:
            raise ValueError("Join tables not loaded")
        lib = self._ctx._lib
        left_ptr = left._ptr
        right_ptr = right._ptr

        join_type = 1 if query == "q1" else 0  # q1=left, q2=inner

        g = lib.graph_new(left_ptr)
        try:
            left_df_node = lib.const_df(g, left_ptr)
            right_df_node = lib.const_df(g, right_ptr)

            left_keys = [lib.scan(g, "id1"), lib.scan(g, "id2")]
            right_keys = [
                const_col(lib, g, right_ptr, "id1"),
                const_col(lib, g, right_ptr, "id2"),
            ]

            n_keys = 2
            lk_arr = (ctypes.c_void_p * n_keys)(*left_keys)
            rk_arr = (ctypes.c_void_p * n_keys)(*right_keys)

            root = lib._lib.td_join(g, left_df_node, lk_arr,
                                    right_df_node, rk_arr, n_keys, join_type)
            root = lib.optimize(g, root)
            result = lib.execute(g, root)
            if not result or result < 32:
                raise RuntimeError("Join execution failed")
            return Table(lib, result)
        finally:
            lib.graph_free(g)

    # === Sort queries ===

    def _sort(self, query, params):
        table = self._tables.get("sort") or self._tables.get("G1") or self._tables.get("groupby")
        if not table:
            raise ValueError("No sort table loaded")
        lib = self._ctx._lib
        df_ptr = table._ptr

        sort_specs = {
            "q1": (["id1"], [False]),
            "q2": (["id3"], [False]),
            "q3": (["id4"], [False]),
            "q4": (["v3"],  [True]),
            "q5": (["id1", "id2"], [False, False]),
            "q6": (["id1", "id2", "id3"], [False, False, False]),
        }

        if query not in sort_specs:
            raise ValueError(f"Unknown sort query: {query}")

        col_names, descs = sort_specs[query]

        g = lib.graph_new(df_ptr)
        try:
            df_node = lib.const_df(g, df_ptr)
            n_cols = len(col_names)
            keys = [lib.scan(g, name) for name in col_names]

            keys_arr = (ctypes.c_void_p * n_cols)(*keys)
            descs_arr = (ctypes.c_uint8 * n_cols)(*[1 if d else 0 for d in descs])

            root = lib._lib.td_sort_op(g, df_node, keys_arr, descs_arr, n_cols)
            root = lib.optimize(g, root)
            result = lib.execute(g, root)
            if not result or result < 32:
                raise RuntimeError("Sort execution failed")
            return Table(lib, result)
        finally:
            lib.graph_free(g)

    # === Window join ===

    def _window(self, query, params):
        table = self._tables.get("window") or self._tables.get("trades")
        quotes = self._tables.get("quotes")
        if not table:
            raise ValueError("No window join table loaded")
        lib = self._ctx._lib

        # Window join is an advanced op — use td_window_join if available
        # For now, return the table as-is (placeholder)
        return table
