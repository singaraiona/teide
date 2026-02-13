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
Teide — High-level Python API.

Usage:
    from teide.api import Context, col, lit

    with Context() as ctx:
        tbl = ctx.read_csv("data.csv")
        result = (
            tbl.filter(col("v1") > 0)
              .group_by("id1")
              .agg(col("v1").sum(), col("v3").mean())
              .sort("v1_sum", descending=True)
              .collect()
        )
        print(result.head(10))
"""

import ctypes
from teide import TeideLib, TD_I64, TD_F64, TD_I32, TD_BOOL, TD_ENUM, TD_TABLE, TD_STR
from teide import OP_SUM, OP_AVG, OP_MIN, OP_MAX, OP_COUNT, OP_FIRST, OP_LAST

# Opcode constants not in __init__
OP_PROD = 51

_DTYPE_NAMES = {
    TD_BOOL: "bool", TD_I32: "i32", TD_I64: "i64",
    TD_F64: "f64", TD_STR: "str", TD_ENUM: "sym",
}


def _format_val(val, dtype):
    """Format a single value for display."""
    if val is None:
        return "null"
    if dtype == TD_F64:
        # Strip trailing zeros: 49.9400 → 49.94, but keep at least one decimal
        s = f"{val:.4f}".rstrip("0")
        if s.endswith("."):
            s += "0"
        return s
    if dtype == TD_BOOL:
        return "true" if val else "false"
    return str(val)


def _is_numeric(dtype):
    return dtype in (TD_I64, TD_I32, TD_F64)


class Expr:
    """Column expression tree node.

    Leaf nodes: col("name") or lit(value).
    Internal nodes: binary/unary ops built via operator overloads.
    Aggregation nodes: .sum(), .mean(), etc.
    """

    def __init__(self, kind, **kw):
        self.kind = kind    # "col", "lit", "binop", "unop", "agg"
        self.kw = kw

    # --- Arithmetic ---
    def __add__(self, other):  return _binop("add", self, _wrap(other))
    def __radd__(self, other): return _binop("add", _wrap(other), self)
    def __sub__(self, other):  return _binop("sub", self, _wrap(other))
    def __rsub__(self, other): return _binop("sub", _wrap(other), self)
    def __mul__(self, other):  return _binop("mul", self, _wrap(other))
    def __rmul__(self, other): return _binop("mul", _wrap(other), self)
    def __truediv__(self, other):  return _binop("div", self, _wrap(other))
    def __rtruediv__(self, other): return _binop("div", _wrap(other), self)
    def __mod__(self, other):  return _binop("mod", self, _wrap(other))

    # --- Comparison ---
    def __eq__(self, other):  return _binop("eq", self, _wrap(other))
    def __ne__(self, other):  return _binop("ne", self, _wrap(other))
    def __lt__(self, other):  return _binop("lt", self, _wrap(other))
    def __le__(self, other):  return _binop("le", self, _wrap(other))
    def __gt__(self, other):  return _binop("gt", self, _wrap(other))
    def __ge__(self, other):  return _binop("ge", self, _wrap(other))

    # --- Logical ---
    def __and__(self, other): return _binop("and", self, _wrap(other))
    def __or__(self, other):  return _binop("or", self, _wrap(other))
    def __invert__(self):     return Expr("unop", op="not", arg=self)

    # --- Unary ---
    def __neg__(self):   return Expr("unop", op="neg", arg=self)
    def __abs__(self):   return Expr("unop", op="abs", arg=self)

    # --- Aggregations ---
    def sum(self):   return Expr("agg", op=OP_SUM,   arg=self)
    def mean(self):  return Expr("agg", op=OP_AVG,   arg=self)
    def min(self):   return Expr("agg", op=OP_MIN,   arg=self)
    def max(self):   return Expr("agg", op=OP_MAX,   arg=self)
    def count(self): return Expr("agg", op=OP_COUNT, arg=self)
    def first(self): return Expr("agg", op=OP_FIRST, arg=self)
    def last(self):  return Expr("agg", op=OP_LAST,  arg=self)

    def alias(self, name):
        """Assign an output name to this expression."""
        return Expr("alias", name=name, arg=self)


def col(name):
    """Reference a column by name."""
    return Expr("col", name=name)


def lit(value):
    """Create a literal constant."""
    return Expr("lit", value=value)


def _wrap(x):
    """Wrap a Python scalar as a lit() if not already an Expr."""
    if isinstance(x, Expr):
        return x
    return lit(x)


def _binop(op, left, right):
    return Expr("binop", op=op, left=left, right=right)


class Series:
    """Single column from a materialized Table."""

    def __init__(self, lib, vec_ptr, name, dtype):
        self._lib = lib
        self._ptr = vec_ptr
        self.name = name
        self.dtype = dtype

    def __len__(self):
        # Validate this is a vector (type > 0), not an atom
        type_byte = ctypes.cast(self._ptr, ctypes.POINTER(ctypes.c_int8))[18]
        if type_byte <= 0:
            return 0
        # Read len from td_t header (offset 24, int64)
        ptr_val = ctypes.cast(self._ptr, ctypes.POINTER(ctypes.c_int64))
        return ptr_val[3]  # offset 24 bytes = 3 int64s

    def _data_ptr(self):
        """Get actual data pointer, resolving slices to parent data."""
        attrs = ctypes.cast(self._ptr, ctypes.POINTER(ctypes.c_uint8))[19]
        if attrs & 0x10:  # TD_ATTR_SLICE
            parent = ctypes.cast(self._ptr, ctypes.POINTER(ctypes.c_void_p))[0]
            offset = ctypes.cast(self._ptr, ctypes.POINTER(ctypes.c_int64))[1]
            esz = {TD_F64: 8, TD_I64: 8, TD_I32: 4, TD_BOOL: 1, TD_ENUM: 4}.get(self.dtype, 1)
            return parent + 32 + offset * esz
        return self._ptr + 32

    def _get_val(self, i):
        """Get a single element by index. Avoids full materialization."""
        data_ptr = self._data_ptr()
        if self.dtype == TD_F64:
            return (ctypes.c_double * 1).from_address(data_ptr + i * 8)[0]
        elif self.dtype == TD_I64:
            return (ctypes.c_int64 * 1).from_address(data_ptr + i * 8)[0]
        elif self.dtype == TD_I32:
            return (ctypes.c_int32 * 1).from_address(data_ptr + i * 4)[0]
        elif self.dtype == TD_BOOL:
            return bool((ctypes.c_uint8 * 1).from_address(data_ptr + i)[0])
        elif self.dtype == TD_ENUM:
            sym_id = (ctypes.c_uint32 * 1).from_address(data_ptr + i * 4)[0]
            sym_ptr = self._lib.sym_str(sym_id)
            if sym_ptr:
                s = self._lib.str_ptr(sym_ptr)
                return s.decode('utf-8') if s else ""
            return ""
        return None

    def to_list(self):
        n = len(self)
        data_ptr = self._data_ptr()

        if self.dtype == TD_F64:
            arr = (ctypes.c_double * n).from_address(data_ptr)
            return [arr[i] for i in range(n)]
        elif self.dtype == TD_I64:
            arr = (ctypes.c_int64 * n).from_address(data_ptr)
            return [arr[i] for i in range(n)]
        elif self.dtype == TD_I32:
            arr = (ctypes.c_int32 * n).from_address(data_ptr)
            return [arr[i] for i in range(n)]
        elif self.dtype == TD_BOOL:
            arr = (ctypes.c_uint8 * n).from_address(data_ptr)
            return [bool(arr[i]) for i in range(n)]
        elif self.dtype == TD_ENUM:
            arr = (ctypes.c_uint32 * n).from_address(data_ptr)
            result = []
            for i in range(n):
                sym_ptr = self._lib.sym_str(arr[i])
                if sym_ptr:
                    s = self._lib.str_ptr(sym_ptr)
                    result.append(s.decode('utf-8') if s else "")
                else:
                    result.append("")
            return result
        else:
            return []

    def to_numpy(self):
        """Zero-copy numpy view for numeric types.

        Warning: The returned array shares memory with the C library.
        It is only valid while the parent Context is alive and the
        source Table has not been freed."""
        import numpy as np
        n = len(self)
        data_ptr = self._data_ptr()

        if self.dtype == TD_F64:
            return np.ctypeslib.as_array(
                (ctypes.c_double * n).from_address(data_ptr)
            )
        elif self.dtype == TD_I64:
            return np.ctypeslib.as_array(
                (ctypes.c_int64 * n).from_address(data_ptr)
            )
        elif self.dtype == TD_I32:
            return np.ctypeslib.as_array(
                (ctypes.c_int32 * n).from_address(data_ptr)
            )
        else:
            raise TypeError(f"to_numpy() not supported for dtype {self.dtype}")

    def __repr__(self):
        n = len(self)
        preview = self.to_list()[:5]
        suffix = ", ..." if n > 5 else ""
        return f"Series('{self.name}', len={n}, [{', '.join(str(x) for x in preview)}{suffix}])"


class Table:
    """Materialized table backed by a C td_t (type=TD_TABLE)."""

    def __init__(self, lib, tbl_ptr):
        self._lib = lib
        self._ptr = tbl_ptr

    @property
    def columns(self):
        ncols = self._lib.table_ncols(self._ptr)
        names = []
        for i in range(ncols):
            name_id = self._lib.table_col_name(self._ptr, i)
            sym_ptr = self._lib.sym_str(name_id)
            if sym_ptr:
                s = self._lib.str_ptr(sym_ptr)
                names.append(s.decode('utf-8') if s else f"V{i}")
            else:
                names.append(f"V{i}")
        return names

    @property
    def shape(self):
        return (self._lib.table_nrows(self._ptr), self._lib.table_ncols(self._ptr))

    def __len__(self):
        return self._lib.table_nrows(self._ptr)

    def __getitem__(self, name):
        """Get a Series by column name."""
        name_id = self._lib.sym_intern(name)
        vec_ptr = self._lib._lib.td_table_get_col(self._ptr, name_id)
        if not vec_ptr:
            raise KeyError(f"Column '{name}' not found")
        # Read type from td_t header (byte 18 is type field)
        type_byte = ctypes.cast(vec_ptr, ctypes.POINTER(ctypes.c_int8))[18]
        return Series(self._lib, vec_ptr, name, type_byte)

    def head(self, n=10):
        """Return a new Table with only the first n rows."""
        nrows = self._lib.table_nrows(self._ptr)
        if n >= nrows:
            return self
        ncols = self._lib.table_ncols(self._ptr)
        new_tbl = self._lib.table_new(ncols)
        for i in range(ncols):
            col_ptr = self._lib.table_get_col_idx(self._ptr, i)
            name_id = self._lib.table_col_name(self._ptr, i)
            sliced = self._lib._lib.td_vec_slice(col_ptr, 0, n)
            new_tbl = self._lib._lib.td_table_add_col(new_tbl, name_id, sliced)
            self._lib._lib.td_release(sliced)
        return Table(self._lib, new_tbl)

    def to_dict(self):
        """Convert to dict of column_name -> list."""
        result = {}
        for name in self.columns:
            result[name] = self[name].to_list()
        return result

    def to_pandas(self):
        """Convert to pandas DataFrame."""
        import pandas as pd
        return pd.DataFrame(self.to_dict())

    # --- Lazy entry points ---

    def filter(self, expr):
        return Query(self._lib, self._ptr).filter(expr)

    def group_by(self, *cols):
        return Query(self._lib, self._ptr).group_by(*cols)

    def sort(self, *cols, descending=False):
        return Query(self._lib, self._ptr).sort(*cols, descending=descending)

    def select(self, *exprs):
        return Query(self._lib, self._ptr).select(*exprs)

    def join(self, right, on, how="inner"):
        """Join with another table on shared key column(s).

        Args:
            right: Table to join with.
            on: Column name (str) or list of column names to join on.
            how: Join type — "inner" (default) or "left".
        Returns:
            Materialized Table with the join result.
        """
        if isinstance(on, str):
            on = [on]
        join_type = {"inner": 0, "left": 1}.get(how, 0)
        lib = self._lib
        g = lib.graph_new(self._ptr)
        try:
            left_table = lib.const_table(g, self._ptr)
            right_table = lib.const_table(g, right._ptr)
            left_keys = [lib.scan(g, k) for k in on]
            right_keys = [lib.const_vec(g, right[k]._ptr) for k in on]
            result_node = lib.join(g, left_table, left_keys, right_table, right_keys, join_type)
            root = lib.optimize(g, result_node)
            result_ptr = lib.execute(g, root)
            if not result_ptr or result_ptr < 32:
                raise RuntimeError(f"Join failed (error code {result_ptr})")
            return Table(lib, result_ptr)
        finally:
            lib.graph_free(g)

    def __repr__(self):
        rows, cols = self.shape
        col_names = self.columns
        return f"Table({rows} rows x {cols} cols: {col_names})"

    def __str__(self):
        return self._pretty(top_n=5, bottom_n=5)

    def _pretty(self, top_n=5, bottom_n=5):
        """Render table with box-drawing frames, top/bottom rows, and types."""
        nrows, ncols = self.shape
        col_names = self.columns

        # Gather Series objects and dtypes
        series = [self[name] for name in col_names]
        dtypes = [s.dtype for s in series]
        dtype_labels = [_DTYPE_NAMES.get(d, "?") for d in dtypes]

        # Determine which row indices to display
        truncated = nrows > top_n + bottom_n
        if truncated:
            row_indices = list(range(top_n)) + list(range(nrows - bottom_n, nrows))
        else:
            row_indices = list(range(nrows))

        # Format cell values
        cells = []  # list of rows, each row is list of formatted strings
        for r in row_indices:
            cells.append([_format_val(s._get_val(r), d) for s, d in zip(series, dtypes)])

        # Compute column widths (header, dtype label, all visible cells)
        widths = []
        for c in range(ncols):
            w = max(len(col_names[c]), len(dtype_labels[c]), 3)
            for row in cells:
                w = max(w, len(row[c]))
            widths.append(w)

        # Alignment: right for numeric, left for strings
        aligns = [">" if _is_numeric(d) else "<" for d in dtypes]

        # Ensure table is wide enough for footer text (with 1-space padding each side)
        footer_label = f"{nrows:,} rows x {ncols} columns"
        min_inner = len(footer_label) + 2  # 1 space on each side
        total_inner = sum(w + 2 for w in widths) + ncols - 1
        if total_inner < min_inner:
            widths[-1] += min_inner - total_inner
            total_inner = min_inner

        # Build lines — double outer edges, single inner dividers
        def hline(left, mid, right, fill="═"):
            segs = [fill * (w + 2) for w in widths]
            return left + mid.join(segs) + right

        def data_row(vals, edge="║"):
            parts = []
            for v, w, a in zip(vals, widths, aligns):
                parts.append(f" {v:{a}{w}} ")
            return edge + "│".join(parts) + edge

        def ellipsis_row():
            parts = []
            for w in widths:
                parts.append(f" {'···':^{w}} ")
            return "║" + "│".join(parts) + "║"

        lines = []
        lines.append(hline("╔", "╤", "╗"))

        # Header: column names + dtype labels (always right-aligned for consistency)
        def header_row(vals):
            parts = []
            for v, w in zip(vals, widths):
                parts.append(f" {v:>{w}} ")
            return "║" + "│".join(parts) + "║"

        lines.append(header_row(col_names))
        lines.append(header_row(dtype_labels))

        lines.append(hline("╠", "╪", "╣"))

        # Data rows
        if truncated:
            for row in cells[:top_n]:
                lines.append(data_row(row))
            lines.append(ellipsis_row())
            for row in cells[top_n:]:
                lines.append(data_row(row))
        else:
            for row in cells:
                lines.append(data_row(row))

        # Footer — ╧ where columns end, centered text
        lines.append(hline("╠", "╧", "╣"))
        lines.append("║" + footer_label.center(total_inner) + "║")
        lines.append("╚" + "═" * total_inner + "╝")

        return "\n".join(lines)


class GroupBy:
    """Intermediate state from .group_by(). Call .agg() to produce a Query."""

    def __init__(self, lib, tbl_ptr, key_cols, prior_ops):
        self._lib = lib
        self._ptr = tbl_ptr
        self._key_cols = key_cols
        self._prior_ops = prior_ops

    def agg(self, *exprs):
        """Aggregate with given expressions, returning a Query."""
        q = Query(self._lib, self._ptr)
        q._ops = list(self._prior_ops)
        q._ops.append(("group", self._key_cols, list(exprs)))
        return q


class Query:
    """Lazy computation builder. Records ops, executes on .collect()."""

    def __init__(self, lib, tbl_ptr):
        self._lib = lib
        self._ptr = tbl_ptr
        self._ops = []

    def filter(self, expr):
        self._ops.append(("filter", expr))
        return self

    def group_by(self, *cols):
        return GroupBy(self._lib, self._ptr, list(cols), self._ops)

    def sort(self, *cols, descending=False):
        if isinstance(descending, bool):
            descs = [descending] * len(cols)
        else:
            descs = list(descending)
        self._ops.append(("sort", list(cols), descs))
        return self

    def head(self, n):
        self._ops.append(("head", n))
        return self

    def select(self, *exprs):
        self._ops.append(("select", list(exprs)))
        return self

    def collect(self):
        """Build graph, optimize, execute, return Table."""
        lib = self._lib
        g = lib.graph_new(self._ptr)
        try:
            result_node, _pinned = self._execute_ops(g)
            root = lib.optimize(g, result_node)
            result_ptr = lib.execute(g, root)
            if not result_ptr or result_ptr < 32:
                raise RuntimeError(f"Execution failed (error code {result_ptr})")
            return Table(lib, result_ptr)
        finally:
            lib.graph_free(g)

    def _execute_ops(self, g):
        """Walk _ops list, emit graph nodes, return (final_node, pinned_arrays).

        pinned_arrays keeps ctypes arrays alive — the C graph stores raw
        pointers into them, so they must not be GC'd before execute().
        """
        lib = self._lib
        current = None  # pipeline state
        filter_pred = None  # pending Table-level filter predicate
        pinned = []  # prevent GC of ctypes arrays passed to C

        for op in self._ops:
            if op[0] == "filter":
                expr = op[1]
                pred_node = _emit_expr(lib, g, expr)
                if current is None:
                    # Table-level filter: store predicate to apply to
                    # downstream scan nodes (group-by keys, agg inputs, etc.)
                    filter_pred = pred_node
                else:
                    current = lib._lib.td_filter(g, current, pred_node)

            elif op[0] == "group":
                key_col_names, agg_exprs = op[1], op[2]

                # Build key scan nodes, applying pending filter if any
                n_keys = len(key_col_names)
                key_nodes = []
                for name in key_col_names:
                    node = lib.scan(g, name)
                    if filter_pred is not None:
                        node = lib._lib.td_filter(g, node, filter_pred)
                    key_nodes.append(node)

                # Decompose agg expressions into (opcode, input_scan_node)
                agg_ops = []
                agg_inputs = []
                for agg_expr in agg_exprs:
                    if agg_expr.kind != "agg":
                        raise ValueError("group_by.agg() requires aggregation expressions")
                    agg_ops.append(agg_expr.kw["op"])
                    inner = agg_expr.kw["arg"]
                    input_node = _emit_expr(lib, g, inner)
                    if filter_pred is not None:
                        input_node = lib._lib.td_filter(g, input_node, filter_pred)
                    agg_inputs.append(input_node)

                filter_pred = None  # consumed

                n_aggs = len(agg_ops)
                keys_arr = (ctypes.c_void_p * n_keys)(*key_nodes)
                ops_arr = (ctypes.c_uint16 * n_aggs)(*agg_ops)
                ins_arr = (ctypes.c_void_p * n_aggs)(*agg_inputs)
                pinned.extend([keys_arr, ops_arr, ins_arr])
                current = lib._lib.td_group(g, keys_arr, n_keys, ops_arr, ins_arr, n_aggs)

            elif op[0] == "sort":
                col_names, descs = op[1], op[2]
                n_cols = len(col_names)

                # Sort needs a table_node — use const_table if we have a result, else use bound table
                if current is not None:
                    table_node = current
                else:
                    table_node = lib.const_table(g, self._ptr)

                key_nodes = []
                for name in col_names:
                    node = lib.scan(g, name)
                    if filter_pred is not None:
                        node = lib._lib.td_filter(g, node, filter_pred)
                    key_nodes.append(node)

                filter_pred = None  # consumed

                keys_arr = (ctypes.c_void_p * n_cols)(*key_nodes)
                descs_arr = (ctypes.c_uint8 * n_cols)(*[1 if d else 0 for d in descs])
                pinned.extend([keys_arr, descs_arr])
                current = lib._lib.td_sort_op(g, table_node, keys_arr, descs_arr, n_cols)

            elif op[0] == "head":
                n = op[1]
                if current is None:
                    current = lib.const_table(g, self._ptr)
                current = lib.head(g, current, n)

            elif op[0] == "select":
                raise NotImplementedError(
                    "select() is not yet supported in lazy queries. "
                    "Use Table['col_name'] after collect() instead."
                )

        if current is None:
            # No ops — just return the bound table
            current = lib.const_table(g, self._ptr)

        return current, pinned


def _emit_expr(lib, g, expr):
    """Recursively emit graph nodes for an Expr tree."""
    kind = expr.kind

    if kind == "col":
        return lib.scan(g, expr.kw["name"])

    elif kind == "lit":
        val = expr.kw["value"]
        if isinstance(val, float):
            return lib.const_f64(g, val)
        elif isinstance(val, bool):
            return lib._lib.td_const_bool(g, val)
        elif isinstance(val, int):
            return lib.const_i64(g, val)
        else:
            raise TypeError(f"Unsupported literal type: {type(val)}")

    elif kind == "binop":
        left = _emit_expr(lib, g, expr.kw["left"])
        right = _emit_expr(lib, g, expr.kw["right"])
        op = expr.kw["op"]

        binop_map = {
            "add": lib._lib.td_add,
            "sub": lib._lib.td_sub,
            "mul": lib._lib.td_mul,
            "div": lib._lib.td_div,
            "mod": lib._lib.td_mod,
            "eq":  lib._lib.td_eq,
            "ne":  lib._lib.td_ne,
            "lt":  lib._lib.td_lt,
            "le":  lib._lib.td_le,
            "gt":  lib._lib.td_gt,
            "ge":  lib._lib.td_ge,
            "and": lib._lib.td_and,
            "or":  lib._lib.td_or,
        }
        fn = binop_map.get(op)
        if not fn:
            raise ValueError(f"Unknown binary op: {op}")
        return fn(g, left, right)

    elif kind == "unop":
        arg = _emit_expr(lib, g, expr.kw["arg"])
        op = expr.kw["op"]

        unop_map = {
            "neg": lib._lib.td_neg,
            "abs": lib._lib.td_abs,
            "not": lib._lib.td_not,
        }
        fn = unop_map.get(op)
        if not fn:
            raise ValueError(f"Unknown unary op: {op}")
        return fn(g, arg)

    elif kind == "agg":
        # Standalone aggregation (not within group_by)
        arg_node = _emit_expr(lib, g, expr.kw["arg"])
        opcode = expr.kw["op"]

        agg_map = {
            OP_SUM:   lib._lib.td_sum,
            OP_AVG:   lib._lib.td_avg,
            OP_MIN:   lib._lib.td_min_op,
            OP_MAX:   lib._lib.td_max_op,
            OP_COUNT: lib._lib.td_count,
            OP_FIRST: lib._lib.td_first,
            OP_LAST:  lib._lib.td_last,
        }
        fn = agg_map.get(opcode)
        if not fn:
            raise ValueError(f"Unknown agg opcode: {opcode}")
        return fn(g, arg_node)

    elif kind == "alias":
        return _emit_expr(lib, g, expr.kw["arg"])

    else:
        raise ValueError(f"Unknown expression kind: {kind}")


class Context:
    """Manages TeideLib lifecycle. Use as context manager."""

    def __init__(self, lib_path=None):
        self._lib = TeideLib(lib_path)
        self._lib.sym_init()
        self._lib.arena_init()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._lib.pool_destroy()   # stop worker threads first
        self._lib.sym_destroy()    # release interned strings (still in arena)
        self._lib.arena_destroy_all()  # unmap arena memory last

    def read_csv(self, path):
        """Read a CSV file into a Table."""
        tbl_ptr = self._lib.csv_read(path)
        if not tbl_ptr or tbl_ptr < 32:
            raise RuntimeError(f"Failed to read CSV: {path}")
        return Table(self._lib, tbl_ptr)

    def splay_save(self, table, path):
        """Save a Table as a splayed directory (one file per column)."""
        err = self._lib.splay_save(table._ptr, path)
        if err != 0:
            raise RuntimeError(f"splay_save failed (error code {err})")

    def splay_load(self, path):
        """Load a splayed table from a directory."""
        ptr = self._lib.splay_load(path)
        if not ptr or ptr < 32:
            raise RuntimeError(f"Failed to load splayed table: {path}")
        return Table(self._lib, ptr)

    def part_load(self, db_root, table_name):
        """Load a date-partitioned table (db_root/YYYY.MM.DD/table_name)."""
        ptr = self._lib.part_load(db_root, table_name)
        if not ptr or ptr < 32:
            raise RuntimeError(f"Failed to load partitioned table: {db_root}/{table_name}")
        return Table(self._lib, ptr)

    def mem_stats(self):
        """Return memory statistics (placeholder)."""
        return {"note": "mem_stats requires td_mem_stats binding"}
