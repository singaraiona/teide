"""
Teide — High-level Python API.

Usage:
    from teide.api import Context, col, lit

    with Context() as ctx:
        df = ctx.read_csv("data.csv")
        result = (
            df.filter(col("v1") > 0)
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
        # Read len from td_t header (offset 16, int64)
        ptr_val = ctypes.cast(self._ptr, ctypes.POINTER(ctypes.c_int64))
        return ptr_val[2]  # offset 16 bytes = 2 int64s

    def to_list(self):
        n = len(self)
        data_ptr = ctypes.cast(
            ctypes.c_void_p(self._ptr + 32),
            ctypes.c_void_p
        ).value

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
        """Zero-copy numpy view for numeric types."""
        import numpy as np
        n = len(self)
        data_ptr = self._ptr + 32

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
    """Materialized dataframe backed by a C td_t (type=TD_TABLE)."""

    def __init__(self, lib, df_ptr):
        self._lib = lib
        self._ptr = df_ptr

    @property
    def columns(self):
        ncols = self._lib.df_ncols(self._ptr)
        names = []
        for i in range(ncols):
            name_id = self._lib.df_col_name(self._ptr, i)
            sym_ptr = self._lib.sym_str(name_id)
            if sym_ptr:
                s = self._lib.str_ptr(sym_ptr)
                names.append(s.decode('utf-8') if s else f"V{i}")
            else:
                names.append(f"V{i}")
        return names

    @property
    def shape(self):
        return (self._lib.df_nrows(self._ptr), self._lib.df_ncols(self._ptr))

    def __len__(self):
        return self._lib.df_nrows(self._ptr)

    def __getitem__(self, name):
        """Get a Series by column name."""
        name_id = self._lib.sym_intern(name)
        vec_ptr = self._lib._lib.td_table_get_col(self._ptr, name_id)
        if not vec_ptr:
            raise KeyError(f"Column '{name}' not found")
        # Read type from td_t header (offset 0, byte 0 is type)
        type_byte = ctypes.cast(vec_ptr, ctypes.POINTER(ctypes.c_int8))[0]
        return Series(self._lib, vec_ptr, name, type_byte)

    def head(self, n=10):
        """Return a new Table with only the first n rows."""
        nrows = self._lib.df_nrows(self._ptr)
        if n >= nrows:
            return self
        ncols = self._lib.df_ncols(self._ptr)
        new_df = self._lib.df_new(ncols)
        for i in range(ncols):
            col_ptr = self._lib.df_get_col_idx(self._ptr, i)
            name_id = self._lib.df_col_name(self._ptr, i)
            sliced = self._lib._lib.td_vec_slice(col_ptr, 0, n)
            new_df = self._lib._lib.td_table_add_col(new_df, name_id, sliced)
            self._lib._lib.td_release(sliced)
        return Table(self._lib, new_df)

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

    def __repr__(self):
        rows, cols = self.shape
        col_names = self.columns
        return f"Table({rows} rows x {cols} cols: {col_names})"


class GroupBy:
    """Intermediate state from .group_by(). Call .agg() to produce a Query."""

    def __init__(self, lib, df_ptr, key_cols, prior_ops):
        self._lib = lib
        self._ptr = df_ptr
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

    def __init__(self, lib, df_ptr):
        self._lib = lib
        self._ptr = df_ptr
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
            result_node = self._execute_ops(g)
            root = lib.optimize(g, result_node)
            result_ptr = lib.execute(g, root)
            if not result_ptr or result_ptr < 32:
                raise RuntimeError(f"Execution failed (error code {result_ptr})")
            return Table(lib, result_ptr)
        finally:
            lib.graph_free(g)

    def _execute_ops(self, g):
        """Walk _ops list, emit graph nodes, return final node."""
        lib = self._lib
        current = None  # pipeline state

        for op in self._ops:
            if op[0] == "filter":
                expr = op[1]
                pred_node = _emit_expr(lib, g, expr)
                if current is None:
                    # Build a scan-based filter: need individual column scans
                    # Actually filter needs an input and a predicate
                    # For the first op, the input is implicitly the bound df columns
                    # We need a dummy input — use scan of first column
                    current = pred_node  # pred is the mask
                    # Filter takes (input_col, pred)—but we operate on DataFrame level
                    # The executor's filter handles it at the column level
                    current = lib._lib.td_filter(g, pred_node, pred_node)
                else:
                    current = lib._lib.td_filter(g, current, pred_node)

            elif op[0] == "group":
                key_col_names, agg_exprs = op[1], op[2]

                # Build key scan nodes
                n_keys = len(key_col_names)
                key_nodes = []
                for name in key_col_names:
                    key_nodes.append(lib.scan(g, name))

                # Decompose agg expressions into (opcode, input_scan_node)
                agg_ops = []
                agg_inputs = []
                for agg_expr in agg_exprs:
                    if agg_expr.kind != "agg":
                        raise ValueError("group_by.agg() requires aggregation expressions")
                    agg_ops.append(agg_expr.kw["op"])
                    inner = agg_expr.kw["arg"]
                    agg_inputs.append(_emit_expr(lib, g, inner))

                n_aggs = len(agg_ops)
                keys_arr = (ctypes.c_void_p * n_keys)(*key_nodes)
                ops_arr = (ctypes.c_uint16 * n_aggs)(*agg_ops)
                ins_arr = (ctypes.c_void_p * n_aggs)(*agg_inputs)
                current = lib._lib.td_group(g, keys_arr, n_keys, ops_arr, ins_arr, n_aggs)

            elif op[0] == "sort":
                col_names, descs = op[1], op[2]
                n_cols = len(col_names)

                # Sort needs a df_node — use td_const_df if we have a result, else use bound df
                if current is not None:
                    df_node = current
                else:
                    df_node = lib.const_df(g, self._ptr)

                key_nodes = []
                for name in col_names:
                    key_nodes.append(lib.scan(g, name))

                keys_arr = (ctypes.c_void_p * n_cols)(*key_nodes)
                descs_arr = (ctypes.c_uint8 * n_cols)(*[1 if d else 0 for d in descs])
                current = lib._lib.td_sort_op(g, df_node, keys_arr, descs_arr, n_cols)

            elif op[0] == "head":
                n = op[1]
                if current is None:
                    current = lib.const_df(g, self._ptr)
                current = lib.head(g, current, n)

            elif op[0] == "select":
                # Project specific columns
                exprs = op[1]
                nodes = [_emit_expr(lib, g, e) for e in exprs]
                # For now, just return the last node (TODO: proper projection)
                if nodes:
                    current = nodes[-1]

        if current is None:
            # No ops — just return the bound df
            current = lib.const_df(g, self._ptr)

        return current


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
        self._lib.arena_destroy_all()
        self._lib.sym_destroy()

    def read_csv(self, path):
        """Read a CSV file into a Table."""
        df_ptr = self._lib.csv_read(path)
        if not df_ptr or df_ptr < 32:
            raise RuntimeError(f"Failed to read CSV: {path}")
        return Table(self._lib, df_ptr)

    def mem_stats(self):
        """Return memory statistics (placeholder)."""
        return {"note": "mem_stats requires td_mem_stats binding"}
