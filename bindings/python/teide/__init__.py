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
Teide — Fast columnar table engine for Python.

Usage:
    from teide import Context, col, lit

    with Context() as ctx:
        tbl = ctx.read_csv("data.csv")
        result = tbl.group_by("id1").agg(col("v1").sum()).collect()
        print(result)
"""

__version__ = "0.1.0"

import ctypes
import os
import sys

# Type aliases
c_td_p = ctypes.c_void_p    # td_t*
c_graph_p = ctypes.c_void_p # td_graph_t*
c_op_p = ctypes.c_void_p    # td_op_t*


def _find_lib():
    """Find libteide shared library.

    Search order:
    1. Same directory as this __init__.py (wheel-bundled layout)
    2. TEIDE_LIB environment variable
    3. Common build directories relative to source tree
    """
    _pkg_dir = os.path.dirname(__file__)
    _lib_names = ["libteide.so", "libteide.dylib"]

    # 1. Wheel-bundled: libteide.so sits next to __init__.py
    for name in _lib_names:
        path = os.path.join(_pkg_dir, name)
        if os.path.exists(path):
            return os.path.abspath(path)

    # 2. Explicit env var
    env_path = os.environ.get("TEIDE_LIB")
    if env_path and os.path.exists(env_path):
        return env_path

    # 3. Source-tree build directories
    search_dirs = [
        os.path.join(_pkg_dir, "..", "..", "..", "build_release"),
        os.path.join(_pkg_dir, "..", "..", "..", "build"),
        ".",
    ]
    for d in search_dirs:
        for name in _lib_names:
            path = os.path.join(d, name)
            if os.path.exists(path):
                return os.path.abspath(path)

    raise OSError(
        "Cannot find libteide.so. Install via 'pip install teide' or set TEIDE_LIB."
    )


class TeideLib:
    """Low-level ctypes wrapper around libteide C API."""

    def __init__(self, lib_path=None):
        if lib_path is None:
            lib_path = _find_lib()
        self._lib = ctypes.CDLL(lib_path)
        self._setup_signatures()

    def _setup_signatures(self):
        lib = self._lib

        # ===== Memory / Arena =====
        lib.td_arena_init.argtypes = []
        lib.td_arena_init.restype = None

        lib.td_arena_destroy_all.argtypes = []
        lib.td_arena_destroy_all.restype = None

        lib.td_pool_destroy.argtypes = []
        lib.td_pool_destroy.restype = None

        lib.td_alloc.argtypes = [ctypes.c_size_t]
        lib.td_alloc.restype = c_td_p

        lib.td_free.argtypes = [c_td_p]
        lib.td_free.restype = None

        # ===== COW / Refcount =====
        lib.td_retain.argtypes = [c_td_p]
        lib.td_retain.restype = None

        lib.td_release.argtypes = [c_td_p]
        lib.td_release.restype = None

        # ===== Symbol Table =====
        lib.td_sym_init.argtypes = []
        lib.td_sym_init.restype = None

        lib.td_sym_destroy.argtypes = []
        lib.td_sym_destroy.restype = None

        lib.td_sym_intern.argtypes = [ctypes.c_char_p, ctypes.c_size_t]
        lib.td_sym_intern.restype = ctypes.c_int64

        lib.td_sym_find.argtypes = [ctypes.c_char_p, ctypes.c_size_t]
        lib.td_sym_find.restype = ctypes.c_int64

        lib.td_sym_str.argtypes = [ctypes.c_int64]
        lib.td_sym_str.restype = c_td_p

        # ===== Atom Constructors =====
        lib.td_i64.argtypes = [ctypes.c_int64]
        lib.td_i64.restype = c_td_p

        lib.td_f64.argtypes = [ctypes.c_double]
        lib.td_f64.restype = c_td_p

        lib.td_bool.argtypes = [ctypes.c_bool]
        lib.td_bool.restype = c_td_p

        # ===== Vector API =====
        lib.td_vec_new.argtypes = [ctypes.c_int8, ctypes.c_int64]
        lib.td_vec_new.restype = c_td_p

        lib.td_vec_append.argtypes = [c_td_p, ctypes.c_void_p]
        lib.td_vec_append.restype = c_td_p

        lib.td_vec_from_raw.argtypes = [ctypes.c_int8, ctypes.c_void_p, ctypes.c_int64]
        lib.td_vec_from_raw.restype = c_td_p

        lib.td_vec_slice.argtypes = [c_td_p, ctypes.c_int64, ctypes.c_int64]
        lib.td_vec_slice.restype = c_td_p

        lib.td_vec_get.argtypes = [c_td_p, ctypes.c_int64]
        lib.td_vec_get.restype = ctypes.c_void_p

        # ===== String API =====
        lib.td_str_ptr.argtypes = [c_td_p]
        lib.td_str_ptr.restype = ctypes.c_char_p

        lib.td_str_len.argtypes = [c_td_p]
        lib.td_str_len.restype = ctypes.c_size_t

        # ===== Table API =====
        lib.td_table_new.argtypes = [ctypes.c_int64]
        lib.td_table_new.restype = c_td_p

        lib.td_table_add_col.argtypes = [c_td_p, ctypes.c_int64, c_td_p]
        lib.td_table_add_col.restype = c_td_p

        lib.td_table_get_col.argtypes = [c_td_p, ctypes.c_int64]
        lib.td_table_get_col.restype = c_td_p

        lib.td_table_get_col_idx.argtypes = [c_td_p, ctypes.c_int64]
        lib.td_table_get_col_idx.restype = c_td_p

        lib.td_table_col_name.argtypes = [c_td_p, ctypes.c_int64]
        lib.td_table_col_name.restype = ctypes.c_int64

        lib.td_table_ncols.argtypes = [c_td_p]
        lib.td_table_ncols.restype = ctypes.c_int64

        lib.td_table_nrows.argtypes = [c_td_p]
        lib.td_table_nrows.restype = ctypes.c_int64

        # ===== Graph API =====
        lib.td_graph_new.argtypes = [c_td_p]
        lib.td_graph_new.restype = c_graph_p

        lib.td_graph_free.argtypes = [c_graph_p]
        lib.td_graph_free.restype = None

        # ===== Source Ops =====
        lib.td_scan.argtypes = [c_graph_p, ctypes.c_char_p]
        lib.td_scan.restype = c_op_p

        lib.td_const_f64.argtypes = [c_graph_p, ctypes.c_double]
        lib.td_const_f64.restype = c_op_p

        lib.td_const_i64.argtypes = [c_graph_p, ctypes.c_int64]
        lib.td_const_i64.restype = c_op_p

        lib.td_const_bool.argtypes = [c_graph_p, ctypes.c_bool]
        lib.td_const_bool.restype = c_op_p

        lib.td_const_vec.argtypes = [c_graph_p, c_td_p]
        lib.td_const_vec.restype = c_op_p

        lib.td_const_table.argtypes = [c_graph_p, c_td_p]
        lib.td_const_table.restype = c_op_p

        # ===== Element-wise Ops =====
        for name in ['td_add', 'td_sub', 'td_mul', 'td_div', 'td_mod',
                      'td_eq', 'td_ne', 'td_lt', 'td_le', 'td_gt', 'td_ge',
                      'td_and', 'td_or', 'td_min2', 'td_max2']:
            fn = getattr(lib, name)
            fn.argtypes = [c_graph_p, c_op_p, c_op_p]
            fn.restype = c_op_p

        for name in ['td_neg', 'td_abs', 'td_not', 'td_sqrt_op', 'td_log_op',
                      'td_exp_op', 'td_ceil_op', 'td_floor_op', 'td_isnull']:
            fn = getattr(lib, name)
            fn.argtypes = [c_graph_p, c_op_p]
            fn.restype = c_op_p

        # ===== Reduction Ops =====
        for name in ['td_sum', 'td_prod', 'td_min_op', 'td_max_op',
                      'td_count', 'td_avg', 'td_first', 'td_last']:
            fn = getattr(lib, name)
            fn.argtypes = [c_graph_p, c_op_p]
            fn.restype = c_op_p

        # ===== Structural Ops =====
        lib.td_filter.argtypes = [c_graph_p, c_op_p, c_op_p]
        lib.td_filter.restype = c_op_p

        # Sort: (graph, table_node, keys**, descs*, nulls_first*, n_cols)
        lib.td_sort_op.argtypes = [c_graph_p, c_op_p,
                                    ctypes.POINTER(c_op_p),
                                    ctypes.POINTER(ctypes.c_uint8),
                                    ctypes.POINTER(ctypes.c_uint8),
                                    ctypes.c_uint8]
        lib.td_sort_op.restype = c_op_p

        # Group: (graph, keys**, n_keys, agg_ops*, agg_ins**, n_aggs)
        lib.td_group.argtypes = [c_graph_p,
                                  ctypes.POINTER(c_op_p), ctypes.c_uint8,
                                  ctypes.POINTER(ctypes.c_uint16),
                                  ctypes.POINTER(c_op_p), ctypes.c_uint8]
        lib.td_group.restype = c_op_p

        # Join: (graph, left, left_keys, right, right_keys, n_keys, join_type)
        lib.td_join.argtypes = [c_graph_p,
                                 c_op_p, ctypes.POINTER(c_op_p),
                                 c_op_p, ctypes.POINTER(c_op_p),
                                 ctypes.c_uint8, ctypes.c_uint8]
        lib.td_join.restype = c_op_p

        lib.td_head.argtypes = [c_graph_p, c_op_p, ctypes.c_int64]
        lib.td_head.restype = c_op_p

        lib.td_tail.argtypes = [c_graph_p, c_op_p, ctypes.c_int64]
        lib.td_tail.restype = c_op_p

        # ===== Optimizer & Executor =====
        lib.td_optimize.argtypes = [c_graph_p, c_op_p]
        lib.td_optimize.restype = c_op_p

        lib.td_execute.argtypes = [c_graph_p, c_op_p]
        lib.td_execute.restype = c_td_p

        # ===== CSV =====
        lib.td_csv_read.argtypes = [ctypes.c_char_p]
        lib.td_csv_read.restype = c_td_p

        lib.td_csv_read_opts.argtypes = [ctypes.c_char_p, ctypes.c_char, ctypes.c_bool,
                                            ctypes.POINTER(ctypes.c_int8), ctypes.c_int32]
        lib.td_csv_read_opts.restype = c_td_p

        # ===== Storage (splay / partitioned) =====
        lib.td_splay_save.argtypes = [c_td_p, ctypes.c_char_p]
        lib.td_splay_save.restype = ctypes.c_int32  # td_err_t

        lib.td_splay_load.argtypes = [ctypes.c_char_p]
        lib.td_splay_load.restype = c_td_p

        lib.td_part_load.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        lib.td_part_load.restype = c_td_p

    # ===== Convenience methods =====

    def sym_init(self):
        self._lib.td_sym_init()

    def sym_destroy(self):
        self._lib.td_sym_destroy()

    def arena_init(self):
        self._lib.td_arena_init()

    def arena_destroy_all(self):
        self._lib.td_arena_destroy_all()

    def pool_destroy(self):
        self._lib.td_pool_destroy()

    def retain(self, ptr):
        self._lib.td_retain(ptr)

    def release(self, ptr):
        self._lib.td_release(ptr)

    def csv_read(self, path):
        return self._lib.td_csv_read(path.encode('utf-8'))

    def graph_new(self, tbl):
        return self._lib.td_graph_new(tbl)

    def graph_free(self, g):
        self._lib.td_graph_free(g)

    def scan(self, g, col_name):
        return self._lib.td_scan(g, col_name.encode('utf-8'))

    def const_f64(self, g, val):
        return self._lib.td_const_f64(g, ctypes.c_double(val))

    def const_i64(self, g, val):
        return self._lib.td_const_i64(g, ctypes.c_int64(val))

    def const_vec(self, g, vec):
        return self._lib.td_const_vec(g, vec)

    def const_table(self, g, tbl):
        return self._lib.td_const_table(g, tbl)

    def add(self, g, a, b):  return self._lib.td_add(g, a, b)
    def sub(self, g, a, b):  return self._lib.td_sub(g, a, b)
    def mul(self, g, a, b):  return self._lib.td_mul(g, a, b)
    def div(self, g, a, b):  return self._lib.td_div(g, a, b)

    def eq(self, g, a, b):   return self._lib.td_eq(g, a, b)
    def ne(self, g, a, b):   return self._lib.td_ne(g, a, b)
    def lt(self, g, a, b):   return self._lib.td_lt(g, a, b)
    def le(self, g, a, b):   return self._lib.td_le(g, a, b)
    def gt(self, g, a, b):   return self._lib.td_gt(g, a, b)
    def ge(self, g, a, b):   return self._lib.td_ge(g, a, b)
    def and_op(self, g, a, b): return self._lib.td_and(g, a, b)
    def or_op(self, g, a, b):  return self._lib.td_or(g, a, b)

    def sum(self, g, a):     return self._lib.td_sum(g, a)
    def avg(self, g, a):     return self._lib.td_avg(g, a)
    def min_op(self, g, a):  return self._lib.td_min_op(g, a)
    def max_op(self, g, a):  return self._lib.td_max_op(g, a)
    def count(self, g, a):   return self._lib.td_count(g, a)
    def first(self, g, a):   return self._lib.td_first(g, a)
    def last(self, g, a):    return self._lib.td_last(g, a)

    def filter(self, g, input_op, pred):
        return self._lib.td_filter(g, input_op, pred)

    def sort_op(self, g, table_node, keys, descs, nulls_first=None):
        n = len(keys)
        keys_arr = (c_op_p * n)(*keys)
        descs_arr = (ctypes.c_uint8 * n)(*descs)
        nf = (ctypes.c_uint8 * n)(*nulls_first) if nulls_first else None
        return self._lib.td_sort_op(g, table_node, keys_arr, descs_arr, nf, n)

    def group(self, g, keys, agg_ops, agg_ins):
        n_keys = len(keys)
        n_aggs = len(agg_ops)
        keys_arr = (c_op_p * n_keys)(*keys)
        ops_arr = (ctypes.c_uint16 * n_aggs)(*agg_ops)
        ins_arr = (c_op_p * n_aggs)(*agg_ins)
        return self._lib.td_group(g, keys_arr, n_keys, ops_arr, ins_arr, n_aggs)

    def join(self, g, left_table, left_keys, right_table, right_keys, join_type=0):
        n = len(left_keys)
        lk = (c_op_p * n)(*left_keys)
        rk = (c_op_p * n)(*right_keys)
        return self._lib.td_join(g, left_table, lk, right_table, rk, n, join_type)

    def head(self, g, input_op, n):
        return self._lib.td_head(g, input_op, n)

    def optimize(self, g, root):
        return self._lib.td_optimize(g, root)

    def execute(self, g, root):
        return self._lib.td_execute(g, root)

    def table_ncols(self, tbl):
        return self._lib.td_table_ncols(tbl)

    def table_nrows(self, tbl):
        return self._lib.td_table_nrows(tbl)

    def table_get_col_idx(self, tbl, idx):
        return self._lib.td_table_get_col_idx(tbl, idx)

    def table_col_name(self, tbl, idx):
        return self._lib.td_table_col_name(tbl, idx)

    def sym_str(self, sym_id):
        return self._lib.td_sym_str(sym_id)

    def str_ptr(self, s):
        return self._lib.td_str_ptr(s)

    def str_len(self, s):
        return self._lib.td_str_len(s)

    def sym_intern(self, s):
        b = s.encode('utf-8')
        return self._lib.td_sym_intern(b, len(b))

    def vec_from_raw_i64(self, data):
        arr = (ctypes.c_int64 * len(data))(*data)
        return self._lib.td_vec_from_raw(6, arr, len(data))  # TD_I64 = 6

    def vec_from_raw_f64(self, data):
        arr = (ctypes.c_double * len(data))(*data)
        return self._lib.td_vec_from_raw(7, arr, len(data))  # TD_F64 = 7

    def table_new(self, ncols):
        return self._lib.td_table_new(ncols)

    def table_add_col(self, tbl, name_id, col):
        return self._lib.td_table_add_col(tbl, name_id, col)

    def splay_save(self, tbl, path):
        return self._lib.td_splay_save(tbl, path.encode('utf-8'))

    def splay_load(self, path):
        return self._lib.td_splay_load(path.encode('utf-8'))

    def part_load(self, db_root, table_name):
        return self._lib.td_part_load(db_root.encode('utf-8'),
                                       table_name.encode('utf-8'))


# Type constants (mirror C defines)
TD_LIST = 0
TD_BOOL = 1
TD_U8 = 2
TD_CHAR = 3
TD_I16 = 4
TD_I32 = 5
TD_I64 = 6
TD_F64 = 7
TD_STR = 8
TD_DATE = 9
TD_TIME = 10
TD_TIMESTAMP = 11
TD_ENUM = 15
TD_TABLE = 13

# Opcode constants
OP_SUM = 50
OP_PROD = 51
OP_MIN = 52
OP_MAX = 53
OP_COUNT = 54
OP_AVG = 55
OP_FIRST = 56
OP_LAST = 57

# Re-export high-level API so users can do: from teide import Context, col, lit
from teide.api import Context, col, lit, Expr, Table, Series, Query  # noqa: E402,F401
