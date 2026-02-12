//! teide: Safe Rust wrappers for the Teide C17 columnar dataframe engine.
//!
//! Provides idiomatic, safe types around the raw FFI layer.

#![deny(unsafe_op_in_unsafe_fn)]

// Force linkage of the Teide C static library via teide-sys's build script.
extern crate teide_sys;

use std::ffi::CString;
use std::marker::PhantomData;

// ---------------------------------------------------------------------------
// Raw FFI declarations — these mirror what teide-sys will export.
// Once teide-sys is fully populated we can replace these with `use teide_sys::*`.
// ---------------------------------------------------------------------------

#[allow(non_camel_case_types, dead_code)]
mod ffi {
    use std::os::raw::{c_char, c_int};

    // ---- Core types -------------------------------------------------------

    /// 32-byte block header — the fundamental C object.
    #[repr(C)]
    pub struct td_t {
        pub _nullmap: [u8; 16],
        pub mmod: u8,
        pub order: u8,
        pub type_: i8,
        pub attrs: u8,
        pub rc: u32,  // _Atomic in C, but we never touch it from Rust
        pub val: [u8; 8], // union: i64, f64, len, etc.
    }

    /// Operation node (32 bytes).
    #[repr(C)]
    pub struct td_op_t {
        pub opcode: u16,
        pub arity: u8,
        pub flags: u8,
        pub out_type: i8,
        pub pad: [u8; 3],
        pub id: u32,
        pub est_rows: u32,
        pub inputs: [*mut td_op_t; 2],
    }

    /// Operation graph.
    #[repr(C)]
    pub struct td_graph_t {
        pub nodes: *mut td_op_t,
        pub node_count: u32,
        pub node_cap: u32,
        pub df: *mut td_t,
        pub ext_nodes: *mut *mut std::ffi::c_void,
        pub ext_count: u32,
        pub ext_cap: u32,
    }

    // ---- Type constants ---------------------------------------------------

    pub const TD_BOOL: i8 = 1;
    pub const TD_I32: i8 = 5;
    pub const TD_I64: i8 = 6;
    pub const TD_F64: i8 = 7;
    pub const TD_STR: i8 = 8;
    pub const TD_TABLE: i8 = 13;
    pub const TD_SYM: i8 = 14;
    pub const TD_ENUM: i8 = 15;

    // ---- Opcode constants for agg_ops -------------------------------------

    pub const OP_SUM: u16 = 50;
    pub const OP_MIN: u16 = 52;
    pub const OP_MAX: u16 = 53;
    pub const OP_COUNT: u16 = 54;
    pub const OP_AVG: u16 = 55;
    pub const OP_FIRST: u16 = 56;
    pub const OP_LAST: u16 = 57;
    pub const OP_COUNT_DISTINCT: u16 = 58;

    // ---- EXTRACT field constants ------------------------------------------

    pub const TD_EXTRACT_YEAR: i64 = 0;
    pub const TD_EXTRACT_MONTH: i64 = 1;
    pub const TD_EXTRACT_DAY: i64 = 2;
    pub const TD_EXTRACT_HOUR: i64 = 3;
    pub const TD_EXTRACT_MINUTE: i64 = 4;
    pub const TD_EXTRACT_SECOND: i64 = 5;
    pub const TD_EXTRACT_DOW: i64 = 6;
    pub const TD_EXTRACT_DOY: i64 = 7;
    pub const TD_EXTRACT_EPOCH: i64 = 8;

    // ---- Error constants --------------------------------------------------

    pub const TD_OK: u32 = 0;
    pub const TD_ERR_OOM: u32 = 1;
    pub const TD_ERR_TYPE: u32 = 2;
    pub const TD_ERR_RANGE: u32 = 3;
    pub const TD_ERR_LENGTH: u32 = 4;
    pub const TD_ERR_RANK: u32 = 5;
    pub const TD_ERR_DOMAIN: u32 = 6;
    pub const TD_ERR_NYI: u32 = 7;
    pub const TD_ERR_IO: u32 = 8;
    pub const TD_ERR_SCHEMA: u32 = 9;
    pub const TD_ERR_CORRUPT: u32 = 10;

    // ---- Accessor helpers -------------------------------------------------

    /// `TD_IS_ERR(p)` → `(uintptr_t)(p) < 32`
    #[inline]
    pub fn td_is_err(p: *mut td_t) -> bool {
        (p as usize) < 32
    }

    /// `TD_ERR_CODE(p)` → `(td_err_t)(uintptr_t)(p)`
    #[inline]
    pub fn td_err_code(p: *mut td_t) -> u32 {
        p as usize as u32
    }

    /// `td_data(v)` → pointer at byte offset 32 from header
    #[inline]
    pub unsafe fn td_data(v: *mut td_t) -> *mut u8 {
        unsafe { (v as *mut u8).add(32) }
    }

    /// `td_len(v)` → interpret val union as i64
    #[inline]
    pub unsafe fn td_len(v: *mut td_t) -> i64 {
        unsafe { *((*v).val.as_ptr() as *const i64) }
    }

    /// Read the type tag.
    #[inline]
    pub unsafe fn td_type(v: *mut td_t) -> i8 {
        unsafe { (*v).type_ }
    }

    // ---- Type sizes lookup ------------------------------------------------

    extern "C" {
        pub static td_type_sizes: [u8; 16];
    }

    // ---- FFI functions ----------------------------------------------------

    extern "C" {
        // Memory / init
        pub fn td_arena_init();
        pub fn td_arena_destroy_all();
        pub fn td_sym_init();
        pub fn td_sym_destroy();
        pub fn td_pool_init(n_workers: u32) -> u32;
        pub fn td_pool_destroy();

        // Ref counting
        pub fn td_retain(v: *mut td_t);
        pub fn td_release(v: *mut td_t);

        // Symbol table
        pub fn td_sym_intern(s: *const c_char, len: usize) -> i64;
        pub fn td_sym_str(id: i64) -> *mut td_t;

        // Table API
        pub fn td_table_new(ncols: i64) -> *mut td_t;
        pub fn td_table_add_col(df: *mut td_t, name: i64, col: *mut td_t) -> *mut td_t;
        pub fn td_table_get_col(df: *mut td_t, name: i64) -> *mut td_t;
        pub fn td_table_nrows(df: *mut td_t) -> i64;
        pub fn td_table_ncols(df: *mut td_t) -> i64;
        pub fn td_table_col_name(df: *mut td_t, idx: i64) -> i64;
        pub fn td_table_get_col_idx(df: *mut td_t, idx: i64) -> *mut td_t;

        // String API
        pub fn td_str_ptr(s: *mut td_t) -> *const c_char;
        pub fn td_str_len(s: *mut td_t) -> usize;

        // Vector API
        pub fn td_vec_new(type_: i8, capacity: i64) -> *mut td_t;
        pub fn td_vec_concat(a: *mut td_t, b: *mut td_t) -> *mut td_t;

        // CSV API
        pub fn td_csv_read(path: *const c_char) -> *mut td_t;
        pub fn td_csv_read_opts(path: *const c_char, delimiter: c_char, header: bool) -> *mut td_t;

        // Graph API
        pub fn td_graph_new(df: *mut td_t) -> *mut td_graph_t;
        pub fn td_graph_free(g: *mut td_graph_t);

        // Source ops
        pub fn td_scan(g: *mut td_graph_t, col_name: *const c_char) -> *mut td_op_t;
        pub fn td_const_f64(g: *mut td_graph_t, val: f64) -> *mut td_op_t;
        pub fn td_const_i64(g: *mut td_graph_t, val: i64) -> *mut td_op_t;
        pub fn td_const_bool(g: *mut td_graph_t, val: bool) -> *mut td_op_t;
        pub fn td_const_str(g: *mut td_graph_t, s: *const c_char) -> *mut td_op_t;
        pub fn td_const_df(g: *mut td_graph_t, df: *mut td_t) -> *mut td_op_t;
        pub fn td_const_vec(g: *mut td_graph_t, vec: *mut td_t) -> *mut td_op_t;

        // Unary element-wise ops
        pub fn td_neg(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_not(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_abs(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_sqrt_op(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_log_op(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_exp_op(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_ceil_op(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_floor_op(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_isnull(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_cast(g: *mut td_graph_t, a: *mut td_op_t, target_type: i8) -> *mut td_op_t;

        // Binary element-wise ops
        pub fn td_add(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_sub(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_mul(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_div(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_mod(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_eq(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_ne(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_lt(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_le(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_gt(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_ge(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_and(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_or(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_min2(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;
        pub fn td_max2(g: *mut td_graph_t, a: *mut td_op_t, b: *mut td_op_t) -> *mut td_op_t;

        // Reduction ops
        pub fn td_sum(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_avg(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_min_op(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_max_op(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_count(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_first(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_last(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_count_distinct(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;

        // Structural ops
        pub fn td_filter(g: *mut td_graph_t, input: *mut td_op_t, predicate: *mut td_op_t) -> *mut td_op_t;
        pub fn td_sort_op(
            g: *mut td_graph_t,
            df_node: *mut td_op_t,
            keys: *mut *mut td_op_t,
            descs: *mut u8,
            nulls_first: *mut u8,
            n_cols: u8,
        ) -> *mut td_op_t;
        pub fn td_group(
            g: *mut td_graph_t,
            keys: *mut *mut td_op_t,
            n_keys: u8,
            agg_ops: *mut u16,
            agg_ins: *mut *mut td_op_t,
            n_aggs: u8,
        ) -> *mut td_op_t;
        pub fn td_project(
            g: *mut td_graph_t,
            input: *mut td_op_t,
            cols: *mut *mut td_op_t,
            n_cols: u8,
        ) -> *mut td_op_t;
        pub fn td_select(
            g: *mut td_graph_t,
            input: *mut td_op_t,
            cols: *mut *mut td_op_t,
            n_cols: u8,
        ) -> *mut td_op_t;
        pub fn td_head(g: *mut td_graph_t, input: *mut td_op_t, n: i64) -> *mut td_op_t;
        pub fn td_tail(g: *mut td_graph_t, input: *mut td_op_t, n: i64) -> *mut td_op_t;
        pub fn td_alias(g: *mut td_graph_t, input: *mut td_op_t, name: *const c_char) -> *mut td_op_t;

        // Join
        pub fn td_join(
            g: *mut td_graph_t,
            left_df: *mut td_op_t,
            left_keys: *mut *mut td_op_t,
            right_df: *mut td_op_t,
            right_keys: *mut *mut td_op_t,
            n_keys: u8,
            join_type: u8,
        ) -> *mut td_op_t;

        // Ternary conditional (IF)
        pub fn td_if(
            g: *mut td_graph_t,
            cond: *mut td_op_t,
            then_val: *mut td_op_t,
            else_val: *mut td_op_t,
        ) -> *mut td_op_t;

        // LIKE pattern matching
        pub fn td_like(
            g: *mut td_graph_t,
            input: *mut td_op_t,
            pattern: *mut td_op_t,
        ) -> *mut td_op_t;

        // String functions
        pub fn td_upper(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_lower(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_strlen(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_trim_op(g: *mut td_graph_t, a: *mut td_op_t) -> *mut td_op_t;
        pub fn td_substr(
            g: *mut td_graph_t,
            str_col: *mut td_op_t,
            start: *mut td_op_t,
            len: *mut td_op_t,
        ) -> *mut td_op_t;
        pub fn td_replace(
            g: *mut td_graph_t,
            str_col: *mut td_op_t,
            from: *mut td_op_t,
            to: *mut td_op_t,
        ) -> *mut td_op_t;
        pub fn td_concat(
            g: *mut td_graph_t,
            args: *mut *mut td_op_t,
            n: c_int,
        ) -> *mut td_op_t;

        // Date/time extraction and truncation
        pub fn td_extract(g: *mut td_graph_t, col: *mut td_op_t, field: i64) -> *mut td_op_t;
        pub fn td_date_trunc(g: *mut td_graph_t, col: *mut td_op_t, field: i64) -> *mut td_op_t;

        // Optimizer + executor
        pub fn td_optimize(g: *mut td_graph_t, root: *mut td_op_t) -> *mut td_op_t;
        pub fn td_execute(g: *mut td_graph_t, root: *mut td_op_t) -> *mut td_t;
    }
}

// ---------------------------------------------------------------------------
// Public API types
// ---------------------------------------------------------------------------

/// Error codes returned by the Teide engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Oom,
    Type,
    Range,
    Length,
    Rank,
    Domain,
    Nyi,
    Io,
    Schema,
    Corrupt,
}

impl Error {
    fn from_code(code: u32) -> Self {
        match code {
            ffi::TD_ERR_OOM => Error::Oom,
            ffi::TD_ERR_TYPE => Error::Type,
            ffi::TD_ERR_RANGE => Error::Range,
            ffi::TD_ERR_LENGTH => Error::Length,
            ffi::TD_ERR_RANK => Error::Rank,
            ffi::TD_ERR_DOMAIN => Error::Domain,
            ffi::TD_ERR_NYI => Error::Nyi,
            ffi::TD_ERR_IO => Error::Io,
            ffi::TD_ERR_SCHEMA => Error::Schema,
            ffi::TD_ERR_CORRUPT => Error::Corrupt,
            _ => Error::Corrupt,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Error::Oom => "out of memory",
            Error::Type => "type error",
            Error::Range => "range error",
            Error::Length => "length error",
            Error::Rank => "rank error",
            Error::Domain => "domain error",
            Error::Nyi => "not yet implemented",
            Error::Io => "I/O error",
            Error::Schema => "schema error",
            Error::Corrupt => "corrupt data",
        };
        f.write_str(s)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// Aggregation operation variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    First,
    Last,
    CountDistinct,
}

impl AggOp {
    fn to_opcode(self) -> u16 {
        match self {
            AggOp::Sum => ffi::OP_SUM,
            AggOp::Avg => ffi::OP_AVG,
            AggOp::Min => ffi::OP_MIN,
            AggOp::Max => ffi::OP_MAX,
            AggOp::Count => ffi::OP_COUNT,
            AggOp::First => ffi::OP_FIRST,
            AggOp::Last => ffi::OP_LAST,
            AggOp::CountDistinct => ffi::OP_COUNT_DISTINCT,
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: check a td_t* return for error sentinel
// ---------------------------------------------------------------------------

fn check_ptr(ptr: *mut ffi::td_t) -> Result<*mut ffi::td_t> {
    if ffi::td_is_err(ptr) {
        Err(Error::from_code(ffi::td_err_code(ptr)))
    } else {
        Ok(ptr)
    }
}

// ---------------------------------------------------------------------------
// Context — manages global engine state (arena, sym table, thread pool)
// ---------------------------------------------------------------------------

/// Intern a string into the global symbol table. Returns a stable i64 ID.
pub fn sym_intern(s: &str) -> i64 {
    unsafe { ffi::td_sym_intern(s.as_ptr() as *const std::ffi::c_char, s.len()) }
}

/// Engine context. Initializes the arena allocator, symbol table, and thread
/// pool on construction. Tears them down in reverse order on drop.
///
/// Only one `Context` should exist at a time. The C engine uses global state.
pub struct Context {
    // *mut () makes Context !Send + !Sync (C engine uses thread-local arenas)
    _not_send_sync: PhantomData<*mut ()>,
}

impl Context {
    /// Create a new engine context. Calls `td_arena_init`, `td_sym_init`,
    /// `td_pool_init(0)` (auto-detect thread count).
    pub fn new() -> Result<Self> {
        unsafe {
            ffi::td_arena_init();
            ffi::td_sym_init();
            ffi::td_pool_init(0);
        }
        Ok(Context { _not_send_sync: PhantomData })
    }

    /// Read a CSV file into a `Table`.
    pub fn read_csv(&self, path: &str) -> Result<Table> {
        let c_path = CString::new(path).map_err(|_| Error::Io)?;
        let ptr = unsafe { ffi::td_csv_read(c_path.as_ptr()) };
        let ptr = check_ptr(ptr)?;
        Ok(Table { raw: ptr, _not_send_sync: PhantomData })
    }

    /// Read a CSV file with custom options.
    pub fn read_csv_opts(&self, path: &str, delimiter: char, header: bool) -> Result<Table> {
        let c_path = CString::new(path).map_err(|_| Error::Io)?;
        let ptr = unsafe {
            ffi::td_csv_read_opts(c_path.as_ptr(), delimiter as std::os::raw::c_char, header)
        };
        let ptr = check_ptr(ptr)?;
        Ok(Table { raw: ptr, _not_send_sync: PhantomData })
    }

    /// Create a new operation graph bound to a table.
    pub fn graph<'a>(&self, table: &'a Table) -> Graph<'a> {
        let raw = unsafe { ffi::td_graph_new(table.raw) };
        Graph {
            raw,
            _table: PhantomData,
            _pinned: Vec::new(),
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe {
            ffi::td_pool_destroy();
            ffi::td_sym_destroy();
            ffi::td_arena_destroy_all();
        }
    }
}

// ---------------------------------------------------------------------------
// Table — RAII wrapper around td_t* (type=TD_TABLE)
// ---------------------------------------------------------------------------

/// A columnar table backed by the Teide engine.
pub struct Table {
    raw: *mut ffi::td_t,
    _not_send_sync: PhantomData<*mut ()>,
}

impl Table {
    /// Wrap a raw pointer as a Table (takes ownership — will call td_release on drop).
    /// The pointer must already be retained.
    pub unsafe fn from_raw(raw: *mut ffi::td_t) -> Self {
        Table { raw, _not_send_sync: PhantomData }
    }

    /// Create a shared reference to this table by incrementing the C ref count.
    /// Both the original and the clone will call `td_release` on drop.
    pub fn clone_ref(&self) -> Self {
        unsafe { ffi::td_retain(self.raw); }
        Table { raw: self.raw, _not_send_sync: PhantomData }
    }

    /// Raw pointer access (for interop).
    pub fn as_raw(&self) -> *mut ffi::td_t {
        self.raw
    }

    /// Number of rows.
    pub fn nrows(&self) -> i64 {
        unsafe { ffi::td_table_nrows(self.raw) }
    }

    /// Number of columns.
    pub fn ncols(&self) -> i64 {
        unsafe { ffi::td_table_ncols(self.raw) }
    }

    /// Symbol ID of the column name at `idx`.
    pub fn col_name(&self, idx: i64) -> i64 {
        unsafe { ffi::td_table_col_name(self.raw, idx) }
    }

    /// Resolve the column name at `idx` to a `&str`.
    pub fn col_name_str(&self, idx: usize) -> &str {
        let sym_id = self.col_name(idx as i64);
        let atom = unsafe { ffi::td_sym_str(sym_id) };
        if atom.is_null() {
            return "";
        }
        unsafe {
            let ptr = ffi::td_str_ptr(atom);
            let len = ffi::td_str_len(atom);
            let slice = std::slice::from_raw_parts(ptr as *const u8, len);
            std::str::from_utf8_unchecked(slice)
        }
    }

    /// Add a column to this table. The table pointer may change (COW semantics).
    /// `name` is a pre-interned symbol ID.
    pub fn add_column_raw(&mut self, name_id: i64, col: *mut ffi::td_t) {
        self.raw = unsafe { ffi::td_table_add_col(self.raw, name_id, col) };
    }

    /// Raw column vector at index (returns None for out-of-range or error).
    pub fn get_col_idx(&self, idx: i64) -> Option<*mut ffi::td_t> {
        let p = unsafe { ffi::td_table_get_col_idx(self.raw, idx) };
        if p.is_null() || ffi::td_is_err(p) {
            None
        } else {
            Some(p)
        }
    }

    /// Type tag of the column at `idx`.
    pub fn col_type(&self, idx: usize) -> i8 {
        match self.get_col_idx(idx as i64) {
            Some(col) => unsafe { ffi::td_type(col) },
            None => 0,
        }
    }

    /// Read an i64 value from column `col`, row `row`.
    pub fn get_i64(&self, col: usize, row: usize) -> Option<i64> {
        let vec = self.get_col_idx(col as i64)?;
        let len = unsafe { ffi::td_len(vec) } as usize;
        if row >= len {
            return None;
        }
        let t = unsafe { ffi::td_type(vec) };
        unsafe {
            let data = ffi::td_data(vec);
            match t {
                ffi::TD_I64 | ffi::TD_SYM => {
                    let p = data as *const i64;
                    Some(*p.add(row))
                }
                ffi::TD_I32 => {
                    let p = data as *const i32;
                    Some(*p.add(row) as i64)
                }
                ffi::TD_ENUM => {
                    let p = data as *const u32;
                    Some(*p.add(row) as i64)
                }
                _ => None,
            }
        }
    }

    /// Read an f64 value from column `col`, row `row`.
    pub fn get_f64(&self, col: usize, row: usize) -> Option<f64> {
        let vec = self.get_col_idx(col as i64)?;
        let len = unsafe { ffi::td_len(vec) } as usize;
        if row >= len {
            return None;
        }
        let t = unsafe { ffi::td_type(vec) };
        if t != ffi::TD_F64 {
            return None;
        }
        unsafe {
            let data = ffi::td_data(vec) as *const f64;
            Some(*data.add(row))
        }
    }

    /// Read a string value from a SYM or ENUM column at `col`, `row`.
    pub fn get_str(&self, col: usize, row: usize) -> Option<&str> {
        let vec = self.get_col_idx(col as i64)?;
        let len = unsafe { ffi::td_len(vec) } as usize;
        if row >= len {
            return None;
        }
        let t = unsafe { ffi::td_type(vec) };
        let sym_id = match t {
            ffi::TD_SYM => unsafe {
                let data = ffi::td_data(vec) as *const i64;
                *data.add(row)
            },
            ffi::TD_ENUM => unsafe {
                let data = ffi::td_data(vec) as *const u32;
                *data.add(row) as i64
            },
            _ => return None,
        };
        let atom = unsafe { ffi::td_sym_str(sym_id) };
        if atom.is_null() {
            return None;
        }
        unsafe {
            let ptr = ffi::td_str_ptr(atom);
            let slen = ffi::td_str_len(atom);
            let slice = std::slice::from_raw_parts(ptr as *const u8, slen);
            Some(std::str::from_utf8_unchecked(slice))
        }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        if !self.raw.is_null() && !ffi::td_is_err(self.raw) {
            unsafe {
                ffi::td_release(self.raw);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Column — thin wrapper around *mut td_op_t (graph-owned, no Drop)
// ---------------------------------------------------------------------------

/// A reference to an operation node in a `Graph`. Does not own memory —
/// the `Graph` owns all nodes.
#[derive(Clone, Copy)]
pub struct Column {
    raw: *mut ffi::td_op_t,
}

impl Column {
    /// Raw pointer access.
    pub fn as_raw(&self) -> *mut ffi::td_op_t {
        self.raw
    }
}

// ---------------------------------------------------------------------------
// Graph<'a> — operation graph bound to a Table's lifetime
// ---------------------------------------------------------------------------

/// A lazy operation graph. Operations build a DAG that is optimized and
/// executed when `execute()` is called.
pub struct Graph<'a> {
    raw: *mut ffi::td_graph_t,
    // Ties lifetime to the Table AND makes Graph !Send + !Sync via *mut ()
    _table: PhantomData<(&'a Table, *mut ())>,
    // Pin arrays passed to C functions that store pointers (td_group, td_sort_op).
    // These must live until the graph is dropped/executed.
    _pinned: Vec<Box<dyn std::any::Any>>,
}

impl Graph<'_> {
    /// Raw pointer access.
    pub fn as_raw(&self) -> *mut ffi::td_graph_t {
        self.raw
    }

    // ---- Source ops -------------------------------------------------------

    /// Scan a column by name from the bound table.
    pub fn scan(&self, col_name: &str) -> Column {
        let c_name = CString::new(col_name).expect("column name must not contain NUL");
        let raw = unsafe { ffi::td_scan(self.raw, c_name.as_ptr()) };
        Column { raw }
    }

    /// Create a constant f64 node.
    pub fn const_f64(&self, val: f64) -> Column {
        Column {
            raw: unsafe { ffi::td_const_f64(self.raw, val) },
        }
    }

    /// Create a constant i64 node.
    pub fn const_i64(&self, val: i64) -> Column {
        Column {
            raw: unsafe { ffi::td_const_i64(self.raw, val) },
        }
    }

    /// Create a constant bool node.
    pub fn const_bool(&self, val: bool) -> Column {
        Column {
            raw: unsafe { ffi::td_const_bool(self.raw, val) },
        }
    }

    /// Create a constant string node.
    pub fn const_str(&self, val: &str) -> Column {
        let c_val = CString::new(val).expect("string must not contain NUL");
        Column {
            raw: unsafe { ffi::td_const_str(self.raw, c_val.as_ptr()) },
        }
    }

    /// Create a constant DataFrame node referencing a table.
    pub fn const_df(&self, table: &Table) -> Column {
        Column {
            raw: unsafe { ffi::td_const_df(self.raw, table.raw) },
        }
    }

    // ---- Binary element-wise ops ------------------------------------------

    pub fn add(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_add(self.raw, a.raw, b.raw) },
        }
    }

    pub fn sub(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_sub(self.raw, a.raw, b.raw) },
        }
    }

    pub fn mul(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_mul(self.raw, a.raw, b.raw) },
        }
    }

    pub fn div(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_div(self.raw, a.raw, b.raw) },
        }
    }

    pub fn modulo(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_mod(self.raw, a.raw, b.raw) },
        }
    }

    pub fn eq(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_eq(self.raw, a.raw, b.raw) },
        }
    }

    pub fn ne(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_ne(self.raw, a.raw, b.raw) },
        }
    }

    pub fn lt(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_lt(self.raw, a.raw, b.raw) },
        }
    }

    pub fn le(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_le(self.raw, a.raw, b.raw) },
        }
    }

    pub fn gt(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_gt(self.raw, a.raw, b.raw) },
        }
    }

    pub fn ge(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_ge(self.raw, a.raw, b.raw) },
        }
    }

    pub fn and(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_and(self.raw, a.raw, b.raw) },
        }
    }

    pub fn or(&self, a: Column, b: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_or(self.raw, a.raw, b.raw) },
        }
    }

    pub fn min2(&self, a: Column, b: Column) -> Column {
        Column { raw: unsafe { ffi::td_min2(self.raw, a.raw, b.raw) } }
    }

    pub fn max2(&self, a: Column, b: Column) -> Column {
        Column { raw: unsafe { ffi::td_max2(self.raw, a.raw, b.raw) } }
    }

    pub fn if_then_else(&self, cond: Column, then_val: Column, else_val: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_if(self.raw, cond.raw, then_val.raw, else_val.raw) },
        }
    }

    pub fn like(&self, input: Column, pattern: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_like(self.raw, input.raw, pattern.raw) },
        }
    }

    // ---- String ops -------------------------------------------------------

    pub fn upper(&self, a: Column) -> Column {
        Column { raw: unsafe { ffi::td_upper(self.raw, a.raw) } }
    }

    pub fn lower(&self, a: Column) -> Column {
        Column { raw: unsafe { ffi::td_lower(self.raw, a.raw) } }
    }

    pub fn strlen(&self, a: Column) -> Column {
        Column { raw: unsafe { ffi::td_strlen(self.raw, a.raw) } }
    }

    pub fn trim(&self, a: Column) -> Column {
        Column { raw: unsafe { ffi::td_trim_op(self.raw, a.raw) } }
    }

    pub fn substr(&self, s: Column, start: Column, len: Column) -> Column {
        Column { raw: unsafe { ffi::td_substr(self.raw, s.raw, start.raw, len.raw) } }
    }

    pub fn replace(&self, s: Column, from: Column, to: Column) -> Column {
        Column { raw: unsafe { ffi::td_replace(self.raw, s.raw, from.raw, to.raw) } }
    }

    pub fn concat(&self, args: &[Column]) -> Column {
        let mut ptrs: Vec<*mut ffi::td_op_t> = args.iter().map(|c| c.raw).collect();
        Column {
            raw: unsafe { ffi::td_concat(self.raw, ptrs.as_mut_ptr(), args.len() as std::ffi::c_int) },
        }
    }

    // ---- Unary ops --------------------------------------------------------

    pub fn not(&self, a: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_not(self.raw, a.raw) },
        }
    }

    pub fn neg(&self, a: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_neg(self.raw, a.raw) },
        }
    }

    pub fn abs(&self, a: Column) -> Column {
        Column { raw: unsafe { ffi::td_abs(self.raw, a.raw) } }
    }

    pub fn sqrt(&self, a: Column) -> Column {
        Column { raw: unsafe { ffi::td_sqrt_op(self.raw, a.raw) } }
    }

    pub fn log(&self, a: Column) -> Column {
        Column { raw: unsafe { ffi::td_log_op(self.raw, a.raw) } }
    }

    pub fn exp(&self, a: Column) -> Column {
        Column { raw: unsafe { ffi::td_exp_op(self.raw, a.raw) } }
    }

    pub fn ceil(&self, a: Column) -> Column {
        Column { raw: unsafe { ffi::td_ceil_op(self.raw, a.raw) } }
    }

    pub fn floor(&self, a: Column) -> Column {
        Column { raw: unsafe { ffi::td_floor_op(self.raw, a.raw) } }
    }

    pub fn isnull(&self, a: Column) -> Column {
        Column { raw: unsafe { ffi::td_isnull(self.raw, a.raw) } }
    }

    pub fn cast(&self, a: Column, target_type: i8) -> Column {
        Column { raw: unsafe { ffi::td_cast(self.raw, a.raw, target_type) } }
    }

    // ---- Date/time extraction ---------------------------------------------

    pub fn extract(&self, col: Column, field: i64) -> Column {
        Column { raw: unsafe { ffi::td_extract(self.raw, col.raw, field) } }
    }

    pub fn date_trunc(&self, col: Column, field: i64) -> Column {
        Column { raw: unsafe { ffi::td_date_trunc(self.raw, col.raw, field) } }
    }

    // ---- Reduction ops ----------------------------------------------------

    pub fn sum(&self, a: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_sum(self.raw, a.raw) },
        }
    }

    pub fn avg(&self, a: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_avg(self.raw, a.raw) },
        }
    }

    pub fn min_op(&self, a: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_min_op(self.raw, a.raw) },
        }
    }

    pub fn max_op(&self, a: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_max_op(self.raw, a.raw) },
        }
    }

    pub fn count(&self, a: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_count(self.raw, a.raw) },
        }
    }

    pub fn first(&self, a: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_first(self.raw, a.raw) },
        }
    }

    pub fn last(&self, a: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_last(self.raw, a.raw) },
        }
    }

    // ---- Structural ops ---------------------------------------------------

    /// Group-by aggregation.
    pub fn group_by(
        &mut self,
        keys: &[Column],
        agg_ops: &[AggOp],
        agg_inputs: &[Column],
    ) -> Column {
        assert_eq!(
            agg_ops.len(),
            agg_inputs.len(),
            "agg_ops and agg_inputs must have the same length"
        );

        let mut key_ptrs: Vec<*mut ffi::td_op_t> = keys.iter().map(|c| c.raw).collect();
        let mut ops: Vec<u16> = agg_ops.iter().map(|op| op.to_opcode()).collect();
        let mut input_ptrs: Vec<*mut ffi::td_op_t> = agg_inputs.iter().map(|c| c.raw).collect();

        let raw = unsafe {
            ffi::td_group(
                self.raw,
                key_ptrs.as_mut_ptr(),
                keys.len() as u8,
                ops.as_mut_ptr(),
                input_ptrs.as_mut_ptr(),
                agg_ops.len() as u8,
            )
        };
        // Pin arrays — td_group stores pointers to them
        self._pinned.push(Box::new(key_ptrs));
        self._pinned.push(Box::new(ops));
        self._pinned.push(Box::new(input_ptrs));
        Column { raw }
    }

    /// Hash join.
    pub fn join(
        &mut self,
        left_df: Column,
        left_keys: &[Column],
        right_df: Column,
        right_keys: &[Column],
        join_type: u8,
    ) -> Column {
        assert_eq!(left_keys.len(), right_keys.len(), "join key count must match");
        let mut lk: Vec<*mut ffi::td_op_t> = left_keys.iter().map(|c| c.raw).collect();
        let mut rk: Vec<*mut ffi::td_op_t> = right_keys.iter().map(|c| c.raw).collect();
        let raw = unsafe {
            ffi::td_join(
                self.raw,
                left_df.raw,
                lk.as_mut_ptr(),
                right_df.raw,
                rk.as_mut_ptr(),
                left_keys.len() as u8,
                join_type,
            )
        };
        self._pinned.push(Box::new(lk));
        self._pinned.push(Box::new(rk));
        Column { raw }
    }

    /// Multi-column sort.
    pub fn sort(
        &mut self,
        df_node: Column,
        keys: &[Column],
        descs: &[bool],
        nulls_first: Option<&[bool]>,
    ) -> Column {
        assert_eq!(
            keys.len(),
            descs.len(),
            "keys and descs must have the same length"
        );

        let mut key_ptrs: Vec<*mut ffi::td_op_t> = keys.iter().map(|c| c.raw).collect();
        let mut desc_u8: Vec<u8> = descs.iter().map(|&d| d as u8).collect();

        let nf_ptr = if let Some(nf) = nulls_first {
            assert_eq!(keys.len(), nf.len(), "nulls_first must match keys length");
            let mut nf_u8: Vec<u8> = nf.iter().map(|&n| n as u8).collect();
            let ptr = nf_u8.as_mut_ptr();
            self._pinned.push(Box::new(nf_u8));
            ptr
        } else {
            std::ptr::null_mut()
        };

        let raw = unsafe {
            ffi::td_sort_op(
                self.raw,
                df_node.raw,
                key_ptrs.as_mut_ptr(),
                desc_u8.as_mut_ptr(),
                nf_ptr,
                keys.len() as u8,
            )
        };
        // Pin arrays — td_sort_op stores pointers to them
        self._pinned.push(Box::new(key_ptrs));
        self._pinned.push(Box::new(desc_u8));
        Column { raw }
    }

    /// Project (select) specific columns from a DataFrame node.
    pub fn project(&self, input: Column, cols: &[Column]) -> Column {
        let mut col_ptrs: Vec<*mut ffi::td_op_t> = cols.iter().map(|c| c.raw).collect();
        Column {
            raw: unsafe {
                ffi::td_project(self.raw, input.raw, col_ptrs.as_mut_ptr(), cols.len() as u8)
            },
        }
    }

    /// Select specific columns from a DataFrame node (alias for project).
    pub fn select(&self, input: Column, cols: &[Column]) -> Column {
        let mut col_ptrs: Vec<*mut ffi::td_op_t> = cols.iter().map(|c| c.raw).collect();
        Column {
            raw: unsafe {
                ffi::td_select(self.raw, input.raw, col_ptrs.as_mut_ptr(), cols.len() as u8)
            },
        }
    }

    /// Filter rows by a boolean predicate column.
    pub fn filter(&self, input: Column, predicate: Column) -> Column {
        Column {
            raw: unsafe { ffi::td_filter(self.raw, input.raw, predicate.raw) },
        }
    }

    /// Take the first `n` rows.
    pub fn head(&self, input: Column, n: i64) -> Column {
        Column {
            raw: unsafe { ffi::td_head(self.raw, input.raw, n) },
        }
    }

    /// Take the last `n` rows.
    pub fn tail(&self, input: Column, n: i64) -> Column {
        Column {
            raw: unsafe { ffi::td_tail(self.raw, input.raw, n) },
        }
    }

    /// Rename/alias a column.
    pub fn alias(&self, input: Column, name: &str) -> Column {
        let c_name = CString::new(name).expect("alias name must not contain NUL");
        Column {
            raw: unsafe { ffi::td_alias(self.raw, input.raw, c_name.as_ptr()) },
        }
    }

    // ---- Execute ----------------------------------------------------------

    /// Optimize the DAG and execute it, returning a result `Table`.
    pub fn execute(&self, root: Column) -> Result<Table> {
        let optimized = unsafe { ffi::td_optimize(self.raw, root.raw) };
        let result = unsafe { ffi::td_execute(self.raw, optimized) };
        let result = check_ptr(result)?;
        // Retain so Table's Drop can release it
        unsafe { ffi::td_retain(result) };
        Ok(Table { raw: result, _not_send_sync: PhantomData })
    }

    /// Execute a graph node and return the raw result (vector or table).
    /// Caller is responsible for releasing the result.
    pub fn execute_raw(&self, root: Column) -> Result<*mut ffi::td_t> {
        let optimized = unsafe { ffi::td_optimize(self.raw, root.raw) };
        let result = unsafe { ffi::td_execute(self.raw, optimized) };
        let result = check_ptr(result)?;
        unsafe { ffi::td_retain(result) };
        Ok(result)
    }

    /// Inject a pre-computed vector as a constant node in the graph.
    pub fn const_vec(&self, vec: *mut ffi::td_t) -> Column {
        Column {
            raw: unsafe { ffi::td_const_vec(self.raw, vec) },
        }
    }
}

impl Drop for Graph<'_> {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                ffi::td_graph_free(self.raw);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Re-exports for downstream crates
// ---------------------------------------------------------------------------

pub use ffi::td_t;
pub use ffi::td_op_t;
pub use ffi::td_graph_t;

/// Low-level FFI access for downstream crates (e.g., teide-sql).
pub mod raw {
    pub use super::ffi::{td_type, td_data, td_len, td_type_sizes, td_vec_new, td_t};
}

/// Low-level helper: get column by symbol ID from a raw table pointer.
/// Returns null if not found. Caller must NOT release the result.
pub unsafe fn ffi_table_get_col(df: *mut ffi::td_t, name_id: i64) -> *mut ffi::td_t {
    unsafe { ffi::td_table_get_col(df, name_id) }
}

/// Low-level helper: create new table.
pub unsafe fn ffi_table_new(ncols: i64) -> *mut ffi::td_t {
    unsafe { ffi::td_table_new(ncols) }
}

/// Low-level helper: add column to table.
pub unsafe fn ffi_table_add_col(df: *mut ffi::td_t, name_id: i64, col: *mut ffi::td_t) -> *mut ffi::td_t {
    unsafe { ffi::td_table_add_col(df, name_id, col) }
}

/// Low-level helper: concatenate two vectors.
pub unsafe fn ffi_vec_concat(a: *mut ffi::td_t, b: *mut ffi::td_t) -> *mut ffi::td_t {
    unsafe { ffi::td_vec_concat(a, b) }
}

/// Low-level helper: release a td_t pointer.
pub unsafe fn ffi_release(v: *mut ffi::td_t) {
    unsafe { ffi::td_release(v) }
}

/// Low-level helper: retain a td_t pointer.
pub unsafe fn ffi_retain(v: *mut ffi::td_t) {
    unsafe { ffi::td_retain(v) }
}

/// Check if a raw pointer is an error sentinel.
pub fn ffi_is_err(p: *mut ffi::td_t) -> bool {
    ffi::td_is_err(p)
}

// Re-export EXTRACT field constants
pub mod extract_field {
    pub const YEAR: i64 = super::ffi::TD_EXTRACT_YEAR;
    pub const MONTH: i64 = super::ffi::TD_EXTRACT_MONTH;
    pub const DAY: i64 = super::ffi::TD_EXTRACT_DAY;
    pub const HOUR: i64 = super::ffi::TD_EXTRACT_HOUR;
    pub const MINUTE: i64 = super::ffi::TD_EXTRACT_MINUTE;
    pub const SECOND: i64 = super::ffi::TD_EXTRACT_SECOND;
    pub const DOW: i64 = super::ffi::TD_EXTRACT_DOW;
    pub const DOY: i64 = super::ffi::TD_EXTRACT_DOY;
    pub const EPOCH: i64 = super::ffi::TD_EXTRACT_EPOCH;
}

// Re-export type constants
pub mod types {
    pub const BOOL: i8 = super::ffi::TD_BOOL;
    pub const I32: i8 = super::ffi::TD_I32;
    pub const I64: i8 = super::ffi::TD_I64;
    pub const F64: i8 = super::ffi::TD_F64;
    pub const STR: i8 = super::ffi::TD_STR;
    pub const TABLE: i8 = super::ffi::TD_TABLE;
    pub const SYM: i8 = super::ffi::TD_SYM;
    pub const ENUM: i8 = super::ffi::TD_ENUM;
}
