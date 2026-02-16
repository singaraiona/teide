//   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
//   All rights reserved.
//
//   Permission is hereby granted, free of charge, to any person obtaining a copy
//   of this software and associated documentation files (the "Software"), to deal
//   in the Software without restriction, including without limitation the rights
//   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//   copies of the Software, and to permit persons to whom the Software is
//   furnished to do so, subject to the following conditions:
//
//   The above copyright notice and this permission notice shall be included in all
//   copies or substantial portions of the Software.
//
//   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//   SOFTWARE.

//! Synthetic responses for PostgreSQL catalog queries.
//!
//! Many clients (psql, pgAdmin, DBeaver, JDBC drivers) probe the server
//! with queries against `pg_catalog`, `information_schema`, and system
//! functions. We intercept these and return canned or session-derived
//! results so that clients can connect and introspect tables without
//! hitting the Teide engine for unsupported catalog tables.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::Type;
use pgwire::error::PgWireResult;

use super::handler::SessionMeta;
use super::types::teide_to_pg_type;

/// Returns true if the SQL is a simple `SELECT <constant>` with no FROM clause.
/// Connection pools and BI tools use `SELECT 1` as a health-check ping.
fn is_select_constant(lower: &str) -> bool {
    if let Some(rest) = lower.strip_prefix("select ") {
        let rest = rest.trim().trim_end_matches(';').trim();
        // Integer literal, string literal, or TRUE/FALSE/NULL
        rest.parse::<i64>().is_ok()
            || (rest.starts_with('\'') && rest.ends_with('\''))
            || rest == "true"
            || rest == "false"
            || rest == "null"
    } else {
        false
    }
}

/// Returns true if the lowercased SQL looks like a catalog/system query
/// that we should intercept rather than forwarding to the Teide engine.
pub fn is_catalog_query(sql: &str) -> bool {
    let lower = sql.to_lowercase();
    let lower = lower.trim();
    lower.contains("pg_catalog")
        || lower.contains("pg_type")
        || lower.contains("pg_tables")
        || lower.contains("pg_namespace")
        || lower.contains("pg_class")
        || lower.contains("pg_attribute")
        || lower.contains("pg_settings")
        || lower.contains("pg_database")
        || lower.contains("information_schema")
        || lower.starts_with("select current_schema")
        || lower.starts_with("select current_database")
        || lower.starts_with("select version()")
        || lower.starts_with("show ")
        || lower.starts_with("set ")
        || lower == "begin"
        || lower == "commit"
        || lower == "rollback"
        || lower == "end"
        || lower.starts_with("begin;")
        || lower.starts_with("commit;")
        || lower.starts_with("rollback;")
        || lower.starts_with("deallocate ")
        || lower.starts_with("close ")
        || lower.starts_with("discard ")
        || is_select_constant(lower)
}

/// Handle a catalog query, returning a pgwire Response.
///
/// Uses `SessionMeta` (a Send-safe snapshot) for table metadata.
/// Unrecognized catalog queries return an empty result set.
///
/// Returns `None` if this is NOT a catalog query.
pub fn handle_catalog_query(sql: &str, meta: &SessionMeta) -> Option<PgWireResult<Vec<Response>>> {
    if !is_catalog_query(sql) {
        return None;
    }

    let lower = sql.to_lowercase();
    let lower = lower.trim();

    // SET commands: acknowledge silently
    if lower.starts_with("set ") {
        return Some(Ok(vec![Response::Execution(Tag::new("SET").with_rows(0))]));
    }

    // Transaction control: acknowledge silently
    if lower == "begin"
        || lower == "commit"
        || lower == "rollback"
        || lower == "end"
        || lower.starts_with("begin;")
        || lower.starts_with("commit;")
        || lower.starts_with("rollback;")
    {
        let tag = if lower.starts_with("begin") {
            "BEGIN"
        } else if lower.starts_with("commit") || lower.starts_with("end") {
            "COMMIT"
        } else {
            "ROLLBACK"
        };
        return Some(Ok(vec![Response::Execution(Tag::new(tag).with_rows(0))]));
    }

    // DEALLOCATE / CLOSE / DISCARD: acknowledge silently
    if lower.starts_with("deallocate ")
        || lower.starts_with("close ")
        || lower.starts_with("discard ")
    {
        return Some(Ok(vec![Response::Execution(
            Tag::new("OK").with_rows(0),
        )]));
    }

    // SHOW commands
    if lower == "show transaction isolation level" {
        return Some(single_text_result(
            "transaction_isolation",
            &["read committed"],
        ));
    }
    if lower == "show standard_conforming_strings" {
        return Some(single_text_result("standard_conforming_strings", &["on"]));
    }
    if lower.starts_with("show server_version") {
        return Some(single_text_result("server_version", &["16.6"]));
    }
    if lower.starts_with("show ") {
        // Generic SHOW fallback — return empty string
        let param = lower.trim_start_matches("show ").trim_end_matches(';').trim();
        return Some(single_text_result(param, &[""]));
    }

    // current_schema / current_database / version
    if lower.starts_with("select current_schema") {
        return Some(single_text_result("current_schema", &["public"]));
    }
    if lower.starts_with("select current_database") {
        return Some(single_text_result("current_database", &["teide"]));
    }
    if lower.starts_with("select version()") || lower.contains("pg_catalog.version()") {
        return Some(single_text_result(
            "version",
            &["PostgreSQL 16.6 (Teide 0.2.0)"],
        ));
    }

    // SELECT <constant> — health-check ping (e.g. SELECT 1)
    if is_select_constant(lower) {
        if let Some(rest) = lower.strip_prefix("select ") {
            let val = rest.trim().trim_end_matches(';').trim();
            return Some(single_text_result("?column?", &[val]));
        }
    }

    // information_schema.tables — list session tables
    if lower.contains("information_schema.tables") {
        return Some(handle_information_schema_tables(meta));
    }

    // information_schema.columns — list columns for all session tables
    if lower.contains("information_schema.columns") {
        return Some(handle_information_schema_columns(meta));
    }

    // pg_type — only match direct FROM (not JOINed)
    if lower.contains("from pg_type") || lower.contains("from pg_catalog.pg_type") {
        return Some(empty_result(&[
            ("oid", Type::INT4),
            ("typname", Type::VARCHAR),
            ("typnamespace", Type::INT4),
            ("typlen", Type::INT2),
            ("typtype", Type::CHAR),
        ]));
    }

    // pg_namespace — only match direct FROM (schema listing), not JOINs
    if lower.contains("from pg_namespace") || lower.contains("from pg_catalog.pg_namespace") {
        return Some(single_text_result(
            "nspname",
            &["public", "information_schema"],
        ));
    }

    // pg_class — table/view listing (SQLAlchemy PG dialect uses this)
    if lower.contains("pg_class") && lower.contains("relname") {
        // Regular tables + partitioned tables in 'public' schema
        if lower.contains("nspname") && lower.contains("'public'") && lower.contains("relkind") {
            let has_tables = lower.contains("'r'") || lower.contains("'p'");
            if has_tables {
                let names: Vec<&str> = meta.tables.iter().map(|(n, _)| n.as_str()).collect();
                return Some(single_text_result("relname", &names));
            }
        }
        // Views, materialized views, foreign tables, or other schemas → empty
        return Some(empty_result(&[("relname", Type::VARCHAR)]));
    }

    // pg_tables — only match direct FROM
    if lower.contains("from pg_tables") || lower.contains("from pg_catalog.pg_tables") {
        return Some(handle_pg_tables(meta));
    }

    // Fallback: return empty result for any other catalog query
    Some(empty_result(&[("result", Type::VARCHAR)]))
}

// ---------------------------------------------------------------------------
// Helper: single-column, single-row text result
// ---------------------------------------------------------------------------

fn single_text_result(col_name: &str, values: &[&str]) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![FieldInfo::new(
        col_name.to_string(),
        None,
        None,
        Type::VARCHAR,
        FieldFormat::Text,
    )]);

    let mut rows = Vec::with_capacity(values.len());
    let mut encoder = DataRowEncoder::new(schema.clone());
    for val in values {
        encoder.encode_field(&Some(val.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    let row_stream = stream::iter(rows);
    Ok(vec![Response::Query(QueryResponse::new(
        schema, row_stream,
    ))])
}

// ---------------------------------------------------------------------------
// Helper: empty result with a given schema
// ---------------------------------------------------------------------------

fn empty_result(cols: &[(&str, Type)]) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(
        cols.iter()
            .map(|(name, ty)| {
                FieldInfo::new(name.to_string(), None, None, ty.clone(), FieldFormat::Text)
            })
            .collect::<Vec<_>>(),
    );
    let row_stream = stream::empty();
    Ok(vec![Response::Query(QueryResponse::new(
        schema, row_stream,
    ))])
}

// ---------------------------------------------------------------------------
// information_schema.tables
// ---------------------------------------------------------------------------

fn handle_information_schema_tables(meta: &SessionMeta) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        FieldInfo::new(
            "table_catalog".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "table_schema".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "table_name".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "table_type".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
    ]);

    let mut rows = Vec::with_capacity(meta.tables.len());
    let mut encoder = DataRowEncoder::new(schema.clone());
    for (name, _) in &meta.tables {
        encoder.encode_field(&Some("teide".to_string()))?;
        encoder.encode_field(&Some("public".to_string()))?;
        encoder.encode_field(&Some(name.clone()))?;
        encoder.encode_field(&Some("BASE TABLE".to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    let row_stream = stream::iter(rows);
    Ok(vec![Response::Query(QueryResponse::new(
        schema, row_stream,
    ))])
}

// ---------------------------------------------------------------------------
// information_schema.columns
// ---------------------------------------------------------------------------

fn handle_information_schema_columns(meta: &SessionMeta) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        FieldInfo::new(
            "table_catalog".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "table_schema".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "table_name".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "column_name".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "ordinal_position".into(),
            None,
            None,
            Type::INT4,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "data_type".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());
    for (table_name, table_meta) in &meta.tables {
        for (i, (col_name, td_type)) in table_meta.columns.iter().enumerate() {
            let pg_type = teide_to_pg_type(*td_type);
            let type_name = pg_type_display_name(&pg_type);

            encoder.encode_field(&Some("teide".to_string()))?;
            encoder.encode_field(&Some("public".to_string()))?;
            encoder.encode_field(&Some(table_name.clone()))?;
            encoder.encode_field(&Some(col_name.clone()))?;
            encoder.encode_field(&Some((i as i32 + 1).to_string()))?;
            encoder.encode_field(&Some(type_name.to_string()))?;
            rows.push(Ok(encoder.take_row()));
        }
    }

    let row_stream = stream::iter(rows);
    Ok(vec![Response::Query(QueryResponse::new(
        schema, row_stream,
    ))])
}

// ---------------------------------------------------------------------------
// pg_tables
// ---------------------------------------------------------------------------

fn handle_pg_tables(meta: &SessionMeta) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        FieldInfo::new(
            "schemaname".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "tablename".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "tableowner".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "hasindexes".into(),
            None,
            None,
            Type::BOOL,
            FieldFormat::Text,
        ),
        FieldInfo::new("hasrules".into(), None, None, Type::BOOL, FieldFormat::Text),
        FieldInfo::new(
            "hastriggers".into(),
            None,
            None,
            Type::BOOL,
            FieldFormat::Text,
        ),
    ]);

    let mut rows = Vec::with_capacity(meta.tables.len());
    let mut encoder = DataRowEncoder::new(schema.clone());
    for (name, _) in &meta.tables {
        encoder.encode_field(&Some("public".to_string()))?;
        encoder.encode_field(&Some(name.clone()))?;
        encoder.encode_field(&Some("teide".to_string()))?;
        encoder.encode_field(&false)?;
        encoder.encode_field(&false)?;
        encoder.encode_field(&false)?;
        rows.push(Ok(encoder.take_row()));
    }

    let row_stream = stream::iter(rows);
    Ok(vec![Response::Query(QueryResponse::new(
        schema, row_stream,
    ))])
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn pg_type_display_name(ty: &Type) -> &'static str {
    match *ty {
        Type::BOOL => "boolean",
        Type::INT2 => "smallint",
        Type::INT4 => "integer",
        Type::INT8 => "bigint",
        Type::FLOAT4 => "real",
        Type::FLOAT8 => "double precision",
        Type::VARCHAR => "character varying",
        Type::TEXT => "text",
        Type::CHAR => "character",
        _ => "character varying",
    }
}
