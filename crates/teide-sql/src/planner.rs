// SQL planner: translates sqlparser AST into Teide execution graph.

use std::collections::HashMap;

use sqlparser::ast::{
    Expr, FunctionArguments, GroupByExpr, ObjectName, ObjectType, Query, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use teide::{AggOp, Column, Context, Graph, Table};

use crate::expr::{is_aggregate, plan_expr};
use crate::{ExecResult, Session, SqlError, SqlResult, StoredTable};

// ---------------------------------------------------------------------------
// Session-aware entry point
// ---------------------------------------------------------------------------

/// Parse and execute a SQL statement within a session context.
/// Supports SELECT, CREATE TABLE AS SELECT, and DROP TABLE.
pub fn session_execute(session: &mut Session, sql: &str) -> Result<ExecResult, SqlError> {
    let dialect = DuckDbDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Parse(e.to_string()))?;

    let stmt = statements
        .into_iter()
        .next()
        .ok_or_else(|| SqlError::Plan("Empty query".into()))?;

    match stmt {
        Statement::Query(q) => {
            let result = plan_query(&session.ctx, &q, Some(&session.tables))?;
            Ok(ExecResult::Query(result))
        }

        Statement::CreateTable(create) => {
            let query = create
                .query
                .ok_or_else(|| SqlError::Plan("CREATE TABLE requires AS SELECT".into()))?;

            let table_name = object_name_to_string(&create.name).to_lowercase();

            if session.tables.contains_key(&table_name) && !create.or_replace {
                if create.if_not_exists {
                    return Ok(ExecResult::Ddl(format!(
                        "Table '{table_name}' already exists (skipped)"
                    )));
                }
                return Err(SqlError::Plan(format!(
                    "Table '{table_name}' already exists (use CREATE OR REPLACE TABLE)"
                )));
            }

            let result = plan_query(&session.ctx, &query, Some(&session.tables))?;
            let nrows = result.table.nrows();
            let ncols = result.columns.len();

            session.tables.insert(
                table_name.clone(),
                StoredTable {
                    table: result.table,
                    columns: result.columns,
                },
            );

            Ok(ExecResult::Ddl(format!(
                "Created table '{table_name}' ({nrows} rows, {ncols} cols)"
            )))
        }

        Statement::Drop {
            object_type: ObjectType::Table,
            names,
            if_exists,
            ..
        } => {
            let mut msgs = Vec::new();
            for name in &names {
                let table_name = object_name_to_string(name).to_lowercase();
                if session.tables.remove(&table_name).is_some() {
                    msgs.push(format!("Dropped table '{table_name}'"));
                } else if if_exists {
                    msgs.push(format!("Table '{table_name}' not found (skipped)"));
                } else {
                    return Err(SqlError::Plan(format!(
                        "Table '{table_name}' not found"
                    )));
                }
            }
            Ok(ExecResult::Ddl(msgs.join("\n")))
        }

        _ => Err(SqlError::Plan(
            "Only SELECT, CREATE TABLE AS, and DROP TABLE are supported".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Stateless entry point (backwards-compatible)
// ---------------------------------------------------------------------------

/// Parse, plan, and execute a SQL query.
pub(crate) fn plan_and_execute(
    ctx: &Context,
    sql: &str,
    tables: Option<&HashMap<String, StoredTable>>,
) -> Result<SqlResult, SqlError> {
    let dialect = DuckDbDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Parse(e.to_string()))?;

    let stmt = statements
        .into_iter()
        .next()
        .ok_or_else(|| SqlError::Plan("Empty query".into()))?;

    let query = match stmt {
        Statement::Query(q) => q,
        _ => return Err(SqlError::Plan("Only SELECT queries are supported".into())),
    };

    plan_query(ctx, &query, tables)
}

// ---------------------------------------------------------------------------
// Query planning
// ---------------------------------------------------------------------------

fn plan_query(
    ctx: &Context,
    query: &Query,
    tables: Option<&HashMap<String, StoredTable>>,
) -> Result<SqlResult, SqlError> {
    if query.with.is_some() {
        return Err(SqlError::Plan("WITH (CTEs) not supported".into()));
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        SetExpr::SetOperation { .. } => {
            return Err(SqlError::Plan(
                "UNION/INTERSECT/EXCEPT not supported".into(),
            ))
        }
        _ => {
            return Err(SqlError::Plan(
                "Only simple SELECT queries are supported".into(),
            ))
        }
    };

    if select.having.is_some() {
        return Err(SqlError::Plan("HAVING not supported".into()));
    }
    for item in &select.from {
        if !item.joins.is_empty() {
            return Err(SqlError::Plan("JOINs not supported".into()));
        }
    }

    // Extract FROM table name
    let from_name = extract_from(&select.from)?;

    // Resolve: check session registry first, then fall back to CSV
    let table = resolve_table(ctx, &from_name, tables)?;

    // Build schema from native table column names
    let schema = build_schema(&table);

    // GROUP BY column names
    let group_by_cols = extract_group_by_columns(&select.group_by, &schema)?;
    let has_group_by = !group_by_cols.is_empty();

    // Detect aggregates in SELECT
    let select_items = &select.projection;
    let has_aggregates = select_items.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            is_aggregate(e)
        }
        _ => false,
    });

    // Stage 1: WHERE filter
    let working_table = if let Some(ref where_expr) = select.selection {
        let mut g = ctx.graph(&table);
        let df_node = g.const_df(&table);
        let pred = plan_expr(&mut g, where_expr, &schema)?;
        let filtered = g.filter(df_node, pred);
        g.execute(filtered)?
    } else {
        table
    };

    // Stage 2: GROUP BY or simple projection
    let (result_table, result_aliases) = if has_group_by || has_aggregates {
        let mut g = ctx.graph(&working_table);
        let (node, aliases) =
            plan_group_by(&mut g, select_items, &group_by_cols, &schema, has_group_by)?;
        (g.execute(node)?, aliases)
    } else {
        let aliases = extract_projection_aliases(select_items, &schema)?;
        (working_table, aliases)
    };

    // Stage 3: ORDER BY
    let order_by_exprs = extract_order_by(query)?;
    let result_table = if !order_by_exprs.is_empty() {
        let mut g = ctx.graph(&result_table);
        let df_node = g.const_df(&result_table);
        let root = plan_order_by(&mut g, df_node, &order_by_exprs, &result_aliases)?;
        g.execute(root)?
    } else {
        result_table
    };

    // Stage 4: LIMIT
    let result_table = if let Some(limit_val) = extract_limit(query)? {
        let g = ctx.graph(&result_table);
        let df_node = g.const_df(&result_table);
        let root = g.head(df_node, limit_val);
        g.execute(root)?
    } else {
        result_table
    };

    Ok(SqlResult {
        table: result_table,
        columns: result_aliases,
    })
}

// ---------------------------------------------------------------------------
// Table resolution
// ---------------------------------------------------------------------------

/// Resolve a table name: check session registry first, then CSV file.
fn resolve_table(
    ctx: &Context,
    name: &str,
    tables: Option<&HashMap<String, StoredTable>>,
) -> Result<Table, SqlError> {
    // Check session registry (case-insensitive)
    if let Some(registry) = tables {
        let lower = name.to_lowercase();
        if let Some(stored) = registry.get(&lower) {
            return Ok(stored.table.clone_ref());
        }
    }

    // Fall back to CSV file
    let path = normalize_path(name);
    ctx.read_csv(&path).map_err(SqlError::from)
}

/// Extract raw table name from FROM clause (not normalized to a file path).
fn extract_from(from: &[TableWithJoins]) -> Result<String, SqlError> {
    if from.is_empty() {
        return Err(SqlError::Plan("Missing FROM clause".into()));
    }
    if from.len() > 1 {
        return Err(SqlError::Plan(
            "Multiple FROM tables not supported (use JOIN syntax)".into(),
        ));
    }

    match &from[0].relation {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
        _ => Err(SqlError::Plan(
            "Only simple table references are supported in FROM".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert ObjectName to a string.
fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|ident| ident.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

/// Ensure path has .csv extension if no extension present.
fn normalize_path(path: &str) -> String {
    let path = path.trim_matches('\'').trim_matches('"');
    if path.contains('.') {
        path.to_string()
    } else {
        format!("{path}.csv")
    }
}

/// Build a column name -> index map from the table.
fn build_schema(table: &Table) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    let ncols = table.ncols() as usize;
    for i in 0..ncols {
        let name = table.col_name_str(i);
        if !name.is_empty() {
            map.insert(name.to_lowercase(), i);
        }
    }
    map
}

/// Extract GROUP BY column names.
fn extract_group_by_columns(
    group_by: &GroupByExpr,
    schema: &HashMap<String, usize>,
) -> Result<Vec<String>, SqlError> {
    match group_by {
        GroupByExpr::All(_) => Err(SqlError::Plan("GROUP BY ALL not supported".into())),
        GroupByExpr::Expressions(exprs, _modifiers) => {
            let mut cols = Vec::new();
            for expr in exprs {
                match expr {
                    Expr::Identifier(ident) => {
                        let name = ident.value.to_lowercase();
                        if !schema.contains_key(&name) {
                            return Err(SqlError::Plan(format!(
                                "GROUP BY column '{}' not found",
                                name
                            )));
                        }
                        cols.push(name);
                    }
                    _ => {
                        return Err(SqlError::Plan(
                            "Only column names supported in GROUP BY".into(),
                        ))
                    }
                }
            }
            Ok(cols)
        }
    }
}

/// Extract column aliases from a SELECT list (for simple projection).
fn extract_projection_aliases(
    select_items: &[SelectItem],
    schema: &HashMap<String, usize>,
) -> Result<Vec<String>, SqlError> {
    let mut aliases = Vec::new();

    for item in select_items {
        match item {
            SelectItem::Wildcard(_) => {
                let mut cols: Vec<_> = schema.iter().collect();
                cols.sort_by_key(|(_name, idx)| **idx);
                for (name, _idx) in cols {
                    aliases.push(name.clone());
                }
            }
            SelectItem::UnnamedExpr(expr) => {
                aliases.push(expr_default_name(expr));
            }
            SelectItem::ExprWithAlias { alias, .. } => {
                aliases.push(alias.value.to_lowercase());
            }
            _ => return Err(SqlError::Plan("Unsupported SELECT item".into())),
        }
    }

    if aliases.is_empty() {
        return Err(SqlError::Plan("SELECT list is empty".into()));
    }

    Ok(aliases)
}

/// Plan a GROUP BY query.
fn plan_group_by(
    g: &mut Graph,
    select_items: &[SelectItem],
    group_by_cols: &[String],
    schema: &HashMap<String, usize>,
    has_group_by: bool,
) -> Result<(Column, Vec<String>), SqlError> {
    let mut key_nodes = Vec::new();
    let mut agg_ops = Vec::new();
    let mut agg_inputs = Vec::new();
    let mut aliases = Vec::new();

    for col_name in group_by_cols {
        key_nodes.push(g.scan(col_name));
        aliases.push(col_name.clone());
    }

    for item in select_items {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => {
                (expr, Some(alias.value.to_lowercase()))
            }
            SelectItem::Wildcard(_) => {
                return Err(SqlError::Plan(
                    "SELECT * not supported with GROUP BY".into(),
                ))
            }
            _ => return Err(SqlError::Plan("Unsupported SELECT item".into())),
        };

        if is_aggregate(expr) {
            if let Expr::Function(func) = expr {
                let (op, input) = plan_aggregate(g, func, schema)?;
                let agg_alias = alias.unwrap_or_else(|| format_agg_name(func));
                agg_ops.push(op);
                agg_inputs.push(input);
                aliases.push(agg_alias);
            }
        } else if let Expr::Identifier(ident) = expr {
            let name = ident.value.to_lowercase();
            if has_group_by && !group_by_cols.contains(&name) {
                return Err(SqlError::Plan(format!(
                    "Column '{}' must appear in GROUP BY or be in an aggregate function",
                    name
                )));
            }
        } else {
            return Err(SqlError::Plan(
                "Only column references and aggregate functions supported in GROUP BY SELECT"
                    .into(),
            ));
        }
    }

    let node = g.group_by(&key_nodes, &agg_ops, &agg_inputs);
    Ok((node, aliases))
}

/// Plan an aggregate function call.
fn plan_aggregate(
    g: &mut Graph,
    func: &sqlparser::ast::Function,
    schema: &HashMap<String, usize>,
) -> Result<(AggOp, Column), SqlError> {
    use crate::expr::extract_agg_arg_name;

    let name = func.name.to_string().to_lowercase();
    let arg_name = extract_agg_arg_name(func, schema)?;
    let col = g.scan(&arg_name);

    let op = match name.as_str() {
        "sum" => AggOp::Sum,
        "avg" => AggOp::Avg,
        "min" => AggOp::Min,
        "max" => AggOp::Max,
        "count" => AggOp::Count,
        _ => return Err(SqlError::Plan(format!("Unknown aggregate function: {name}"))),
    };

    Ok((op, col))
}

/// Extract ORDER BY expressions from the query.
fn extract_order_by(query: &Query) -> Result<Vec<(String, bool)>, SqlError> {
    match &query.order_by {
        None => Ok(Vec::new()),
        Some(order_by) => {
            let mut result = Vec::new();
            for ob in &order_by.exprs {
                let name = match &ob.expr {
                    Expr::Identifier(ident) => ident.value.to_lowercase(),
                    _ => {
                        return Err(SqlError::Plan(
                            "Only column/alias names supported in ORDER BY".into(),
                        ))
                    }
                };
                let desc = ob.asc.map(|asc| !asc).unwrap_or(false);
                result.push((name, desc));
            }
            Ok(result)
        }
    }
}

/// Plan ORDER BY.
fn plan_order_by(
    g: &mut Graph,
    input: Column,
    order_by: &[(String, bool)],
    result_aliases: &[String],
) -> Result<Column, SqlError> {
    let mut sort_keys = Vec::new();
    let mut descs = Vec::new();

    for (name, desc) in order_by {
        if !result_aliases.contains(name) {
            return Err(SqlError::Plan(format!(
                "ORDER BY column '{}' not found in result columns",
                name
            )));
        }
        sort_keys.push(g.scan(name));
        descs.push(*desc);
    }

    Ok(g.sort(input, &sort_keys, &descs))
}

/// Extract LIMIT value.
fn extract_limit(query: &Query) -> Result<Option<i64>, SqlError> {
    match &query.limit {
        None => Ok(None),
        Some(expr) => match expr {
            Expr::Value(Value::Number(n, _)) => {
                let limit = n
                    .parse::<i64>()
                    .map_err(|_| SqlError::Plan(format!("Invalid LIMIT value: {n}")))?;
                if limit < 0 {
                    return Err(SqlError::Plan("LIMIT must be non-negative".into()));
                }
                Ok(Some(limit))
            }
            _ => Err(SqlError::Plan("LIMIT must be an integer literal".into())),
        },
    }
}

/// Generate a default name for an aggregate function expression.
fn format_agg_name(func: &sqlparser::ast::Function) -> String {
    let fname = func.name.to_string().to_lowercase();
    let arg_str = match &func.args {
        FunctionArguments::List(args) => {
            if args.args.is_empty() {
                "*".to_string()
            } else {
                args.args
                    .iter()
                    .map(|a| format!("{a}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        }
        _ => "*".to_string(),
    };
    format!("{fname}({arg_str})")
}

/// Get a default display name for a bare expression.
fn expr_default_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.to_lowercase(),
        _ => format!("{expr}"),
    }
}
