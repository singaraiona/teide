// SQL planner: translates sqlparser AST into Teide execution graph.

use std::collections::HashMap;

use sqlparser::ast::{
    Distinct, Expr, GroupByExpr, ObjectName, ObjectType, Query, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use teide::{Column, Context, Graph, Table};

use crate::expr::{
    agg_op_from_name, collect_aggregates, expr_default_name, format_agg_name, is_aggregate,
    is_pure_aggregate, plan_agg_input, plan_expr, plan_having_expr, plan_post_agg_expr,
};
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
                    return Err(SqlError::Plan(format!("Table '{table_name}' not found")));
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
        return Err(SqlError::Plan("WITH (CTEs) not supported yet".into()));
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        SetExpr::SetOperation { .. } => {
            return Err(SqlError::Plan(
                "UNION/INTERSECT/EXCEPT not supported yet".into(),
            ))
        }
        _ => {
            return Err(SqlError::Plan(
                "Only simple SELECT queries are supported".into(),
            ))
        }
    };

    for item in &select.from {
        if !item.joins.is_empty() {
            return Err(SqlError::Plan("JOINs not supported yet".into()));
        }
    }

    // DISTINCT flag
    let is_distinct = matches!(&select.distinct, Some(Distinct::Distinct));

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
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => is_aggregate(e),
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

    // Stage 2: GROUP BY / aggregation / DISTINCT
    let (result_table, result_aliases) = if has_group_by || has_aggregates {
        plan_group_select(ctx, &working_table, select_items, &group_by_cols, &schema)?
    } else if is_distinct {
        // DISTINCT without GROUP BY: use GROUP BY on all selected columns
        let aliases = extract_projection_aliases(select_items, &schema)?;
        plan_distinct(ctx, &working_table, &aliases, &schema)?
    } else {
        let aliases = extract_projection_aliases(select_items, &schema)?;
        // Check if projection needs expression evaluation (CAST, arithmetic, etc.)
        let needs_expr = select_items.iter().any(|item| match item {
            SelectItem::Wildcard(_) => false,
            SelectItem::UnnamedExpr(Expr::Identifier(_)) => false,
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(_),
                ..
            } => false,
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(_)) => false,
            SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(_),
                ..
            } => false,
            _ => true, // CAST, arithmetic, function calls, etc.
        });
        if needs_expr {
            plan_expr_select(ctx, &working_table, select_items, &schema)?
        } else {
            (working_table, aliases)
        }
    };

    // Stage 2.5: HAVING (filter on aggregation result)
    let result_table = if let Some(ref having_expr) = select.having {
        // Build HAVING schema from native table column names + display aliases
        // so HAVING can resolve both "v1_sum" (native) and "sum(v1)" (SQL style)
        let mut having_schema = build_schema(&result_table);
        for (i, alias) in result_aliases.iter().enumerate() {
            having_schema.entry(alias.clone()).or_insert(i);
        }
        let mut g = ctx.graph(&result_table);
        let df_node = g.const_df(&result_table);
        let pred = plan_having_expr(&mut g, having_expr, &having_schema, &schema)?;
        let filtered = g.filter(df_node, pred);
        g.execute(filtered)?
    } else {
        result_table
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

    // Stage 4: OFFSET + LIMIT
    let offset_val = extract_offset(query)?;
    let limit_val = extract_limit(query)?;

    let result_table = match (offset_val, limit_val) {
        (Some(off), Some(lim)) => {
            // Skip first `off` rows, then take `lim`
            let total = off + lim;
            let g = ctx.graph(&result_table);
            let df_node = g.const_df(&result_table);
            let head_node = g.head(df_node, total);
            let trimmed = g.execute(head_node)?;
            skip_rows(ctx, &trimmed, off)?
        }
        (Some(off), None) => skip_rows(ctx, &result_table, off)?,
        (None, Some(lim)) => {
            let g = ctx.graph(&result_table);
            let df_node = g.const_df(&result_table);
            let root = g.head(df_node, lim);
            g.execute(root)?
        }
        (None, None) => result_table,
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
// GROUP BY with post-aggregation expressions
// ---------------------------------------------------------------------------

/// Plan a GROUP BY query with support for:
/// - Expressions as aggregate inputs: SUM(v1 + v2)
/// - Post-aggregation arithmetic: SUM(v1) * 2, SUM(v1) / COUNT(v1)
/// - Mixed expressions in SELECT
fn plan_group_select(
    ctx: &Context,
    working_table: &Table,
    select_items: &[SelectItem],
    group_by_cols: &[String],
    schema: &HashMap<String, usize>,
) -> Result<(Table, Vec<String>), SqlError> {
    let has_group_by = !group_by_cols.is_empty();

    // Phase 1: Analyze SELECT items, collect all unique aggregates
    let key_names: Vec<String> = group_by_cols.to_vec();
    let mut all_aggs: Vec<AggInfo> = Vec::new(); // (op, func_ref, alias)
    let mut select_plan: Vec<SelectPlan> = Vec::new();
    let mut final_aliases: Vec<String> = Vec::new();

    for item in select_items {
        let (expr, explicit_alias) = match item {
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

        if let Expr::Identifier(ident) = expr {
            let name = ident.value.to_lowercase();
            if has_group_by && !group_by_cols.contains(&name) {
                return Err(SqlError::Plan(format!(
                    "Column '{}' must appear in GROUP BY or be in an aggregate function",
                    name
                )));
            }
            // Key reference — already included via GROUP BY keys
            let alias = explicit_alias.unwrap_or(name);
            select_plan.push(SelectPlan::KeyRef(alias.clone()));
            final_aliases.push(alias);
        } else if is_pure_aggregate(expr) {
            // Pure aggregate: SUM(v1), COUNT(*)
            let func = match expr {
                Expr::Function(f) => f,
                _ => unreachable!(),
            };
            let agg_alias = format_agg_name(func);
            let agg_idx = register_agg(&mut all_aggs, func, &agg_alias);
            let display = explicit_alias.unwrap_or(agg_alias);
            select_plan.push(SelectPlan::PureAgg(agg_idx, display.clone()));
            final_aliases.push(display);
        } else if is_aggregate(expr) {
            // Mixed expression containing aggregates: SUM(v1) * 2
            let agg_refs = collect_aggregates(expr);
            for (_agg_expr, agg_alias) in &agg_refs {
                if let Expr::Function(f) = _agg_expr {
                    register_agg(&mut all_aggs, f, agg_alias);
                }
            }
            let display = explicit_alias.unwrap_or_else(|| expr_default_name(expr));
            select_plan.push(SelectPlan::PostAggExpr(expr.clone(), display.clone()));
            final_aliases.push(display);
        } else {
            return Err(SqlError::Plan(format!(
                "Expression '{}' must be in GROUP BY or contain an aggregate",
                expr
            )));
        }
    }

    // Phase 2: Execute GROUP BY with keys + all unique aggregates
    let mut g = ctx.graph(working_table);

    let key_nodes: Vec<Column> = key_names.iter().map(|k| g.scan(k)).collect();

    let mut agg_ops = Vec::new();
    let mut agg_inputs = Vec::new();
    for agg in &all_aggs {
        let op = agg_op_from_name(&agg.func_name)?;
        let input = plan_agg_input(&mut g, &agg.func, schema)?;
        agg_ops.push(op);
        agg_inputs.push(input);
    }

    let group_node = g.group_by(&key_nodes, &agg_ops, &agg_inputs);
    let group_result = g.execute(group_node)?;

    // Build result schema from NATIVE column names + our format_agg_name aliases.
    // The C engine names agg columns as "{col}_{suffix}" (e.g., "v1_sum").
    // We also add our aliases so plan_post_agg_expr can resolve either style.
    let mut group_schema = build_schema(&group_result);
    for (i, agg) in all_aggs.iter().enumerate() {
        group_schema
            .entry(agg.alias.clone())
            .or_insert(key_names.len() + i);
    }

    // Phase 3: Check if post-processing is needed
    let needs_post_processing = select_plan
        .iter()
        .any(|p| matches!(p, SelectPlan::PostAggExpr(..)));

    if !needs_post_processing {
        // Simple case: result columns match GROUP BY output directly
        return Ok((group_result, final_aliases));
    }

    // Phase 4: Post-aggregation expressions
    // Build mapping: display alias → native column name
    // SCAN nodes must use native names (what the C table actually contains).
    let mut alias_to_native: HashMap<String, String> = HashMap::new();
    for (i, agg) in all_aggs.iter().enumerate() {
        let col_idx = key_names.len() + i;
        let native = group_result.col_name_str(col_idx);
        alias_to_native.insert(agg.alias.clone(), native.to_string());
    }

    let mut pg = ctx.graph(&group_result);
    let df_node = pg.const_df(&group_result);

    let mut proj_cols = Vec::new();
    let mut proj_aliases = Vec::new();

    for plan in &select_plan {
        match plan {
            SelectPlan::KeyRef(alias) => {
                proj_cols.push(pg.scan(alias));
                proj_aliases.push(alias.clone());
            }
            SelectPlan::PureAgg(idx, alias) => {
                // Always scan by native C engine name
                let col_idx = key_names.len() + *idx;
                let native = group_result.col_name_str(col_idx);
                proj_cols.push(pg.scan(&native));
                proj_aliases.push(alias.clone());
            }
            SelectPlan::PostAggExpr(expr, alias) => {
                let col = plan_post_agg_expr(&mut pg, expr, &alias_to_native)?;
                proj_cols.push(col);
                proj_aliases.push(alias.clone());
            }
        }
    }

    let proj = pg.select(df_node, &proj_cols);
    let result = pg.execute(proj)?;

    Ok((result, final_aliases))
}

struct AggInfo {
    func_name: String,
    func: sqlparser::ast::Function,
    alias: String,
}

fn register_agg(
    all_aggs: &mut Vec<AggInfo>,
    func: &sqlparser::ast::Function,
    alias: &str,
) -> usize {
    // Check for existing aggregate with same alias
    if let Some(idx) = all_aggs.iter().position(|a| a.alias == alias) {
        return idx;
    }
    let idx = all_aggs.len();
    all_aggs.push(AggInfo {
        func_name: func.name.to_string().to_lowercase(),
        func: func.clone(),
        alias: alias.to_string(),
    });
    idx
}

enum SelectPlan {
    KeyRef(String),
    PureAgg(usize, String), // (agg index, display alias)
    PostAggExpr(Expr, String),
}

// ---------------------------------------------------------------------------
// DISTINCT via GROUP BY
// ---------------------------------------------------------------------------

fn plan_distinct(
    ctx: &Context,
    working_table: &Table,
    col_names: &[String],
    schema: &HashMap<String, usize>,
) -> Result<(Table, Vec<String>), SqlError> {
    // GROUP BY all selected columns with a dummy COUNT(*) aggregate
    let mut g = ctx.graph(working_table);
    let key_nodes: Vec<Column> = col_names.iter().map(|k| g.scan(k)).collect();

    // Need at least one aggregate for td_group — use COUNT on first column
    let first_col = schema
        .iter()
        .min_by_key(|(_k, v)| **v)
        .map(|(k, _)| k.clone())
        .ok_or_else(|| SqlError::Plan("DISTINCT on empty table".into()))?;
    let count_input = g.scan(&first_col);
    let group_node = g.group_by(&key_nodes, &[teide::AggOp::Count], &[count_input]);
    let group_result = g.execute(group_node)?;

    // The result has keys + 1 COUNT column. We only want the keys.
    // Build a SELECT projection to drop the count column.
    let pg = ctx.graph(&group_result);
    let df_node = pg.const_df(&group_result);
    let proj_cols: Vec<Column> = col_names.iter().map(|k| pg.scan(k)).collect();
    let proj = pg.select(df_node, &proj_cols);
    let result = pg.execute(proj)?;

    Ok((result, col_names.to_vec()))
}

// ---------------------------------------------------------------------------
// Expression SELECT (non-GROUP-BY with computed columns)
// ---------------------------------------------------------------------------

fn plan_expr_select(
    ctx: &Context,
    working_table: &Table,
    select_items: &[SelectItem],
    schema: &HashMap<String, usize>,
) -> Result<(Table, Vec<String>), SqlError> {
    let mut g = ctx.graph(working_table);
    let df_node = g.const_df(working_table);

    let mut proj_cols = Vec::new();
    let mut aliases = Vec::new();

    for item in select_items {
        match item {
            SelectItem::Wildcard(_) => {
                let mut cols: Vec<_> = schema.iter().collect();
                cols.sort_by_key(|(_name, idx)| **idx);
                for (name, _) in cols {
                    proj_cols.push(g.scan(name));
                    aliases.push(name.clone());
                }
            }
            SelectItem::UnnamedExpr(expr) => {
                let col = plan_expr(&mut g, expr, schema)?;
                proj_cols.push(col);
                aliases.push(expr_default_name(expr));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let col = plan_expr(&mut g, expr, schema)?;
                proj_cols.push(col);
                aliases.push(alias.value.to_lowercase());
            }
            _ => return Err(SqlError::Plan("Unsupported SELECT item".into())),
        }
    }

    let proj = g.select(df_node, &proj_cols);
    let result = g.execute(proj)?;
    Ok((result, aliases))
}

// ---------------------------------------------------------------------------
// OFFSET: skip first N rows
// ---------------------------------------------------------------------------

fn skip_rows(ctx: &Context, table: &Table, offset: i64) -> Result<Table, SqlError> {
    let nrows = table.nrows();
    if offset >= nrows {
        let g = ctx.graph(table);
        let df_node = g.const_df(table);
        let root = g.head(df_node, 0);
        return Ok(g.execute(root)?);
    }
    // td_tail takes the last N rows — exactly what we need after skipping offset
    let keep = nrows - offset;
    let g = ctx.graph(table);
    let df_node = g.const_df(table);
    let root = g.tail(df_node, keep);
    Ok(g.execute(root)?)
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

/// Extract OFFSET value.
fn extract_offset(query: &Query) -> Result<Option<i64>, SqlError> {
    match &query.offset {
        None => Ok(None),
        Some(offset) => match &offset.value {
            Expr::Value(Value::Number(n, _)) => {
                let off = n
                    .parse::<i64>()
                    .map_err(|_| SqlError::Plan(format!("Invalid OFFSET value: {n}")))?;
                if off < 0 {
                    return Err(SqlError::Plan("OFFSET must be non-negative".into()));
                }
                Ok(Some(off))
            }
            _ => Err(SqlError::Plan("OFFSET must be an integer literal".into())),
        },
    }
}
