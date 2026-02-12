// SQL planner: translates sqlparser AST into Teide execution graph.

use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator, Distinct, Expr, GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName,
    ObjectType, Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use teide::{Column, Context, Graph, Table};

use crate::expr::{
    agg_op_from_name, collect_aggregates, collect_window_functions, expr_default_name,
    format_agg_name, has_window_functions, is_aggregate, is_count_distinct, is_pure_aggregate,
    is_window_function, parse_window_frame, plan_agg_input, plan_expr, plan_having_expr,
    plan_post_agg_expr,
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
    // Handle CTEs (WITH clause)
    let cte_tables: HashMap<String, StoredTable>;
    let effective_tables: Option<&HashMap<String, StoredTable>>;

    if let Some(with) = &query.with {
        let mut cte_map: HashMap<String, StoredTable> = match tables {
            Some(t) => t.clone(),
            None => HashMap::new(),
        };
        for cte in &with.cte_tables {
            let cte_name = cte.alias.name.value.to_lowercase();
            let result = plan_query(ctx, &cte.query, Some(&cte_map))?;
            cte_map.insert(
                cte_name,
                StoredTable {
                    table: result.table,
                    columns: result.columns,
                },
            );
        }
        cte_tables = cte_map;
        effective_tables = Some(&cte_tables);
    } else {
        effective_tables = tables;
    }

    // Handle UNION ALL / set operations
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        SetExpr::SetOperation {
            op: sqlparser::ast::SetOperator::Union,
            set_quantifier,
            left,
            right,
        } => {
            if !matches!(set_quantifier, sqlparser::ast::SetQuantifier::All) {
                return Err(SqlError::Plan(
                    "UNION (without ALL) not supported yet; use UNION ALL".into(),
                ));
            }
            // Execute both sides
            let left_query = Query {
                with: None,
                body: left.clone(),
                order_by: None,
                limit: None,
                offset: None,
                fetch: None,
                locks: vec![],
                limit_by: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
            };
            let right_query = Query {
                with: None,
                body: right.clone(),
                order_by: None,
                limit: None,
                offset: None,
                fetch: None,
                locks: vec![],
                limit_by: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
            };
            let left_result = plan_query(ctx, &left_query, effective_tables)?;
            let right_result = plan_query(ctx, &right_query, effective_tables)?;

            if left_result.columns.len() != right_result.columns.len() {
                return Err(SqlError::Plan(format!(
                    "UNION ALL: column count mismatch ({} vs {})",
                    left_result.columns.len(),
                    right_result.columns.len()
                )));
            }

            // Concatenate tables column by column
            let result = concat_tables(ctx, &left_result.table, &right_result.table)?;

            // Apply ORDER BY and LIMIT from the outer query
            return apply_post_processing(ctx, query, result, left_result.columns, effective_tables);
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let is_all = matches!(
                set_quantifier,
                sqlparser::ast::SetQuantifier::All
            );

            let left_query = Query {
                with: None,
                body: left.clone(),
                order_by: None,
                limit: None,
                offset: None,
                fetch: None,
                locks: vec![],
                limit_by: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
            };
            let right_query = Query {
                with: None,
                body: right.clone(),
                order_by: None,
                limit: None,
                offset: None,
                fetch: None,
                locks: vec![],
                limit_by: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
            };
            let left_result = plan_query(ctx, &left_query, effective_tables)?;
            let right_result = plan_query(ctx, &right_query, effective_tables)?;

            if left_result.columns.len() != right_result.columns.len() {
                return Err(SqlError::Plan(format!(
                    "{:?}: column count mismatch ({} vs {})",
                    op,
                    left_result.columns.len(),
                    right_result.columns.len()
                )));
            }

            let keep_matches = matches!(op, sqlparser::ast::SetOperator::Intersect);

            let result = exec_set_operation(
                ctx,
                &left_result.table,
                &right_result.table,
                keep_matches,
            )?;

            // Without ALL: apply DISTINCT
            let result = if !is_all {
                let aliases: Vec<String> = (0..result.ncols() as usize)
                    .map(|i| result.col_name_str(i).to_string())
                    .collect();
                let schema = build_schema(&result);
                let (distinct_result, _) = plan_distinct(ctx, &result, &aliases, &schema)?;
                distinct_result
            } else {
                result
            };

            return apply_post_processing(
                ctx,
                query,
                result,
                left_result.columns,
                effective_tables,
            );
        }
        _ => {
            return Err(SqlError::Plan(
                "Only simple SELECT queries are supported".into(),
            ))
        }
    };

    // DISTINCT flag
    let is_distinct = matches!(&select.distinct, Some(Distinct::Distinct));

    // Resolve FROM clause (possibly with JOINs)
    let (table, schema) = resolve_from(ctx, &select.from, effective_tables)?;

    // Build SELECT alias → expression map (for GROUP BY on aliases)
    let select_items = &select.projection;
    let mut alias_exprs: HashMap<String, Expr> = HashMap::new();
    for item in select_items {
        if let SelectItem::ExprWithAlias { expr, alias } = item {
            alias_exprs.insert(alias.value.to_lowercase(), expr.clone());
        }
    }

    // Collect SELECT aliases for positional GROUP BY (GROUP BY 1, 2)
    let select_aliases_for_gb: Vec<String> = select_items
        .iter()
        .map(|item| match item {
            SelectItem::ExprWithAlias { alias, .. } => alias.value.to_lowercase(),
            SelectItem::UnnamedExpr(e) => crate::expr::expr_default_name(e),
            _ => String::new(),
        })
        .collect();

    // GROUP BY column names (accepts table columns, SELECT aliases, expressions, and positions)
    let group_by_cols = extract_group_by_columns(
        &select.group_by,
        &schema,
        &mut alias_exprs,
        &select_aliases_for_gb,
    )?;
    let has_group_by = !group_by_cols.is_empty();

    // Detect aggregates in SELECT
    let has_aggregates = select_items.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => is_aggregate(e),
        _ => false,
    });

    // Stage 1: WHERE filter (resolve subqueries first)
    // For GROUP BY queries, push predicate down as a mask instead of materializing.
    let (working_table, filter_mask): (Table, Option<*mut teide::td_t>) =
        if let Some(ref where_expr) = select.selection {
            let resolved = if has_subqueries(where_expr) {
                resolve_subqueries(ctx, where_expr, effective_tables)?
            } else {
                where_expr.clone()
            };
            if has_group_by || has_aggregates {
                // Evaluate predicate only — pass as mask to GROUP BY
                let mask_ptr = {
                    let mut g = ctx.graph(&table);
                    let pred = plan_expr(&mut g, &resolved, &schema)?;
                    g.execute_raw(pred)?
                };
                (table, Some(mask_ptr))
            } else {
                // Non-GROUP BY: materialize as before
                let mut g = ctx.graph(&table);
                let df_node = g.const_df(&table);
                let pred = plan_expr(&mut g, &resolved, &schema)?;
                let filtered = g.filter(df_node, pred);
                (g.execute(filtered)?, None)
            }
        } else {
            (table, None)
        };

    // Stage 1.5: Window functions (before GROUP BY)
    let has_windows = has_window_functions(select_items);
    let (working_table, schema, select_items) = if has_windows {
        let (wt, ws, wi) =
            plan_window_stage(ctx, &working_table, select_items, &schema)?;
        (wt, ws, std::borrow::Cow::Owned(wi))
    } else {
        (working_table, schema, std::borrow::Cow::Borrowed(select_items))
    };
    let select_items: &[SelectItem] = &select_items;

    // Stage 2: GROUP BY / aggregation / DISTINCT
    let (result_table, result_aliases) = if has_group_by || has_aggregates {
        plan_group_select(ctx, &working_table, select_items, &group_by_cols, &schema, &alias_exprs, filter_mask)?
    } else if is_distinct {
        // DISTINCT without GROUP BY: use GROUP BY on all selected columns
        let aliases = extract_projection_aliases(select_items, &schema)?;
        plan_distinct(ctx, &working_table, &aliases, &schema)?
    } else {
        let aliases = extract_projection_aliases(select_items, &schema)?;
        // Check if projection needs expression evaluation (CAST, arithmetic, etc.)
        // Force projection when window functions added extra columns,
        // or when SELECT items contain expressions needing evaluation.
        let needs_expr = has_windows || select_items.iter().any(|item| match item {
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

    // Stage 3+4: ORDER BY (optionally fused with LIMIT)
    let order_by_exprs = extract_order_by(query)?;
    let offset_val = extract_offset(query)?;
    let limit_val = extract_limit(query)?;

    let (result_table, limit_fused) = if !order_by_exprs.is_empty() {
        let table_col_names: Vec<String> = (0..result_table.ncols() as usize)
            .map(|i| result_table.col_name_str(i).to_string())
            .collect();
        let mut g = ctx.graph(&result_table);
        let df_node = g.const_df(&result_table);
        let sort_node = plan_order_by(&mut g, df_node, &order_by_exprs, &result_aliases, &table_col_names)?;

        // Fuse LIMIT into HEAD(SORT) so the engine only gathers N rows
        let total_limit = match (offset_val, limit_val) {
            (Some(off), Some(lim)) => Some(off + lim),
            (None, Some(lim)) => Some(lim),
            _ => None,
        };
        let root = match total_limit {
            Some(n) => g.head(sort_node, n),
            None => sort_node,
        };
        (g.execute(root)?, total_limit.is_some())
    } else {
        (result_table, false)
    };

    // Stage 4: OFFSET + LIMIT (only parts not already fused)
    let result_table = if limit_fused {
        match offset_val {
            Some(off) => skip_rows(ctx, &result_table, off)?,
            None => result_table,
        }
    } else {
        match (offset_val, limit_val) {
            (Some(off), Some(lim)) => {
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
        }
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

    // Fall back to CSV file (only if it looks like a path)
    let path = normalize_path(name);
    ctx.read_csv(&path).map_err(|e| {
        // If the name looks like a bare identifier (no path separators, no extension),
        // report it as a missing table rather than an I/O error.
        if !name.contains('/') && !name.contains('\\') && !name.contains('.') {
            SqlError::Plan(format!("Table '{}' not found", name))
        } else {
            SqlError::from(e)
        }
    })
}

/// Extract equi-join keys from an ON condition.
/// Returns (left_col_name, right_col_name) pairs.
fn extract_join_keys(
    expr: &Expr,
    left_schema: &HashMap<String, usize>,
    right_schema: &HashMap<String, usize>,
) -> Result<Vec<(String, String)>, SqlError> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let l_name = extract_col_name(left)?;
            let r_name = extract_col_name(right)?;

            // Determine which side belongs to which table
            if left_schema.contains_key(&l_name) && right_schema.contains_key(&r_name) {
                Ok(vec![(l_name, r_name)])
            } else if left_schema.contains_key(&r_name) && right_schema.contains_key(&l_name) {
                Ok(vec![(r_name, l_name)])
            } else {
                Err(SqlError::Plan(format!(
                    "JOIN ON columns '{l_name}' and '{r_name}' not found in respective tables"
                )))
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut keys = extract_join_keys(left, left_schema, right_schema)?;
            keys.extend(extract_join_keys(right, left_schema, right_schema)?);
            Ok(keys)
        }
        _ => Err(SqlError::Plan(
            "Only equi-join conditions (col1 = col2 [AND ...]) are supported".into(),
        )),
    }
}

/// Extract a column name from an expression (handles Identifier and CompoundIdentifier).
fn extract_col_name(expr: &Expr) -> Result<String, SqlError> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.to_lowercase()),
        Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                Ok(parts[1].value.to_lowercase())
            } else {
                Err(SqlError::Plan(format!(
                    "Unsupported compound identifier in JOIN: {expr}"
                )))
            }
        }
        _ => Err(SqlError::Plan(format!(
            "Unsupported expression in JOIN ON: {expr}"
        ))),
    }
}

/// Resolve the FROM clause into a working table + schema.
/// Handles simple tables, table aliases, FROM subqueries, and JOINs.
fn resolve_from(
    ctx: &Context,
    from: &[TableWithJoins],
    tables: Option<&HashMap<String, StoredTable>>,
) -> Result<(Table, HashMap<String, usize>), SqlError> {
    if from.is_empty() {
        return Err(SqlError::Plan("Missing FROM clause".into()));
    }

    // Multiple FROM tables = implicit CROSS JOIN: SELECT * FROM t1, t2
    if from.len() > 1 {
        let (mut result_table, mut result_schema) =
            resolve_table_factor(ctx, &from[0].relation, tables)?;
        // Process joins on first table
        for join in &from[0].joins {
            let (right_table, right_schema) = resolve_table_factor(ctx, &join.relation, tables)?;
            result_table = exec_cross_join(ctx, &result_table, &right_table)?;
            result_schema = build_schema(&result_table);
            let _ = right_schema;
        }
        // Cross join with subsequent FROM tables
        for twj in &from[1..] {
            let (right_table, _) = resolve_table_factor(ctx, &twj.relation, tables)?;
            result_table = exec_cross_join(ctx, &result_table, &right_table)?;
            result_schema = build_schema(&result_table);
            for join in &twj.joins {
                let (right_table2, _) = resolve_table_factor(ctx, &join.relation, tables)?;
                result_table = exec_cross_join(ctx, &result_table, &right_table2)?;
                result_schema = build_schema(&result_table);
            }
        }
        return Ok((result_table, result_schema));
    }

    let twj = &from[0];

    // Resolve the base (left) table
    let (mut left_table, mut left_schema) = resolve_table_factor(ctx, &twj.relation, tables)?;

    // Process JOINs
    for join in &twj.joins {
        let (right_table, right_schema) = resolve_table_factor(ctx, &join.relation, tables)?;

        // Determine join type
        let join_type: u8 = match &join.join_operator {
            JoinOperator::Inner(_) => 0,
            JoinOperator::LeftOuter(_) => 1,
            JoinOperator::RightOuter(_) => {
                // RIGHT JOIN = swap left/right then LEFT JOIN
                // We'll handle this by swapping and post-reordering
                1
            }
            JoinOperator::CrossJoin => {
                let result = exec_cross_join(ctx, &left_table, &right_table)?;
                let merged_schema = build_schema(&result);
                left_table = result;
                left_schema = merged_schema;
                continue;
            }
            _ => {
                return Err(SqlError::Plan(format!(
                    "Unsupported join type: {:?}",
                    join.join_operator
                )));
            }
        };

        let is_right_join = matches!(&join.join_operator, JoinOperator::RightOuter(_));

        // Extract ON condition
        let on_expr = match &join.join_operator {
            JoinOperator::Inner(c)
            | JoinOperator::LeftOuter(c)
            | JoinOperator::RightOuter(c) => match c {
                JoinConstraint::On(expr) => expr.clone(),
                _ => {
                    return Err(SqlError::Plan(
                        "Only ON conditions are supported for JOINs".into(),
                    ))
                }
            },
            _ => {
                return Err(SqlError::Plan("JOIN requires ON condition".into()));
            }
        };

        // For RIGHT JOIN, determine which is actual_left/right
        // We clone_ref to avoid borrow conflicts with the graph
        let (al_table, al_schema, ar_table, ar_schema) = if is_right_join {
            (right_table.clone_ref(), right_schema.clone(), left_table.clone_ref(), left_schema.clone())
        } else {
            (left_table.clone_ref(), left_schema.clone(), right_table.clone_ref(), right_schema.clone())
        };

        // Extract equi-join keys
        let join_keys = extract_join_keys(&on_expr, &al_schema, &ar_schema)?;
        if join_keys.is_empty() {
            return Err(SqlError::Plan("JOIN ON must have at least one equi-join key".into()));
        }

        // Build join graph (scoped to avoid borrow conflict)
        let result = {
            let mut g = ctx.graph(&al_table);
            let left_df_node = g.const_df(&al_table);
            let right_df_node = g.const_df(&ar_table);

            let left_key_nodes: Vec<teide::Column> =
                join_keys.iter().map(|(lk, _)| g.scan(lk)).collect();

            // Right keys: use const_vec to avoid cross-graph references
            let mut right_key_nodes: Vec<teide::Column> = Vec::new();
            for (_, rk) in &join_keys {
                let right_sym = teide::sym_intern(rk);
                let right_col_ptr =
                    unsafe { teide::ffi_table_get_col(ar_table.as_raw(), right_sym) };
                if right_col_ptr.is_null() {
                    return Err(SqlError::Plan(format!(
                        "Right key column '{}' not found",
                        rk
                    )));
                }
                right_key_nodes.push(g.const_vec(right_col_ptr));
            }

            let joined = g.join(
                left_df_node,
                &left_key_nodes,
                right_df_node,
                &right_key_nodes,
                join_type,
            );

            g.execute(joined)?
        };

        // Build merged schema
        let merged_schema = build_schema(&result);

        left_table = result;
        left_schema = merged_schema;
    }

    Ok((left_table, left_schema))
}

/// Resolve a single TableFactor (table name or FROM subquery) into a table + schema.
fn resolve_table_factor(
    ctx: &Context,
    factor: &TableFactor,
    tables: Option<&HashMap<String, StoredTable>>,
) -> Result<(Table, HashMap<String, usize>), SqlError> {
    match factor {
        TableFactor::Table { name, .. } => {
            let table_name = object_name_to_string(name);
            let table = resolve_table(ctx, &table_name, tables)?;
            let schema = build_schema(&table);
            Ok((table, schema))
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let alias_name = alias
                .as_ref()
                .map(|a| a.name.value.to_lowercase())
                .ok_or_else(|| SqlError::Plan("Subquery in FROM requires an alias".into()))?;
            let result = plan_query(ctx, subquery, tables)?;
            let schema = build_result_schema(&result.table, &result.columns);
            let _ = alias_name; // alias used for qualification, schema uses column names
            Ok((result.table, schema))
        }
        _ => Err(SqlError::Plan(
            "Only simple table references and subqueries are supported in FROM".into(),
        )),
    }
}

/// Build a schema from a result table using provided column aliases.
fn build_result_schema(table: &Table, aliases: &[String]) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    for (i, alias) in aliases.iter().enumerate() {
        map.insert(alias.clone(), i);
    }
    // Also add native column names
    let ncols = table.ncols() as usize;
    for i in 0..ncols {
        let name = table.col_name_str(i);
        if !name.is_empty() {
            map.entry(name.to_lowercase()).or_insert(i);
        }
    }
    map
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
    alias_exprs: &HashMap<String, Expr>,
    filter_mask: Option<*mut teide::td_t>,
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

        // Check if this SELECT item is an expression whose alias is a GROUP BY key
        if let Some(ref alias) = explicit_alias {
            if group_by_cols.contains(alias) && alias_exprs.contains_key(alias) {
                select_plan.push(SelectPlan::KeyRef(alias.clone()));
                final_aliases.push(alias.clone());
                continue;
            }
        }

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
            // Check if this expression matches a GROUP BY expression key
            let expr_str = format!("{expr}").to_lowercase();
            let mut matched_key = None;
            for (alias, gb_expr) in alias_exprs.iter() {
                if group_by_cols.contains(alias) {
                    let gb_str = format!("{gb_expr}").to_lowercase();
                    if gb_str == expr_str {
                        matched_key = Some(alias.clone());
                        break;
                    }
                }
            }
            if let Some(key) = matched_key {
                let display = explicit_alias.unwrap_or_else(|| expr_default_name(expr));
                select_plan.push(SelectPlan::KeyRef(key));
                final_aliases.push(display);
            } else if !has_group_by {
                // No GROUP BY — this shouldn't happen (would have been caught earlier)
                return Err(SqlError::Plan(format!(
                    "Expression '{}' must be in GROUP BY or contain an aggregate",
                    expr
                )));
            } else {
                return Err(SqlError::Plan(format!(
                    "Expression '{}' must be in GROUP BY or contain an aggregate",
                    expr
                )));
            }
        }
    }

    // Check for COUNT(DISTINCT col) — handle via two-phase aggregation
    let has_count_distinct = all_aggs.iter().any(|a| is_count_distinct(&a.func));
    if has_count_distinct {
        return plan_count_distinct_group(
            ctx,
            working_table,
            &key_names,
            &all_aggs,
            &select_plan,
            &final_aliases,
            schema,
            alias_exprs,
        );
    }

    // Phase 2: Execute GROUP BY with keys + all unique aggregates
    let mut g = ctx.graph(working_table);

    let mut key_nodes: Vec<Column> = Vec::new();
    for k in &key_names {
        if let Some(expr) = alias_exprs.get(k) {
            // Expression-based key (e.g., CASE WHEN ... AS bucket, GROUP BY bucket)
            key_nodes.push(plan_expr(&mut g, expr, schema)?);
        } else {
            key_nodes.push(g.scan(k));
        }
    }

    let mut agg_ops = Vec::new();
    let mut agg_inputs = Vec::new();
    for agg in &all_aggs {
        let op = agg_op_from_name(&agg.func_name)?;
        let input = plan_agg_input(&mut g, &agg.func, schema)?;
        agg_ops.push(op);
        agg_inputs.push(input);
    }

    let group_node = g.group_by(&key_nodes, &agg_ops, &agg_inputs);

    // Push filter mask into the graph so exec_group skips filtered rows
    if let Some(mask) = filter_mask {
        unsafe { g.set_filter_mask(mask); }
    }
    let group_result = g.execute(group_node)?;
    // Release the mask reference (graph holds its own via set_filter_mask)
    if let Some(mask) = filter_mask {
        unsafe { teide::ffi_release(mask); }
    }

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
// COUNT(DISTINCT) via two-phase GROUP BY
// ---------------------------------------------------------------------------

/// Handle GROUP BY queries containing COUNT(DISTINCT col).
/// Phase 1: GROUP BY [original_keys + distinct_col] to get unique combos
/// Phase 2: GROUP BY [original_keys] with COUNT(*) to count unique values
/// Non-DISTINCT aggregates are computed in phase 1 and use FIRST in phase 2.
#[allow(clippy::too_many_arguments)]
fn plan_count_distinct_group(
    ctx: &Context,
    working_table: &Table,
    key_names: &[String],
    all_aggs: &[AggInfo],
    _select_plan: &[SelectPlan],
    final_aliases: &[String],
    schema: &HashMap<String, usize>,
    alias_exprs: &HashMap<String, Expr>,
) -> Result<(Table, Vec<String>), SqlError> {
    // Collect the DISTINCT column names and regular aggs
    let mut distinct_cols: Vec<String> = Vec::new();
    let mut regular_aggs: Vec<&AggInfo> = Vec::new();

    for agg in all_aggs {
        if is_count_distinct(&agg.func) {
            // Extract the column name from the aggregate argument
            if let sqlparser::ast::FunctionArguments::List(args) = &agg.func.args {
                if let Some(sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(ident)),
                )) = args.args.first()
                {
                    let col = ident.value.to_lowercase();
                    if !distinct_cols.contains(&col) {
                        distinct_cols.push(col);
                    }
                } else {
                    return Err(SqlError::Plan(
                        "COUNT(DISTINCT) requires a simple column reference".into(),
                    ));
                }
            }
        } else {
            regular_aggs.push(agg);
        }
    }

    // Phase 1: GROUP BY [original_keys + distinct_cols] with regular aggs
    let mut phase1_keys: Vec<String> = key_names.to_vec();
    for dc in &distinct_cols {
        if !phase1_keys.contains(dc) {
            phase1_keys.push(dc.clone());
        }
    }

    let mut g = ctx.graph(working_table);
    let mut key_nodes: Vec<Column> = Vec::new();
    for k in &phase1_keys {
        if let Some(expr) = alias_exprs.get(k) {
            key_nodes.push(plan_expr(&mut g, expr, schema)?);
        } else {
            key_nodes.push(g.scan(k));
        }
    }

    // Regular aggregates computed in phase 1
    let mut phase1_agg_ops = Vec::new();
    let mut phase1_agg_inputs = Vec::new();
    for agg in &regular_aggs {
        let op = agg_op_from_name(&agg.func_name)?;
        let input = plan_agg_input(&mut g, &agg.func, schema)?;
        phase1_agg_ops.push(op);
        phase1_agg_inputs.push(input);
    }

    // Need at least one aggregate; if only COUNT(DISTINCT), use dummy COUNT(*)
    if phase1_agg_ops.is_empty() {
        let first_col = schema
            .iter()
            .min_by_key(|(_, v)| **v)
            .map(|(k, _)| k.clone())
            .ok_or_else(|| SqlError::Plan("Empty schema".into()))?;
        phase1_agg_ops.push(teide::AggOp::Count);
        phase1_agg_inputs.push(g.scan(&first_col));
    }

    let group_node = g.group_by(&key_nodes, &phase1_agg_ops, &phase1_agg_inputs);
    let phase1_result = g.execute(group_node)?;

    // Phase 2: GROUP BY [original_keys] with COUNT(*) for each distinct col
    // and FIRST for each regular aggregate
    let mut g2 = ctx.graph(&phase1_result);
    let phase2_keys: Vec<Column> = key_names.iter().map(|k| g2.scan(k)).collect();

    // For no-GROUP-BY case (e.g., SELECT COUNT(DISTINCT id1) FROM t),
    // we need a scalar reduction. Use the distinct col as key in phase 1,
    // then count rows.
    if key_names.is_empty() {
        // Phase 1 grouped by distinct_cols → nrows = unique count.
        // Use a scalar GROUP BY (no keys) with COUNT(*) to produce a 1-row table.
        let first_col_name = phase1_result.col_name_str(0).to_string();
        let mut g2 = ctx.graph(&phase1_result);
        let count_input = g2.scan(&first_col_name);
        let group_node = g2.group_by(&[], &[teide::AggOp::Count], &[count_input]);
        let result = g2.execute(group_node)?;
        return Ok((result, final_aliases.to_vec()));
    }

    let mut phase2_agg_ops = Vec::new();
    let mut phase2_agg_inputs = Vec::new();

    // COUNT(DISTINCT col) → COUNT(*) on the distinct col (counts unique groups)
    for dc in &distinct_cols {
        phase2_agg_ops.push(teide::AggOp::Count);
        phase2_agg_inputs.push(g2.scan(dc));
    }

    // Regular aggs → re-aggregate in phase 2 with compatible ops:
    // SUM→SUM, MIN→MIN, MAX→MAX, COUNT→SUM (sum of partial counts), AVG→not directly supported
    let phase1_schema = build_schema(&phase1_result);
    for agg in &regular_aggs {
        let native = predict_phase1_col(&phase1_result, &agg.alias, phase1_keys.len(), all_aggs, agg);
        if phase1_schema.contains_key(&native) {
            let phase2_op = match agg.func_name.as_str() {
                "sum" => teide::AggOp::Sum,
                "min" => teide::AggOp::Min,
                "max" => teide::AggOp::Max,
                "count" => teide::AggOp::Sum, // sum of partial counts
                "avg" => {
                    return Err(SqlError::Plan(
                        "AVG cannot be mixed with COUNT(DISTINCT) yet".into(),
                    ));
                }
                _ => teide::AggOp::First,
            };
            phase2_agg_ops.push(phase2_op);
            phase2_agg_inputs.push(g2.scan(&native));
        } else {
            return Err(SqlError::Plan(format!(
                "Aggregate '{}' not found in phase 1 result (looked for '{}')",
                agg.alias, native
            )));
        }
    }

    let group_node2 = g2.group_by(&phase2_keys, &phase2_agg_ops, &phase2_agg_inputs);
    let phase2_result = g2.execute(group_node2)?;

    Ok((phase2_result, final_aliases.to_vec()))
}

/// Predict the native column name for an aggregate in the phase 1 result.
fn predict_phase1_col(
    result: &Table,
    _alias: &str,
    n_keys: usize,
    all_aggs: &[AggInfo],
    target: &AggInfo,
) -> String {
    // Find the index of this agg among non-count-distinct aggs
    let mut reg_idx = 0;
    for agg in all_aggs {
        if std::ptr::eq(agg, target) {
            break;
        }
        if !is_count_distinct(&agg.func) {
            reg_idx += 1;
        }
    }
    // The phase1 result has keys first, then regular agg columns
    let col_idx = n_keys + reg_idx;
    result.col_name_str(col_idx).to_string()
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
// Window function stage: execute window functions and append result columns
// ---------------------------------------------------------------------------

/// Execute window functions and return (updated_table, updated_schema, rewritten_select_items).
/// Window function calls in SELECT are replaced with identifier references to the new columns.
fn plan_window_stage(
    ctx: &Context,
    table: &Table,
    select_items: &[SelectItem],
    schema: &HashMap<String, usize>,
) -> Result<(Table, HashMap<String, usize>, Vec<SelectItem>), SqlError> {
    let win_funcs = collect_window_functions(select_items)?;
    if win_funcs.is_empty() {
        // No actual window functions found (shouldn't happen, caller checked)
        let new_schema = schema.clone();
        return Ok((table.clone_ref(), new_schema, select_items.to_vec()));
    }

    // Build a graph for the window operation
    let mut g = ctx.graph(table);
    let df_node = g.const_df(table);

    // For each window function, we need partition keys, order keys, and func info.
    // Group by identical WindowSpec for efficiency, but for simplicity we execute
    // one OP_WINDOW per unique spec (most queries have one spec).

    // Map: display_name -> column_name_in_result (e.g. "_w0", "_w1")
    let mut win_col_names: Vec<String> = Vec::new();
    let mut win_display_names: Vec<String> = Vec::new();

    // Collect all window funcs and build arrays for td_window_op
    let mut part_key_cols: Vec<Column> = Vec::new();
    let mut order_key_cols: Vec<Column> = Vec::new();
    let mut order_descs: Vec<bool> = Vec::new();
    let mut funcs: Vec<teide::WindowFunc> = Vec::new();
    let mut func_input_cols: Vec<Column> = Vec::new();

    // Use the spec from the first window function (for now, all must share same spec)
    let first_spec = &win_funcs[0].1.spec;
    let (frame_type, frame_start, frame_end) = parse_window_frame(first_spec);

    // Build partition key columns
    for part_expr in &first_spec.partition_by {
        let col = plan_expr(&mut g, part_expr, schema)?;
        part_key_cols.push(col);
    }

    // Build order key columns
    for ob in &first_spec.order_by {
        let col = plan_expr(&mut g, &ob.expr, schema)?;
        order_key_cols.push(col);
        order_descs.push(ob.asc == Some(false));
    }

    // Build function list
    for (_idx, info) in &win_funcs {
        funcs.push(info.func);

        // For functions that need an input column
        let input_col = if let Some(ref input_expr) = info.input_expr {
            plan_expr(&mut g, input_expr, schema)?
        } else {
            // ROW_NUMBER, RANK, etc. don't use input — pass first column as dummy
            let first_col_name = schema
                .iter()
                .min_by_key(|(_, v)| **v)
                .map(|(k, _)| k.clone())
                .unwrap_or_default();
            g.scan(&first_col_name)
        };
        func_input_cols.push(input_col);

        let col_name = format!("_w{}", win_col_names.len());
        win_display_names.push(info.display_name.clone());
        win_col_names.push(col_name);
    }

    // Execute the window operation
    let win_node = g.window_op(
        df_node,
        &part_key_cols,
        &order_key_cols,
        &order_descs,
        &funcs,
        &func_input_cols,
        frame_type,
        frame_start,
        frame_end,
    );

    let result = g.execute(win_node)?;

    // Build updated schema (original columns + window columns)
    let mut new_schema = build_schema(&result);
    // Also add display names as aliases (e.g. "row_number()" -> _w0 column index)
    for (i, display) in win_display_names.iter().enumerate() {
        let col_idx = new_schema.get(&win_col_names[i]).copied().unwrap_or(0);
        new_schema.entry(display.clone()).or_insert(col_idx);
    }

    // Rewrite SELECT items: replace window function calls with identifier refs
    let mut win_idx = 0;
    let new_items: Vec<SelectItem> = select_items
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) if is_window_function(expr) => {
                let col_name = win_col_names[win_idx].clone();
                win_idx += 1;
                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(col_name)))
            }
            SelectItem::ExprWithAlias { expr, alias } if is_window_function(expr) => {
                let col_name = win_col_names[win_idx].clone();
                win_idx += 1;
                SelectItem::ExprWithAlias {
                    expr: Expr::Identifier(Ident::new(col_name)),
                    alias: alias.clone(),
                }
            }
            other => other.clone(),
        })
        .collect();

    Ok((result, new_schema, new_items))
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
// UNION ALL: concatenate two tables
// ---------------------------------------------------------------------------

fn concat_tables(_ctx: &Context, left: &Table, right: &Table) -> Result<Table, SqlError> {
    let ncols = left.ncols();
    if ncols != right.ncols() {
        return Err(SqlError::Plan("UNION ALL: column count mismatch".into()));
    }

    // Build a new table with concatenated columns
    let result_raw = unsafe { teide::ffi_table_new(ncols) };
    if result_raw.is_null() {
        return Err(SqlError::Engine(teide::Error::Oom));
    }

    let mut result_raw = result_raw;
    for c in 0..ncols {
        let l_col = left.get_col_idx(c).ok_or(SqlError::Plan(
            "UNION ALL: left column missing".into(),
        ))?;
        let r_col = right.get_col_idx(c).ok_or(SqlError::Plan(
            "UNION ALL: right column missing".into(),
        ))?;
        let merged = unsafe { teide::ffi_vec_concat(l_col, r_col) };
        if merged.is_null() || teide::ffi_is_err(merged) {
            return Err(SqlError::Engine(teide::Error::Oom));
        }
        let name_id = left.col_name(c);
        result_raw = unsafe { teide::ffi_table_add_col(result_raw, name_id, merged) };
        unsafe { teide::ffi_release(merged) };
    }

    // Wrap in a Table with proper RAII
    unsafe { teide::ffi_retain(result_raw) };
    Ok(unsafe { Table::from_raw(result_raw) })
}

/// Execute a CROSS JOIN (Cartesian product) of two tables.
fn exec_cross_join(_ctx: &Context, left: &Table, right: &Table) -> Result<Table, SqlError> {
    let l_nrows = left.nrows() as usize;
    let r_nrows = right.nrows() as usize;
    let out_nrows = l_nrows.checked_mul(r_nrows).ok_or_else(|| {
        SqlError::Plan("CROSS JOIN result too large".into())
    })?;
    let l_ncols = left.ncols();
    let r_ncols = right.ncols();

    let result_raw = unsafe { teide::ffi_table_new(l_ncols + r_ncols) };
    if result_raw.is_null() {
        return Err(SqlError::Engine(teide::Error::Oom));
    }
    let mut result_raw = result_raw;

    // Left columns: repeat each row r_nrows times
    for c in 0..l_ncols {
        let col = left.get_col_idx(c).ok_or(SqlError::Plan(
            "CROSS JOIN: left column missing".into(),
        ))?;
        let name_id = left.col_name(c);
        let col_type = unsafe { teide::raw::td_type(col) };
        let esz = unsafe { teide::raw::td_type_sizes[col_type as usize] } as usize;
        let new_col = unsafe { teide::raw::td_vec_new(col_type, out_nrows as i64) };
        if new_col.is_null() || teide::ffi_is_err(new_col) {
            return Err(SqlError::Engine(teide::Error::Oom));
        }
        unsafe { *((*new_col).val.as_mut_ptr() as *mut i64) = out_nrows as i64 };
        let src = unsafe { teide::raw::td_data(col) };
        let dst = unsafe { teide::raw::td_data(new_col) };
        for lr in 0..l_nrows {
            for rr in 0..r_nrows {
                let out_row = lr * r_nrows + rr;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        src.add(lr * esz),
                        dst.add(out_row * esz),
                        esz,
                    );
                }
            }
        }
        result_raw = unsafe { teide::ffi_table_add_col(result_raw, name_id, new_col) };
        unsafe { teide::ffi_release(new_col) };
    }

    // Right columns: tile the entire column l_nrows times
    for c in 0..r_ncols {
        let col = right.get_col_idx(c).ok_or(SqlError::Plan(
            "CROSS JOIN: right column missing".into(),
        ))?;
        let name_id = right.col_name(c);
        let col_type = unsafe { teide::raw::td_type(col) };
        let esz = unsafe { teide::raw::td_type_sizes[col_type as usize] } as usize;
        let new_col = unsafe { teide::raw::td_vec_new(col_type, out_nrows as i64) };
        if new_col.is_null() || teide::ffi_is_err(new_col) {
            return Err(SqlError::Engine(teide::Error::Oom));
        }
        unsafe { *((*new_col).val.as_mut_ptr() as *mut i64) = out_nrows as i64 };
        let src = unsafe { teide::raw::td_data(col) };
        let dst = unsafe { teide::raw::td_data(new_col) };
        for lr in 0..l_nrows {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src,
                    dst.add(lr * r_nrows * esz),
                    r_nrows * esz,
                );
            }
        }
        result_raw = unsafe { teide::ffi_table_add_col(result_raw, name_id, new_col) };
        unsafe { teide::ffi_release(new_col) };
    }

    unsafe { teide::ffi_retain(result_raw) };
    Ok(unsafe { Table::from_raw(result_raw) })
}

/// Execute EXCEPT ALL or INTERSECT ALL between two tables.
/// `keep_matches = true` → INTERSECT (keep left rows that exist in right).
/// `keep_matches = false` → EXCEPT (keep left rows that do NOT exist in right).
fn exec_set_operation(
    _ctx: &Context,
    left: &Table,
    right: &Table,
    keep_matches: bool,
) -> Result<Table, SqlError> {
    use std::collections::HashMap as StdMap;

    let l_nrows = left.nrows() as usize;
    let r_nrows = right.nrows() as usize;
    let ncols = left.ncols();

    // Hash all right-side rows
    let mut right_counts: StdMap<u64, usize> = StdMap::new();
    for r in 0..r_nrows {
        let h = hash_table_row(right, r, ncols as usize);
        *right_counts.entry(h).or_insert(0) += 1;
    }

    // Probe with left-side rows, collect indices to keep
    let mut keep_indices: Vec<usize> = Vec::new();
    let mut remaining = right_counts.clone();
    for r in 0..l_nrows {
        let h = hash_table_row(left, r, ncols as usize);
        let in_right = remaining.get(&h).copied().unwrap_or(0) > 0;
        if keep_matches {
            // INTERSECT: keep if in right
            if in_right {
                keep_indices.push(r);
                *remaining.get_mut(&h).unwrap() -= 1;
            }
        } else {
            // EXCEPT: keep if NOT in right
            if in_right {
                *remaining.get_mut(&h).unwrap() -= 1;
            } else {
                keep_indices.push(r);
            }
        }
    }

    // Build result table with kept rows
    let result_raw = unsafe { teide::ffi_table_new(ncols) };
    if result_raw.is_null() {
        return Err(SqlError::Engine(teide::Error::Oom));
    }
    let mut result_raw = result_raw;
    let out_nrows = keep_indices.len();

    for c in 0..ncols {
        let col = left.get_col_idx(c).ok_or(SqlError::Plan(
            "SET operation: column missing".into(),
        ))?;
        let name_id = left.col_name(c);
        let col_type = unsafe { teide::raw::td_type(col) };
        let esz = unsafe { teide::raw::td_type_sizes[col_type as usize] } as usize;
        let new_col = unsafe { teide::raw::td_vec_new(col_type, out_nrows as i64) };
        if new_col.is_null() || teide::ffi_is_err(new_col) {
            return Err(SqlError::Engine(teide::Error::Oom));
        }
        unsafe { *((*new_col).val.as_mut_ptr() as *mut i64) = out_nrows as i64 };
        let src = unsafe { teide::raw::td_data(col) };
        let dst = unsafe { teide::raw::td_data(new_col) };
        for (out_row, &in_row) in keep_indices.iter().enumerate() {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src.add(in_row * esz),
                    dst.add(out_row * esz),
                    esz,
                );
            }
        }
        result_raw = unsafe { teide::ffi_table_add_col(result_raw, name_id, new_col) };
        unsafe { teide::ffi_release(new_col) };
    }

    unsafe { teide::ffi_retain(result_raw) };
    Ok(unsafe { Table::from_raw(result_raw) })
}

/// Hash a table row across all columns for set operations.
fn hash_table_row(table: &Table, row: usize, ncols: usize) -> u64 {
    let mut h: u64 = 0;
    for c in 0..ncols {
        let col = match table.get_col_idx(c as i64) {
            Some(p) => p,
            None => continue,
        };
        let col_type = unsafe { teide::raw::td_type(col) };
        let data = unsafe { teide::raw::td_data(col) };
        let kh: u64 = match col_type {
            6 | 14 | 11 => {
                // TD_I64, TD_SYM, TD_TIMESTAMP
                let v = unsafe { *(data as *const i64).add(row) };
                v as u64
            }
            7 => {
                // TD_F64
                let v = unsafe { *(data as *const f64).add(row) };
                v.to_bits()
            }
            5 => {
                // TD_I32
                let v = unsafe { *(data as *const i32).add(row) };
                v as u64
            }
            15 => {
                // TD_ENUM
                let v = unsafe { *(data as *const u32).add(row) };
                v as u64
            }
            _ => 0,
        };
        h = if c == 0 {
            kh.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(kh >> 32)
        } else {
            h ^ kh.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(h >> 16)
        };
    }
    h
}

/// Apply ORDER BY and LIMIT from the outer query to a result.
fn apply_post_processing(
    ctx: &Context,
    query: &Query,
    result_table: Table,
    result_aliases: Vec<String>,
    _tables: Option<&HashMap<String, StoredTable>>,
) -> Result<SqlResult, SqlError> {
    // ORDER BY (optionally fused with LIMIT)
    let order_by_exprs = extract_order_by(query)?;
    let offset_val = extract_offset(query)?;
    let limit_val = extract_limit(query)?;

    let (result_table, limit_fused) = if !order_by_exprs.is_empty() {
        let table_col_names: Vec<String> = (0..result_table.ncols() as usize)
            .map(|i| result_table.col_name_str(i).to_string())
            .collect();
        let mut g = ctx.graph(&result_table);
        let df_node = g.const_df(&result_table);
        let sort_node = plan_order_by(&mut g, df_node, &order_by_exprs, &result_aliases, &table_col_names)?;

        let total_limit = match (offset_val, limit_val) {
            (Some(off), Some(lim)) => Some(off + lim),
            (None, Some(lim)) => Some(lim),
            _ => None,
        };
        let root = match total_limit {
            Some(n) => g.head(sort_node, n),
            None => sort_node,
        };
        (g.execute(root)?, total_limit.is_some())
    } else {
        (result_table, false)
    };

    // OFFSET + LIMIT (only parts not already fused)
    let result_table = if limit_fused {
        match offset_val {
            Some(off) => skip_rows(ctx, &result_table, off)?,
            None => result_table,
        }
    } else {
        match (offset_val, limit_val) {
            (Some(off), Some(lim)) => {
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
        }
    };

    Ok(SqlResult {
        table: result_table,
        columns: result_aliases,
    })
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

// ---------------------------------------------------------------------------
// Subquery resolution: walk AST, execute subqueries, replace with literals
// ---------------------------------------------------------------------------

/// Recursively walk an expression and replace scalar subqueries and IN subqueries
/// with their evaluated values.
fn resolve_subqueries(
    ctx: &Context,
    expr: &Expr,
    tables: Option<&HashMap<String, StoredTable>>,
) -> Result<Expr, SqlError> {
    match expr {
        // Scalar subquery: (SELECT single_value FROM ...)
        Expr::Subquery(query) => {
            let result = plan_query(ctx, query, tables)?;
            if result.columns.len() != 1 {
                return Err(SqlError::Plan(format!(
                    "Scalar subquery must return exactly 1 column, got {}",
                    result.columns.len()
                )));
            }
            let nrows = result.table.nrows();
            if nrows != 1 {
                return Err(SqlError::Plan(format!(
                    "Scalar subquery must return exactly 1 row, got {}",
                    nrows
                )));
            }
            scalar_value_from_table(&result.table, 0, 0)
        }

        // IN (subquery): rewrite to IN (value_list)
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            let resolved_inner = resolve_subqueries(ctx, inner, tables)?;
            let result = plan_query(ctx, subquery, tables)?;
            if result.columns.len() != 1 {
                return Err(SqlError::Plan(format!(
                    "IN subquery must return exactly 1 column, got {}",
                    result.columns.len()
                )));
            }
            let nrows = result.table.nrows() as usize;
            let mut values = Vec::with_capacity(nrows);
            for r in 0..nrows {
                values.push(scalar_value_from_table(&result.table, 0, r)?);
            }
            Ok(Expr::InList {
                expr: Box::new(resolved_inner),
                list: values,
                negated: *negated,
            })
        }

        // EXISTS (subquery): evaluate and replace with boolean literal
        Expr::Exists { subquery, negated } => {
            let result = plan_query(ctx, subquery, tables)?;
            let exists = result.table.nrows() > 0;
            Ok(Expr::Value(Value::Boolean(exists ^ negated)))
        }

        // Recurse into compound expressions
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(resolve_subqueries(ctx, left, tables)?),
            op: op.clone(),
            right: Box::new(resolve_subqueries(ctx, right, tables)?),
        }),
        Expr::UnaryOp { op, expr: inner } => Ok(Expr::UnaryOp {
            op: op.clone(),
            expr: Box::new(resolve_subqueries(ctx, inner, tables)?),
        }),
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(
            resolve_subqueries(ctx, inner, tables)?,
        ))),
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(resolve_subqueries(ctx, inner, tables)?),
            negated: *negated,
            low: Box::new(resolve_subqueries(ctx, low, tables)?),
            high: Box::new(resolve_subqueries(ctx, high, tables)?),
        }),
        Expr::IsNull(inner) => Ok(Expr::IsNull(Box::new(
            resolve_subqueries(ctx, inner, tables)?,
        ))),
        Expr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(
            resolve_subqueries(ctx, inner, tables)?,
        ))),

        // Leaf nodes: no subqueries to resolve
        _ => Ok(expr.clone()),
    }
}

/// Extract a scalar value from a result table cell as an AST expression literal.
fn scalar_value_from_table(table: &Table, col: usize, row: usize) -> Result<Expr, SqlError> {
    let col_type = table.col_type(col);
    match col_type {
        teide::types::F64 => {
            let v = table.get_f64(col, row).unwrap_or(f64::NAN);
            if v.is_nan() {
                Ok(Expr::Value(Value::Null))
            } else {
                Ok(Expr::Value(Value::Number(format!("{v}"), false)))
            }
        }
        teide::types::I64 | teide::types::I32 => {
            let v = table.get_i64(col, row).unwrap_or(0);
            Ok(Expr::Value(Value::Number(format!("{v}"), false)))
        }
        teide::types::SYM | teide::types::ENUM => {
            let v = table.get_str(col, row).unwrap_or("");
            Ok(Expr::Value(Value::SingleQuotedString(v.to_string())))
        }
        teide::types::BOOL => {
            let v = table.get_i64(col, row).unwrap_or(0);
            Ok(Expr::Value(Value::Boolean(v != 0)))
        }
        _ => Err(SqlError::Plan(format!(
            "Unsupported column type {} in subquery result",
            col_type
        ))),
    }
}

/// Check if an expression tree contains any subqueries that need resolution.
fn has_subqueries(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => true,
        Expr::BinaryOp { left, right, .. } => has_subqueries(left) || has_subqueries(right),
        Expr::UnaryOp { expr, .. } => has_subqueries(expr),
        Expr::Nested(inner) => has_subqueries(inner),
        Expr::Between {
            expr, low, high, ..
        } => has_subqueries(expr) || has_subqueries(low) || has_subqueries(high),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => has_subqueries(inner),
        _ => false,
    }
}

/// Extract GROUP BY column names.
/// Accepts table column names or SELECT alias names (for expression-based keys).
fn extract_group_by_columns(
    group_by: &GroupByExpr,
    schema: &HashMap<String, usize>,
    alias_exprs: &mut HashMap<String, Expr>,
    select_aliases: &[String],
) -> Result<Vec<String>, SqlError> {
    match group_by {
        GroupByExpr::All(_) => Err(SqlError::Plan("GROUP BY ALL not supported".into())),
        GroupByExpr::Expressions(exprs, _modifiers) => {
            let mut cols = Vec::new();
            let mut gb_counter = 0usize;
            for expr in exprs {
                match expr {
                    Expr::Identifier(ident) => {
                        let name = ident.value.to_lowercase();
                        if !schema.contains_key(&name) && !alias_exprs.contains_key(&name) {
                            return Err(SqlError::Plan(format!(
                                "GROUP BY column '{}' not found",
                                name
                            )));
                        }
                        cols.push(name);
                    }
                    // Positional GROUP BY: GROUP BY 1, 2
                    Expr::Value(Value::Number(n, _)) => {
                        let pos = n.parse::<usize>().map_err(|_| {
                            SqlError::Plan(format!("Invalid positional GROUP BY: {n}"))
                        })?;
                        if pos == 0 || pos > select_aliases.len() {
                            return Err(SqlError::Plan(format!(
                                "GROUP BY position {} out of range (1-{})",
                                pos,
                                select_aliases.len()
                            )));
                        }
                        cols.push(select_aliases[pos - 1].clone());
                    }
                    // Expression GROUP BY: GROUP BY FLOOR(v1/10)
                    other => {
                        let alias = format!("_gb_{}", gb_counter);
                        gb_counter += 1;
                        alias_exprs.insert(alias.clone(), other.clone());
                        cols.push(alias);
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

/// An ORDER BY item: either a column name, positional index, or arbitrary expression.
enum OrderByItem {
    Name(String),
    Position(usize), // 1-based index
    Expression(Expr),
}

/// Extract ORDER BY items from the query.
fn extract_order_by(query: &Query) -> Result<Vec<(OrderByItem, bool, Option<bool>)>, SqlError> {
    match &query.order_by {
        None => Ok(Vec::new()),
        Some(order_by) => {
            let mut result = Vec::new();
            for ob in &order_by.exprs {
                let item = match &ob.expr {
                    Expr::Identifier(ident) => OrderByItem::Name(ident.value.to_lowercase()),
                    Expr::Value(Value::Number(n, _)) => {
                        let pos = n.parse::<usize>().map_err(|_| {
                            SqlError::Plan(format!("Invalid positional ORDER BY: {n}"))
                        })?;
                        if pos == 0 {
                            return Err(SqlError::Plan(
                                "ORDER BY position is 1-based, got 0".into(),
                            ));
                        }
                        OrderByItem::Position(pos)
                    }
                    other => OrderByItem::Expression(other.clone()),
                };
                let desc = ob.asc.map(|asc| !asc).unwrap_or(false);
                // ob.nulls_first: Some(true)=NULLS FIRST, Some(false)=NULLS LAST, None=default
                result.push((item, desc, ob.nulls_first));
            }
            Ok(result)
        }
    }
}

/// Plan ORDER BY: handles column names, positional refs, and expressions.
/// `table_col_names` are the actual C-level column names in the table (may differ
/// from `result_aliases` when expressions produce internal names like `_e1`).
fn plan_order_by(
    g: &mut Graph,
    input: Column,
    order_by: &[(OrderByItem, bool, Option<bool>)],
    result_aliases: &[String],
    table_col_names: &[String],
) -> Result<Column, SqlError> {
    // Build a schema from the result aliases for expression planning
    let schema: HashMap<String, usize> = result_aliases
        .iter()
        .enumerate()
        .map(|(i, name)| (name.clone(), i))
        .collect();

    let mut sort_keys = Vec::new();
    let mut descs = Vec::new();
    let mut has_explicit_nulls = false;
    let mut nulls_first_flags: Vec<bool> = Vec::new();

    for (item, desc, nulls_first) in order_by {
        let key = match item {
            OrderByItem::Name(name) => {
                let idx = result_aliases.iter().position(|a| a == name).ok_or_else(|| {
                    SqlError::Plan(format!(
                        "ORDER BY column '{}' not found in result columns",
                        name
                    ))
                })?;
                // Use actual table column name, not the SQL alias
                g.scan(&table_col_names[idx])
            }
            OrderByItem::Position(pos) => {
                if *pos > result_aliases.len() {
                    return Err(SqlError::Plan(format!(
                        "ORDER BY position {} exceeds column count {}",
                        pos,
                        result_aliases.len()
                    )));
                }
                g.scan(&table_col_names[*pos - 1])
            }
            OrderByItem::Expression(expr) => plan_expr(g, expr, &schema)?,
        };
        sort_keys.push(key);
        descs.push(*desc);
        if nulls_first.is_some() {
            has_explicit_nulls = true;
        }
        // Default: NULLS LAST for ASC, NULLS FIRST for DESC (PostgreSQL convention)
        nulls_first_flags.push(nulls_first.unwrap_or(*desc));
    }

    let nf = if has_explicit_nulls {
        Some(nulls_first_flags.as_slice())
    } else {
        None // let C-side apply defaults
    };
    Ok(g.sort(input, &sort_keys, &descs, nf))
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
