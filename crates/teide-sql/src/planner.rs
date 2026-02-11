// SQL planner: translates sqlparser AST into Teide execution graph.

use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator, Distinct, Expr, GroupByExpr, JoinConstraint, JoinOperator, ObjectName,
    ObjectType, Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value,
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
        SetExpr::SetOperation { .. } => {
            return Err(SqlError::Plan(
                "Only UNION ALL is supported for set operations".into(),
            ))
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

    // GROUP BY column names (accepts table columns and SELECT aliases)
    let group_by_cols = extract_group_by_columns(&select.group_by, &schema, &alias_exprs)?;
    let has_group_by = !group_by_cols.is_empty();

    // Detect aggregates in SELECT
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
        plan_group_select(ctx, &working_table, select_items, &group_by_cols, &schema, &alias_exprs)?
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
    if from.len() > 1 {
        return Err(SqlError::Plan(
            "Multiple FROM tables not supported (use JOIN syntax)".into(),
        ));
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
                return Err(SqlError::Plan("CROSS JOIN not supported yet".into()));
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
            return Err(SqlError::Plan(format!(
                "Expression '{}' must be in GROUP BY or contain an aggregate",
                expr
            )));
        }
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

/// Apply ORDER BY and LIMIT from the outer query to a result.
fn apply_post_processing(
    ctx: &Context,
    query: &Query,
    result_table: Table,
    result_aliases: Vec<String>,
    _tables: Option<&HashMap<String, StoredTable>>,
) -> Result<SqlResult, SqlError> {
    // ORDER BY
    let order_by_exprs = extract_order_by(query)?;
    let result_table = if !order_by_exprs.is_empty() {
        let mut g = ctx.graph(&result_table);
        let df_node = g.const_df(&result_table);
        let root = plan_order_by(&mut g, df_node, &order_by_exprs, &result_aliases)?;
        g.execute(root)?
    } else {
        result_table
    };

    // OFFSET + LIMIT
    let offset_val = extract_offset(query)?;
    let limit_val = extract_limit(query)?;
    let result_table = match (offset_val, limit_val) {
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



/// Extract GROUP BY column names.
/// Accepts table column names or SELECT alias names (for expression-based keys).
fn extract_group_by_columns(
    group_by: &GroupByExpr,
    schema: &HashMap<String, usize>,
    alias_exprs: &HashMap<String, Expr>,
) -> Result<Vec<String>, SqlError> {
    match group_by {
        GroupByExpr::All(_) => Err(SqlError::Plan("GROUP BY ALL not supported".into())),
        GroupByExpr::Expressions(exprs, _modifiers) => {
            let mut cols = Vec::new();
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
