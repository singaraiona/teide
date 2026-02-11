// Expression tree walker: translates sqlparser AST expressions into Teide DAG nodes.

use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator, CastKind, DataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, UnaryOperator, Value,
};

use teide::{AggOp, Column, Graph};

use crate::SqlError;

/// Recursively plan a scalar expression into a Teide DAG column node.
pub fn plan_expr(
    g: &mut Graph,
    expr: &Expr,
    schema: &HashMap<String, usize>,
) -> Result<Column, SqlError> {
    match expr {
        Expr::Identifier(ident) => {
            let name = ident.value.to_lowercase();
            if !schema.contains_key(&name) {
                return Err(SqlError::Plan(format!("Column '{}' not found", name)));
            }
            Ok(g.scan(&name))
        }

        Expr::Value(val) => match val {
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(g.const_i64(i))
                } else {
                    let f = n
                        .parse::<f64>()
                        .map_err(|_| SqlError::Plan(format!("Invalid number literal: {n}")))?;
                    Ok(g.const_f64(f))
                }
            }
            Value::SingleQuotedString(s) => Ok(g.const_str(s)),
            Value::Boolean(b) => Ok(g.const_bool(*b)),
            Value::Null => {
                // NULL represented as f64 NaN constant — C engine uses NaN-as-null
                Ok(g.const_f64(f64::NAN))
            }
            _ => Err(SqlError::Plan(format!("Unsupported value: {val}"))),
        },

        Expr::BinaryOp { left, op, right } => {
            let l = plan_expr(g, left, schema)?;
            let r = plan_expr(g, right, schema)?;
            match op {
                BinaryOperator::Plus => Ok(g.add(l, r)),
                BinaryOperator::Minus => Ok(g.sub(l, r)),
                BinaryOperator::Multiply => Ok(g.mul(l, r)),
                BinaryOperator::Divide => Ok(g.div(l, r)),
                BinaryOperator::Modulo => Ok(g.modulo(l, r)),
                BinaryOperator::Eq => Ok(g.eq(l, r)),
                BinaryOperator::NotEq => Ok(g.ne(l, r)),
                BinaryOperator::Lt => Ok(g.lt(l, r)),
                BinaryOperator::LtEq => Ok(g.le(l, r)),
                BinaryOperator::Gt => Ok(g.gt(l, r)),
                BinaryOperator::GtEq => Ok(g.ge(l, r)),
                BinaryOperator::And => Ok(g.and(l, r)),
                BinaryOperator::Or => Ok(g.or(l, r)),
                _ => Err(SqlError::Plan(format!("Unsupported operator: {op}"))),
            }
        }

        Expr::UnaryOp { op, expr: inner } => {
            let e = plan_expr(g, inner, schema)?;
            match op {
                UnaryOperator::Not => Ok(g.not(e)),
                UnaryOperator::Minus => Ok(g.neg(e)),
                _ => Err(SqlError::Plan(format!("Unsupported unary operator: {op}"))),
            }
        }

        Expr::Nested(inner) => plan_expr(g, inner, schema),

        // IS NULL / IS NOT NULL
        Expr::IsNull(inner) => {
            let e = plan_expr(g, inner, schema)?;
            Ok(g.isnull(e))
        }
        Expr::IsNotNull(inner) => {
            let e = plan_expr(g, inner, schema)?;
            Ok(g.not(g.isnull(e)))
        }

        // BETWEEN: x BETWEEN a AND b  →  x >= a AND x <= b
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            let x = plan_expr(g, inner, schema)?;
            let lo = plan_expr(g, low, schema)?;
            let hi = plan_expr(g, high, schema)?;
            let ge = g.ge(x, lo);
            let le = g.le(x, hi);
            let result = g.and(ge, le);
            if *negated {
                Ok(g.not(result))
            } else {
                Ok(result)
            }
        }

        // IN list: x IN (a, b, c)  →  x = a OR x = b OR x = c
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            if list.is_empty() {
                return Ok(g.const_bool(*negated));
            }
            let x = plan_expr(g, inner, schema)?;
            let first_val = plan_expr(g, &list[0], schema)?;
            let mut result = g.eq(x, first_val);
            for item in &list[1..] {
                let x_again = plan_expr(g, inner, schema)?;
                let val = plan_expr(g, item, schema)?;
                let cmp = g.eq(x_again, val);
                result = g.or(result, cmp);
            }
            if *negated {
                Ok(g.not(result))
            } else {
                Ok(result)
            }
        }

        // CAST(expr AS type) / expr::type
        Expr::Cast {
            expr: inner,
            data_type,
            kind,
            ..
        } => {
            if *kind == CastKind::TryCast {
                return Err(SqlError::Plan("TRY_CAST not supported".into()));
            }
            let e = plan_expr(g, inner, schema)?;
            let target = map_sql_type(data_type)?;
            Ok(g.cast(e, target))
        }

        // CASE WHEN → nested IF
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let else_val = match else_result {
                Some(e) => plan_expr(g, e, schema)?,
                None => g.const_f64(f64::NAN), // NULL default
            };
            let mut result = else_val;

            if let Some(op) = operand {
                // Simple CASE: CASE x WHEN v1 THEN r1 ...
                for (cond_val, then_val) in conditions.iter().zip(results.iter()).rev() {
                    let x = plan_expr(g, op, schema)?;
                    let v = plan_expr(g, cond_val, schema)?;
                    let c = g.eq(x, v);
                    let t = plan_expr(g, then_val, schema)?;
                    result = g.if_then_else(c, t, result);
                }
            } else {
                // Searched CASE: CASE WHEN c1 THEN r1 ...
                for (cond_expr, then_val) in conditions.iter().zip(results.iter()).rev() {
                    let c = plan_expr(g, cond_expr, schema)?;
                    let t = plan_expr(g, then_val, schema)?;
                    result = g.if_then_else(c, t, result);
                }
            }
            Ok(result)
        }

        // LIKE / NOT LIKE
        Expr::Like {
            negated,
            expr: inner,
            pattern,
            ..
        } => {
            let input = plan_expr(g, inner, schema)?;
            let pat = plan_expr(g, pattern, schema)?;
            let result = g.like(input, pat);
            if *negated {
                Ok(g.not(result))
            } else {
                Ok(result)
            }
        }

        // ILIKE (case-insensitive) — treat same as LIKE for now
        Expr::ILike {
            negated,
            expr: inner,
            pattern,
            ..
        } => {
            let input = plan_expr(g, inner, schema)?;
            let pat = plan_expr(g, pattern, schema)?;
            let result = g.like(input, pat);
            if *negated {
                Ok(g.not(result))
            } else {
                Ok(result)
            }
        }

        // CompoundIdentifier: table_alias.column
        Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                let col_name = parts[1].value.to_lowercase();
                if schema.contains_key(&col_name) {
                    return Ok(g.scan(&col_name));
                }
                // Try fully qualified name "alias.col"
                let full = format!("{}.{}", parts[0].value.to_lowercase(), col_name);
                if schema.contains_key(&full) {
                    return Ok(g.scan(&full));
                }
                return Err(SqlError::Plan(format!("Column '{}' not found", col_name)));
            }
            Err(SqlError::Plan(format!("Unsupported compound identifier: {expr}")))
        }

        // Subquery: (SELECT ...) used as a scalar value
        Expr::Subquery(_query) => {
            // Execute inner query and extract single value
            Err(SqlError::Plan(
                "Scalar subqueries are only supported in WHERE clause via plan_query context".into(),
            ))
        }

        // IN (subquery)
        Expr::InSubquery {
            expr: _inner,
            subquery: _sq,
            negated: _neg,
        } => {
            Err(SqlError::Plan(
                "IN (subquery) not yet supported; use IN (value_list) instead".into(),
            ))
        }

        // Scalar functions and aggregate detection
        Expr::Function(f) => {
            let name = f.name.to_string().to_lowercase();
            if is_aggregate_name(&name) {
                Err(SqlError::Plan(format!(
                    "Aggregate function '{name}' not allowed in this context"
                )))
            } else {
                plan_scalar_function(g, &name, f, schema)
            }
        }

        _ => Err(SqlError::Plan(format!("Unsupported expression: {expr}"))),
    }
}

// ---------------------------------------------------------------------------
// Scalar functions
// ---------------------------------------------------------------------------

fn plan_scalar_function(
    g: &mut Graph,
    name: &str,
    func: &Function,
    schema: &HashMap<String, usize>,
) -> Result<Column, SqlError> {
    let args = extract_func_args(func)?;

    match name {
        // 1-arg math functions
        "abs" => {
            check_arg_count(name, &args, 1)?;
            let a = plan_expr(g, &args[0], schema)?;
            Ok(g.abs(a))
        }
        "ceil" | "ceiling" => {
            check_arg_count(name, &args, 1)?;
            let a = plan_expr(g, &args[0], schema)?;
            Ok(g.ceil(a))
        }
        "floor" => {
            check_arg_count(name, &args, 1)?;
            let a = plan_expr(g, &args[0], schema)?;
            Ok(g.floor(a))
        }
        "sqrt" => {
            check_arg_count(name, &args, 1)?;
            let a = plan_expr(g, &args[0], schema)?;
            Ok(g.sqrt(a))
        }
        "ln" | "log" => {
            check_arg_count(name, &args, 1)?;
            let a = plan_expr(g, &args[0], schema)?;
            Ok(g.log(a))
        }
        "exp" => {
            check_arg_count(name, &args, 1)?;
            let a = plan_expr(g, &args[0], schema)?;
            Ok(g.exp(a))
        }

        // 2-arg functions
        "round" => {
            // ROUND(x) or ROUND(x, n) — for now just truncate to integer via cast
            if args.is_empty() || args.len() > 2 {
                return Err(SqlError::Plan("ROUND takes 1 or 2 arguments".into()));
            }
            let a = plan_expr(g, &args[0], schema)?;
            if args.len() == 1 {
                // round to integer: cast to i64 then back to f64
                let as_int = g.cast(a, teide::types::I64);
                Ok(g.cast(as_int, teide::types::F64))
            } else {
                // ROUND(x, n) — not easily expressible without a native op
                Err(SqlError::Plan(
                    "ROUND with precision not yet supported".into(),
                ))
            }
        }

        "least" => {
            if args.len() < 2 {
                return Err(SqlError::Plan("LEAST requires at least 2 arguments".into()));
            }
            let mut result = plan_expr(g, &args[0], schema)?;
            for arg in &args[1..] {
                let b = plan_expr(g, arg, schema)?;
                result = g.min2(result, b);
            }
            Ok(result)
        }
        "greatest" => {
            if args.len() < 2 {
                return Err(SqlError::Plan(
                    "GREATEST requires at least 2 arguments".into(),
                ));
            }
            let mut result = plan_expr(g, &args[0], schema)?;
            for arg in &args[1..] {
                let b = plan_expr(g, arg, schema)?;
                result = g.max2(result, b);
            }
            Ok(result)
        }

        // COALESCE(a, b, ...) → first non-null (approximated via isnull chains)
        "coalesce" => {
            if args.is_empty() {
                return Err(SqlError::Plan("COALESCE requires at least 1 argument".into()));
            }
            if args.len() == 1 {
                return plan_expr(g, &args[0], schema);
            }
            // COALESCE(a, b) → if a IS NOT NULL then a else b
            // We approximate: a * (1 - isnull(a)) + b * isnull(a)
            // This only works for numeric types; for strings it will fail at runtime
            let a = plan_expr(g, &args[0], schema)?;
            let is_null_a = g.isnull(a);
            let not_null_a = g.not(is_null_a);
            let cast_not_null = g.cast(not_null_a, teide::types::F64);
            let cast_null = g.cast(is_null_a, teide::types::F64);
            let a2 = plan_expr(g, &args[0], schema)?;
            let b = plan_expr(g, &args[1], schema)?;
            let part_a = g.mul(a2, cast_not_null);
            let part_b = g.mul(b, cast_null);
            Ok(g.add(part_a, part_b))
        }

        // NULLIF(a, b) → if a = b then NULL else a
        "nullif" => {
            check_arg_count(name, &args, 2)?;
            // Return a / (a != b) — division by zero (false=0) produces NaN (= NULL)
            let a = plan_expr(g, &args[0], schema)?;
            let b = plan_expr(g, &args[1], schema)?;
            let ne = g.ne(a, b);
            let a2 = plan_expr(g, &args[0], schema)?;
            let cast_ne = g.cast(ne, teide::types::F64);
            Ok(g.mul(a2, cast_ne))
        }

        _ => Err(SqlError::Plan(format!("Unsupported function: {name}"))),
    }
}

fn extract_func_args(func: &Function) -> Result<Vec<Expr>, SqlError> {
    match &func.args {
        FunctionArguments::List(arg_list) => {
            let mut exprs = Vec::new();
            for arg in &arg_list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => exprs.push(e.clone()),
                    _ => {
                        return Err(SqlError::Plan(format!(
                            "Unsupported argument syntax in {}()",
                            func.name
                        )))
                    }
                }
            }
            Ok(exprs)
        }
        FunctionArguments::None => Ok(Vec::new()),
        _ => Err(SqlError::Plan(format!(
            "Unsupported argument syntax for '{}'",
            func.name
        ))),
    }
}

fn check_arg_count(name: &str, args: &[Expr], expected: usize) -> Result<(), SqlError> {
    if args.len() != expected {
        Err(SqlError::Plan(format!(
            "{name}() expects {expected} argument(s), got {}",
            args.len()
        )))
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

fn map_sql_type(dt: &DataType) -> Result<i8, SqlError> {
    match dt {
        DataType::Boolean | DataType::Bool => Ok(teide::types::BOOL),
        DataType::Int(None) | DataType::Integer(None) | DataType::Int4(_) => {
            Ok(teide::types::I32)
        }
        DataType::BigInt(None) | DataType::Int8(_) | DataType::Int64 => Ok(teide::types::I64),
        DataType::Float(None)
        | DataType::Float64
        | DataType::Double
        | DataType::DoublePrecision
        | DataType::Real => Ok(teide::types::F64),
        DataType::Varchar(_) | DataType::Text | DataType::String(_) => Ok(teide::types::STR),
        _ => Err(SqlError::Plan(format!("Unsupported CAST target type: {dt}"))),
    }
}

// ---------------------------------------------------------------------------
// Aggregate helpers
// ---------------------------------------------------------------------------

/// Check if a function name is a known aggregate.
pub fn is_aggregate_name(name: &str) -> bool {
    matches!(name, "sum" | "avg" | "min" | "max" | "count")
}

/// Check if a sqlparser Expr contains any aggregate function call.
pub fn is_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(f) => {
            let name = f.name.to_string().to_lowercase();
            is_aggregate_name(&name)
        }
        Expr::BinaryOp { left, right, .. } => is_aggregate(left) || is_aggregate(right),
        Expr::UnaryOp { expr, .. } => is_aggregate(expr),
        Expr::Nested(inner) => is_aggregate(inner),
        Expr::Cast { expr, .. } => is_aggregate(expr),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_ref().map_or(false, |e| is_aggregate(e))
                || conditions.iter().any(|c| is_aggregate(c))
                || results.iter().any(|r| is_aggregate(r))
                || else_result.as_ref().map_or(false, |e| is_aggregate(e))
        }
        _ => false,
    }
}

/// Check if an expression is a pure aggregate call (not wrapped in arithmetic).
pub fn is_pure_aggregate(expr: &Expr) -> bool {
    matches!(expr, Expr::Function(f) if is_aggregate_name(&f.name.to_string().to_lowercase()))
}

/// Collect all aggregate sub-expressions from an expression tree.
/// Returns (agg_expr, auto_alias) pairs.
pub fn collect_aggregates(expr: &Expr) -> Vec<(&Expr, String)> {
    let mut aggs = Vec::new();
    collect_aggregates_inner(expr, &mut aggs);
    aggs
}

fn collect_aggregates_inner<'a>(expr: &'a Expr, aggs: &mut Vec<(&'a Expr, String)>) {
    match expr {
        Expr::Function(f) => {
            let name = f.name.to_string().to_lowercase();
            if is_aggregate_name(&name) {
                let alias = format_agg_name(f);
                // Avoid duplicates
                if !aggs.iter().any(|(_, a)| *a == alias) {
                    aggs.push((expr, alias));
                }
                return; // Don't recurse into aggregate args
            }
            // Non-aggregate function: recurse into args
            if let FunctionArguments::List(arg_list) = &f.args {
                for arg in &arg_list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                        collect_aggregates_inner(e, aggs);
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_aggregates_inner(left, aggs);
            collect_aggregates_inner(right, aggs);
        }
        Expr::UnaryOp { expr, .. } => collect_aggregates_inner(expr, aggs),
        Expr::Nested(inner) => collect_aggregates_inner(inner, aggs),
        Expr::Cast { expr, .. } => collect_aggregates_inner(expr, aggs),
        _ => {}
    }
}

/// Plan an expression in a post-aggregation context, where aggregate references
/// resolve to column scans on the GROUP BY result.
pub fn plan_post_agg_expr(
    g: &mut Graph,
    expr: &Expr,
    alias_to_native: &HashMap<String, String>,
) -> Result<Column, SqlError> {
    match expr {
        Expr::Function(f) => {
            let name = f.name.to_string().to_lowercase();
            if is_aggregate_name(&name) {
                // This aggregate should already be in the result — scan by native name
                let alias = format_agg_name(f);
                if let Some(native) = alias_to_native.get(&alias) {
                    return Ok(g.scan(native));
                }
                return Err(SqlError::Plan(format!(
                    "Aggregate '{alias}' not found in GROUP BY result"
                )));
            }
            // Non-aggregate function: resolve args recursively
            Err(SqlError::Plan(format!(
                "Non-aggregate function '{name}' in post-agg expression not yet supported"
            )))
        }
        Expr::BinaryOp { left, op, right } => {
            let l = plan_post_agg_expr(g, left, alias_to_native)?;
            let r = plan_post_agg_expr(g, right, alias_to_native)?;
            match op {
                BinaryOperator::Plus => Ok(g.add(l, r)),
                BinaryOperator::Minus => Ok(g.sub(l, r)),
                BinaryOperator::Multiply => Ok(g.mul(l, r)),
                BinaryOperator::Divide => Ok(g.div(l, r)),
                BinaryOperator::Modulo => Ok(g.modulo(l, r)),
                BinaryOperator::Eq => Ok(g.eq(l, r)),
                BinaryOperator::NotEq => Ok(g.ne(l, r)),
                BinaryOperator::Lt => Ok(g.lt(l, r)),
                BinaryOperator::LtEq => Ok(g.le(l, r)),
                BinaryOperator::Gt => Ok(g.gt(l, r)),
                BinaryOperator::GtEq => Ok(g.ge(l, r)),
                BinaryOperator::And => Ok(g.and(l, r)),
                BinaryOperator::Or => Ok(g.or(l, r)),
                _ => Err(SqlError::Plan(format!("Unsupported operator: {op}"))),
            }
        }
        Expr::UnaryOp { op, expr: inner } => {
            let e = plan_post_agg_expr(g, inner, alias_to_native)?;
            match op {
                UnaryOperator::Not => Ok(g.not(e)),
                UnaryOperator::Minus => Ok(g.neg(e)),
                _ => Err(SqlError::Plan(format!("Unsupported unary operator: {op}"))),
            }
        }
        Expr::Nested(inner) => plan_post_agg_expr(g, inner, alias_to_native),
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let e = plan_post_agg_expr(g, inner, alias_to_native)?;
            let target = map_sql_type(data_type)?;
            Ok(g.cast(e, target))
        }
        Expr::Value(val) => match val {
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(g.const_i64(i))
                } else {
                    let f = n
                        .parse::<f64>()
                        .map_err(|_| SqlError::Plan(format!("Invalid number literal: {n}")))?;
                    Ok(g.const_f64(f))
                }
            }
            Value::SingleQuotedString(s) => Ok(g.const_str(s)),
            Value::Boolean(b) => Ok(g.const_bool(*b)),
            Value::Null => Ok(g.const_f64(f64::NAN)),
            _ => Err(SqlError::Plan(format!("Unsupported value: {val}"))),
        },
        Expr::Identifier(ident) => {
            // Could be a key column name or an aggregate alias
            let name = ident.value.to_lowercase();
            if let Some(native) = alias_to_native.get(&name) {
                return Ok(g.scan(native));
            }
            // Try as direct column name (key columns use native names)
            Ok(g.scan(&name))
        }
        _ => Err(SqlError::Plan(format!(
            "Unsupported expression in post-agg context: {expr}"
        ))),
    }
}

/// Plan a HAVING expression. Like plan_post_agg_expr but also resolves aggregates
/// via C engine naming convention ({col}_{suffix}) when the SQL-style alias isn't found.
pub fn plan_having_expr(
    g: &mut Graph,
    expr: &Expr,
    result_schema: &HashMap<String, usize>,
    original_schema: &HashMap<String, usize>,
) -> Result<Column, SqlError> {
    match expr {
        Expr::Function(f) => {
            let name = f.name.to_string().to_lowercase();
            if is_aggregate_name(&name) {
                // Try format_agg_name alias first ("sum(v1)")
                let alias = format_agg_name(f);
                if result_schema.contains_key(&alias) {
                    return Ok(g.scan(&alias));
                }
                // Try C engine naming convention ("v1_sum")
                if let Some(native) = predict_c_agg_name(f, original_schema) {
                    if result_schema.contains_key(&native) {
                        return Ok(g.scan(&native));
                    }
                }
                return Err(SqlError::Plan(format!(
                    "Aggregate '{alias}' not found in GROUP BY result"
                )));
            }
            plan_expr(g, expr, result_schema)
        }
        Expr::BinaryOp { left, op, right } => {
            let l = plan_having_expr(g, left, result_schema, original_schema)?;
            let r = plan_having_expr(g, right, result_schema, original_schema)?;
            match op {
                BinaryOperator::Plus => Ok(g.add(l, r)),
                BinaryOperator::Minus => Ok(g.sub(l, r)),
                BinaryOperator::Multiply => Ok(g.mul(l, r)),
                BinaryOperator::Divide => Ok(g.div(l, r)),
                BinaryOperator::Modulo => Ok(g.modulo(l, r)),
                BinaryOperator::Eq => Ok(g.eq(l, r)),
                BinaryOperator::NotEq => Ok(g.ne(l, r)),
                BinaryOperator::Lt => Ok(g.lt(l, r)),
                BinaryOperator::LtEq => Ok(g.le(l, r)),
                BinaryOperator::Gt => Ok(g.gt(l, r)),
                BinaryOperator::GtEq => Ok(g.ge(l, r)),
                BinaryOperator::And => Ok(g.and(l, r)),
                BinaryOperator::Or => Ok(g.or(l, r)),
                _ => Err(SqlError::Plan(format!("Unsupported operator: {op}"))),
            }
        }
        Expr::UnaryOp { op, expr: inner } => {
            let e = plan_having_expr(g, inner, result_schema, original_schema)?;
            match op {
                UnaryOperator::Not => Ok(g.not(e)),
                UnaryOperator::Minus => Ok(g.neg(e)),
                _ => Err(SqlError::Plan(format!("Unsupported unary operator: {op}"))),
            }
        }
        Expr::Nested(inner) => plan_having_expr(g, inner, result_schema, original_schema),
        Expr::Value(_) => plan_expr(g, expr, result_schema),
        Expr::Identifier(ident) => {
            let name = ident.value.to_lowercase();
            if result_schema.contains_key(&name) {
                return Ok(g.scan(&name));
            }
            Err(SqlError::Plan(format!(
                "Column '{}' not found in HAVING result",
                name
            )))
        }
        _ => plan_expr(g, expr, result_schema),
    }
}

/// Predict the C engine's naming convention for an aggregate output column.
/// SUM(v1) → "v1_sum", COUNT(v1) → "v1_count", AVG(v1) → "v1_mean", etc.
fn predict_c_agg_name(
    func: &Function,
    original_schema: &HashMap<String, usize>,
) -> Option<String> {
    let op = func.name.to_string().to_lowercase();
    let suffix = match op.as_str() {
        "sum" => "_sum",
        "count" => "_count",
        "avg" => "_mean",
        "min" => "_min",
        "max" => "_max",
        _ => return None,
    };
    if let FunctionArguments::List(args) = &func.args {
        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident)))) =
            args.args.first()
        {
            return Some(format!("{}{}", ident.value.to_lowercase(), suffix));
        }
        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) = args.args.first() {
            // COUNT(*) uses first column as proxy
            let first = original_schema
                .iter()
                .min_by_key(|(_, v)| **v)
                .map(|(k, _)| k.clone())?;
            return Some(format!("{}{}", first, suffix));
        }
    }
    None
}

/// Extract the column name or plan expression from an aggregate function's argument.
pub fn plan_agg_input(
    g: &mut Graph,
    func: &Function,
    schema: &HashMap<String, usize>,
) -> Result<Column, SqlError> {
    let name = func.name.to_string().to_lowercase();

    let args = match &func.args {
        FunctionArguments::List(arg_list) => &arg_list.args,
        FunctionArguments::None => {
            return Err(SqlError::Plan(format!(
                "Function '{}' requires an argument",
                func.name
            )));
        }
        _ => {
            return Err(SqlError::Plan(format!(
                "Unsupported function argument syntax for '{}'",
                func.name
            )));
        }
    };

    if args.len() != 1 {
        return Err(SqlError::Plan(format!(
            "Function '{}' expects 1 argument, got {}",
            func.name,
            args.len()
        )));
    }

    match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
            // Plan arbitrary expression as aggregate input
            plan_expr(g, expr, schema)
        }
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
            // COUNT(*) — use first column as proxy
            if name != "count" {
                return Err(SqlError::Plan(format!(
                    "Wildcard (*) not supported for {name}()"
                )));
            }
            let first_col = schema
                .iter()
                .min_by_key(|(_k, v)| **v)
                .map(|(k, _)| k.clone())
                .ok_or_else(|| SqlError::Plan("COUNT(*) on empty schema".into()))?;
            Ok(g.scan(&first_col))
        }
        _ => Err(SqlError::Plan(format!(
            "Only expressions and * supported as arguments to {}()",
            func.name
        ))),
    }
}

/// Map aggregate function name to AggOp.
pub fn agg_op_from_name(name: &str) -> Result<AggOp, SqlError> {
    match name {
        "sum" => Ok(AggOp::Sum),
        "avg" => Ok(AggOp::Avg),
        "min" => Ok(AggOp::Min),
        "max" => Ok(AggOp::Max),
        "count" => Ok(AggOp::Count),
        _ => Err(SqlError::Plan(format!(
            "Unknown aggregate function: {name}"
        ))),
    }
}

/// Generate a default name for an aggregate function expression.
pub fn format_agg_name(func: &Function) -> String {
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
pub fn expr_default_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.to_lowercase(),
        Expr::CompoundIdentifier(parts) => {
            // Return just the column name, not the full qualified name
            parts.last().map(|p| p.value.to_lowercase()).unwrap_or_default()
        }
        _ => format!("{expr}").to_lowercase(),
    }
}
