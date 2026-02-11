// Expression tree walker: translates sqlparser AST expressions into Teide DAG nodes.

use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    UnaryOperator, Value,
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
            Value::Null => Err(SqlError::Plan("NULL literals not yet supported".into())),
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

        Expr::Function(f) => {
            let name = f.name.to_string().to_lowercase();
            if is_aggregate_name(&name) {
                Err(SqlError::Plan(format!(
                    "Aggregate function '{name}' not allowed in this context"
                )))
            } else {
                Err(SqlError::Plan(format!("Unsupported function: {name}")))
            }
        }

        _ => Err(SqlError::Plan(format!("Unsupported expression: {expr}"))),
    }
}

/// Check if a function name is a known aggregate.
pub fn is_aggregate_name(name: &str) -> bool {
    matches!(name, "sum" | "avg" | "min" | "max" | "count")
}

/// Check if a sqlparser Expr is an aggregate function call.
pub fn is_aggregate(expr: &Expr) -> bool {
    if let Expr::Function(f) = expr {
        let name = f.name.to_string().to_lowercase();
        is_aggregate_name(&name)
    } else {
        false
    }
}

/// Extract the column name from an aggregate function's argument.
/// For COUNT(*), returns the first column in the schema.
pub fn extract_agg_arg_name(
    func: &Function,
    schema: &HashMap<String, usize>,
) -> Result<String, SqlError> {
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
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
            let col_name = ident.value.to_lowercase();
            if !schema.contains_key(&col_name) {
                return Err(SqlError::Plan(format!("Column '{}' not found", col_name)));
            }
            Ok(col_name)
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
            Ok(first_col)
        }
        _ => Err(SqlError::Plan(format!(
            "Only column references supported as arguments to {}()",
            func.name
        ))),
    }
}

/// Plan an aggregate function call, returning the AggOp and the input Column.
pub fn plan_aggregate(
    g: &mut Graph,
    func: &Function,
    schema: &HashMap<String, usize>,
) -> Result<(AggOp, Column), SqlError> {
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
