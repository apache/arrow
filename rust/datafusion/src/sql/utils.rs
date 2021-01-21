// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::logical_plan::{DFSchema, Expr, LogicalPlan};
use crate::{
    error::{DataFusionError, Result},
    logical_plan::{ExpressionVisitor, Recursion},
};

/// Resolves an `Expr::Wildcard` to a collection of `Expr::Column`'s.
pub(crate) fn expand_wildcard(expr: &Expr, schema: &DFSchema) -> Vec<Expr> {
    match expr {
        Expr::Wildcard => schema
            .fields()
            .iter()
            .map(|f| Expr::Column(f.name().to_string()))
            .collect::<Vec<Expr>>(),
        _ => vec![expr.clone()],
    }
}

/// Collect all deeply nested `Expr::AggregateFunction` and
/// `Expr::AggregateUDF`. They are returned in order of occurrence (depth
/// first), with duplicates omitted.
pub(crate) fn find_aggregate_exprs(exprs: &Vec<Expr>) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(
            nested_expr,
            Expr::AggregateFunction { .. } | Expr::AggregateUDF { .. }
        )
    })
}

/// Collect all deeply nested `Expr::Column`'s. They are returned in order of
/// appearance (depth first), with duplicates omitted.
pub(crate) fn find_column_exprs(exprs: &Vec<Expr>) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| matches!(nested_expr, Expr::Column(_)))
}

/// Search the provided `Expr`'s, and all of their nested `Expr`, for any that
/// pass the provided test. The returned `Expr`'s are deduplicated and returned
/// in order of appearance (depth first).
fn find_exprs_in_exprs<F>(exprs: &Vec<Expr>, test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    exprs
        .iter()
        .flat_map(|expr| find_exprs_in_expr(expr, test_fn))
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}

// Visitor that find expressions that match a particular predicate
struct Finder<'a, F>
where
    F: Fn(&Expr) -> bool,
{
    test_fn: &'a F,
    exprs: Vec<Expr>,
}

impl<'a, F> Finder<'a, F>
where
    F: Fn(&Expr) -> bool,
{
    /// Create a new finder with the `test_fn`
    fn new(test_fn: &'a F) -> Self {
        Self {
            test_fn,
            exprs: Vec::new(),
        }
    }
}

impl<'a, F> ExpressionVisitor for Finder<'a, F>
where
    F: Fn(&Expr) -> bool,
{
    fn pre_visit(mut self, expr: &Expr) -> Result<Recursion<Self>> {
        if (self.test_fn)(expr) {
            if !(self.exprs.contains(expr)) {
                self.exprs.push(expr.clone())
            }
            // stop recursing down this expr once we find a match
            return Ok(Recursion::Stop(self));
        }

        Ok(Recursion::Continue(self))
    }
}

/// Search an `Expr`, and all of its nested `Expr`'s, for any that pass the
/// provided test. The returned `Expr`'s are deduplicated and returned in order
/// of appearance (depth first).
fn find_exprs_in_expr<F>(expr: &Expr, test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    let Finder { exprs, .. } = expr
        .accept(Finder::new(test_fn))
        // pre_visit always returns OK, so this will always too
        .expect("no way to return error during recursion");
    exprs
}

/// Convert any `Expr` to an `Expr::Column`.
pub(crate) fn expr_as_column_expr(expr: &Expr, plan: &LogicalPlan) -> Result<Expr> {
    match expr {
        Expr::Column(_) => Ok(expr.clone()),
        _ => Ok(Expr::Column(expr.name(&plan.schema())?)),
    }
}

/// Rebuilds an `Expr` as a projection on top of a collection of `Expr`'s.
///
/// For example, the expression `a + b < 1` would require, as input, the 2
/// individual columns, `a` and `b`. But, if the base expressions already
/// contain the `a + b` result, then that may be used in lieu of the `a` and
/// `b` columns.
///
/// This is useful in the context of a query like:
///
/// SELECT a + b < 1 ... GROUP BY a + b
///
/// where post-aggregation, `a + b` need not be a projection against the
/// individual columns `a` and `b`, but rather it is a projection against the
/// `a + b` found in the GROUP BY.
pub(crate) fn rebase_expr(
    expr: &Expr,
    base_exprs: &Vec<Expr>,
    plan: &LogicalPlan,
) -> Result<Expr> {
    clone_with_replacement(expr, &|nested_expr| {
        if base_exprs.contains(nested_expr) {
            Ok(Some(expr_as_column_expr(nested_expr, plan)?))
        } else {
            Ok(None)
        }
    })
}

/// Determines if the set of `Expr`'s are a valid projection on the input
/// `Expr::Column`'s.
pub(crate) fn can_columns_satisfy_exprs(
    columns: &Vec<Expr>,
    exprs: &Vec<Expr>,
) -> Result<bool> {
    columns.iter().try_for_each(|c| match c {
        Expr::Column(_) => Ok(()),
        _ => Err(DataFusionError::Internal(
            "Expr::Column are required".to_string(),
        )),
    })?;

    Ok(find_column_exprs(exprs).iter().all(|c| columns.contains(c)))
}

/// Returns a cloned `Expr`, but any of the `Expr`'s in the tree may be
/// replaced/customized by the replacement function.
///
/// The replacement function is called repeatedly with `Expr`, starting with
/// the argument `expr`, then descending depth-first through its
/// descendants. The function chooses to replace or keep (clone) each `Expr`.
///
/// The function's return type is `Result<Option<Expr>>>`, where:
///
/// * `Ok(Some(replacement_expr))`: A replacement `Expr` is provided; it is
///       swapped in at the particular node in the tree. Any nested `Expr` are
///       not subject to cloning/replacement.
/// * `Ok(None)`: A replacement `Expr` is not provided. The `Expr` is
///       recreated, with all of its nested `Expr`'s subject to
///       cloning/replacement.
/// * `Err(err)`: Any error returned by the function is returned as-is by
///       `clone_with_replacement()`.
fn clone_with_replacement<F>(expr: &Expr, replacement_fn: &F) -> Result<Expr>
where
    F: Fn(&Expr) -> Result<Option<Expr>>,
{
    let replacement_opt = replacement_fn(expr)?;

    match replacement_opt {
        // If we were provided a replacement, use the replacement. Do not
        // descend further.
        Some(replacement) => Ok(replacement),
        // No replacement was provided, clone the node and recursively call
        // clone_with_replacement() on any nested expressions.
        None => match expr {
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
            } => Ok(Expr::AggregateFunction {
                fun: fun.clone(),
                args: args
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
                distinct: *distinct,
            }),
            Expr::AggregateUDF { fun, args } => Ok(Expr::AggregateUDF {
                fun: fun.clone(),
                args: args
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
            }),
            Expr::Alias(nested_expr, alias_name) => Ok(Expr::Alias(
                Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                alias_name.clone(),
            )),
            Expr::Between {
                expr: nested_expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                negated: *negated,
                low: Box::new(clone_with_replacement(&**low, replacement_fn)?),
                high: Box::new(clone_with_replacement(&**high, replacement_fn)?),
            }),
            Expr::InList {
                expr: nested_expr,
                list,
                negated,
            } => Ok(Expr::InList {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                list: list
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
                negated: *negated,
            }),
            Expr::BinaryExpr { left, right, op } => Ok(Expr::BinaryExpr {
                left: Box::new(clone_with_replacement(&**left, replacement_fn)?),
                op: *op,
                right: Box::new(clone_with_replacement(&**right, replacement_fn)?),
            }),
            Expr::Case {
                expr: case_expr_opt,
                when_then_expr,
                else_expr: else_expr_opt,
            } => Ok(Expr::Case {
                expr: match case_expr_opt {
                    Some(case_expr) => Some(Box::new(clone_with_replacement(
                        &**case_expr,
                        replacement_fn,
                    )?)),
                    None => None,
                },
                when_then_expr: when_then_expr
                    .iter()
                    .map(|(a, b)| {
                        Ok((
                            Box::new(clone_with_replacement(&**a, replacement_fn)?),
                            Box::new(clone_with_replacement(&**b, replacement_fn)?),
                        ))
                    })
                    .collect::<Result<Vec<(_, _)>>>()?,
                else_expr: match else_expr_opt {
                    Some(else_expr) => Some(Box::new(clone_with_replacement(
                        &**else_expr,
                        replacement_fn,
                    )?)),
                    None => None,
                },
            }),
            Expr::ScalarFunction { fun, args } => Ok(Expr::ScalarFunction {
                fun: fun.clone(),
                args: args
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
            }),
            Expr::ScalarUDF { fun, args } => Ok(Expr::ScalarUDF {
                fun: fun.clone(),
                args: args
                    .iter()
                    .map(|arg| clone_with_replacement(arg, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
            }),
            Expr::Negative(nested_expr) => Ok(Expr::Negative(Box::new(
                clone_with_replacement(&**nested_expr, replacement_fn)?,
            ))),
            Expr::Not(nested_expr) => Ok(Expr::Not(Box::new(clone_with_replacement(
                &**nested_expr,
                replacement_fn,
            )?))),
            Expr::IsNotNull(nested_expr) => Ok(Expr::IsNotNull(Box::new(
                clone_with_replacement(&**nested_expr, replacement_fn)?,
            ))),
            Expr::IsNull(nested_expr) => Ok(Expr::IsNull(Box::new(
                clone_with_replacement(&**nested_expr, replacement_fn)?,
            ))),
            Expr::Cast {
                expr: nested_expr,
                data_type,
            } => Ok(Expr::Cast {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                data_type: data_type.clone(),
            }),
            Expr::Sort {
                expr: nested_expr,
                asc,
                nulls_first,
            } => Ok(Expr::Sort {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                asc: *asc,
                nulls_first: *nulls_first,
            }),

            Expr::Column(_) | Expr::Literal(_) | Expr::ScalarVariable(_) => {
                Ok(expr.clone())
            }
            Expr::Wildcard => Ok(Expr::Wildcard),
        },
    }
}
