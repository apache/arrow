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

//! Filter Push Down optimizer rule ensures that filters are applied as early as possible in the plan

use crate::error::Result;
use crate::logical_plan::{and, LogicalPlan};
use crate::logical_plan::{DFSchema, Expr};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Filter Push Down optimizer rule pushes filter clauses down the plan
/// # Introduction
/// A filter-commutative operation is an operation whose result of filter(op(data)) = op(filter(data)).
/// An example of a filter-commutative operation is a projection; a counter-example is `limit`.
///
/// The filter-commutative property is column-specific. An aggregate grouped by A on SUM(B)
/// can commute with a filter that depends on A only, but does not commute with a filter that depends
/// on SUM(B).
///
/// This optimizer commutes filters with filter-commutative operations to push the filters
/// the closest possible to the scans, re-writing the filter expressions by every
/// projection that changes the filter's expression.
///
/// Filter: #b Gt Int64(10)
///     Projection: #a AS b
///
/// is optimized to
///
/// Projection: #a AS b
///     Filter: #a Gt Int64(10)  <--- changed from #b to #a
///
/// This performs a single pass trought the plan. When it passes trought a filter, it stores that filter,
/// and when it reaches a node that does not commute with it, it adds the filter to that place.
/// When it passes through a projection, it re-writes the filter's expression taking into accoun that projection.
/// When multiple filters would have been written, it `AND` their expressions into a single expression.
pub struct FilterPushDown {}

#[derive(Debug, Clone, Default)]
struct State {
    // (predicate, columns on the predicate)
    filters: Vec<(Expr, HashSet<String>)>,
}

type Predicates<'a> = (Vec<&'a Expr>, Vec<&'a HashSet<String>>);

/// returns all predicates in `state` that depend on any of `used_columns`
fn get_predicates<'a>(
    state: &'a State,
    used_columns: &HashSet<String>,
) -> Predicates<'a> {
    state
        .filters
        .iter()
        .filter(|(_, columns)| {
            columns
                .intersection(used_columns)
                .collect::<HashSet<_>>()
                .len()
                > 0
        })
        .map(|&(ref a, ref b)| (a, b))
        .unzip()
}

// returns 3 (potentially overlaping) sets of predicates:
// * pushable to left: its columns are all on the left
// * pushable to right: its columns is all on the right
// * keep: the set of columns is not in only either left or right
// Note that a predicate can be both pushed to the left and to the right.
fn get_join_predicates<'a>(
    state: &'a State,
    left: &DFSchema,
    right: &DFSchema,
) -> (
    Vec<&'a HashSet<String>>,
    Vec<&'a HashSet<String>>,
    Predicates<'a>,
) {
    let left_columns = &left
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<HashSet<_>>();
    let right_columns = &right
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<HashSet<_>>();

    let filters = state
        .filters
        .iter()
        .map(|(predicate, columns)| {
            (
                (predicate, columns),
                (
                    columns,
                    left_columns.intersection(columns).collect::<HashSet<_>>(),
                    right_columns.intersection(columns).collect::<HashSet<_>>(),
                ),
            )
        })
        .collect::<Vec<_>>();

    let pushable_to_left = filters
        .iter()
        .filter(|(_, (columns, left, _))| left.len() == columns.len())
        .map(|((_, b), _)| *b)
        .collect();
    let pushable_to_right = filters
        .iter()
        .filter(|(_, (columns, _, right))| right.len() == columns.len())
        .map(|((_, b), _)| *b)
        .collect();
    let keep = filters
        .iter()
        .filter(|(_, (columns, left, right))| {
            // predicates whose columns are not in only one side of the join need to remain
            let all_in_left = left.len() == columns.len();
            let all_in_right = right.len() == columns.len();
            !all_in_left && !all_in_right
        })
        .map(|((ref a, ref b), _)| (a, b))
        .unzip();
    (pushable_to_left, pushable_to_right, keep)
}

/// Optimizes the plan
fn push_down(state: &State, plan: &LogicalPlan) -> Result<LogicalPlan> {
    let new_inputs = utils::inputs(&plan)
        .iter()
        .map(|input| optimize(input, state.clone()))
        .collect::<Result<Vec<_>>>()?;

    let expr = utils::expressions(&plan);
    utils::from_plan(&plan, &expr, &new_inputs)
}

/// returns a new [LogicalPlan] that wraps `plan` in a [LogicalPlan::Filter] with
/// its predicate be all `predicates` ANDed.
fn add_filter(plan: LogicalPlan, predicates: &[&Expr]) -> LogicalPlan {
    // reduce filters to a single filter with an AND
    let predicate = predicates
        .iter()
        .skip(1)
        .fold(predicates[0].clone(), |acc, predicate| {
            and(acc, (*predicate).to_owned())
        });

    LogicalPlan::Filter {
        predicate,
        input: Arc::new(plan),
    }
}

// remove all filters from `filters` that are in `predicate_columns`
fn remove_filters(
    filters: &[(Expr, HashSet<String>)],
    predicate_columns: &[&HashSet<String>],
) -> Vec<(Expr, HashSet<String>)> {
    filters
        .iter()
        .filter(|(_, columns)| !predicate_columns.contains(&columns))
        .cloned()
        .collect::<Vec<_>>()
}

// keeps all filters from `filters` that are in `predicate_columns`
fn keep_filters(
    filters: &[(Expr, HashSet<String>)],
    predicate_columns: &[&HashSet<String>],
) -> Vec<(Expr, HashSet<String>)> {
    filters
        .iter()
        .filter(|(_, columns)| predicate_columns.contains(&columns))
        .cloned()
        .collect::<Vec<_>>()
}

/// builds a new [LogicalPlan] from `plan` by issuing new [LogicalPlan::Filter] if any of the filters
/// in `state` depend on the columns `used_columns`.
fn issue_filters(
    mut state: State,
    used_columns: HashSet<String>,
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    let (predicates, predicate_columns) = get_predicates(&state, &used_columns);

    if predicates.is_empty() {
        // all filters can be pushed down => optimize inputs and return new plan
        return push_down(&state, plan);
    }

    let plan = add_filter(plan.clone(), &predicates);

    state.filters = remove_filters(&state.filters, &predicate_columns);

    // continue optimization over all input nodes by cloning the current state (i.e. each node is independent)
    push_down(&state, &plan)
}

fn optimize(plan: &LogicalPlan, mut state: State) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let mut columns: HashSet<String> = HashSet::new();
            utils::expr_to_column_names(predicate, &mut columns)?;
            // collect the predicate
            state.filters.push((predicate.clone(), columns));
            optimize(input, state)
        }
        LogicalPlan::Projection {
            input,
            expr,
            schema,
        } => {
            // A projection is filter-commutable, but re-writes all predicate expressions
            // collect projection.
            let mut projection = HashMap::new();
            schema.fields().iter().enumerate().for_each(|(i, field)| {
                // strip alias, as they should not be part of filters
                let expr = match &expr[i] {
                    Expr::Alias(expr, _) => expr.as_ref().clone(),
                    expr => expr.clone(),
                };

                projection.insert(field.name().clone(), expr);
            });

            // re-write all filters based on this projection
            // E.g. in `Filter: #b\n  Projection: #a > 1 as b`, we can swap them, but the filter must be "#a > 1"
            for (predicate, columns) in state.filters.iter_mut() {
                *predicate = rewrite(predicate, &projection)?;

                columns.clear();
                utils::expr_to_column_names(predicate, columns)?;
            }

            // optimize inner
            let new_input = optimize(input, state)?;

            utils::from_plan(&plan, &expr, &vec![new_input])
        }
        LogicalPlan::Aggregate {
            input, aggr_expr, ..
        } => {
            // An aggregate's aggreagate columns are _not_ filter-commutable => collect these:
            // * columns whose aggregation expression depends on
            // * the aggregation columns themselves

            // construct set of columns that `aggr_expr` depends on
            let mut used_columns = HashSet::new();
            utils::exprlist_to_column_names(aggr_expr, &mut used_columns)?;

            let agg_columns = aggr_expr
                .iter()
                .map(|x| x.name(input.schema()))
                .collect::<Result<HashSet<_>>>()?;
            used_columns.extend(agg_columns);

            issue_filters(state, used_columns, plan)
        }
        LogicalPlan::Sort { .. } => {
            // sort is filter-commutable
            push_down(&state, plan)
        }
        LogicalPlan::Limit { input, .. } => {
            // limit is _not_ filter-commutable => collect all columns from its input
            let used_columns = input
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<HashSet<_>>();
            issue_filters(state, used_columns, plan)
        }
        LogicalPlan::Join { left, right, .. } => {
            let (pushable_to_left, pushable_to_right, keep) =
                get_join_predicates(&state, &left.schema(), &right.schema());

            let mut left_state = state.clone();
            left_state.filters = keep_filters(&left_state.filters, &pushable_to_left);
            let left = optimize(left, left_state)?;

            let mut right_state = state.clone();
            right_state.filters = keep_filters(&right_state.filters, &pushable_to_right);
            let right = optimize(right, right_state)?;

            // create a new Join with the new `left` and `right`
            let expr = utils::expressions(&plan);
            let plan = utils::from_plan(&plan, &expr, &vec![left, right])?;

            if keep.0.is_empty() {
                Ok(plan)
            } else {
                // wrap the join on the filter whose predicates must be kept
                let plan = add_filter(plan, &keep.0);
                state.filters = remove_filters(&state.filters, &keep.1);

                Ok(plan)
            }
        }
        _ => {
            // all other plans are _not_ filter-commutable
            let used_columns = plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<HashSet<_>>();
            issue_filters(state, used_columns, plan)
        }
    }
}

impl OptimizerRule for FilterPushDown {
    fn name(&self) -> &str {
        return "filter_push_down";
    }

    fn optimize(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        optimize(plan, State::default())
    }
}

impl FilterPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// replaces columns by its name on the projection.
fn rewrite(expr: &Expr, projection: &HashMap<String, Expr>) -> Result<Expr> {
    let expressions = utils::expr_sub_expressions(&expr)?;

    let expressions = expressions
        .iter()
        .map(|e| rewrite(e, &projection))
        .collect::<Result<Vec<_>>>()?;

    match expr {
        Expr::Column(name) => {
            if let Some(expr) = projection.get(name) {
                return Ok(expr.clone());
            }
        }
        _ => {}
    }

    utils::rewrite_expression(&expr, &expressions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{lit, sum, Expr, LogicalPlanBuilder, Operator};
    use crate::test::*;
    use crate::{logical_plan::col, prelude::JoinType};

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let mut rule = FilterPushDown::new();
        let optimized_plan = rule.optimize(plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn filter_before_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a"), col("b")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter is before projection
        let expected = "\
            Projection: #a, #b\
            \n  Filter: #a Eq Int64(1)\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_after_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a"), col("b")])?
            .limit(10)?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter is before single projection
        let expected = "\
            Filter: #a Eq Int64(1)\
            \n  Limit: 10\
            \n    Projection: #a, #b\
            \n      TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_jump_2_plans() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .project(vec![col("c"), col("b")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter is before double projection
        let expected = "\
            Projection: #c, #b\
            \n  Projection: #a, #b, #c\
            \n    Filter: #a Eq Int64(1)\
            \n      TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_move_agg() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b")).alias("total_salary")])?
            .filter(col("a").gt(lit(10i64)))?
            .build()?;
        // filter of key aggregation is commutative
        let expected = "\
            Aggregate: groupBy=[[#a]], aggr=[[SUM(#b) AS total_salary]]\
            \n  Filter: #a Gt Int64(10)\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_keep_agg() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b")).alias("b")])?
            .filter(col("b").gt(lit(10i64)))?
            .build()?;
        // filter of aggregate is after aggregation since they are non-commutative
        let expected = "\
            Filter: #b Gt Int64(10)\
            \n  Aggregate: groupBy=[[#a]], aggr=[[SUM(#b) AS b]]\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// verifies that a filter is pushed to before a projection, the filter expression is correctly re-written
    #[test]
    fn alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .filter(col("b").eq(lit(1i64)))?
            .build()?;
        // filter is before projection
        let expected = "\
            Projection: #a AS b, #c\
            \n  Filter: #a Eq Int64(1)\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    fn add(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(left),
            op: Operator::Plus,
            right: Box::new(right),
        }
    }

    fn multiply(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(left),
            op: Operator::Multiply,
            right: Box::new(right),
        }
    }

    /// verifies that a filter is pushed to before a projection with a complex expression, the filter expression is correctly re-written
    #[test]
    fn complex_expression() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![
                add(multiply(col("a"), lit(2)), col("c")).alias("b"),
                col("c"),
            ])?
            .filter(col("b").eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #b Eq Int64(1)\
            \n  Projection: #a Multiply Int32(2) Plus #c AS b, #c\
            \n    TableScan: test projection=None"
        );

        // filter is before projection
        let expected = "\
            Projection: #a Multiply Int32(2) Plus #c AS b, #c\
            \n  Filter: #a Multiply Int32(2) Plus #c Eq Int64(1)\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// verifies that when a filter is pushed to after 2 projections, the filter expression is correctly re-written
    #[test]
    fn complex_plan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![
                add(multiply(col("a"), lit(2)), col("c")).alias("b"),
                col("c"),
            ])?
            // second projection where we rename columns, just to make it difficult
            .project(vec![multiply(col("b"), lit(3)).alias("a"), col("c")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #a Eq Int64(1)\
            \n  Projection: #b Multiply Int32(3) AS a, #c\
            \n    Projection: #a Multiply Int32(2) Plus #c AS b, #c\
            \n      TableScan: test projection=None"
        );

        // filter is before the projections
        let expected = "\
        Projection: #b Multiply Int32(3) AS a, #c\
        \n  Projection: #a Multiply Int32(2) Plus #c AS b, #c\
        \n    Filter: #a Multiply Int32(2) Plus #c Multiply Int32(3) Eq Int64(1)\
        \n      TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// verifies that when two filters apply after an aggregation that only allows one to be pushed, one is pushed
    /// and the other not.
    #[test]
    fn multi_filter() -> Result<()> {
        // the aggregation allows one filter to pass (b), and the other one to not pass (SUM(c))
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .aggregate(vec![col("b")], vec![sum(col("c"))])?
            .filter(col("b").gt(lit(10i64)))?
            .filter(col("SUM(c)").gt(lit(10i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #SUM(c) Gt Int64(10)\
            \n  Filter: #b Gt Int64(10)\
            \n    Aggregate: groupBy=[[#b]], aggr=[[SUM(#c)]]\
            \n      Projection: #a AS b, #c\
            \n        TableScan: test projection=None"
        );

        // filter is before the projections
        let expected = "\
        Filter: #SUM(c) Gt Int64(10)\
        \n  Aggregate: groupBy=[[#b]], aggr=[[SUM(#c)]]\
        \n    Projection: #a AS b, #c\
        \n      Filter: #a Gt Int64(10)\
        \n        TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    /// verifies that when two limits are in place, we jump neither
    #[test]
    fn double_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a"), col("b")])?
            .limit(20)?
            .limit(10)?
            .project(vec![col("a"), col("b")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter does not just any of the limits
        let expected = "\
            Projection: #a, #b\
            \n  Filter: #a Eq Int64(1)\
            \n    Limit: 10\
            \n      Limit: 20\
            \n        Projection: #a, #b\
            \n          TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// verifies that filters with the same columns are correctly placed
    #[test]
    fn filter_2_breaks_limits() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a")])?
            .filter(col("a").lt_eq(lit(1i64)))?
            .limit(1)?
            .project(vec![col("a")])?
            .filter(col("a").gt_eq(lit(1i64)))?
            .build()?;
        // Should be able to move both filters below the projections

        // not part of the test
        assert_eq!(
            format!("{:?}", plan),
            "Filter: #a GtEq Int64(1)\
             \n  Projection: #a\
             \n    Limit: 1\
             \n      Filter: #a LtEq Int64(1)\
             \n        Projection: #a\
             \n          TableScan: test projection=None"
        );

        let expected = "\
        Projection: #a\
        \n  Filter: #a GtEq Int64(1)\
        \n    Limit: 1\
        \n      Projection: #a\
        \n        Filter: #a LtEq Int64(1)\
        \n          TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// verifies that filters to be placed on the same depth are ANDed
    #[test]
    fn two_filters_on_same_depth() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .limit(1)?
            .filter(col("a").lt_eq(lit(1i64)))?
            .filter(col("a").gt_eq(lit(1i64)))?
            .project(vec![col("a")])?
            .build()?;

        // not part of the test
        assert_eq!(
            format!("{:?}", plan),
            "Projection: #a\
            \n  Filter: #a GtEq Int64(1)\
            \n    Filter: #a LtEq Int64(1)\
            \n      Limit: 1\
            \n        TableScan: test projection=None"
        );

        let expected = "\
        Projection: #a\
        \n  Filter: #a GtEq Int64(1) And #a LtEq Int64(1)\
        \n    Limit: 1\
        \n      TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// verifies that filters on a plan with user nodes are not lost
    /// (ARROW-10547)
    #[test]
    fn filters_user_defined_node() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        let plan = crate::test::user_defined::new(plan);

        let expected = "\
            TestUserDefined\
             \n  Filter: #a LtEq Int64(1)\
             \n    TableScan: test projection=None";

        // not part of the test
        assert_eq!(format!("{:?}", plan), expected);

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// post-join predicates on a column common to both sides is pushed to both sides
    #[test]
    fn filter_join_on_common_independent() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(&table_scan).build()?;
        let right = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(&left)
            .join(&right, JoinType::Inner, &["a"], &["a"])?
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #a LtEq Int64(1)\
            \n  Join: a = a\
            \n    TableScan: test projection=None\
            \n    Projection: #a\
            \n      TableScan: test projection=None"
        );

        // filter sent to side before the join
        let expected = "\
        Join: a = a\
        \n  Filter: #a LtEq Int64(1)\
        \n    TableScan: test projection=None\
        \n  Projection: #a\
        \n    Filter: #a LtEq Int64(1)\
        \n      TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// post-join predicates with columns from both sides are not pushed
    #[test]
    fn filter_join_on_common_dependent() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a"), col("c")])?
            .build()?;
        let right = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(&left)
            .join(&right, JoinType::Inner, &["a"], &["a"])?
            // "b" and "c" are not shared by either side: they are only available together after the join
            .filter(col("c").lt_eq(col("b")))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #c LtEq #b\
            \n  Join: a = a\
            \n    Projection: #a, #c\
            \n      TableScan: test projection=None\
            \n    Projection: #a, #b\
            \n      TableScan: test projection=None"
        );

        // expected is equal: no push-down
        let expected = &format!("{:?}", plan);
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// post-join predicates with columns from one side of a join are pushed only to that side
    #[test]
    fn filter_join_on_one_side() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let right = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a"), col("c")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(&left)
            .join(&right, JoinType::Inner, &["a"], &["a"])?
            .filter(col("b").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #b LtEq Int64(1)\
            \n  Join: a = a\
            \n    Projection: #a, #b\
            \n      TableScan: test projection=None\
            \n    Projection: #a, #c\
            \n      TableScan: test projection=None"
        );

        let expected = "\
        Join: a = a\
        \n  Projection: #a, #b\
        \n    Filter: #b LtEq Int64(1)\
        \n      TableScan: test projection=None\
        \n  Projection: #a, #c\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
