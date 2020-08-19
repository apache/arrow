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
use crate::logicalplan::Expr;
use crate::logicalplan::{and, LogicalPlan};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Filter Push Down optimizer rule pushes filter clauses down the plan
///
/// This optimization looks for the maximum depth of each column in the plan where a filter can be applied and
/// re-writes the plan with filters on those locations.
/// It performs two passes on the plan:
/// 1. identify filters, which columns they use, and projections along the path
/// 2. move filters down, re-writing the expressions using the projections
/*
A filter-commutative operation is an operation whose result of filter(op(data)) = op(filter(data)).
An example of a filter-commutative operation is a projection; a counter-example is `limit`.

The filter-commutative property is column-specific. An aggregate grouped by A on SUM(B)
can commute with a filter that depends on A only, but does not commute with a filter that depends
on SUM(B).

A location in this module is identified by a number, depth, which is 0 for the last operation
and highest for the first operation (typically a scan).

This optimizer commutes filters with filter-commutative operations to push the filters
to the maximum possible depth, consequently re-writing the filter expressions by every
projection that changes the filter's expression.

    Selection: #b Gt Int64(10)
        Projection: #a AS b

is optimized to

    Projection: #a AS b
        Selection: #a Gt Int64(10)  <--- changed from #b to #a

To perform such optimization, we first analyze the plan to identify three items:

1. Where are the filters located in the plan
2. Where are non-commutable operations' columns located in the plan (break_points)
3. Where are projections located in the plan

With this information, we re-write the plan by:

1. Computing the maximum possible depth of each column between breakpoints
2. Computing the maximum possible depth of each filter expression based on the columns it depends on
3. re-write the filter expression for every projection that it commutes with from its original depth to its max possible depth
4. recursively re-write the plan by deleting old filter expressions and adding new filter expressions on their max possible depth.
*/
pub struct FilterPushDown {}

impl OptimizerRule for FilterPushDown {
    fn name(&self) -> &str {
        return "filter_push_down";
    }

    fn optimize(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let result = analyze_plan(plan, 0)?;
        let break_points = result.break_points.clone();

        // get max depth over all breakpoints
        let max_depth = break_points.keys().max();
        if max_depth.is_none() {
            // it is unlikely that the plan is correct without break points as all scans
            // adds breakpoints. We just return the plan and let others handle the error
            return Ok(plan.clone());
        }
        let max_depth = *max_depth.unwrap(); // unwrap is safe by previous if

        // construct optimized position of each of the new selections
        // E.g. when we have a filter (c1 + c2 > 2), c1's max depth is 10 and c2 is 11, we
        // can push the filter to depth 10
        let mut new_selections: BTreeMap<usize, Expr> = BTreeMap::new();
        for (selection_depth, expr) in result.selections {
            // get all columns on the filter expression
            let mut selection_columns: HashSet<String> = HashSet::new();
            utils::expr_to_column_names(&expr, &mut selection_columns)?;

            // identify the depths that are filter-commutable with this selection
            let mut new_depth = selection_depth;
            for depth in selection_depth..max_depth {
                if let Some(break_columns) = break_points.get(&depth) {
                    if selection_columns
                        .intersection(break_columns)
                        .peekable()
                        .peek()
                        .is_none()
                    {
                        new_depth += 1
                    } else {
                        // non-commutable: can't advance any further
                        break;
                    }
                } else {
                    new_depth += 1
                }
            }

            // re-write the new selections based on all projections that it crossed.
            // E.g. in `Selection: #b\n  Projection: #a > 1 as b`, we can swap them, but the selection must be "#a > 1"
            let mut new_expression = expr.clone();
            for depth_i in selection_depth..new_depth {
                if let Some(projection) = result.projections.get(&depth_i) {
                    new_expression = rewrite(&new_expression, projection)?;
                }
            }

            // AND filter expressions that would be placed on the same depth
            if let Some(existing_expression) = new_selections.get(&new_depth) {
                new_expression = and(existing_expression, &new_expression)
            }
            new_selections.insert(new_depth, new_expression);
        }

        optimize_plan(plan, &new_selections, 0)
    }
}

/// The result of a plan analysis suitable to perform a filter push down optimization
// BTreeMap are ordered, which ensures stability in ordered operations.
// Also, most inserts here are at the end
struct AnalysisResult {
    /// maps the depths of non filter-commutative nodes to their columns
    /// depths not in here indicate that the node is commutative
    pub break_points: BTreeMap<usize, HashSet<String>>,
    /// maps the depths of filter nodes to expressions
    pub selections: BTreeMap<usize, Expr>,
    /// maps the depths of projection nodes to their expressions
    pub projections: BTreeMap<usize, HashMap<String, Expr>>,
}

/// Recursively transverses the logical plan looking for depths that break filter pushdown
fn analyze_plan(plan: &LogicalPlan, depth: usize) -> Result<AnalysisResult> {
    match plan {
        LogicalPlan::Selection { input, expr } => {
            let mut result = analyze_plan(&input, depth + 1)?;
            result.selections.insert(depth, expr.clone());
            Ok(result)
        }
        LogicalPlan::Projection {
            input,
            expr,
            schema,
        } => {
            let mut result = analyze_plan(&input, depth + 1)?;

            // collect projection.
            let mut projection = HashMap::new();
            schema.fields().iter().enumerate().for_each(|(i, field)| {
                // strip alias, as they should not be part of selections
                let expr = match &expr[i] {
                    Expr::Alias(expr, _) => expr.as_ref().clone(),
                    expr => expr.clone(),
                };

                projection.insert(field.name().clone(), expr);
            });
            result.projections.insert(depth, projection);
            Ok(result)
        }
        LogicalPlan::Aggregate {
            input, aggr_expr, ..
        } => {
            let mut result = analyze_plan(&input, depth + 1)?;

            // construct set of columns that `aggr_expr` depends on
            let mut agg_columns = HashSet::new();
            utils::exprlist_to_column_names(aggr_expr, &mut agg_columns)?;

            // collect all columns that break at this depth:
            // * columns whose aggregation expression depends on
            // * the aggregation columns themselves
            let mut columns = agg_columns.iter().cloned().collect::<HashSet<_>>();
            let agg_columns = aggr_expr
                .iter()
                .map(|x| x.name(input.schema()))
                .collect::<Result<HashSet<_>>>()?;
            columns.extend(agg_columns);
            result.break_points.insert(depth, columns);

            Ok(result)
        }
        LogicalPlan::Sort { input, .. } => analyze_plan(&input, depth + 1),
        LogicalPlan::Limit { input, .. } => {
            let mut result = analyze_plan(&input, depth + 1)?;

            // collect all columns that break at this depth
            let columns = input
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<HashSet<_>>();
            result.break_points.insert(depth, columns);
            Ok(result)
        }
        // all other plans add breaks to all their columns to indicate that filters can't proceed further.
        _ => {
            let columns = plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<HashSet<_>>();
            let mut break_points = BTreeMap::new();

            break_points.insert(depth, columns);
            Ok(AnalysisResult {
                break_points,
                selections: BTreeMap::new(),
                projections: BTreeMap::new(),
            })
        }
    }
}

impl FilterPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Returns a re-written logical plan where all old filters are removed and the new ones are added.
fn optimize_plan(
    plan: &LogicalPlan,
    new_selections: &BTreeMap<usize, Expr>,
    depth: usize,
) -> Result<LogicalPlan> {
    // optimize the plan recursively:
    let new_plan = match plan {
        LogicalPlan::Selection { input, .. } => {
            // ignore old selections
            Ok(optimize_plan(&input, new_selections, depth + 1)?)
        }
        _ => {
            // all other nodes are copied, optimizing recursively.
            let expr = utils::expressions(plan);

            let inputs = utils::inputs(plan);
            let new_inputs = inputs
                .iter()
                .map(|plan| optimize_plan(plan, new_selections, depth + 1))
                .collect::<Result<Vec<_>>>()?;

            utils::from_plan(plan, &expr, &new_inputs)
        }
    }?;

    // if a new selection is to be applied, apply it
    if let Some(expr) = new_selections.get(&depth) {
        return Ok(LogicalPlan::Selection {
            expr: expr.clone(),
            input: Box::new(new_plan),
        });
    } else {
        Ok(new_plan)
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
    use crate::logicalplan::col;
    use crate::logicalplan::{aggregate_expr, lit, Expr, LogicalPlanBuilder, Operator};
    use crate::test::*;

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
        // selection is before projection
        let expected = "\
            Projection: #a, #b\
            \n  Selection: #a Eq Int64(1)\
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
        // selection is before single projection
        let expected = "\
            Selection: #a Eq Int64(1)\
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
        // selection is before double projection
        let expected = "\
            Projection: #c, #b\
            \n  Projection: #a, #b, #c\
            \n    Selection: #a Eq Int64(1)\
            \n      TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_move_agg() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .aggregate(
                vec![col("a")],
                vec![aggregate_expr("SUM", col("b")).alias("total_salary")],
            )?
            .filter(col("a").gt(lit(10i64)))?
            .build()?;
        // selection of key aggregation is commutative
        let expected = "\
            Aggregate: groupBy=[[#a]], aggr=[[SUM(#b) AS total_salary]]\
            \n  Selection: #a Gt Int64(10)\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_keep_agg() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .aggregate(
                vec![col("a")],
                vec![aggregate_expr("SUM", col("b")).alias("b")],
            )?
            .filter(col("b").gt(lit(10i64)))?
            .build()?;
        // selection of aggregate is after aggregation since they are non-commutative
        let expected = "\
            Selection: #b Gt Int64(10)\
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
        // selection is before projection
        let expected = "\
            Projection: #a AS b, #c\
            \n  Selection: #a Eq Int64(1)\
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
            Selection: #b Eq Int64(1)\
            \n  Projection: #a Multiply Int32(2) Plus #c AS b, #c\
            \n    TableScan: test projection=None"
        );

        // selection is before projection
        let expected = "\
            Projection: #a Multiply Int32(2) Plus #c AS b, #c\
            \n  Selection: #a Multiply Int32(2) Plus #c Eq Int64(1)\
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
            Selection: #a Eq Int64(1)\
            \n  Projection: #b Multiply Int32(3) AS a, #c\
            \n    Projection: #a Multiply Int32(2) Plus #c AS b, #c\
            \n      TableScan: test projection=None"
        );

        // selection is before the projections
        let expected = "\
        Projection: #b Multiply Int32(3) AS a, #c\
        \n  Projection: #a Multiply Int32(2) Plus #c AS b, #c\
        \n    Selection: #a Multiply Int32(2) Plus #c Multiply Int32(3) Eq Int64(1)\
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
            .aggregate(vec![col("b")], vec![aggregate_expr("SUM", col("c"))])?
            .filter(col("b").gt(lit(10i64)))?
            .filter(col("SUM(c)").gt(lit(10i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Selection: #SUM(c) Gt Int64(10)\
            \n  Selection: #b Gt Int64(10)\
            \n    Aggregate: groupBy=[[#b]], aggr=[[SUM(#c)]]\
            \n      Projection: #a AS b, #c\
            \n        TableScan: test projection=None"
        );

        // selection is before the projections
        let expected = "\
        Selection: #SUM(c) Gt Int64(10)\
        \n  Aggregate: groupBy=[[#b]], aggr=[[SUM(#c)]]\
        \n    Projection: #a AS b, #c\
        \n      Selection: #a Gt Int64(10)\
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
        // selection does not just any of the limits
        let expected = "\
            Projection: #a, #b\
            \n  Selection: #a Eq Int64(1)\
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
            "Selection: #a GtEq Int64(1)\
             \n  Projection: #a\
             \n    Limit: 1\
             \n      Selection: #a LtEq Int64(1)\
             \n        Projection: #a\
             \n          TableScan: test projection=None"
        );

        let expected = "\
        Projection: #a\
        \n  Selection: #a GtEq Int64(1)\
        \n    Limit: 1\
        \n      Projection: #a\
        \n        Selection: #a LtEq Int64(1)\
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
            \n  Selection: #a GtEq Int64(1)\
            \n    Selection: #a LtEq Int64(1)\
            \n      Limit: 1\
            \n        TableScan: test projection=None"
        );

        let expected = "\
        Projection: #a\
        \n  Selection: #a GtEq Int64(1) And #a LtEq Int64(1)\
        \n    Limit: 1\
        \n      TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
