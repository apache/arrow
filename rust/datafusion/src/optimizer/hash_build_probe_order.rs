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
// under the License

//! Optimizer rule to switch build and probe order of hash join
//! based on statistics of a `TableProvider`. If the number of
//! rows of both sources is known, the order can be switched
//! for a faster hash join.

use std::sync::Arc;

use crate::logical_plan::LogicalPlan;
use crate::optimizer::optimizer::OptimizerRule;
use crate::{error::Result, prelude::JoinType};

use super::utils;

/// BuildProbeOrder reorders the build and probe phase of
/// hash joins. This uses the amount of rows that a datasource has.
/// The rule optimizes the order such that the left (build) side of the join
/// is the smallest.
/// If the information is not available, the order stays the same,
/// so that it could be optimized manually in a query.
pub struct HashBuildProbeOrder {}

// Gets exact number of rows, if known by the statistics of the underlying
fn get_num_rows(logical_plan: &LogicalPlan) -> Option<usize> {
    match logical_plan {
        LogicalPlan::Projection { input, .. } => get_num_rows(input),
        LogicalPlan::Sort { input, .. } => get_num_rows(input),
        LogicalPlan::TableScan { source, .. } => source.statistics().num_rows,
        LogicalPlan::EmptyRelation {
            produce_one_row, ..
        } => {
            if *produce_one_row {
                Some(1)
            } else {
                Some(0)
            }
        }
        LogicalPlan::Limit { n: limit, input } => {
            let num_rows_input = get_num_rows(input);
            num_rows_input.map(|rows| std::cmp::min(*limit, rows))
        }
        _ => None,
    }
}

// Finds out whether to swap left vs right order based on statistics
fn should_swap_join_order(left: &LogicalPlan, right: &LogicalPlan) -> bool {
    let left_rows = get_num_rows(left);
    let right_rows = get_num_rows(right);

    match (left_rows, right_rows) {
        (Some(l), Some(r)) => l > r,
        _ => false,
    }
}

impl OptimizerRule for HashBuildProbeOrder {
    fn name(&self) -> &str {
        "hash_build_probe_order"
    }

    fn optimize(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            // Main optimization rule, swaps order of left and right
            // based on number of rows in each table
            LogicalPlan::Join {
                left,
                right,
                on,
                join_type,
                schema,
            } => {
                let left = self.optimize(left)?;
                let right = self.optimize(right)?;
                if should_swap_join_order(&left, &right) {
                    // Swap left and right, change join type and (equi-)join key order
                    Ok(LogicalPlan::Join {
                        left: Arc::new(right),
                        right: Arc::new(left),
                        on: on
                            .iter()
                            .map(|(l, r)| (r.to_string(), l.to_string()))
                            .collect(),
                        join_type: swap_join_type(*join_type),
                        schema: schema.clone(),
                    })
                } else {
                    // Keep join as is
                    Ok(LogicalPlan::Join {
                        left: Arc::new(left),
                        right: Arc::new(right),
                        on: on.clone(),
                        join_type: *join_type,
                        schema: schema.clone(),
                    })
                }
            }
            // Rest: recurse into plan, apply optimization where possible
            LogicalPlan::Projection { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::TableScan { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Repartition { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::CreateExternalTable { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Extension { .. } => {
                let expr = utils::expressions(plan);

                // apply the optimization to all inputs of the plan
                let inputs = utils::inputs(plan);
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize(plan))
                    .collect::<Result<Vec<_>>>()?;

                utils::from_plan(plan, &expr, &new_inputs)
            }
        }
    }
}

impl HashBuildProbeOrder {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn swap_join_type(join_type: JoinType) -> JoinType {
    match join_type {
        JoinType::Inner => JoinType::Inner,
        JoinType::Left => JoinType::Right,
        JoinType::Right => JoinType::Left,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::{
        datasource::{datasource::Statistics, TableProvider},
        logical_plan::{DFSchema, Expr},
        test::*,
    };

    struct TestTableProvider {
        num_rows: usize,
    }

    impl TableProvider for TestTableProvider {
        fn as_any(&self) -> &dyn std::any::Any {
            unimplemented!()
        }
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            unimplemented!()
        }

        fn scan(
            &self,
            _projection: &Option<Vec<usize>>,
            _batch_size: usize,
            _filters: &[Expr],
        ) -> Result<std::sync::Arc<dyn crate::physical_plan::ExecutionPlan>> {
            unimplemented!()
        }
        fn statistics(&self) -> crate::datasource::datasource::Statistics {
            Statistics {
                num_rows: Some(self.num_rows),
                total_byte_size: None,
                column_statistics: None,
            }
        }
    }

    #[test]
    fn test_num_rows() -> Result<()> {
        let table_scan = test_table_scan()?;

        assert_eq!(get_num_rows(&table_scan), Some(0));

        Ok(())
    }

    #[test]
    fn test_swap_order() -> Result<()> {
        let lp_left = LogicalPlan::TableScan {
            table_name: "left".to_string(),
            projection: None,
            source: Arc::new(TestTableProvider { num_rows: 1000 }),
            projected_schema: Arc::new(DFSchema::empty()),
            filters: vec![],
        };

        let lp_right = LogicalPlan::TableScan {
            table_name: "right".to_string(),
            projection: None,
            source: Arc::new(TestTableProvider { num_rows: 100 }),
            projected_schema: Arc::new(DFSchema::empty()),
            filters: vec![],
        };

        assert!(should_swap_join_order(&lp_left, &lp_right));
        assert!(!should_swap_join_order(&lp_right, &lp_left));

        Ok(())
    }
}
