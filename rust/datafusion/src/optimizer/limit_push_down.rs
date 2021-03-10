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

//! Optimizer rule to push down LIMIT in the query plan
use std::sync::Arc;

use super::utils;
use crate::error::Result;
use crate::logical_plan::LogicalPlan;
use crate::optimizer::optimizer::OptimizerRule;

/// Optimization rule that tries pushes down LIMIT n
/// where applicable to reduce the scanned or necessary in outer join
pub struct LimitPushDown {}

impl LimitPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn limit_push_down(
    upper_limit: Option<usize>,
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Limit { n, input } => {
            println!("limit");
            return Ok(LogicalPlan::Limit {
                n: *n,
                // push down limit to plan (minimum of upper limit and current limit)
                input: Arc::new(limit_push_down(
                    upper_limit.map(|x| std::cmp::min(x, *n)).or(Some(*n)),
                    input.as_ref(),
                )?),
            });
        }
        LogicalPlan::TableScan {
            table_name,
            source,
            projection,
            filters,
            limit,
            projected_schema,
        } => {
            if let Some(n) = upper_limit {
                return Ok(LogicalPlan::TableScan {
                    table_name: table_name.clone(),
                    source: source.clone(),
                    projection: projection.clone(),
                    filters: filters.clone(),
                    limit: limit.map(|x| std::cmp::min(x, n)).or(Some(n)),
                    projected_schema: projected_schema.clone(),
                });
            } else {
                return Ok(plan.clone());
            }
        }

        _ => {
            let expr = plan.expressions();

            // apply the optimization to all inputs of the plan
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|plan| limit_push_down(None, plan))
                .collect::<Result<Vec<_>>>()?;

            utils::from_plan(plan, &expr, &new_inputs)
        }
    }
}

impl OptimizerRule for LimitPushDown {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        limit_push_down(None, plan)
    }

    fn name(&self) -> &str {
        "limit_push_down"
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        logical_plan::{LogicalPlan, LogicalPlanBuilder},
        test::*,
    };

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = LimitPushDown::new();
        let optimized_plan = rule.optimize(plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn limit_pushdown_table_provider() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(&table_scan).limit(1000)?.build()?;

        // Pushes the limit down to table provider
        // TableScan returns at least the limit nr. of rows (if it has rows)
        let expected = "Limit: 1000\
        \n  TableScan: test projection=None, limit=1000";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
