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

use super::utils;
use crate::error::Result;
use crate::logical_plan::LogicalPlan;
use crate::optimizer::optimizer::OptimizerRule;

/// Optimization rule that tries pushes down LIMIT n
/// where applicable to reduce the scanned or necessary in outer join
pub struct LimitPushdown {}

fn limit_push_down(n: Option<usize>, plan: &LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        expr => {
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

impl OptimizerRule for LimitPushdown {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        limit_push_down(None, plan)
    }

    fn name(&self) -> &str {
        "limit_push_down"
    }
}
