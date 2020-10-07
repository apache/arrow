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

//! Query optimizer traits

use super::utils;
use crate::error::Result;
use crate::logical_plan::LogicalPlan;

/// An optimizer rules performs a transformation on a logical plan to produce an optimized
/// logical plan.
pub trait OptimizerRule {
    /// Perform optimizations on the plan
    fn optimize(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan>;
    /// Produce a human readable name for this optimizer rule
    fn name(&self) -> &str;

    /// Convenience rule for writing optimizers: recursively invoke
    /// optimize on plan's children and then return a node of the same
    /// type. Useful for optimizer rules which want to leave the type
    /// of plan unchanged but still apply to the children.
    fn optimize_children(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let new_exprs = utils::expressions(&plan);
        let new_inputs = utils::inputs(&plan)
            .into_iter()
            .map(|plan| self.optimize(plan))
            .collect::<Result<Vec<_>>>()?;

        utils::from_plan(plan, &new_exprs, &new_inputs)
    }
}
