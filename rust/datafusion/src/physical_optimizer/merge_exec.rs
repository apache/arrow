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

//! AddMergeExec adds MergeExec to merge plans
//! with more partitions into one partition when the node
//! needs a single partition
use super::optimizer::PhysicalOptimizerRule;
use crate::{
    error::Result,
    physical_plan::{merge::MergeExec, Distribution},
};
use std::sync::Arc;

/// Introduces MergeExec
pub struct AddMergeExec {}

impl AddMergeExec {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for AddMergeExec {
    fn optimize(
        &self,
        plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
        config: &crate::execution::context::ExecutionConfig,
    ) -> Result<Arc<dyn crate::physical_plan::ExecutionPlan>> {
        if plan.children().is_empty() {
            // leaf node, children cannot be replaced
            Ok(plan.clone())
        } else {
            let children = plan
                .children()
                .iter()
                .map(|child| self.optimize(child.clone(), config))
                .collect::<Result<Vec<_>>>()?;
            match plan.required_child_distribution() {
                Distribution::UnspecifiedDistribution => plan.with_new_children(children),
                Distribution::SinglePartition => plan.with_new_children(
                    children
                        .iter()
                        .map(|child| {
                            if child.output_partitioning().partition_count() == 1 {
                                child.clone()
                            } else {
                                Arc::new(MergeExec::new(child.clone()))
                            }
                        })
                        .collect(),
                ),
            }
        }
    }

    fn name(&self) -> &str {
        "add_merge_exec"
    }
}
