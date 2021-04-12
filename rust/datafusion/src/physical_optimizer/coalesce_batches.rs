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

//! CoalesceBatches optimizer that groups batches together rows
//! in bigger batches to avoid overhead with small batches

use super::optimizer::PhysicalOptimizerRule;
use crate::{
    error::Result,
    physical_plan::{
        coalesce_batches::CoalesceBatchesExec, filter::FilterExec,
        hash_join::HashJoinExec, repartition::RepartitionExec,
    },
};
use std::sync::Arc;

/// Optimizer that introduces CoalesceBatchesExec to avoid overhead with small batches
pub struct CoalesceBatches {}

impl CoalesceBatches {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
impl PhysicalOptimizerRule for CoalesceBatches {
    fn optimize(
        &self,
        plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
        config: &crate::execution::context::ExecutionConfig,
    ) -> Result<Arc<dyn crate::physical_plan::ExecutionPlan>> {
        // wrap operators in CoalesceBatches to avoid lots of tiny batches when we have
        // highly selective filters
        let children = plan
            .children()
            .iter()
            .map(|child| self.optimize(child.clone(), config))
            .collect::<Result<Vec<_>>>()?;

        let plan_any = plan.as_any();
        //TODO we should do this in a more generic way either by wrapping all operators
        // or having an API so that operators can declare when their inputs or outputs
        // need to be wrapped in a coalesce batches operator.
        // See https://issues.apache.org/jira/browse/ARROW-11068
        let wrap_in_coalesce = plan_any.downcast_ref::<FilterExec>().is_some()
            || plan_any.downcast_ref::<HashJoinExec>().is_some()
            || plan_any.downcast_ref::<RepartitionExec>().is_some();

        //TODO we should also do this for HashAggregateExec but we need to update tests
        // as part of this work - see https://issues.apache.org/jira/browse/ARROW-11068
        // || plan_any.downcast_ref::<HashAggregateExec>().is_some();

        if plan.children().is_empty() {
            // leaf node, children cannot be replaced
            Ok(plan.clone())
        } else {
            let plan = plan.with_new_children(children)?;
            Ok(if wrap_in_coalesce {
                //TODO we should add specific configuration settings for coalescing batches and
                // we should do that once https://issues.apache.org/jira/browse/ARROW-11059 is
                // implemented. For now, we choose half the configured batch size to avoid copies
                // when a small number of rows are removed from a batch
                let target_batch_size = config.batch_size / 2;
                Arc::new(CoalesceBatchesExec::new(plan.clone(), target_batch_size))
            } else {
                plan.clone()
            })
        }
    }

    fn name(&self) -> &str {
        "coalesce_batches"
    }
}
