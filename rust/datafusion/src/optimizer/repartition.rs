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

//! Repartition optimizer that introduces repartition nodes to increase the level of parallism available
use std::sync::Arc;

use crate::physical_plan::{repartition::RepartitionExec, ExecutionPlan};
use crate::{error::Result, execution::context::ExecutionConfig};

use super::optimizer::PhysicalOptimizerRule;
use crate::physical_plan::{Distribution, Partitioning::*};

/// Optimizer that introduces repartition to introduce more parallelism in the plan
pub struct Repartition {}

impl Repartition {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn optimize_concurrency(
    concurrency: usize,
    requires_single_partition: bool,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Recurse into children bottom-up (added nodes should be as deep as possible)
    let children = plan
        .children()
        .iter()
        .map(|child| {
            optimize_concurrency(
                concurrency,
                plan.required_child_distribution() == Distribution::SinglePartition,
                child.clone(),
            )
        })
        .collect::<Result<_>>()?;

    let new_plan = plan.with_new_children(children)?;

    let partitioning = plan.output_partitioning();

    let perform_repartition = match partitioning {
        // Apply when underlying node has less than `self.concurrency` amount of concurrency
        RoundRobinBatch(x) => x < concurrency,
        UnknownPartitioning(x) => x < concurrency,
        // we don't want to introduce partitioning after hash partitioning
        // as the plan will likely depend on this
        Hash(_, _) => false,
    };

    if perform_repartition && !requires_single_partition {
        Ok(Arc::new(RepartitionExec::try_new(
            new_plan,
            RoundRobinBatch(concurrency),
        )?))
    } else {
        Ok(new_plan)
    }
}

impl PhysicalOptimizerRule for Repartition {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ExecutionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        optimize_concurrency(config.concurrency, true, plan)
    }

    fn name(&self) -> &str {
        "repartition"
    }
}
#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;

    use super::*;
    use crate::datasource::datasource::Statistics;
    use crate::physical_plan::parquet::{ParquetExec, ParquetPartition};
    #[test]
    fn added_repartition_to_single_partition() -> Result<()> {
        let parquet = ParquetExec::new(
            vec![ParquetPartition {
                filenames: vec!["x".to_string()],
                statistics: Statistics::default(),
            }],
            Schema::empty(),
            None,
            None,
            2048,
            None,
        );

        let optimizer = Repartition {};

        let optimized = optimizer.optimize(
            Arc::new(parquet),
            &ExecutionConfig::new().with_concurrency(10),
        )?;

        assert_eq!(optimized.output_partitioning().partition_count(), 10);

        Ok(())
    }
}
