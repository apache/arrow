use std::sync::Arc;

use crate::error::Result;
use crate::physical_plan::{repartition::RepartitionExec, ExecutionPlan};

use super::optimizer::PhysicalOptimizerRule;
use crate::physical_plan::Partitioning::*;

/// Optimizer that introduces repartition to introduce more parallelism in the plan
pub struct Repartition {
    // Concurrency wanted downstream.
    // will create more concurrency
    concurrency: usize,
}

impl PhysicalOptimizerRule for Repartition {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let partitioning = plan.output_partitioning();
        let perform_repartition = match partitioning {
            // Apply when underlying node has less than `self.concurrency` amount of concurrency
            RoundRobinBatch(x) => x < self.concurrency,
            UnknownPartitioning(x) => x < self.concurrency,
            // we don't want to introduce partitioning after hash partitioning
            // as the plan will likely depend on this
            Hash(_, _) => false,
        };

        // Recurse into children 
        let children = plan
            .children()
            .iter()
            .map(|child| self.optimize(child.clone()))
            .collect::<Result<_>>()?;

        let new_plan = plan.with_new_children(children)?;

        if perform_repartition {
            Ok(Arc::new(RepartitionExec::try_new(
                new_plan,
                RoundRobinBatch(self.concurrency),
            )?))
        } else {
            Ok(new_plan)
        }
    }

    fn name(&self) -> &str {
        "repartition"
    }
}
