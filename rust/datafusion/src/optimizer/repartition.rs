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

        // Recurse into children bottom-up (added nodes should be as deep as possible)
        let children = plan
            .children()
            .iter()
            .map(|child| self.optimize(child.clone()))
            .collect::<Result<_>>()?;

        let new_plan = plan.with_new_children(children)?;

        let perform_repartition = match partitioning {
            // Apply when underlying node has less than `self.concurrency` amount of concurrency
            RoundRobinBatch(x) => x < self.concurrency,
            UnknownPartitioning(x) => x < self.concurrency,
            // we don't want to introduce partitioning after hash partitioning
            // as the plan will likely depend on this
            Hash(_, _) => false,
        };

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

        let optimizer = Repartition { concurrency: 10 };

        let optimized = optimizer.optimize(Arc::new(parquet))?;

        assert_eq!(optimized.output_partitioning().partition_count(), 10);

        Ok(())
    }
}
