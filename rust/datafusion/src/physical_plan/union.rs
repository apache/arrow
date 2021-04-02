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

// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! The Union operator combines multiple inputs with the same schema

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;

use super::{ExecutionPlan, Partitioning, PhysicalExpr, SendableRecordBatchStream};
use crate::error::Result;
use async_trait::async_trait;

/// UNION ALL execution plan
#[derive(Debug)]
pub struct UnionExec {
    /// Input execution plan
    inputs: Vec<Arc<dyn ExecutionPlan>>,
}

impl UnionExec {
    /// Create a new UnionExec
    pub fn new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        UnionExec { inputs }
    }
}

#[async_trait]
impl ExecutionPlan for UnionExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inputs[0].schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.inputs.clone()
    }

    /// Output of the union is the combination of all output partitions of the inputs
    fn output_partitioning(&self) -> Partitioning {
        let intial: std::result::Result<(Vec<Arc<dyn PhysicalExpr>>, usize), usize> =
            Ok((vec![], 0));
        let input = self.inputs.iter().fold(intial, |acc, plan| {
            match (acc, plan.output_partitioning()) {
                (Ok((mut v, s)), Partitioning::Hash(vp, sp)) => {
                    v.append(&mut vp.clone());
                    Ok((v, s + sp))
                }
                (Ok((_, s)), p) | (Err(s), p) => Err(s + p.partition_count()),
            }
        });
        match input {
            Ok((v, s)) => Partitioning::Hash(v, s),
            Err(s) => Partitioning::UnknownPartitioning(s),
        }
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UnionExec::new(children)))
    }

    async fn execute(&self, mut partition: usize) -> Result<SendableRecordBatchStream> {
        // find partition to execute
        for input in self.inputs.iter() {
            // Calculate whether partition belongs to the current partition
            if partition < input.output_partitioning().partition_count() {
                return input.execute(partition).await;
            } else {
                partition -= input.output_partitioning().partition_count();
            }
        }

        Err(crate::error::DataFusionError::Execution(format!(
            "Partition {} not found in Union",
            partition
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::expressions::Column;
    use crate::physical_plan::{
        collect,
        csv::{CsvExec, CsvReadOptions},
        repartition::RepartitionExec,
    };
    use crate::test;
    use arrow::record_batch::RecordBatch;

    fn get_csv_exec() -> Result<(CsvExec, CsvExec)> {
        let schema = test::aggr_test_schema();
        // Create csv's with different partitioning
        let path = test::create_partitioned_csv("aggregate_test_100.csv", 4)?;
        let path2 = test::create_partitioned_csv("aggregate_test_100.csv", 5)?;

        let csv = CsvExec::try_new(
            &path,
            CsvReadOptions::new().schema(&schema),
            None,
            1024,
            None,
        )?;

        let csv2 = CsvExec::try_new(
            &path2,
            CsvReadOptions::new().schema(&schema),
            None,
            1024,
            None,
        )?;
        Ok((csv, csv2))
    }

    #[tokio::test]
    async fn test_union_partitions() -> Result<()> {
        let (csv, csv2) = get_csv_exec()?;

        let union_exec = Arc::new(UnionExec::new(vec![Arc::new(csv), Arc::new(csv2)]));

        // Should have 9 partitions and 9 output batches
        assert!(matches!(
            union_exec.output_partitioning(),
            Partitioning::UnknownPartitioning(9)
        ));

        let result: Vec<RecordBatch> = collect(union_exec).await?;
        assert_eq!(result.len(), 9);

        Ok(())
    }

    #[tokio::test]
    async fn test_union_partitions_hash() -> Result<()> {
        let (csv, csv2) = get_csv_exec()?;
        let repartition = RepartitionExec::try_new(
            Arc::new(csv),
            Partitioning::Hash(vec![Arc::new(Column::new("c1"))], 5),
        )?;
        let repartition2 = RepartitionExec::try_new(
            Arc::new(csv2),
            Partitioning::Hash(vec![Arc::new(Column::new("c2"))], 5),
        )?;

        let union_exec = Arc::new(UnionExec::new(vec![
            Arc::new(repartition),
            Arc::new(repartition2),
        ]));

        // should be hash, have 10 partitions and 45 output batches
        assert!(matches!(
            union_exec.output_partitioning(),
            Partitioning::Hash(_, 10)
        ));

        let result: Vec<RecordBatch> = collect(union_exec).await?;
        assert_eq!(result.len(), 45);

        Ok(())
    }
}
