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

//! Defines the SORT plan

use std::sync::{Arc, Mutex};

use arrow::array::ArrayRef;
pub use arrow::compute::SortOptions;
use arrow::compute::{concat, lexsort_to_indices, take, SortColumn, TakeOptions};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::common::RecordBatchIterator;
use crate::execution::physical_plan::expressions::PhysicalSortExpr;
use crate::execution::physical_plan::{
    common, Distribution, ExecutionPlan, Partitioning,
};

/// Sort execution plan
#[derive(Debug)]
pub struct SortExec {
    /// Input schema
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    /// Number of threads to execute input partitions on before combining into a single partition
    concurrency: usize,
}

impl SortExec {
    /// Create a new sort execution plan
    pub fn try_new(
        expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        concurrency: usize,
    ) -> Result<Self> {
        Ok(Self {
            expr,
            input,
            concurrency,
        })
    }
}

impl ExecutionPlan for SortExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema().clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::SinglePartition
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(SortExec::try_new(
                self.expr.clone(),
                children[0].clone(),
                self.concurrency,
            )?)),
            _ => Err(ExecutionError::General(
                "SortExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        if 0 != partition {
            return Err(ExecutionError::General(format!(
                "SortExec invalid partition {}",
                partition
            )));
        }

        // sort needs to operate on a single partition currently
        if 1 != self.input.output_partitioning().partition_count() {
            return Err(ExecutionError::General(
                "SortExec requires a single input partition".to_owned(),
            ));
        }
        let it = self.input.execute(0)?;
        let batches = common::collect(it)?;

        // combine all record batches into one for each column
        let combined_batch = RecordBatch::try_new(
            self.schema(),
            self.schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _)| -> Result<ArrayRef> {
                    Ok(concat(
                        &batches
                            .iter()
                            .map(|batch| batch.columns()[i].clone())
                            .collect::<Vec<ArrayRef>>(),
                    )?)
                })
                .collect::<Result<Vec<ArrayRef>>>()?,
        )?;

        // sort combined record batch
        let indices = lexsort_to_indices(
            &self
                .expr
                .iter()
                .map(|e| e.evaluate_to_sort_column(&combined_batch))
                .collect::<Result<Vec<SortColumn>>>()?,
        )?;

        // reorder all rows based on sorted indices
        let sorted_batch = RecordBatch::try_new(
            self.schema(),
            combined_batch
                .columns()
                .iter()
                .map(|column| -> Result<ArrayRef> {
                    Ok(take(
                        column,
                        &indices,
                        // disable bound check overhead since indices are already generated from
                        // the same record batch
                        Some(TakeOptions {
                            check_bounds: false,
                        }),
                    )?)
                })
                .collect::<Result<Vec<ArrayRef>>>()?,
        )?;

        Ok(Arc::new(Mutex::new(RecordBatchIterator::new(
            self.schema(),
            vec![Arc::new(sorted_batch)],
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::execution::physical_plan::expressions::col;
    use crate::execution::physical_plan::merge::MergeExec;
    use crate::test;
    use arrow::array::*;
    use arrow::datatypes::*;

    #[test]
    fn test_sort() -> Result<()> {
        let schema = test::aggr_test_schema();
        let partitions = 4;
        let path = test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;
        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;

        let sort_exec = Arc::new(SortExec::try_new(
            vec![
                // c1 string column
                PhysicalSortExpr {
                    expr: col("c1"),
                    options: SortOptions::default(),
                },
                // c2 uin32 column
                PhysicalSortExpr {
                    expr: col("c2"),
                    options: SortOptions::default(),
                },
                // c7 uin8 column
                PhysicalSortExpr {
                    expr: col("c7"),
                    options: SortOptions::default(),
                },
            ],
            Arc::new(MergeExec::new(Arc::new(csv), 2)),
            2,
        )?);

        let result: Vec<RecordBatch> = test::execute(sort_exec)?;
        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        let c1 = as_string_array(&columns[0]);
        assert_eq!(c1.value(0), "a");
        assert_eq!(c1.value(c1.len() - 1), "e");

        let c2 = as_primitive_array::<UInt32Type>(&columns[1]);
        assert_eq!(c2.value(0), 1);
        assert_eq!(c2.value(c2.len() - 1), 5,);

        let c7 = as_primitive_array::<UInt8Type>(&columns[6]);
        assert_eq!(c7.value(0), 15);
        assert_eq!(c7.value(c7.len() - 1), 254,);

        Ok(())
    }
}
