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

//! Defines the selection execution plan. A selection filters rows based on a predicate

use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::{ExecutionPlan, Partition, PhysicalExpr};
use arrow::array::BooleanArray;
use arrow::compute::filter;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

/// Execution plan for a Selection
pub struct SelectionExec {
    /// The selection predicate expression
    expr: Arc<dyn PhysicalExpr>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
}

impl SelectionExec {
    /// Create a selection on an input
    pub fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        Ok(Self {
            expr: expr.clone(),
            input: input.clone(),
        })
    }
}

impl ExecutionPlan for SelectionExec {
    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        // The selection operator does not make any changes to the schema of its input
        self.input.schema()
    }

    /// Get the partitions for this execution plan
    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        let partitions: Vec<Arc<dyn Partition>> = self
            .input
            .partitions()?
            .iter()
            .map(|p| {
                let expr = self.expr.clone();
                let partition: Arc<dyn Partition> = Arc::new(SelectionPartition {
                    schema: self.input.schema(),
                    expr,
                    input: p.clone() as Arc<dyn Partition>,
                });

                partition
            })
            .collect();

        Ok(partitions)
    }
}

/// Represents a single partition of a Selection execution plan
struct SelectionPartition {
    schema: SchemaRef,
    expr: Arc<dyn PhysicalExpr>,
    input: Arc<dyn Partition>,
}

impl Partition for SelectionPartition {
    /// Execute the Selection
    fn execute(&self) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        Ok(Arc::new(Mutex::new(SelectionIterator {
            schema: self.schema.clone(),
            expr: self.expr.clone(),
            input: self.input.execute()?,
        })))
    }
}

/// Selection iterator
struct SelectionIterator {
    schema: SchemaRef,
    expr: Arc<dyn PhysicalExpr>,
    input: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
}

impl RecordBatchReader for SelectionIterator {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the next batch
    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut input = self.input.lock().unwrap();
        match input.next_batch()? {
            Some(batch) => {
                // evaluate the selection predicate to get a boolean array
                let predicate_result = self
                    .expr
                    .evaluate(&batch)
                    .map_err(ExecutionError::into_arrow_external_error)?;

                if let Some(f) = predicate_result.as_any().downcast_ref::<BooleanArray>()
                {
                    // filter each array
                    let mut filtered_arrays = vec![];
                    for i in 0..batch.num_columns() {
                        let array = batch.column(i);
                        let filtered_array = filter(array.as_ref(), f)?;
                        filtered_arrays.push(filtered_array);
                    }
                    Ok(Some(RecordBatch::try_new(
                        batch.schema().clone(),
                        filtered_arrays,
                    )?))
                } else {
                    Err(ExecutionError::InternalError(
                        "Predicate evaluated to non-boolean value".to_string(),
                    )
                    .into_arrow_external_error())
                }
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::execution::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::execution::physical_plan::expressions::*;
    use crate::execution::physical_plan::ExecutionPlan;
    use crate::logicalplan::{Operator, ScalarValue};
    use crate::test;
    use std::iter::Iterator;

    #[test]
    fn simple_predicate() -> Result<()> {
        let schema = test::aggr_test_schema();

        let partitions = 4;
        let path = test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;

        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;

        let predicate: Arc<dyn PhysicalExpr> = binary(
            binary(
                col(1, schema.as_ref()),
                Operator::Gt,
                lit(ScalarValue::UInt32(1)),
            ),
            Operator::And,
            binary(
                col(1, schema.as_ref()),
                Operator::Lt,
                lit(ScalarValue::UInt32(4)),
            ),
        );

        let selection: Arc<dyn ExecutionPlan> =
            Arc::new(SelectionExec::try_new(predicate, Arc::new(csv))?);

        let results = test::execute(selection.as_ref())?;

        results
            .iter()
            .for_each(|batch| assert_eq!(13, batch.num_columns()));
        let row_count: usize = results.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(41, row_count);

        Ok(())
    }
}
