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

//! FilterExec evaluates a boolean predicate against all input batches to determine which rows to
//! include in its output batches.

use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::physical_plan::{ExecutionPlan, Partitioning, PhysicalExpr};
use arrow::array::BooleanArray;
use arrow::compute::filter;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

/// FilterExec evaluates a boolean predicate against all input batches to determine which rows to
/// include in its output batches.
#[derive(Debug)]
pub struct FilterExec {
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
}

impl FilterExec {
    /// Create a FilterExec on an input
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        match predicate.data_type(input.schema().as_ref())? {
            DataType::Boolean => Ok(Self {
                predicate: predicate.clone(),
                input: input.clone(),
            }),
            other => Err(ExecutionError::General(format!(
                "Filter predicate must return boolean values, not {:?}",
                other
            ))),
        }
    }
}

impl ExecutionPlan for FilterExec {
    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        // The filter operator does not make any changes to the schema of its input
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(FilterExec::try_new(
                self.predicate.clone(),
                children[0].clone(),
            )?)),
            _ => Err(ExecutionError::General(
                "FilterExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        Ok(Arc::new(Mutex::new(FilterExecIter {
            schema: self.input.schema().clone(),
            predicate: self.predicate.clone(),
            input: self.input.execute(partition)?,
        })))
    }
}

/// The FilterExec iterator wraps the input iterator and applies the predicate expression to
/// determine which rows to include in its output batches
struct FilterExecIter {
    /// Output schema, which is the same as the input schema for this operator
    schema: SchemaRef,
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input partition to filter.
    input: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
}

impl RecordBatchReader for FilterExecIter {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the next batch
    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut input = self.input.lock().unwrap();
        match input.next_batch()? {
            Some(batch) => {
                // evaluate the filter predicate to get a boolean array indicating which rows
                // to include in the output
                let result = self
                    .predicate
                    .evaluate(&batch)
                    .map_err(ExecutionError::into_arrow_external_error)?;

                if let Some(f) = result.as_any().downcast_ref::<BooleanArray>() {
                    // filter each array
                    let mut filtered_arrays = Vec::with_capacity(batch.num_columns());
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
                        "Filter predicate evaluated to non-boolean value".to_string(),
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
    use crate::logical_plan::{Operator, ScalarValue};
    use crate::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::physical_plan::expressions::*;
    use crate::physical_plan::ExecutionPlan;
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
                col("c2"),
                Operator::Gt,
                lit(ScalarValue::UInt32(1)),
                &schema,
            )?,
            Operator::And,
            binary(
                col("c2"),
                Operator::Lt,
                lit(ScalarValue::UInt32(4)),
                &schema,
            )?,
            &schema,
        )?;

        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, Arc::new(csv))?);

        let results = test::execute(filter)?;

        results
            .iter()
            .for_each(|batch| assert_eq!(13, batch.num_columns()));
        let row_count: usize = results.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(41, row_count);

        Ok(())
    }
}
