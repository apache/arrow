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

//! Defines the projection execution plan. A projection determines which columns or expressions
//! are returned from a query. The SQL statement `SELECT a, b, a+b FROM t1` is an example
//! of a projection on table `t1` where the expressions `a`, `b`, and `a+b` are the
//! projection expressions.

use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::{ExecutionPlan, Partition, PhysicalExpr};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

/// Execution plan for a projection
pub struct ProjectionExec {
    /// The projection expressions
    expr: Vec<Arc<dyn PhysicalExpr>>,
    /// The schema once the projection has been applied to the input
    schema: SchemaRef,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
}

impl ProjectionExec {
    /// Create a projection on an input
    pub fn try_new(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let fields: Result<Vec<_>> = expr
            .iter()
            .map(|e| e.to_schema_field(&input_schema))
            .collect();

        let schema = Arc::new(Schema::new(fields?));

        Ok(Self {
            expr: expr.clone(),
            schema,
            input: input.clone(),
        })
    }
}

impl ExecutionPlan for ProjectionExec {
    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the partitions for this execution plan
    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        let partitions: Vec<Arc<dyn Partition>> = self
            .input
            .partitions()?
            .iter()
            .map(|p| {
                let projection: Arc<dyn Partition> = Arc::new(ProjectionPartition {
                    schema: self.schema.clone(),
                    expr: self.expr.clone(),
                    input: p.clone() as Arc<dyn Partition>,
                });

                projection
            })
            .collect();

        Ok(partitions)
    }
}

/// Represents a single partition of a projection execution plan
struct ProjectionPartition {
    schema: SchemaRef,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    input: Arc<dyn Partition>,
}

impl Partition for ProjectionPartition {
    /// Execute the projection
    fn execute(&self) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        Ok(Arc::new(Mutex::new(ProjectionIterator {
            schema: self.schema.clone(),
            expr: self.expr.clone(),
            input: self.input.execute()?,
        })))
    }
}

/// Projection iterator
struct ProjectionIterator {
    schema: SchemaRef,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    input: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
}

impl RecordBatchReader for ProjectionIterator {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the next batch
    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut input = self.input.lock().unwrap();
        match input.next_batch()? {
            Some(batch) => {
                let arrays: Result<Vec<_>> =
                    self.expr.iter().map(|expr| expr.evaluate(&batch)).collect();
                Ok(Some(RecordBatch::try_new(
                    self.schema.clone(),
                    arrays.map_err(ExecutionError::into_arrow_external_error)?,
                )?))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::execution::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::execution::physical_plan::expressions::Column;
    use crate::test;

    #[test]
    fn project_first_column() -> Result<()> {
        let schema = test::aggr_test_schema();

        let partitions = 4;
        let path = test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;

        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;

        let projection = ProjectionExec::try_new(
            vec![Arc::new(Column::new(0, &schema.as_ref().field(0).name()))],
            Arc::new(csv),
        )?;

        assert_eq!("c1", projection.schema.field(0).name().as_str());

        let mut partition_count = 0;
        let mut row_count = 0;
        for partition in projection.partitions()? {
            partition_count += 1;
            let iterator = partition.execute()?;
            let mut iterator = iterator.lock().unwrap();
            while let Some(batch) = iterator.next_batch()? {
                assert_eq!(1, batch.num_columns());
                row_count += batch.num_rows();
            }
        }
        assert_eq!(partitions, partition_count);
        assert_eq!(100, row_count);

        Ok(())
    }
}
