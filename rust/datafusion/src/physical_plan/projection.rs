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

use std::any::Any;
use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::physical_plan::{ExecutionPlan, Partitioning, PhysicalExpr};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use async_trait::async_trait;

/// Execution plan for a projection
#[derive(Debug)]
pub struct ProjectionExec {
    /// The projection expressions stored as tuples of (expression, output column name)
    expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// The schema once the projection has been applied to the input
    schema: SchemaRef,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
}

impl ProjectionExec {
    /// Create a projection on an input
    pub fn try_new(
        expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let fields: Result<Vec<_>> = expr
            .iter()
            .map(|(e, name)| {
                Ok(Field::new(
                    name,
                    e.data_type(&input_schema)?,
                    e.nullable(&input_schema)?,
                ))
            })
            .collect();

        let schema = Arc::new(Schema::new(fields?));

        Ok(Self {
            expr,
            schema,
            input: input.clone(),
        })
    }
}

#[async_trait]
impl ExecutionPlan for ProjectionExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
            1 => Ok(Arc::new(ProjectionExec::try_new(
                self.expr.clone(),
                children[0].clone(),
            )?)),
            _ => Err(ExecutionError::General(
                "ProjectionExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        Ok(Arc::new(Mutex::new(ProjectionIterator {
            schema: self.schema.clone(),
            expr: self.expr.iter().map(|x| x.0.clone()).collect(),
            input: self.input.execute(partition).await?,
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
    use crate::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::physical_plan::expressions::col;
    use crate::test;

    #[tokio::test]
    async fn project_first_column() -> Result<()> {
        let schema = test::aggr_test_schema();

        let partitions = 4;
        let path = test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;

        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;

        // pick column c1 and name it column c1 in the output schema
        let projection =
            ProjectionExec::try_new(vec![(col("c1"), "c1".to_string())], Arc::new(csv))?;

        let mut partition_count = 0;
        let mut row_count = 0;
        for partition in 0..projection.output_partitioning().partition_count() {
            partition_count += 1;
            let iterator = projection.execute(partition).await?;
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
