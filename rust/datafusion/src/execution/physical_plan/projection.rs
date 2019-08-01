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

use std::sync::Arc;

use crate::error::Result;
use crate::execution::physical_plan::{
    BatchIterator, ExecutionPlan, Partition, PhysicalExpr,
};
use arrow::datatypes::Schema;

/// Execution plan for a projection
pub struct ProjectionExec {
    /// The projection expressions
    expr: Vec<Arc<dyn PhysicalExpr>>,
    /// The schema once the projection has been applied to the input
    schema: Arc<Schema>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
}

impl ExecutionPlan for ProjectionExec {
    /// Get the schema for this execution plan
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Get the partitions for this execution plan
    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        let partitions: Vec<Arc<dyn Partition>> = self
            .input
            .partitions()?
            .iter()
            .map(|p| {
                let expr = self.expr.clone();
                let projection: Arc<Partition> = Arc::new(ProjectionPartition {
                    expr,
                    input: p.clone() as Arc<Partition>,
                });

                projection
            })
            .collect();

        Ok(partitions)
    }
}

/// Represents a single partition of a projection execution plan
struct ProjectionPartition {
    expr: Vec<Arc<dyn PhysicalExpr>>,
    input: Arc<dyn Partition>,
}

impl Partition for ProjectionPartition {
    /// Execute the projection
    fn execute(&self) -> Result<Arc<dyn BatchIterator>> {
        // execute the input partition and get an iterator
        let it = self.input.execute()?;
        //TODO wrap the iterator in a new one that performs the projection by evaluating the
        // expressions against the batches
        Ok(it)
    }
}
