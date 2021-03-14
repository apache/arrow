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

use super::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
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

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        // Sums all the output partitions
        let num_partitions = self
            .inputs
            .iter()
            .map(|plan| plan.output_partitioning().partition_count())
            .sum();
        // TODO: this loses partitioning info in case of same partitioning scheme
        Partitioning::UnknownPartitioning(num_partitions)
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
