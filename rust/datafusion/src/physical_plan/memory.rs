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

//! Execution plan for reading in-memory batches of data

use std::any::Any;
use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::physical_plan::{ExecutionPlan, Partitioning};
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use async_trait::async_trait;

/// Execution plan for reading in-memory batches of data
#[derive(Debug)]
pub struct MemoryExec {
    /// The partitions to query
    partitions: Vec<Vec<RecordBatch>>,
    /// Schema representing the data after the optional projection is applied
    schema: SchemaRef,
    /// Optional projection
    projection: Option<Vec<usize>>,
}

#[async_trait]
impl ExecutionPlan for MemoryExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn with_new_children(
        &self,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(ExecutionError::General(format!(
            "Children cannot be replaced in {:?}",
            self
        )))
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        Ok(Arc::new(Mutex::new(MemoryIterator::try_new(
            self.partitions[partition].clone(),
            self.schema.clone(),
            self.projection.clone(),
        )?)))
    }
}

impl MemoryExec {
    /// Create a new execution plan for reading in-memory record batches
    pub fn try_new(
        partitions: &Vec<Vec<RecordBatch>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            partitions: partitions.clone(),
            schema,
            projection,
        })
    }
}

/// Iterator over batches
pub(crate) struct MemoryIterator {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Schema representing the data
    schema: SchemaRef,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Index into the data
    index: usize,
}

impl MemoryIterator {
    /// Create an iterator for a vector of record batches
    pub fn try_new(
        data: Vec<RecordBatch>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            data: data.clone(),
            schema: schema.clone(),
            projection,
            index: 0,
        })
    }
}

impl RecordBatchReader for MemoryIterator {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the next RecordBatch
    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        if self.index < self.data.len() {
            self.index += 1;
            let batch = &self.data[self.index - 1];
            // apply projection
            match &self.projection {
                Some(columns) => Ok(Some(RecordBatch::try_new(
                    self.schema.clone(),
                    columns.iter().map(|i| batch.column(*i).clone()).collect(),
                )?)),
                None => Ok(Some(batch.clone())),
            }
        } else {
            Ok(None)
        }
    }
}
