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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::error::Result;
use crate::execution::physical_plan::{ExecutionPlan, Partition};
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

impl ExecutionPlan for MemoryExec {
    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the partitions for this execution plan. Each partition can be executed in parallel.
    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        let partitions = self
            .partitions
            .iter()
            .map(|vec| {
                Arc::new(MemoryPartition::new(
                    vec.clone(),
                    self.schema.clone(),
                    self.projection.clone(),
                )) as Arc<dyn Partition>
            })
            .collect();
        Ok(partitions)
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

/// Memory partition
#[derive(Debug)]
struct MemoryPartition {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Schema representing the data
    schema: SchemaRef,
    /// Optional projection
    projection: Option<Vec<usize>>,
}

impl MemoryPartition {
    /// Create a new in-memory partition
    fn new(
        data: Vec<RecordBatch>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            data,
            schema,
            projection,
        }
    }
}

#[async_trait]
impl Partition for MemoryPartition {
    /// Execute this partition and return an iterator over RecordBatch
    async fn execute(&self) -> Result<Arc<dyn RecordBatchReader + Send + Sync>> {
        Ok(Arc::new(MemoryIterator::try_new(
            self.data.clone(),
            self.schema.clone(),
            self.projection.clone(),
        )?))
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
    index: AtomicUsize,
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
            index: AtomicUsize::new(0),
        })
    }
}

impl RecordBatchReader for MemoryIterator {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the next RecordBatch
    fn next_batch(&self) -> ArrowResult<Option<RecordBatch>> {
        if self.index.load(Ordering::SeqCst) < self.data.len() {
            let index = self.index.fetch_add(1, Ordering::SeqCst);
            let batch = &self.data[index];
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
