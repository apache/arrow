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

//! Traits for physical query plan, supporting parallel execution for partitioned relations.

use std::sync::{Arc, Mutex};

use crate::error::Result;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

/// Partition-aware execution plan for a relation
pub trait ExecutionPlan {
    /// Get the schema for this execution plan
    fn schema(&self) -> Arc<Schema>;
    /// Get the partitions for this execution plan. Each partition can be executed in parallel.
    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>>;
}

/// Represents a partition of an execution plan that can be executed on a thread
pub trait Partition: Send + Sync {
    /// Execute this partition and return an iterator over RecordBatch
    fn execute(&self) -> Result<Arc<Mutex<dyn BatchIterator>>>;
}

/// Iterator over RecordBatch that can be sent between threads
pub trait BatchIterator: Send + Sync {
    /// Get the schema for the batches returned by this iterator
    fn schema(&self) -> Arc<Schema>;
    /// Get the next RecordBatch
    fn next(&mut self) -> Result<Option<RecordBatch>>;
}

/// Expression that can be evaluated against a RecordBatch
pub trait PhysicalExpr: Send + Sync {
    /// Get the name to use in a schema to represent the result of this expression
    fn name(&self) -> String;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Evaluate an expression against a RecordBatch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;
}

pub mod csv;
pub mod expressions;
pub mod projection;
