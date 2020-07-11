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

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::logicalplan::ScalarValue;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchReader};

/// Partition-aware execution plan for a relation
pub trait ExecutionPlan {
    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef;
    /// Get the partitions for this execution plan. Each partition can be executed in parallel.
    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>>;
}

/// Represents a partition of an execution plan that can be executed on a thread
pub trait Partition: Send + Sync {
    /// Execute this partition and return an iterator over RecordBatch
    fn execute(&self) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>>;
}

/// Expression that can be evaluated against a RecordBatch
pub trait PhysicalExpr: Send + Sync {
    /// Get the name to use in a schema to represent the result of this expression
    fn name(&self) -> String;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Decide whehter this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    /// Evaluate an expression against a RecordBatch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;
    /// Generate schema Field type for this expression
    fn to_schema_field(&self, input_schema: &Schema) -> Result<Field> {
        Ok(Field::new(
            &self.name(),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        ))
    }
}

/// Aggregate expression that can be evaluated against a RecordBatch
pub trait AggregateExpr: Send + Sync {
    /// Get the name to use in a schema to represent the result of this expression
    fn name(&self) -> String;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Evaluate the expression being aggregated
    fn evaluate_input(&self, batch: &RecordBatch) -> Result<ArrayRef>;
    /// Create an accumulator for this aggregate expression
    fn create_accumulator(&self) -> Rc<RefCell<dyn Accumulator>>;
    /// Create an aggregate expression for combining the results of accumulators from partitions.
    /// For example, to combine the results of a parallel SUM we just need to do another SUM, but
    /// to combine the results of parallel COUNT we would also use SUM.
    fn create_reducer(&self, column_index: usize) -> Arc<dyn AggregateExpr>;
}

/// Aggregate accumulator
pub trait Accumulator {
    /// Update the accumulator based on a row in a batch
    fn accumulate_scalar(&mut self, value: Option<ScalarValue>) -> Result<()>;
    /// Update the accumulator based on an array in a batch
    fn accumulate_batch(&mut self, array: &ArrayRef) -> Result<()>;
    /// Get the final value for the accumulator
    fn get_value(&self) -> Result<Option<ScalarValue>>;
}

pub mod common;
pub mod csv;
pub mod datasource;
pub mod expressions;
pub mod hash_aggregate;
pub mod limit;
pub mod math_expressions;
pub mod memory;
pub mod merge;
pub mod parquet;
pub mod projection;
pub mod selection;
pub mod sort;
pub mod udf;
