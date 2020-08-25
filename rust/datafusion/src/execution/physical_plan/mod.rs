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
use std::fmt::{Debug, Display};
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::execution::context::ExecutionContextState;
use crate::logicalplan::{LogicalPlan, ScalarValue};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::{
    compute::kernels::length::length,
    record_batch::{RecordBatch, RecordBatchReader},
};
use udf::ScalarFunction;

/// Physical query planner that converts a `LogicalPlan` to an
/// `ExecutionPlan` suitable for execution.
pub trait PhysicalPlanner {
    /// Create a physical plan from a logical plan
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Partition-aware execution plan for a relation
pub trait ExecutionPlan: Debug + Send + Sync {
    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef;
    /// Specifies the output partitioning scheme of this plan
    fn output_partitioning(&self) -> Partitioning;
    /// Execute one partition and return an iterator over RecordBatch
    fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>>;
}

/// Partitioning schemes supported by operators.
#[derive(Debug, Clone)]
pub enum Partitioning {
    /// Unknown partitioning scheme
    UnknownPartitioning(usize),
}

impl Partitioning {
    /// Returns the number of partitions in this partitioning scheme
    pub fn partition_count(&self) -> usize {
        use Partitioning::*;
        match self {
            UnknownPartitioning(n) => *n,
        }
    }
}

/// Expression that can be evaluated against a RecordBatch
/// A Physical expression knows its type, nullability and how to evaluate itself.
pub trait PhysicalExpr: Send + Sync + Display + Debug {
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Determine whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    /// Evaluate an expression against a RecordBatch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;
}

/// Aggregate expression that can be evaluated against a RecordBatch
pub trait AggregateExpr: Send + Sync + Debug {
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Determine whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    /// Evaluate the expression being aggregated
    fn evaluate_input(&self, batch: &RecordBatch) -> Result<ArrayRef>;
    /// Create an accumulator for this aggregate expression
    fn create_accumulator(&self) -> Rc<RefCell<dyn Accumulator>>;
    /// Create an aggregate expression for combining the results of accumulators from partitions.
    /// For example, to combine the results of a parallel SUM we just need to do another SUM, but
    /// to combine the results of parallel COUNT we would also use SUM.
    fn create_reducer(&self, column_name: &str) -> Arc<dyn AggregateExpr>;
}

/// Aggregate accumulator
pub trait Accumulator: Debug {
    /// Update the accumulator based on a row in a batch
    fn accumulate_scalar(&mut self, value: Option<ScalarValue>) -> Result<()>;
    /// Update the accumulator based on an array in a batch
    fn accumulate_batch(&mut self, array: &ArrayRef) -> Result<()>;
    /// Get the final value for the accumulator
    fn get_value(&self) -> Result<Option<ScalarValue>>;
}

/// Vector of scalar functions declared in this module
pub fn scalar_functions() -> Vec<ScalarFunction> {
    let mut udfs = vec![ScalarFunction::new(
        "length",
        vec![Field::new("n", DataType::Utf8, true)],
        DataType::UInt32,
        Arc::new(|args: &[ArrayRef]| Ok(Arc::new(length(args[0].as_ref())?))),
    )];
    udfs.append(&mut math_expressions::scalar_functions());
    udfs
}

pub mod common;
pub mod csv;
pub mod explain;
pub mod expressions;
pub mod filter;
pub mod hash_aggregate;
pub mod limit;
pub mod math_expressions;
pub mod memory;
pub mod merge;
pub mod parquet;
pub mod planner;
pub mod projection;
pub mod sort;
pub mod udf;
