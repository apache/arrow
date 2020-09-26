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

use std::any::Any;
use std::cell::RefCell;
use std::fmt::{Debug, Display};
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crate::execution::context::ExecutionContextState;
use crate::logical_plan::LogicalPlan;
use crate::{error::Result, scalar::ScalarValue};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::{array::ArrayRef, datatypes::Field};

use async_trait::async_trait;

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
#[async_trait]
pub trait ExecutionPlan: Debug + Send + Sync {
    /// Returns the execution plan as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef;
    /// Specifies the output partitioning scheme of this plan
    fn output_partitioning(&self) -> Partitioning;
    /// Specifies the data distribution requirements of all the children for this operator
    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }
    /// Get a list of child execution plans that provide the input for this plan. The returned list
    /// will be empty for leaf nodes, will contain a single value for unary nodes, or two
    /// values for binary nodes (such as joins).
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>>;
    /// Returns a new plan where all children were replaced by new plans.
    /// The size of `children` must be equal to the size of `ExecutionPlan::children()`.
    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
    /// Execute one partition and return an iterator over RecordBatch
    async fn execute(
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

/// Distribution schemes
#[derive(Debug, Clone)]
pub enum Distribution {
    /// Unspecified distribution
    UnspecifiedDistribution,
    /// A single partition is required
    SinglePartition,
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

/// An aggregate expression that:
/// * knows its resulting field
/// * knows how to create its accumulator
/// * knows its accumulator's state's field
/// * knows the expressions from whose its accumulator will receive values
pub trait AggregateExpr: Send + Sync + Debug {
    /// the field of the final result of this aggregation.
    fn field(&self) -> Result<Field>;

    /// the accumulator used to accumulate values from the expressions.
    /// the accumulator expects the same number of arguments as `expressions` and must
    /// return states with the same description as `state_fields`
    fn create_accumulator(&self) -> Result<Rc<RefCell<dyn Accumulator>>>;

    /// the fields that encapsulate the Accumulator's state
    /// the number of fields here equals the number of states that the accumulator contains
    fn state_fields(&self) -> Result<Vec<Field>>;

    /// expressions that are passed to the Accumulator.
    /// Single-column aggregations such as `sum` return a single value, others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;
}

/// An accumulator represents a stateful object that lives throughout the evaluation of multiple rows and
/// generically accumulates values. An accumulator knows how to:
/// * update its state from inputs via `update`
/// * convert its internal state to a vector of scalar values
/// * update its state from multiple accumulators' states via `merge`
/// * compute the final value from its internal state via `evaluate`
pub trait Accumulator: Send + Sync + Debug {
    /// Returns the state of the accumulator at the end of the accumulation.
    // in the case of an average on which we track `sum` and `n`, this function should return a vector
    // of two values, sum and n.
    fn state(&self) -> Result<Vec<ScalarValue>>;

    /// updates the accumulator's state from a vector of scalars.
    fn update(&mut self, values: &Vec<ScalarValue>) -> Result<()>;

    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(&mut self, values: &Vec<ArrayRef>) -> Result<()> {
        if values.len() == 0 {
            return Ok(());
        };
        (0..values[0].len())
            .map(|index| {
                let v = values
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, index))
                    .collect::<Result<Vec<_>>>()?;
                self.update(&v)
            })
            .collect::<Result<_>>()
    }

    /// updates the accumulator's state from a vector of scalars.
    fn merge(&mut self, states: &Vec<ScalarValue>) -> Result<()>;

    /// updates the accumulator's state from a vector of states.
    fn merge_batch(&mut self, states: &Vec<ArrayRef>) -> Result<()> {
        if states.len() == 0 {
            return Ok(());
        };
        (0..states[0].len())
            .map(|index| {
                let v = states
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, index))
                    .collect::<Result<Vec<_>>>()?;
                self.merge(&v)
            })
            .collect::<Result<_>>()
    }

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<ScalarValue>;
}

pub mod aggregates;
pub mod array_expressions;
pub mod common;
pub mod csv;
pub mod datetime_expressions;
pub mod empty;
pub mod explain;
pub mod expressions;
pub mod filter;
pub mod functions;
pub mod hash_aggregate;
pub mod limit;
pub mod math_expressions;
pub mod memory;
pub mod merge;
pub mod parquet;
pub mod planner;
pub mod projection;
pub mod sort;
pub mod string_expressions;
pub mod type_coercion;
pub mod udaf;
pub mod udf;
