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

//! Execution of a simple aggregate relation containing MIN, MAX, COUNT, SUM aggregate
//! functions with optional GROUP BY columns

use std::cell::RefCell;
use std::rc::Rc;
use std::str;
use std::sync::Arc;

use arrow::array::*;
use arrow::builder::*;
use arrow::compute;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::{ExecutionError, Result};
use crate::execution::expression::AggregateType;
use crate::execution::relation::Relation;
use crate::logicalplan::ScalarValue;

use crate::execution::expression::CompiledAggregateExpression;
use crate::execution::expression::CompiledExpr;
use fnv::FnvHashMap;

/// An aggregate relation is made up of zero or more grouping expressions and one
/// or more aggregate expressions
pub(super) struct AggregateRelation {
    schema: Arc<Schema>,
    input: Rc<RefCell<Relation>>,
    group_expr: Vec<CompiledExpr>,
    aggr_expr: Vec<CompiledAggregateExpression>,
    end_of_results: bool,
}

impl AggregateRelation {
    pub fn new(
        schema: Arc<Schema>,
        input: Rc<RefCell<Relation>>,
        group_expr: Vec<CompiledExpr>,
        aggr_expr: Vec<CompiledAggregateExpression>,
    ) -> Self {
        AggregateRelation {
            schema,
            input,
            group_expr,
            aggr_expr,
            end_of_results: false,
        }
    }
}

/// Enumeration of types that can be used in a GROUP BY expression (all primitives except
/// for floating point numerics)
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum GroupByScalar {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Utf8(String),
}

/// Common trait for all aggregation functions
trait AggregateFunction {
    /// Get the function name (used for debugging)
    fn name(&self) -> &str;
    fn accumulate_scalar(&mut self, value: &Option<ScalarValue>) -> Result<()>;
    fn result(&self) -> &Option<ScalarValue>;
    fn data_type(&self) -> &DataType;
}

/// Implemntation of MIN aggregate function
#[derive(Debug)]
struct MinFunction {
    data_type: DataType,
    value: Option<ScalarValue>,
}

impl MinFunction {
    fn new(data_type: &DataType) -> Self {
        Self {
            data_type: data_type.clone(),
            value: None,
        }
    }
}

impl AggregateFunction for MinFunction {
    fn name(&self) -> &str {
        "min"
    }

    fn accumulate_scalar(&mut self, value: &Option<ScalarValue>) -> Result<()> {
        if self.value.is_none() {
            self.value = value.clone();
        } else if value.is_some() {
            self.value = match (&self.value, value) {
                (Some(ScalarValue::UInt8(a)), Some(ScalarValue::UInt8(b))) => {
                    Some(ScalarValue::UInt8(*a.min(b)))
                }
                (Some(ScalarValue::UInt16(a)), Some(ScalarValue::UInt16(b))) => {
                    Some(ScalarValue::UInt16(*a.min(b)))
                }
                (Some(ScalarValue::UInt32(a)), Some(ScalarValue::UInt32(b))) => {
                    Some(ScalarValue::UInt32(*a.min(b)))
                }
                (Some(ScalarValue::UInt64(a)), Some(ScalarValue::UInt64(b))) => {
                    Some(ScalarValue::UInt64(*a.min(b)))
                }
                (Some(ScalarValue::Int8(a)), Some(ScalarValue::Int8(b))) => {
                    Some(ScalarValue::Int8(*a.min(b)))
                }
                (Some(ScalarValue::Int16(a)), Some(ScalarValue::Int16(b))) => {
                    Some(ScalarValue::Int16(*a.min(b)))
                }
                (Some(ScalarValue::Int32(a)), Some(ScalarValue::Int32(b))) => {
                    Some(ScalarValue::Int32(*a.min(b)))
                }
                (Some(ScalarValue::Int64(a)), Some(ScalarValue::Int64(b))) => {
                    Some(ScalarValue::Int64(*a.min(b)))
                }
                (Some(ScalarValue::Float32(a)), Some(ScalarValue::Float32(b))) => {
                    Some(ScalarValue::Float32(a.min(*b)))
                }
                (Some(ScalarValue::Float64(a)), Some(ScalarValue::Float64(b))) => {
                    Some(ScalarValue::Float64(a.min(*b)))
                }
                _ => {
                    return Err(ExecutionError::ExecutionError(
                        "unsupported data type for MIN".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn result(&self) -> &Option<ScalarValue> {
        &self.value
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

/// Implemntation of MAX aggregate function
#[derive(Debug)]
struct MaxFunction {
    data_type: DataType,
    value: Option<ScalarValue>,
}

impl MaxFunction {
    fn new(data_type: &DataType) -> Self {
        Self {
            data_type: data_type.clone(),
            value: None,
        }
    }
}

impl AggregateFunction for MaxFunction {
    fn name(&self) -> &str {
        "max"
    }

    fn accumulate_scalar(&mut self, value: &Option<ScalarValue>) -> Result<()> {
        if self.value.is_none() {
            self.value = value.clone();
        } else if value.is_some() {
            self.value = match (&self.value, value) {
                (Some(ScalarValue::UInt8(a)), Some(ScalarValue::UInt8(b))) => {
                    Some(ScalarValue::UInt8(*a.max(b)))
                }
                (Some(ScalarValue::UInt16(a)), Some(ScalarValue::UInt16(b))) => {
                    Some(ScalarValue::UInt16(*a.max(b)))
                }
                (Some(ScalarValue::UInt32(a)), Some(ScalarValue::UInt32(b))) => {
                    Some(ScalarValue::UInt32(*a.max(b)))
                }
                (Some(ScalarValue::UInt64(a)), Some(ScalarValue::UInt64(b))) => {
                    Some(ScalarValue::UInt64(*a.max(b)))
                }
                (Some(ScalarValue::Int8(a)), Some(ScalarValue::Int8(b))) => {
                    Some(ScalarValue::Int8(*a.max(b)))
                }
                (Some(ScalarValue::Int16(a)), Some(ScalarValue::Int16(b))) => {
                    Some(ScalarValue::Int16(*a.max(b)))
                }
                (Some(ScalarValue::Int32(a)), Some(ScalarValue::Int32(b))) => {
                    Some(ScalarValue::Int32(*a.max(b)))
                }
                (Some(ScalarValue::Int64(a)), Some(ScalarValue::Int64(b))) => {
                    Some(ScalarValue::Int64(*a.max(b)))
                }
                (Some(ScalarValue::Float32(a)), Some(ScalarValue::Float32(b))) => {
                    Some(ScalarValue::Float32(a.max(*b)))
                }
                (Some(ScalarValue::Float64(a)), Some(ScalarValue::Float64(b))) => {
                    Some(ScalarValue::Float64(a.max(*b)))
                }
                _ => {
                    return Err(ExecutionError::ExecutionError(
                        "unsupported data type for MAX".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn result(&self) -> &Option<ScalarValue> {
        &self.value
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

/// Implemntation of SUM aggregate function
#[derive(Debug)]
struct SumFunction {
    data_type: DataType,
    value: Option<ScalarValue>,
}

impl SumFunction {
    fn new(data_type: &DataType) -> Self {
        Self {
            data_type: data_type.clone(),
            value: None,
        }
    }
}

impl AggregateFunction for SumFunction {
    fn name(&self) -> &str {
        "sum"
    }

    fn accumulate_scalar(&mut self, value: &Option<ScalarValue>) -> Result<()> {
        if self.value.is_none() {
            self.value = value.clone();
        } else if value.is_some() {
            self.value = match (&self.value, value) {
                (Some(ScalarValue::UInt8(a)), Some(ScalarValue::UInt8(b))) => {
                    Some(ScalarValue::UInt8(*a + b))
                }
                (Some(ScalarValue::UInt16(a)), Some(ScalarValue::UInt16(b))) => {
                    Some(ScalarValue::UInt16(*a + b))
                }
                (Some(ScalarValue::UInt32(a)), Some(ScalarValue::UInt32(b))) => {
                    Some(ScalarValue::UInt32(*a + b))
                }
                (Some(ScalarValue::UInt64(a)), Some(ScalarValue::UInt64(b))) => {
                    Some(ScalarValue::UInt64(*a + b))
                }
                (Some(ScalarValue::Int8(a)), Some(ScalarValue::Int8(b))) => {
                    Some(ScalarValue::Int8(*a + b))
                }
                (Some(ScalarValue::Int16(a)), Some(ScalarValue::Int16(b))) => {
                    Some(ScalarValue::Int16(*a + b))
                }
                (Some(ScalarValue::Int32(a)), Some(ScalarValue::Int32(b))) => {
                    Some(ScalarValue::Int32(*a + b))
                }
                (Some(ScalarValue::Int64(a)), Some(ScalarValue::Int64(b))) => {
                    Some(ScalarValue::Int64(*a + b))
                }
                (Some(ScalarValue::Float32(a)), Some(ScalarValue::Float32(b))) => {
                    Some(ScalarValue::Float32(a + *b))
                }
                (Some(ScalarValue::Float64(a)), Some(ScalarValue::Float64(b))) => {
                    Some(ScalarValue::Float64(a + *b))
                }
                _ => {
                    return Err(ExecutionError::ExecutionError(
                        "unsupported data type for SUM".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn result(&self) -> &Option<ScalarValue> {
        &self.value
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

struct AccumulatorSet {
    aggr_values: Vec<Rc<RefCell<AggregateFunction>>>,
}

impl AccumulatorSet {
    fn accumulate_scalar(&mut self, i: usize, value: Option<ScalarValue>) -> Result<()> {
        let mut accumulator = self.aggr_values[i].borrow_mut();
        accumulator.accumulate_scalar(&value)
    }

    fn values(&self) -> Vec<Option<ScalarValue>> {
        self.aggr_values
            .iter()
            .map(|x| x.borrow().result().clone())
            .collect()
    }
}

#[derive(Debug)]
struct MapEntry {
    k: Vec<GroupByScalar>,
    v: Vec<Option<ScalarValue>>,
}

/// Create an initial aggregate entry
fn create_accumulators(
    aggr_expr: &Vec<CompiledAggregateExpression>,
) -> Result<AccumulatorSet> {
    let aggr_values: Vec<Rc<RefCell<AggregateFunction>>> = aggr_expr
        .iter()
        .map(|e| match e.aggr_type() {
            AggregateType::Min => {
                Ok(Rc::new(RefCell::new(MinFunction::new(e.data_type())))
                    as Rc<RefCell<AggregateFunction>>)
            }
            AggregateType::Max => {
                Ok(Rc::new(RefCell::new(MaxFunction::new(e.data_type())))
                    as Rc<RefCell<AggregateFunction>>)
            }
            AggregateType::Sum => {
                Ok(Rc::new(RefCell::new(SumFunction::new(e.data_type())))
                    as Rc<RefCell<AggregateFunction>>)
            }
            _ => Err(ExecutionError::ExecutionError(
                "unsupported aggregate function".to_string(),
            )),
        })
        .collect::<Result<Vec<Rc<RefCell<_>>>>>()?;

    Ok(AccumulatorSet { aggr_values })
}

fn array_min(array: ArrayRef, dt: &DataType) -> Result<Option<ScalarValue>> {
    match dt {
        DataType::UInt8 => {
            match compute::min(array.as_any().downcast_ref::<UInt8Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::UInt8(n))),
                None => Ok(None),
            }
        }
        DataType::UInt16 => {
            match compute::min(array.as_any().downcast_ref::<UInt16Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::UInt16(n))),
                None => Ok(None),
            }
        }
        DataType::UInt32 => {
            match compute::min(array.as_any().downcast_ref::<UInt32Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::UInt32(n))),
                None => Ok(None),
            }
        }
        DataType::UInt64 => {
            match compute::min(array.as_any().downcast_ref::<UInt64Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::UInt64(n))),
                None => Ok(None),
            }
        }
        DataType::Int8 => {
            match compute::min(array.as_any().downcast_ref::<Int8Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Int8(n))),
                None => Ok(None),
            }
        }
        DataType::Int16 => {
            match compute::min(array.as_any().downcast_ref::<Int16Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Int16(n))),
                None => Ok(None),
            }
        }
        DataType::Int32 => {
            match compute::min(array.as_any().downcast_ref::<Int32Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Int32(n))),
                None => Ok(None),
            }
        }
        DataType::Int64 => {
            match compute::min(array.as_any().downcast_ref::<Int64Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Int64(n))),
                None => Ok(None),
            }
        }
        DataType::Float32 => {
            match compute::min(array.as_any().downcast_ref::<Float32Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Float32(n))),
                None => Ok(None),
            }
        }
        DataType::Float64 => {
            match compute::min(array.as_any().downcast_ref::<Float64Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Float64(n))),
                None => Ok(None),
            }
        }
        _ => Err(ExecutionError::ExecutionError(
            "Unsupported data type for MIN".to_string(),
        )),
    }
}

fn array_max(array: ArrayRef, dt: &DataType) -> Result<Option<ScalarValue>> {
    match dt {
        DataType::UInt8 => {
            match compute::max(array.as_any().downcast_ref::<UInt8Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::UInt8(n))),
                None => Ok(None),
            }
        }
        DataType::UInt16 => {
            match compute::max(array.as_any().downcast_ref::<UInt16Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::UInt16(n))),
                None => Ok(None),
            }
        }
        DataType::UInt32 => {
            match compute::max(array.as_any().downcast_ref::<UInt32Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::UInt32(n))),
                None => Ok(None),
            }
        }
        DataType::UInt64 => {
            match compute::max(array.as_any().downcast_ref::<UInt64Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::UInt64(n))),
                None => Ok(None),
            }
        }
        DataType::Int8 => {
            match compute::max(array.as_any().downcast_ref::<Int8Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Int8(n))),
                None => Ok(None),
            }
        }
        DataType::Int16 => {
            match compute::max(array.as_any().downcast_ref::<Int16Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Int16(n))),
                None => Ok(None),
            }
        }
        DataType::Int32 => {
            match compute::max(array.as_any().downcast_ref::<Int32Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Int32(n))),
                None => Ok(None),
            }
        }
        DataType::Int64 => {
            match compute::max(array.as_any().downcast_ref::<Int64Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Int64(n))),
                None => Ok(None),
            }
        }
        DataType::Float32 => {
            match compute::max(array.as_any().downcast_ref::<Float32Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Float32(n))),
                None => Ok(None),
            }
        }
        DataType::Float64 => {
            match compute::max(array.as_any().downcast_ref::<Float64Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Float64(n))),
                None => Ok(None),
            }
        }
        _ => Err(ExecutionError::ExecutionError(
            "Unsupported data type for MAX".to_string(),
        )),
    }
}

fn array_sum(array: ArrayRef, dt: &DataType) -> Result<Option<ScalarValue>> {
    match dt {
        DataType::UInt8 => {
            match compute::sum(array.as_any().downcast_ref::<UInt8Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::UInt8(n))),
                None => Ok(None),
            }
        }
        DataType::UInt16 => {
            match compute::sum(array.as_any().downcast_ref::<UInt16Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::UInt16(n))),
                None => Ok(None),
            }
        }
        DataType::UInt32 => {
            match compute::sum(array.as_any().downcast_ref::<UInt32Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::UInt32(n))),
                None => Ok(None),
            }
        }
        DataType::UInt64 => {
            match compute::sum(array.as_any().downcast_ref::<UInt64Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::UInt64(n))),
                None => Ok(None),
            }
        }
        DataType::Int8 => {
            match compute::sum(array.as_any().downcast_ref::<Int8Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Int8(n))),
                None => Ok(None),
            }
        }
        DataType::Int16 => {
            match compute::sum(array.as_any().downcast_ref::<Int16Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Int16(n))),
                None => Ok(None),
            }
        }
        DataType::Int32 => {
            match compute::sum(array.as_any().downcast_ref::<Int32Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Int32(n))),
                None => Ok(None),
            }
        }
        DataType::Int64 => {
            match compute::sum(array.as_any().downcast_ref::<Int64Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Int64(n))),
                None => Ok(None),
            }
        }
        DataType::Float32 => {
            match compute::sum(array.as_any().downcast_ref::<Float32Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Float32(n))),
                None => Ok(None),
            }
        }
        DataType::Float64 => {
            match compute::sum(array.as_any().downcast_ref::<Float64Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Float64(n))),
                None => Ok(None),
            }
        }
        _ => Err(ExecutionError::ExecutionError(
            "Unsupported data type for SUM".to_string(),
        )),
    }
}

fn update_accumulators(
    batch: &RecordBatch,
    row: usize,
    accumulator_set: &mut AccumulatorSet,
    aggr_expr: &Vec<CompiledAggregateExpression>,
) -> Result<()> {
    // update the accumulators
    for j in 0..accumulator_set.aggr_values.len() {
        // evaluate the argument to the aggregate function
        let array = aggr_expr[j].invoke(batch)?;

        let value: Option<ScalarValue> = match aggr_expr[j].data_type() {
            DataType::UInt8 => {
                let z = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                Some(ScalarValue::UInt8(z.value(row)))
            }
            DataType::UInt16 => {
                let z = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                Some(ScalarValue::UInt16(z.value(row)))
            }
            DataType::UInt32 => {
                let z = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                Some(ScalarValue::UInt32(z.value(row)))
            }
            DataType::UInt64 => {
                let z = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                Some(ScalarValue::UInt64(z.value(row)))
            }
            DataType::Int8 => {
                let z = array.as_any().downcast_ref::<Int8Array>().unwrap();
                Some(ScalarValue::Int8(z.value(row)))
            }
            DataType::Int16 => {
                let z = array.as_any().downcast_ref::<Int16Array>().unwrap();
                Some(ScalarValue::Int16(z.value(row)))
            }
            DataType::Int32 => {
                let z = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Some(ScalarValue::Int32(z.value(row)))
            }
            DataType::Int64 => {
                let z = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Some(ScalarValue::Int64(z.value(row)))
            }
            DataType::Float32 => {
                let z = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Some(ScalarValue::Float32(z.value(row)))
            }
            DataType::Float64 => {
                let z = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Some(ScalarValue::Float64(z.value(row)))
            }
            other => {
                return Err(ExecutionError::ExecutionError(format!(
                    "Unsupported data type {:?} for result of aggregate expression",
                    other
                )));
            }
        };

        accumulator_set.accumulate_scalar(j, value)?;
    }
    Ok(())
}

impl Relation for AggregateRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.end_of_results {
            Ok(None)
        } else {
            self.end_of_results = true;
            if self.group_expr.is_empty() {
                self.without_group_by()
            } else {
                self.with_group_by()
            }
        }
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

macro_rules! array_from_scalar {
    ($BUILDER:ident, $TY:ident, $ACCUM:expr) => {{
        let mut b = $BUILDER::new(1);
        let mut err = false;
        match $ACCUM.result() {
            Some(ScalarValue::$TY(n)) => {
                b.append_value(*n)?;
            }
            None => {
                b.append_null()?;
            }
            Some(_) => {
                err = true;
            }
        };
        if err {
            Err(ExecutionError::ExecutionError(
                "unexpected type when creating array from scalar value".to_string(),
            ))
        } else {
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
    }};
}

/// Create array from `key` attribute in map entry (representing a grouping scalar value)
macro_rules! group_array_from_map_entries {
    ($BUILDER:ident, $TY:ident, $ENTRIES:expr, $COL_INDEX:expr) => {{
        let mut builder = $BUILDER::new($ENTRIES.len());
        let mut err = false;
        for j in 0..$ENTRIES.len() {
            match $ENTRIES[j].k[$COL_INDEX] {
                GroupByScalar::$TY(n) => builder.append_value(n).unwrap(),
                _ => err = true,
            }
        }
        if err {
            Err(ExecutionError::ExecutionError(
                "unexpected type when creating array from aggregate map".to_string(),
            ))
        } else {
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
    }};
}

/// Create array from `value` attribute in map entry (representing an aggregate scalar
/// value)
macro_rules! aggr_array_from_map_entries {
    ($BUILDER:ident, $TY:ident, $ENTRIES:expr, $COL_INDEX:expr) => {{
        let mut builder = $BUILDER::new($ENTRIES.len());
        let mut err = false;
        for j in 0..$ENTRIES.len() {
            match $ENTRIES[j].v[$COL_INDEX] {
                Some(ScalarValue::$TY(n)) => builder.append_value(n).unwrap(),
                None => builder.append_null().unwrap(),
                _ => err = true,
            }
        }
        if err {
            Err(ExecutionError::ExecutionError(
                "unexpected type when creating array from aggregate map".to_string(),
            ))
        } else {
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
    }};
}

impl AggregateRelation {
    /// perform simple aggregate on entire columns without grouping logic
    fn without_group_by(&mut self) -> Result<Option<RecordBatch>> {
        let aggr_expr_count = self.aggr_expr.len();
        let mut accumulator_set = create_accumulators(&self.aggr_expr)?;

        while let Some(batch) = self.input.borrow_mut().next()? {
            for i in 0..aggr_expr_count {
                // evaluate the argument to the aggregate function
                let array = self.aggr_expr[i].invoke(&batch)?;

                let t = self.aggr_expr[i].data_type();

                match self.aggr_expr[i].aggr_type() {
                    AggregateType::Min => {
                        accumulator_set.accumulate_scalar(i, array_min(array, &t)?)?
                    }
                    AggregateType::Max => {
                        accumulator_set.accumulate_scalar(i, array_max(array, &t)?)?
                    }
                    AggregateType::Sum => {
                        accumulator_set.accumulate_scalar(i, array_sum(array, &t)?)?
                    }
                    _ => {
                        return Err(ExecutionError::NotImplemented(
                            "Unsupported aggregate function".to_string(),
                        ));
                    }
                }
            }
        }

        let mut result_columns: Vec<ArrayRef> = vec![];

        for i in 0..aggr_expr_count {
            let accum = accumulator_set.aggr_values[i].borrow();
            match accum.data_type() {
                DataType::UInt8 => {
                    result_columns.push(array_from_scalar!(UInt8Builder, UInt8, accum)?)
                }
                DataType::UInt16 => {
                    result_columns.push(array_from_scalar!(UInt16Builder, UInt16, accum)?)
                }
                DataType::UInt32 => {
                    result_columns.push(array_from_scalar!(UInt32Builder, UInt32, accum)?)
                }
                DataType::UInt64 => {
                    result_columns.push(array_from_scalar!(UInt64Builder, UInt64, accum)?)
                }
                DataType::Int8 => {
                    result_columns.push(array_from_scalar!(Int8Builder, Int8, accum)?)
                }
                DataType::Int16 => {
                    result_columns.push(array_from_scalar!(Int16Builder, Int16, accum)?)
                }
                DataType::Int32 => {
                    result_columns.push(array_from_scalar!(Int32Builder, Int32, accum)?)
                }
                DataType::Int64 => {
                    result_columns.push(array_from_scalar!(Int64Builder, Int64, accum)?)
                }
                DataType::Float32 => result_columns.push(array_from_scalar!(
                    Float32Builder,
                    Float32,
                    accum
                )?),
                DataType::Float64 => result_columns.push(array_from_scalar!(
                    Float64Builder,
                    Float64,
                    accum
                )?),
                _ => return Err(ExecutionError::NotImplemented("tbd".to_string())),
            }
        }

        Ok(Some(RecordBatch::try_new(
            self.schema.clone(),
            result_columns,
        )?))
    }

    fn with_group_by(&mut self) -> Result<Option<RecordBatch>> {
        //NOTE this whole method is currently very inefficient with too many per-row
        // operations involving pattern matching and downcasting ... I'm sure this
        // can be re-implemented in a much more efficient way that takes better
        // advantage of Arrow

        // create map to store aggregate results
        let mut map: FnvHashMap<Vec<GroupByScalar>, Rc<RefCell<AccumulatorSet>>> =
            FnvHashMap::default();

        while let Some(batch) = self.input.borrow_mut().next()? {
            // evaulate the group by expressions on this batch
            let group_by_keys: Vec<ArrayRef> = self
                .group_expr
                .iter()
                .map(|e| e.invoke(&batch))
                .collect::<Result<Vec<ArrayRef>>>()?;

            // iterate over each row in the batch
            for row in 0..batch.num_rows() {
                // create key
                let key: Vec<GroupByScalar> = group_by_keys
                    .iter()
                    .map(|col| match col.data_type() {
                        DataType::UInt8 => {
                            let array =
                                col.as_any().downcast_ref::<UInt8Array>().unwrap();
                            Ok(GroupByScalar::UInt8(array.value(row)))
                        }
                        DataType::UInt16 => {
                            let array =
                                col.as_any().downcast_ref::<UInt16Array>().unwrap();
                            Ok(GroupByScalar::UInt16(array.value(row)))
                        }
                        DataType::UInt32 => {
                            let array =
                                col.as_any().downcast_ref::<UInt32Array>().unwrap();
                            Ok(GroupByScalar::UInt32(array.value(row)))
                        }
                        DataType::UInt64 => {
                            let array =
                                col.as_any().downcast_ref::<UInt64Array>().unwrap();
                            Ok(GroupByScalar::UInt64(array.value(row)))
                        }
                        DataType::Int8 => {
                            let array = col.as_any().downcast_ref::<Int8Array>().unwrap();
                            Ok(GroupByScalar::Int8(array.value(row)))
                        }
                        DataType::Int16 => {
                            let array =
                                col.as_any().downcast_ref::<Int16Array>().unwrap();
                            Ok(GroupByScalar::Int16(array.value(row)))
                        }
                        DataType::Int32 => {
                            let array =
                                col.as_any().downcast_ref::<Int32Array>().unwrap();
                            Ok(GroupByScalar::Int32(array.value(row)))
                        }
                        DataType::Int64 => {
                            let array =
                                col.as_any().downcast_ref::<Int64Array>().unwrap();
                            Ok(GroupByScalar::Int64(array.value(row)))
                        }
                        DataType::Utf8 => {
                            let array =
                                col.as_any().downcast_ref::<BinaryArray>().unwrap();
                            Ok(GroupByScalar::Utf8(String::from(
                                str::from_utf8(array.value(row)).unwrap(),
                            )))
                        }
                        _ => Err(ExecutionError::ExecutionError(
                            "Unsupported GROUP BY data type".to_string(),
                        )),
                    })
                    .collect::<Result<Vec<GroupByScalar>>>()?;

                //TODO: find more elegant way to write this instead of hacking around
                // ownership issues

                let updated = match map.get(&key) {
                    Some(entry) => {
                        let mut accumulator_set = entry.borrow_mut();
                        update_accumulators(
                            &batch,
                            row,
                            &mut accumulator_set,
                            &self.aggr_expr,
                        )?;
                        true
                    }
                    None => false,
                };

                if !updated {
                    let accumulator_set =
                        Rc::new(RefCell::new(create_accumulators(&self.aggr_expr)?));
                    {
                        let mut entry_mut = accumulator_set.borrow_mut();
                        update_accumulators(
                            &batch,
                            row,
                            &mut entry_mut,
                            &self.aggr_expr,
                        )?;
                    }
                    map.insert(key.clone(), accumulator_set);
                }
            }
        }

        // convert the map to a vec to make it easier to build arrays
        let entries: Vec<MapEntry> = map
            .iter()
            .map(|(k, v)| {
                let x = v.borrow();
                MapEntry {
                    k: k.clone(),
                    v: x.values(),
                }
            })
            .collect();

        // build the result arrays
        let mut result_arrays: Vec<ArrayRef> =
            Vec::with_capacity(self.group_expr.len() + self.aggr_expr.len());

        // grouping values
        for i in 0..self.group_expr.len() {
            let array: Result<ArrayRef> = match self.group_expr[i].data_type() {
                DataType::UInt8 => {
                    group_array_from_map_entries!(UInt8Builder, UInt8, entries, i)
                }
                DataType::UInt16 => {
                    group_array_from_map_entries!(UInt16Builder, UInt16, entries, i)
                }
                DataType::UInt32 => {
                    group_array_from_map_entries!(UInt32Builder, UInt32, entries, i)
                }
                DataType::UInt64 => {
                    group_array_from_map_entries!(UInt64Builder, UInt64, entries, i)
                }
                DataType::Int8 => {
                    group_array_from_map_entries!(Int8Builder, Int8, entries, i)
                }
                DataType::Int16 => {
                    group_array_from_map_entries!(Int16Builder, Int16, entries, i)
                }
                DataType::Int32 => {
                    group_array_from_map_entries!(Int32Builder, Int32, entries, i)
                }
                DataType::Int64 => {
                    group_array_from_map_entries!(Int64Builder, Int64, entries, i)
                }
                DataType::Utf8 => {
                    let mut builder = BinaryBuilder::new(1);
                    for j in 0..entries.len() {
                        match &entries[j].k[i] {
                            GroupByScalar::Utf8(s) => builder.append_string(&s).unwrap(),
                            _ => {}
                        }
                    }
                    Ok(Arc::new(builder.finish()) as ArrayRef)
                }
                _ => Err(ExecutionError::ExecutionError(
                    "Unsupported group by expr".to_string(),
                )),
            };
            result_arrays.push(array?);
        }

        // aggregate values
        for i in 0..self.aggr_expr.len() {
            let array = match self.aggr_expr[i].data_type() {
                DataType::UInt8 => {
                    aggr_array_from_map_entries!(UInt8Builder, UInt8, entries, i)
                }
                DataType::UInt16 => {
                    aggr_array_from_map_entries!(UInt16Builder, UInt16, entries, i)
                }
                DataType::UInt32 => {
                    aggr_array_from_map_entries!(UInt32Builder, UInt32, entries, i)
                }
                DataType::UInt64 => {
                    aggr_array_from_map_entries!(UInt64Builder, UInt64, entries, i)
                }
                DataType::Int8 => {
                    group_array_from_map_entries!(Int8Builder, Int8, entries, i)
                }
                DataType::Int16 => {
                    aggr_array_from_map_entries!(Int16Builder, Int16, entries, i)
                }
                DataType::Int32 => {
                    aggr_array_from_map_entries!(Int32Builder, Int32, entries, i)
                }
                DataType::Int64 => {
                    aggr_array_from_map_entries!(Int64Builder, Int64, entries, i)
                }
                DataType::Float32 => {
                    aggr_array_from_map_entries!(Float32Builder, Float32, entries, i)
                }
                DataType::Float64 => {
                    aggr_array_from_map_entries!(Float64Builder, Float64, entries, i)
                }
                _ => Err(ExecutionError::ExecutionError(
                    "Unsupported aggregate expr".to_string(),
                )),
            };
            result_arrays.push(array?);
        }

        Ok(Some(RecordBatch::try_new(
            self.schema.clone(),
            result_arrays,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::CsvBatchIterator;
    use crate::execution::context::ExecutionContext;
    use crate::execution::expression;
    use crate::execution::relation::DataSourceRelation;
    use crate::logicalplan::Expr;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Mutex;

    #[test]
    fn min_f64_group_by_string() {
        let schema = aggr_test_schema();
        let relation = load_csv("../../testing/data/csv/aggregate_test_100.csv", &schema);
        let context = ExecutionContext::new();

        let aggr_expr = vec![expression::compile_aggregate_expr(
            &context,
            &Expr::AggregateFunction {
                name: String::from("min"),
                args: vec![Expr::Column(11)],
                return_type: DataType::Float64,
            },
            &schema,
        )
        .unwrap()];

        let aggr_schema = Arc::new(Schema::new(vec![Field::new(
            "min_lat",
            DataType::Float64,
            false,
        )]));

        let mut projection =
            AggregateRelation::new(aggr_schema, relation, vec![], aggr_expr);
        let batch = projection.next().unwrap().unwrap();
        assert_eq!(1, batch.num_columns());
        let min_lat = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(0.01479305307777301, min_lat.value(0));
    }

    #[test]
    fn max_f64_group_by_string() {
        let schema = aggr_test_schema();
        let relation = load_csv("../../testing/data/csv/aggregate_test_100.csv", &schema);
        let context = ExecutionContext::new();

        let aggr_expr = vec![expression::compile_aggregate_expr(
            &context,
            &Expr::AggregateFunction {
                name: String::from("max"),
                args: vec![Expr::Column(11)],
                return_type: DataType::Float64,
            },
            &schema,
        )
        .unwrap()];

        let aggr_schema = Arc::new(Schema::new(vec![Field::new(
            "max_lat",
            DataType::Float64,
            false,
        )]));

        let mut projection =
            AggregateRelation::new(aggr_schema, relation, vec![], aggr_expr);
        let batch = projection.next().unwrap().unwrap();
        assert_eq!(1, batch.num_columns());
        let max_lat = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(0.9965400387585364, max_lat.value(0));
    }

    #[test]
    fn test_min_max_sum_f64_group_by_uint32() {
        let schema = aggr_test_schema();
        let relation = load_csv("../../testing/data/csv/aggregate_test_100.csv", &schema);

        let context = ExecutionContext::new();

        let group_by_expr =
            expression::compile_expr(&context, &Expr::Column(1), &schema).unwrap();

        let min_expr = expression::compile_aggregate_expr(
            &context,
            &Expr::AggregateFunction {
                name: String::from("min"),
                args: vec![Expr::Column(11)],
                return_type: DataType::Float64,
            },
            &schema,
        )
        .unwrap();

        let max_expr = expression::compile_aggregate_expr(
            &context,
            &Expr::AggregateFunction {
                name: String::from("max"),
                args: vec![Expr::Column(11)],
                return_type: DataType::Float64,
            },
            &schema,
        )
        .unwrap();

        let sum_expr = expression::compile_aggregate_expr(
            &context,
            &Expr::AggregateFunction {
                name: String::from("sum"),
                args: vec![Expr::Column(11)],
                return_type: DataType::Float64,
            },
            &schema,
        )
        .unwrap();

        let aggr_schema = Arc::new(Schema::new(vec![
            Field::new("c2", DataType::UInt32, false),
            Field::new("min", DataType::Float64, false),
            Field::new("max", DataType::Float64, false),
            Field::new("sum", DataType::Float64, false),
        ]));

        let mut projection = AggregateRelation::new(
            aggr_schema,
            relation,
            vec![group_by_expr],
            vec![min_expr, max_expr, sum_expr],
        );
        let batch = projection.next().unwrap().unwrap();
        assert_eq!(4, batch.num_columns());
        assert_eq!(5, batch.num_rows());

        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let min = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let max = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let sum = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(4, a.value(0));
        assert_eq!(0.02182578039211991, min.value(0));
        assert_eq!(0.9237877978193884, max.value(0));
        assert_eq!(9.253864188402662, sum.value(0));

        assert_eq!(2, a.value(1));
        assert_eq!(0.16301110515739792, min.value(1));
        assert_eq!(0.991517828651004, max.value(1));
        assert_eq!(14.400412325480858, sum.value(1));

        assert_eq!(5, a.value(2));
        assert_eq!(0.01479305307777301, min.value(2));
        assert_eq!(0.9723580396501548, max.value(2));
        assert_eq!(6.037181692266781, sum.value(2));
    }

    fn aggr_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::UInt32, false),
            Field::new("c3", DataType::Int8, false),
            Field::new("c3", DataType::Int16, false),
            Field::new("c4", DataType::Int32, false),
            Field::new("c5", DataType::Int64, false),
            Field::new("c6", DataType::UInt8, false),
            Field::new("c7", DataType::UInt16, false),
            Field::new("c8", DataType::UInt32, false),
            Field::new("c9", DataType::UInt64, false),
            Field::new("c10", DataType::Float32, false),
            Field::new("c11", DataType::Float64, false),
            Field::new("c12", DataType::Utf8, false),
        ]))
    }

    fn load_csv(filename: &str, schema: &Arc<Schema>) -> Rc<RefCell<Relation>> {
        let ds = CsvBatchIterator::new(filename, schema.clone(), true, &None, 1024);
        Rc::new(RefCell::new(DataSourceRelation::new(Arc::new(Mutex::new(
            ds,
        )))))
    }

}
