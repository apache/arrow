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

//! Defines physical expressions that can evaluated at runtime during query execution

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use super::ColumnarValue;
use crate::error::{DataFusionError, Result};
use crate::logical_plan::Operator;
use crate::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use crate::scalar::ScalarValue;
use arrow::array::{self, Array, BooleanBuilder, LargeStringArray};
use arrow::compute;
use arrow::compute::kernels;
use arrow::compute::kernels::arithmetic::{add, divide, multiply, negate, subtract};
use arrow::compute::kernels::boolean::{and, nullif, or};
use arrow::compute::kernels::comparison::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::kernels::comparison::{
    eq_scalar, gt_eq_scalar, gt_scalar, lt_eq_scalar, lt_scalar, neq_scalar,
};
use arrow::compute::kernels::comparison::{
    eq_utf8, gt_eq_utf8, gt_utf8, like_utf8, like_utf8_scalar, lt_eq_utf8, lt_utf8,
    neq_utf8, nlike_utf8, nlike_utf8_scalar,
};
use arrow::compute::kernels::comparison::{
    eq_utf8_scalar, gt_eq_utf8_scalar, gt_utf8_scalar, lt_eq_utf8_scalar, lt_utf8_scalar,
    neq_utf8_scalar,
};
use arrow::compute::kernels::sort::{SortColumn, SortOptions};
use arrow::datatypes::{DataType, DateUnit, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::{
    array::{
        ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
        TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::Field,
};
use compute::can_cast_types;

/// returns the name of the state
pub fn format_state_name(name: &str, state_name: &str) -> String {
    format!("{}[{}]", name, state_name)
}

/// Represents the column at a given index in a RecordBatch
#[derive(Debug)]
pub struct Column {
    name: String,
}

impl Column {
    /// Create a new column expression
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
        }
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl PhysicalExpr for Column {
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        Ok(input_schema
            .field_with_name(&self.name)?
            .data_type()
            .clone())
    }

    /// Decide whehter this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(input_schema.field_with_name(&self.name)?.is_nullable())
    }

    /// Evaluate the expression
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Array(
            batch.column(batch.schema().index_of(&self.name)?).clone(),
        ))
    }
}

/// Create a column expression
pub fn col(name: &str) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(name))
}

/// SUM aggregate expression
#[derive(Debug)]
pub struct Sum {
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

/// function return type of a sum
pub fn sum_return_type(arg_type: &DataType) -> Result<DataType> {
    match arg_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Ok(DataType::Int64)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Ok(DataType::UInt64)
        }
        DataType::Float32 => Ok(DataType::Float32),
        DataType::Float64 => Ok(DataType::Float64),
        other => Err(DataFusionError::Plan(format!(
            "SUM does not support type \"{:?}\"",
            other
        ))),
    }
}

impl Sum {
    /// Create a new SUM aggregate function
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: String, data_type: DataType) -> Self {
        Self {
            name,
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for Sum {
    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "sum"),
            self.data_type.clone(),
            self.nullable,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SumAccumulator::try_new(&self.data_type)?))
    }
}

#[derive(Debug)]
struct SumAccumulator {
    sum: ScalarValue,
}

impl SumAccumulator {
    /// new sum accumulator
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            sum: ScalarValue::try_from(data_type)?,
        })
    }
}

// returns the new value after sum with the new values, taking nullability into account
macro_rules! typed_sum_delta_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let delta = compute::sum(array);
        ScalarValue::$SCALAR(delta)
    }};
}

// sums the array and returns a ScalarValue of its corresponding type.
fn sum_batch(values: &ArrayRef) -> Result<ScalarValue> {
    Ok(match values.data_type() {
        DataType::Float64 => typed_sum_delta_batch!(values, Float64Array, Float64),
        DataType::Float32 => typed_sum_delta_batch!(values, Float32Array, Float32),
        DataType::Int64 => typed_sum_delta_batch!(values, Int64Array, Int64),
        DataType::Int32 => typed_sum_delta_batch!(values, Int32Array, Int32),
        DataType::Int16 => typed_sum_delta_batch!(values, Int16Array, Int16),
        DataType::Int8 => typed_sum_delta_batch!(values, Int8Array, Int8),
        DataType::UInt64 => typed_sum_delta_batch!(values, UInt64Array, UInt64),
        DataType::UInt32 => typed_sum_delta_batch!(values, UInt32Array, UInt32),
        DataType::UInt16 => typed_sum_delta_batch!(values, UInt16Array, UInt16),
        DataType::UInt8 => typed_sum_delta_batch!(values, UInt8Array, UInt8),
        e => {
            return Err(DataFusionError::Internal(format!(
                "Sum is not expected to receive the type {:?}",
                e
            )))
        }
    })
}

// returns the sum of two scalar values, including coercion into $TYPE.
macro_rules! typed_sum {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        ScalarValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some(a + (*b as $TYPE)),
        })
    }};
}

fn sum(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
    Ok(match (lhs, rhs) {
        // float64 coerces everything to f64
        (ScalarValue::Float64(lhs), ScalarValue::Float64(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::Float32(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::Int64(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::Int32(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::Int16(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::Int8(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::UInt64(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::UInt32(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::UInt16(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::UInt8(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        // float32 has no cast
        (ScalarValue::Float32(lhs), ScalarValue::Float32(rhs)) => {
            typed_sum!(lhs, rhs, Float32, f32)
        }
        // u64 coerces u* to u64
        (ScalarValue::UInt64(lhs), ScalarValue::UInt64(rhs)) => {
            typed_sum!(lhs, rhs, UInt64, u64)
        }
        (ScalarValue::UInt64(lhs), ScalarValue::UInt32(rhs)) => {
            typed_sum!(lhs, rhs, UInt64, u64)
        }
        (ScalarValue::UInt64(lhs), ScalarValue::UInt16(rhs)) => {
            typed_sum!(lhs, rhs, UInt64, u64)
        }
        (ScalarValue::UInt64(lhs), ScalarValue::UInt8(rhs)) => {
            typed_sum!(lhs, rhs, UInt64, u64)
        }
        // i64 coerces i* to u64
        (ScalarValue::Int64(lhs), ScalarValue::Int64(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        (ScalarValue::Int64(lhs), ScalarValue::Int32(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        (ScalarValue::Int64(lhs), ScalarValue::Int16(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        (ScalarValue::Int64(lhs), ScalarValue::Int8(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        e => {
            return Err(DataFusionError::Internal(format!(
                "Sum is not expected to receive a scalar {:?}",
                e
            )))
        }
    })
}

impl Accumulator for SumAccumulator {
    fn update_batch(&mut self, values: &Vec<ArrayRef>) -> Result<()> {
        let values = &values[0];
        self.sum = sum(&self.sum, &sum_batch(values)?)?;
        Ok(())
    }

    fn update(&mut self, values: &Vec<ScalarValue>) -> Result<()> {
        // sum(v1, v2, v3) = v1 + v2 + v3
        self.sum = sum(&self.sum, &values[0])?;
        Ok(())
    }

    fn merge(&mut self, states: &Vec<ScalarValue>) -> Result<()> {
        // sum(sum1, sum2) = sum1 + sum2
        self.update(states)
    }

    fn merge_batch(&mut self, states: &Vec<ArrayRef>) -> Result<()> {
        // sum(sum1, sum2, sum3, ...) = sum1 + sum2 + sum3 + ...
        self.update_batch(states)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.sum.clone()])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.sum.clone())
    }
}

/// AVG aggregate expression
#[derive(Debug)]
pub struct Avg {
    name: String,
    data_type: DataType,
    nullable: bool,
    expr: Arc<dyn PhysicalExpr>,
}

/// function return type of an average
pub fn avg_return_type(arg_type: &DataType) -> Result<DataType> {
    match arg_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => Ok(DataType::Float64),
        other => Err(DataFusionError::Plan(format!(
            "AVG does not support {:?}",
            other
        ))),
    }
}

impl Avg {
    /// Create a new AVG aggregate function
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: String, data_type: DataType) -> Self {
        Self {
            name,
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for Avg {
    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                &format_state_name(&self.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "sum"),
                DataType::Float64,
                true,
            ),
        ])
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(AvgAccumulator::try_new(
            // avg is f64
            &DataType::Float64,
        )?))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }
}

/// An accumulator to compute the average
#[derive(Debug)]
pub(crate) struct AvgAccumulator {
    // sum is used for null
    sum: ScalarValue,
    count: u64,
}

impl AvgAccumulator {
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            sum: ScalarValue::try_from(datatype)?,
            count: 0,
        })
    }
}

impl Accumulator for AvgAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::from(self.count), self.sum.clone()])
    }

    fn update(&mut self, values: &Vec<ScalarValue>) -> Result<()> {
        let values = &values[0];

        self.count += (!values.is_null()) as u64;
        self.sum = sum(&self.sum, values)?;

        Ok(())
    }

    fn update_batch(&mut self, values: &Vec<ArrayRef>) -> Result<()> {
        let values = &values[0];

        self.count += (values.len() - values.data().null_count()) as u64;
        self.sum = sum(&self.sum, &sum_batch(values)?)?;
        Ok(())
    }

    fn merge(&mut self, states: &Vec<ScalarValue>) -> Result<()> {
        let count = &states[0];
        // counts are summed
        if let ScalarValue::UInt64(Some(c)) = count {
            self.count += c
        } else {
            unreachable!()
        };

        // sums are summed
        self.sum = sum(&self.sum, &states[1])?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &Vec<ArrayRef>) -> Result<()> {
        let counts = states[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        // counts are summed
        self.count += compute::sum(counts).unwrap_or(0);

        // sums are summed
        self.sum = sum(&self.sum, &sum_batch(&states[1])?)?;
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match self.sum {
            ScalarValue::Float64(e) => {
                Ok(ScalarValue::Float64(e.map(|f| f / self.count as f64)))
            }
            _ => Err(DataFusionError::Internal(
                "Sum should be f64 on average".to_string(),
            )),
        }
    }
}

/// MAX aggregate expression
#[derive(Debug)]
pub struct Max {
    name: String,
    data_type: DataType,
    nullable: bool,
    expr: Arc<dyn PhysicalExpr>,
}

impl Max {
    /// Create a new MAX aggregate function
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: String, data_type: DataType) -> Self {
        Self {
            name,
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for Max {
    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "max"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MaxAccumulator::try_new(&self.data_type)?))
    }
}

// Statically-typed version of min/max(array) -> ScalarValue for string types.
macro_rules! typed_min_max_batch_string {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let value = compute::$OP(array);
        let value = value.and_then(|e| Some(e.to_string()));
        ScalarValue::$SCALAR(value)
    }};
}

// Statically-typed version of min/max(array) -> ScalarValue for non-string types.
macro_rules! typed_min_max_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let value = compute::$OP(array);
        ScalarValue::$SCALAR(value)
    }};
}

// Statically-typed version of min/max(array) -> ScalarValue  for non-string types.
// this is a macro to support both operations (min and max).
macro_rules! min_max_batch {
    ($VALUES:expr, $OP:ident) => {{
        match $VALUES.data_type() {
            // all types that have a natural order
            DataType::Float64 => {
                typed_min_max_batch!($VALUES, Float64Array, Float64, $OP)
            }
            DataType::Float32 => {
                typed_min_max_batch!($VALUES, Float32Array, Float32, $OP)
            }
            DataType::Int64 => typed_min_max_batch!($VALUES, Int64Array, Int64, $OP),
            DataType::Int32 => typed_min_max_batch!($VALUES, Int32Array, Int32, $OP),
            DataType::Int16 => typed_min_max_batch!($VALUES, Int16Array, Int16, $OP),
            DataType::Int8 => typed_min_max_batch!($VALUES, Int8Array, Int8, $OP),
            DataType::UInt64 => typed_min_max_batch!($VALUES, UInt64Array, UInt64, $OP),
            DataType::UInt32 => typed_min_max_batch!($VALUES, UInt32Array, UInt32, $OP),
            DataType::UInt16 => typed_min_max_batch!($VALUES, UInt16Array, UInt16, $OP),
            DataType::UInt8 => typed_min_max_batch!($VALUES, UInt8Array, UInt8, $OP),
            other => {
                // This should have been handled before
                return Err(DataFusionError::Internal(format!(
                    "Min/Max accumulator not implemented for type {:?}",
                    other
                )));
            }
        }
    }};
}

/// dynamically-typed min(array) -> ScalarValue
fn min_batch(values: &ArrayRef) -> Result<ScalarValue> {
    Ok(match values.data_type() {
        DataType::Utf8 => {
            typed_min_max_batch_string!(values, StringArray, Utf8, min_string)
        }
        DataType::LargeUtf8 => {
            typed_min_max_batch_string!(values, LargeStringArray, LargeUtf8, min_string)
        }
        _ => min_max_batch!(values, min),
    })
}

/// dynamically-typed max(array) -> ScalarValue
fn max_batch(values: &ArrayRef) -> Result<ScalarValue> {
    Ok(match values.data_type() {
        DataType::Utf8 => {
            typed_min_max_batch_string!(values, StringArray, Utf8, max_string)
        }
        DataType::LargeUtf8 => {
            typed_min_max_batch_string!(values, LargeStringArray, LargeUtf8, max_string)
        }
        _ => min_max_batch!(values, max),
    })
}

// min/max of two non-string scalar values.
macro_rules! typed_min_max {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        ScalarValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (Some(a), Some(b)) => Some((*a).$OP(*b)),
        })
    }};
}

// min/max of two scalar string values.
macro_rules! typed_min_max_string {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        ScalarValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (Some(a), Some(b)) => Some((a).$OP(b).clone()),
        })
    }};
}

// min/max of two scalar values of the same type
macro_rules! min_max {
    ($VALUE:expr, $DELTA:expr, $OP:ident) => {{
        Ok(match ($VALUE, $DELTA) {
            (ScalarValue::Float64(lhs), ScalarValue::Float64(rhs)) => {
                typed_min_max!(lhs, rhs, Float64, $OP)
            }
            (ScalarValue::Float32(lhs), ScalarValue::Float32(rhs)) => {
                typed_min_max!(lhs, rhs, Float32, $OP)
            }
            (ScalarValue::UInt64(lhs), ScalarValue::UInt64(rhs)) => {
                typed_min_max!(lhs, rhs, UInt64, $OP)
            }
            (ScalarValue::UInt32(lhs), ScalarValue::UInt32(rhs)) => {
                typed_min_max!(lhs, rhs, UInt32, $OP)
            }
            (ScalarValue::UInt16(lhs), ScalarValue::UInt16(rhs)) => {
                typed_min_max!(lhs, rhs, UInt16, $OP)
            }
            (ScalarValue::UInt8(lhs), ScalarValue::UInt8(rhs)) => {
                typed_min_max!(lhs, rhs, UInt8, $OP)
            }
            (ScalarValue::Int64(lhs), ScalarValue::Int64(rhs)) => {
                typed_min_max!(lhs, rhs, Int64, $OP)
            }
            (ScalarValue::Int32(lhs), ScalarValue::Int32(rhs)) => {
                typed_min_max!(lhs, rhs, Int32, $OP)
            }
            (ScalarValue::Int16(lhs), ScalarValue::Int16(rhs)) => {
                typed_min_max!(lhs, rhs, Int16, $OP)
            }
            (ScalarValue::Int8(lhs), ScalarValue::Int8(rhs)) => {
                typed_min_max!(lhs, rhs, Int8, $OP)
            }
            (ScalarValue::Utf8(lhs), ScalarValue::Utf8(rhs)) => {
                typed_min_max_string!(lhs, rhs, Utf8, $OP)
            }
            (ScalarValue::LargeUtf8(lhs), ScalarValue::LargeUtf8(rhs)) => {
                typed_min_max_string!(lhs, rhs, LargeUtf8, $OP)
            }
            e => {
                return Err(DataFusionError::Internal(format!(
                    "MIN/MAX is not expected to receive a scalar {:?}",
                    e
                )))
            }
        })
    }};
}

/// the minimum of two scalar values
fn min(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
    min_max!(lhs, rhs, min)
}

/// the maximum of two scalar values
fn max(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
    min_max!(lhs, rhs, max)
}

#[derive(Debug)]
struct MaxAccumulator {
    max: ScalarValue,
}

impl MaxAccumulator {
    /// new max accumulator
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            max: ScalarValue::try_from(datatype)?,
        })
    }
}

impl Accumulator for MaxAccumulator {
    fn update_batch(&mut self, values: &Vec<ArrayRef>) -> Result<()> {
        let values = &values[0];
        let delta = &max_batch(values)?;
        self.max = max(&self.max, delta)?;
        Ok(())
    }

    fn update(&mut self, values: &Vec<ScalarValue>) -> Result<()> {
        let value = &values[0];
        self.max = max(&self.max, value)?;
        Ok(())
    }

    fn merge(&mut self, states: &Vec<ScalarValue>) -> Result<()> {
        self.update(states)
    }

    fn merge_batch(&mut self, states: &Vec<ArrayRef>) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.max.clone()])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.max.clone())
    }
}

/// MIN aggregate expression
#[derive(Debug)]
pub struct Min {
    name: String,
    data_type: DataType,
    nullable: bool,
    expr: Arc<dyn PhysicalExpr>,
}

impl Min {
    /// Create a new MIN aggregate function
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: String, data_type: DataType) -> Self {
        Self {
            name,
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for Min {
    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "min"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MinAccumulator::try_new(&self.data_type)?))
    }
}

#[derive(Debug)]
struct MinAccumulator {
    min: ScalarValue,
}

impl MinAccumulator {
    /// new min accumulator
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            min: ScalarValue::try_from(datatype)?,
        })
    }
}

impl Accumulator for MinAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.min.clone()])
    }

    fn update_batch(&mut self, values: &Vec<ArrayRef>) -> Result<()> {
        let values = &values[0];
        let delta = &min_batch(values)?;
        self.min = min(&self.min, delta)?;
        Ok(())
    }

    fn update(&mut self, values: &Vec<ScalarValue>) -> Result<()> {
        let value = &values[0];
        self.min = min(&self.min, value)?;
        Ok(())
    }

    fn merge(&mut self, states: &Vec<ScalarValue>) -> Result<()> {
        self.update(states)
    }

    fn merge_batch(&mut self, states: &Vec<ArrayRef>) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.min.clone())
    }
}

/// COUNT aggregate expression
/// Returns the amount of non-null values of the given expression.
#[derive(Debug)]
pub struct Count {
    name: String,
    data_type: DataType,
    nullable: bool,
    expr: Arc<dyn PhysicalExpr>,
}

impl Count {
    /// Create a new COUNT aggregate function.
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: String, data_type: DataType) -> Self {
        Self {
            name,
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for Count {
    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "count"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountAccumulator::new()))
    }
}

#[derive(Debug)]
struct CountAccumulator {
    count: u64,
}

impl CountAccumulator {
    /// new count accumulator
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn update_batch(&mut self, values: &Vec<ArrayRef>) -> Result<()> {
        let array = &values[0];
        self.count += (array.len() - array.data().null_count()) as u64;
        Ok(())
    }

    fn update(&mut self, values: &Vec<ScalarValue>) -> Result<()> {
        let value = &values[0];
        if !value.is_null() {
            self.count += 1;
        }
        Ok(())
    }

    fn merge(&mut self, states: &Vec<ScalarValue>) -> Result<()> {
        let count = &states[0];
        if let ScalarValue::UInt64(Some(delta)) = count {
            self.count += *delta;
        } else {
            unreachable!()
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &Vec<ArrayRef>) -> Result<()> {
        let counts = states[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        let delta = &compute::sum(counts);
        if let Some(d) = delta {
            self.count += *d;
        }
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::UInt64(Some(self.count))])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(self.count)))
    }
}

/// Invoke a compute kernel on a pair of binary data arrays
macro_rules! compute_utf8_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new(paste::expr! {[<$OP _utf8>]}(&ll, &rr)?))
    }};
}

/// Invoke a compute kernel on a data array and a scalar value
macro_rules! compute_utf8_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        if let ScalarValue::Utf8(Some(string_value)) = $RIGHT {
            Ok(Arc::new(paste::expr! {[<$OP _utf8_scalar>]}(
                &ll,
                &string_value,
            )?))
        } else {
            Err(DataFusionError::Internal(format!(
                "compute_utf8_op_scalar failed to cast literal value {}",
                $RIGHT
            )))
        }
    }};
}

/// Invoke a compute kernel on a data array and a scalar value
macro_rules! compute_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        use std::convert::TryInto;
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        // generate the scalar function name, such as lt_scalar, from the $OP parameter
        // (which could have a value of lt) and the suffix _scalar
        Ok(Arc::new(paste::expr! {[<$OP _scalar>]}(
            &ll,
            $RIGHT.try_into()?,
        )?))
    }};
}

/// Invoke a compute kernel on array(s)
macro_rules! compute_op {
    // invoke binary operator
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
    // invoke unary operator
    ($OPERAND:expr, $OP:ident, $DT:ident) => {{
        let operand = $OPERAND
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&operand)?))
    }};
}

macro_rules! binary_string_array_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        let result = match $LEFT.data_type() {
            DataType::Utf8 => compute_utf8_op_scalar!($LEFT, $RIGHT, $OP, StringArray),
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?}",
                other
            ))),
        };
        Some(result)
    }};
}

macro_rules! binary_string_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Utf8 => compute_utf8_op!($LEFT, $RIGHT, $OP, StringArray),
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?}",
                other
            ))),
        }
    }};
}

/// Invoke a compute kernel on a pair of arrays
/// The binary_primitive_array_op macro only evaluates for primitive types
/// like integers and floats.
macro_rules! binary_primitive_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Int8 => compute_op!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?}",
                other
            ))),
        }
    }};
}

/// The binary_array_op_scalar macro includes types that extend beyond the primitive,
/// such as Utf8 strings.
macro_rules! binary_array_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        let result = match $LEFT.data_type() {
            DataType::Int8 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op_scalar!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op_scalar!($LEFT, $RIGHT, $OP, Float64Array),
            DataType::Utf8 => compute_utf8_op_scalar!($LEFT, $RIGHT, $OP, StringArray),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                compute_op_scalar!($LEFT, $RIGHT, $OP, TimestampNanosecondArray)
            }
            DataType::Date32(DateUnit::Day) => {
                compute_op_scalar!($LEFT, $RIGHT, $OP, Date32Array)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?}",
                other
            ))),
        };
        Some(result)
    }};
}

/// The binary_array_op macro includes types that extend beyond the primitive,
/// such as Utf8 strings.
macro_rules! binary_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Int8 => compute_op!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            DataType::Utf8 => compute_utf8_op!($LEFT, $RIGHT, $OP, StringArray),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                compute_op!($LEFT, $RIGHT, $OP, TimestampNanosecondArray)
            }
            DataType::Date32(DateUnit::Day) => {
                compute_op!($LEFT, $RIGHT, $OP, Date32Array)
            }
            DataType::Date64(DateUnit::Millisecond) => {
                compute_op!($LEFT, $RIGHT, $OP, Date64Array)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?}",
                other
            ))),
        }
    }};
}

/// Invoke a boolean kernel on a pair of arrays
macro_rules! boolean_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
}
/// Binary expression
#[derive(Debug)]
pub struct BinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
}

impl BinaryExpr {
    /// Create new binary expression
    pub fn new(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self { left, op, right }
    }
}

impl fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

/// Coercion rules for dictionary values (aka the type of the  dictionary itself)
fn dictionary_value_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    numerical_coercion(lhs_type, rhs_type).or_else(|| string_coercion(lhs_type, rhs_type))
}

/// Coercion rules for Dictionaries: the type that both lhs and rhs
/// can be casted to for the purpose of a computation.
///
/// It would likely be preferable to cast primitive values to
/// dictionaries, and thus avoid unpacking dictionary as well as doing
/// faster comparisons. However, the arrow compute kernels (e.g. eq)
/// don't have DictionaryArray support yet, so fall back to unpacking
/// the dictionaries
fn dictionary_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    match (lhs_type, rhs_type) {
        (
            DataType::Dictionary(_lhs_index_type, lhs_value_type),
            DataType::Dictionary(_rhs_index_type, rhs_value_type),
        ) => dictionary_value_coercion(lhs_value_type, rhs_value_type),
        (DataType::Dictionary(_index_type, value_type), _) => {
            dictionary_value_coercion(value_type, rhs_type)
        }
        (_, DataType::Dictionary(_index_type, value_type)) => {
            dictionary_value_coercion(lhs_type, value_type)
        }
        _ => None,
    }
}

/// Coercion rules for Strings: the type that both lhs and rhs can be
/// casted to for the purpose of a string computation
fn string_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, Utf8) => Some(Utf8),
        (LargeUtf8, Utf8) => Some(LargeUtf8),
        (Utf8, LargeUtf8) => Some(LargeUtf8),
        (LargeUtf8, LargeUtf8) => Some(LargeUtf8),
        _ => None,
    }
}

/// Coercion rules for Temporal columns: the type that both lhs and rhs can be
/// casted to for the purpose of a date computation
fn temporal_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, Date32(DateUnit::Day)) => Some(Date32(DateUnit::Day)),
        (Date32(DateUnit::Day), Utf8) => Some(Date32(DateUnit::Day)),
        (Utf8, Date64(DateUnit::Millisecond)) => Some(Date64(DateUnit::Millisecond)),
        (Date64(DateUnit::Millisecond), Utf8) => Some(Date64(DateUnit::Millisecond)),
        _ => None,
    }
}

/// Coercion rule for numerical types: The type that both lhs and rhs
/// can be casted to for numerical calculation, while maintaining
/// maximum precision
pub fn numerical_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    // error on any non-numeric type
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    // same type => all good
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        (Float64, _) => Some(Float64),
        (_, Float64) => Some(Float64),

        (_, Float32) => Some(Float32),
        (Float32, _) => Some(Float32),

        (Int64, _) => Some(Int64),
        (_, Int64) => Some(Int64),

        (Int32, _) => Some(Int32),
        (_, Int32) => Some(Int32),

        (Int16, _) => Some(Int16),
        (_, Int16) => Some(Int16),

        (Int8, _) => Some(Int8),
        (_, Int8) => Some(Int8),

        (UInt64, _) => Some(UInt64),
        (_, UInt64) => Some(UInt64),

        (UInt32, _) => Some(UInt32),
        (_, UInt32) => Some(UInt32),

        (UInt16, _) => Some(UInt16),
        (_, UInt16) => Some(UInt16),

        (UInt8, _) => Some(UInt8),
        (_, UInt8) => Some(UInt8),

        _ => None,
    }
}

// coercion rules for equality operations. This is a superset of all numerical coercion rules.
fn eq_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if lhs_type == rhs_type {
        // same type => equality is possible
        return Some(lhs_type.clone());
    }
    numerical_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type))
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
}

// coercion rules that assume an ordered set, such as "less than".
// These are the union of all numerical coercion rules and all string coercion rules
fn order_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if lhs_type == rhs_type {
        // same type => all good
        return Some(lhs_type.clone());
    }

    numerical_coercion(lhs_type, rhs_type)
        .or_else(|| string_coercion(lhs_type, rhs_type))
        .or_else(|| dictionary_coercion(lhs_type, rhs_type))
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
}

/// Coercion rules for all binary operators. Returns the output type
/// of applying `op` to an argument of `lhs_type` and `rhs_type`.
fn common_binary_type(
    lhs_type: &DataType,
    op: &Operator,
    rhs_type: &DataType,
) -> Result<DataType> {
    // This result MUST be compatible with `binary_coerce`
    let result = match op {
        Operator::And | Operator::Or => match (lhs_type, rhs_type) {
            // logical binary boolean operators can only be evaluated in bools
            (DataType::Boolean, DataType::Boolean) => Some(DataType::Boolean),
            _ => None,
        },
        // logical equality operators have their own rules, and always return a boolean
        Operator::Eq | Operator::NotEq => eq_coercion(lhs_type, rhs_type),
        // "like" operators operate on strings and always return a boolean
        Operator::Like | Operator::NotLike => string_coercion(lhs_type, rhs_type),
        // order-comparison operators have their own rules
        Operator::Lt | Operator::Gt | Operator::GtEq | Operator::LtEq => {
            order_coercion(lhs_type, rhs_type)
        }
        // for math expressions, the final value of the coercion is also the return type
        // because coercion favours higher information types
        Operator::Plus | Operator::Minus | Operator::Divide | Operator::Multiply => {
            numerical_coercion(lhs_type, rhs_type)
        }
        Operator::Modulus => {
            return Err(DataFusionError::NotImplemented(
                "Modulus operator is still not supported".to_string(),
            ))
        }
    };

    // re-write the error message of failed coercions to include the operator's information
    match result {
        None => Err(DataFusionError::Plan(
            format!(
                "'{:?} {} {:?}' can't be evaluated because there isn't a common type to coerce the types to",
                lhs_type, op, rhs_type
            ),
        )),
        Some(t) => Ok(t)
    }
}

/// Returns the return type of a binary operator or an error when the binary operator cannot
/// perform the computation between the argument's types, even after type coercion.
///
/// This function makes some assumptions about the underlying available computations.
pub fn binary_operator_data_type(
    lhs_type: &DataType,
    op: &Operator,
    rhs_type: &DataType,
) -> Result<DataType> {
    // validate that it is possible to perform the operation on incoming types.
    // (or the return datatype cannot be infered)
    let common_type = common_binary_type(lhs_type, op, rhs_type)?;

    match op {
        // operators that return a boolean
        Operator::Eq
        | Operator::NotEq
        | Operator::And
        | Operator::Or
        | Operator::Like
        | Operator::NotLike
        | Operator::Lt
        | Operator::Gt
        | Operator::GtEq
        | Operator::LtEq => Ok(DataType::Boolean),
        // math operations return the same value as the common coerced type
        Operator::Plus | Operator::Minus | Operator::Divide | Operator::Multiply => {
            Ok(common_type)
        }
        Operator::Modulus => Err(DataFusionError::NotImplemented(
            "Modulus operator is still not supported".to_string(),
        )),
    }
}

/// return two physical expressions that are optionally coerced to a
/// common type that the binary operator supports.
fn binary_cast(
    lhs: Arc<dyn PhysicalExpr>,
    op: &Operator,
    rhs: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
    let lhs_type = &lhs.data_type(input_schema)?;
    let rhs_type = &rhs.data_type(input_schema)?;

    let cast_type = common_binary_type(lhs_type, op, rhs_type)?;

    Ok((
        cast(lhs, input_schema, cast_type.clone())?,
        cast(rhs, input_schema, cast_type)?,
    ))
}

impl PhysicalExpr for BinaryExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        binary_operator_data_type(
            &self.left.data_type(input_schema)?,
            &self.op,
            &self.right.data_type(input_schema)?,
        )
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.left.nullable(input_schema)? || self.right.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let left_value = self.left.evaluate(batch)?;
        let right_value = self.right.evaluate(batch)?;
        let left_data_type = left_value.data_type();
        let right_data_type = right_value.data_type();

        if left_data_type != right_data_type {
            return Err(DataFusionError::Internal(format!(
                "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                self.op, left_data_type, right_data_type
            )));
        }

        let scalar_result = match (&left_value, &right_value) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(scalar)) => {
                // if left is array and right is literal - use scalar operations
                match &self.op {
                    Operator::Lt => binary_array_op_scalar!(array, scalar.clone(), lt),
                    Operator::LtEq => {
                        binary_array_op_scalar!(array, scalar.clone(), lt_eq)
                    }
                    Operator::Gt => binary_array_op_scalar!(array, scalar.clone(), gt),
                    Operator::GtEq => {
                        binary_array_op_scalar!(array, scalar.clone(), gt_eq)
                    }
                    Operator::Eq => binary_array_op_scalar!(array, scalar.clone(), eq),
                    Operator::NotEq => {
                        binary_array_op_scalar!(array, scalar.clone(), neq)
                    }
                    Operator::Like => {
                        binary_string_array_op_scalar!(array, scalar.clone(), like)
                    }
                    Operator::NotLike => {
                        binary_string_array_op_scalar!(array, scalar.clone(), nlike)
                    }
                    // if scalar operation is not supported - fallback to array implementation
                    _ => None,
                }
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Array(array)) => {
                // if right is literal and left is array - reverse operator and parameters
                match &self.op {
                    Operator::Lt => binary_array_op_scalar!(array, scalar.clone(), gt),
                    Operator::LtEq => {
                        binary_array_op_scalar!(array, scalar.clone(), gt_eq)
                    }
                    Operator::Gt => binary_array_op_scalar!(array, scalar.clone(), lt),
                    Operator::GtEq => {
                        binary_array_op_scalar!(array, scalar.clone(), lt_eq)
                    }
                    Operator::Eq => binary_array_op_scalar!(array, scalar.clone(), eq),
                    Operator::NotEq => {
                        binary_array_op_scalar!(array, scalar.clone(), neq)
                    }
                    // if scalar operation is not supported - fallback to array implementation
                    _ => None,
                }
            }
            (_, _) => None,
        };

        if let Some(result) = scalar_result {
            return result.map(|a| ColumnarValue::Array(a));
        }

        // if both arrays or both literals - extract arrays and continue execution
        let (left, right) = (
            left_value.into_array(batch.num_rows()),
            right_value.into_array(batch.num_rows()),
        );

        let result: Result<ArrayRef> = match &self.op {
            Operator::Like => binary_string_array_op!(left, right, like),
            Operator::NotLike => binary_string_array_op!(left, right, nlike),
            Operator::Lt => binary_array_op!(left, right, lt),
            Operator::LtEq => binary_array_op!(left, right, lt_eq),
            Operator::Gt => binary_array_op!(left, right, gt),
            Operator::GtEq => binary_array_op!(left, right, gt_eq),
            Operator::Eq => binary_array_op!(left, right, eq),
            Operator::NotEq => binary_array_op!(left, right, neq),
            Operator::Plus => binary_primitive_array_op!(left, right, add),
            Operator::Minus => binary_primitive_array_op!(left, right, subtract),
            Operator::Multiply => binary_primitive_array_op!(left, right, multiply),
            Operator::Divide => binary_primitive_array_op!(left, right, divide),
            Operator::And => {
                if left_data_type == DataType::Boolean {
                    boolean_op!(left, right, and)
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                        self.op,
                        left.data_type(),
                        right.data_type()
                    )));
                }
            }
            Operator::Or => {
                if left_data_type == DataType::Boolean {
                    boolean_op!(left, right, or)
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                        self.op, left_data_type, right_data_type
                    )));
                }
            }
            Operator::Modulus => Err(DataFusionError::NotImplemented(
                "Modulus operator is still not supported".to_string(),
            )),
        };
        result.map(|a| ColumnarValue::Array(a))
    }
}

/// Create a binary expression whose arguments are correctly coerced.
/// This function errors if it is not possible to coerce the arguments
/// to computational types supported by the operator.
pub fn binary(
    lhs: Arc<dyn PhysicalExpr>,
    op: Operator,
    rhs: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let (l, r) = binary_cast(lhs, &op, rhs, input_schema)?;
    Ok(Arc::new(BinaryExpr::new(l, op, r)))
}

/// Invoke a compute kernel on a primitive array and a Boolean Array
macro_rules! compute_bool_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
}

/// Binary op between primitive and boolean arrays
macro_rules! primitive_bool_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Int8 => compute_bool_array_op!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_bool_array_op!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_bool_array_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_bool_array_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_bool_array_op!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_bool_array_op!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_bool_array_op!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_bool_array_op!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_bool_array_op!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_bool_array_op!($LEFT, $RIGHT, $OP, Float64Array),
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for NULLIF/primitive/boolean operator",
                other
            ))),
        }
    }};
}

///
/// Implements NULLIF(expr1, expr2)
/// Args: 0 - left expr is any array
///       1 - if the left is equal to this expr2, then the result is NULL, otherwise left value is passed.
///
pub fn nullif_func(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "{:?} args were supplied but NULLIF takes exactly two args",
            args.len(),
        )));
    }

    // Get args0 == args1 evaluated and produce a boolean array
    let cond_array = binary_array_op!(args[0], args[1], eq)?;

    // Now, invoke nullif on the result
    primitive_bool_array_op!(args[0], *cond_array, nullif)
}

/// Currently supported types by the nullif function.
/// The order of these types correspond to the order on which coercion applies
/// This should thus be from least informative to most informative
pub static SUPPORTED_NULLIF_TYPES: &[DataType] = &[
    DataType::Boolean,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
];

/// Not expression
#[derive(Debug)]
pub struct NotExpr {
    arg: Arc<dyn PhysicalExpr>,
}

impl NotExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }
}

impl fmt::Display for NotExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NOT {}", self.arg)
    }
}

impl PhysicalExpr for NotExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let array =
                    array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "boolean_op failed to downcast array".to_owned(),
                            )
                        })?;
                Ok(ColumnarValue::Array(Arc::new(
                    arrow::compute::kernels::boolean::not(array)?,
                )))
            }
            ColumnarValue::Scalar(scalar) => {
                use std::convert::TryInto;
                let bool_value: bool = scalar.try_into()?;
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                    !bool_value,
                ))))
            }
        }
    }
}

/// Creates a unary expression NOT
///
/// # Errors
///
/// This function errors when the argument's type is not boolean
pub fn not(
    arg: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let data_type = arg.data_type(input_schema)?;
    if data_type != DataType::Boolean {
        Err(DataFusionError::Internal(format!(
            "NOT '{:?}' can't be evaluated because the expression's type is {:?}, not boolean",
            arg, data_type,
        )))
    } else {
        Ok(Arc::new(NotExpr::new(arg)))
    }
}

/// Negative expression
#[derive(Debug)]
pub struct NegativeExpr {
    arg: Arc<dyn PhysicalExpr>,
}

impl NegativeExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }
}

impl fmt::Display for NegativeExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(- {})", self.arg)
    }
}

impl PhysicalExpr for NegativeExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.arg.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let result: Result<ArrayRef> = match array.data_type() {
                    DataType::Int8 => compute_op!(array, negate, Int8Array),
                    DataType::Int16 => compute_op!(array, negate, Int16Array),
                    DataType::Int32 => compute_op!(array, negate, Int32Array),
                    DataType::Int64 => compute_op!(array, negate, Int64Array),
                    DataType::Float32 => compute_op!(array, negate, Float32Array),
                    DataType::Float64 => compute_op!(array, negate, Float64Array),
                    _ => Err(DataFusionError::Internal(format!(
                        "(- '{:?}') can't be evaluated because the expression's type is {:?}, not signed numeric",
                        self,
                        array.data_type(),
                    ))),
                };
                result.map(|a| ColumnarValue::Array(a))
            }
            ColumnarValue::Scalar(scalar) => {
                Ok(ColumnarValue::Scalar(scalar.arithmetic_negate()))
            }
        }
    }
}

/// Creates a unary expression NEGATIVE
///
/// # Errors
///
/// This function errors when the argument's type is not signed numeric
pub fn negative(
    arg: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let data_type = arg.data_type(input_schema)?;
    if !is_signed_numeric(&data_type) {
        Err(DataFusionError::Internal(
            format!(
                "(- '{:?}') can't be evaluated because the expression's type is {:?}, not signed numeric",
                arg, data_type,
            ),
        ))
    } else {
        Ok(Arc::new(NegativeExpr::new(arg)))
    }
}

/// IS NULL expression
#[derive(Debug)]
pub struct IsNullExpr {
    arg: Arc<dyn PhysicalExpr>,
}

impl IsNullExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }
}

impl fmt::Display for IsNullExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} IS NULL", self.arg)
    }
}
impl PhysicalExpr for IsNullExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                arrow::compute::is_null(array.as_ref())?,
            ))),
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(Some(scalar.is_null())),
            )),
        }
    }
}

/// Create an IS NULL expression
pub fn is_null(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(IsNullExpr::new(arg)))
}

/// IS NULL expression
#[derive(Debug)]
pub struct IsNotNullExpr {
    arg: Arc<dyn PhysicalExpr>,
}

impl IsNotNullExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }
}

impl fmt::Display for IsNotNullExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} IS NOT NULL", self.arg)
    }
}
impl PhysicalExpr for IsNotNullExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                arrow::compute::is_not_null(array.as_ref())?,
            ))),
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(Some(!scalar.is_null())),
            )),
        }
    }
}

/// Create an IS NOT NULL expression
pub fn is_not_null(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(IsNotNullExpr::new(arg)))
}

/// The CASE expression is similar to a series of nested if/else and there are two forms that
/// can be used. The first form consists of a series of boolean "when" expressions with
/// corresponding "then" expressions, and an optional "else" expression.
///
/// CASE WHEN condition THEN result
///      [WHEN ...]
///      [ELSE result]
/// END
///
/// The second form uses a base expression and then a series of "when" clauses that match on a
/// literal value.
///
/// CASE expression
///     WHEN value THEN result
///     [WHEN ...]
///     [ELSE result]
/// END
#[derive(Debug)]
pub struct CaseExpr {
    /// Optional base expression that can be compared to literal values in the "when" expressions
    expr: Option<Arc<dyn PhysicalExpr>>,
    /// One or more when/then expressions
    when_then_expr: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    /// Optional "else" expression
    else_expr: Option<Arc<dyn PhysicalExpr>>,
}

impl fmt::Display for CaseExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CASE ")?;
        if let Some(e) = &self.expr {
            write!(f, "{} ", e)?;
        }
        for (w, t) in &self.when_then_expr {
            write!(f, "WHEN {} THEN {} ", w, t)?;
        }
        if let Some(e) = &self.else_expr {
            write!(f, "ELSE {} ", e)?;
        }
        write!(f, "END")
    }
}

impl CaseExpr {
    /// Create a new CASE WHEN expression
    pub fn try_new(
        expr: Option<Arc<dyn PhysicalExpr>>,
        when_then_expr: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
        else_expr: Option<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        if when_then_expr.is_empty() {
            Err(DataFusionError::Execution(
                "There must be at least one WHEN clause".to_string(),
            ))
        } else {
            Ok(Self {
                expr,
                when_then_expr: when_then_expr.to_vec(),
                else_expr,
            })
        }
    }
}

/// Create a CASE expression
pub fn case(
    expr: Option<Arc<dyn PhysicalExpr>>,
    when_thens: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
    else_expr: Option<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(CaseExpr::try_new(expr, when_thens, else_expr)?))
}

macro_rules! if_then_else {
    ($BUILDER_TYPE:ty, $ARRAY_TYPE:ty, $BOOLS:expr, $TRUE:expr, $FALSE:expr) => {{
        let true_values = $TRUE
            .as_ref()
            .as_any()
            .downcast_ref::<$ARRAY_TYPE>()
            .expect("true_values downcast failed");

        let false_values = $FALSE
            .as_ref()
            .as_any()
            .downcast_ref::<$ARRAY_TYPE>()
            .expect("false_values downcast failed");

        let mut builder = <$BUILDER_TYPE>::new($BOOLS.len());
        for i in 0..$BOOLS.len() {
            if $BOOLS.is_null(i) {
                if false_values.is_null(i) {
                    builder.append_null()?;
                } else {
                    builder.append_value(false_values.value(i))?;
                }
            } else if $BOOLS.value(i) {
                if true_values.is_null(i) {
                    builder.append_null()?;
                } else {
                    builder.append_value(true_values.value(i))?;
                }
            } else {
                if false_values.is_null(i) {
                    builder.append_null()?;
                } else {
                    builder.append_value(false_values.value(i))?;
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

fn if_then_else(
    bools: &BooleanArray,
    true_values: ArrayRef,
    false_values: ArrayRef,
    data_type: &DataType,
) -> Result<ArrayRef> {
    match data_type {
        DataType::UInt8 => if_then_else!(
            array::UInt8Builder,
            array::UInt8Array,
            bools,
            true_values,
            false_values
        ),
        DataType::UInt16 => if_then_else!(
            array::UInt16Builder,
            array::UInt16Array,
            bools,
            true_values,
            false_values
        ),
        DataType::UInt32 => if_then_else!(
            array::UInt32Builder,
            array::UInt32Array,
            bools,
            true_values,
            false_values
        ),
        DataType::UInt64 => if_then_else!(
            array::UInt64Builder,
            array::UInt64Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Int8 => if_then_else!(
            array::Int8Builder,
            array::Int8Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Int16 => if_then_else!(
            array::Int16Builder,
            array::Int16Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Int32 => if_then_else!(
            array::Int32Builder,
            array::Int32Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Int64 => if_then_else!(
            array::Int64Builder,
            array::Int64Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Float32 => if_then_else!(
            array::Float32Builder,
            array::Float32Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Float64 => if_then_else!(
            array::Float64Builder,
            array::Float64Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Utf8 => if_then_else!(
            array::StringBuilder,
            array::StringArray,
            bools,
            true_values,
            false_values
        ),
        other => Err(DataFusionError::Execution(format!(
            "CASE does not support '{:?}'",
            other
        ))),
    }
}

macro_rules! make_null_array {
    ($TY:ty, $N:expr) => {{
        let mut builder = <$TY>::new($N);
        for _ in 0..$N {
            builder.append_null()?;
        }
        Ok(Arc::new(builder.finish()))
    }};
}

fn build_null_array(data_type: &DataType, num_rows: usize) -> Result<ArrayRef> {
    match data_type {
        DataType::UInt8 => make_null_array!(array::UInt8Builder, num_rows),
        DataType::UInt16 => make_null_array!(array::UInt16Builder, num_rows),
        DataType::UInt32 => make_null_array!(array::UInt32Builder, num_rows),
        DataType::UInt64 => make_null_array!(array::UInt64Builder, num_rows),
        DataType::Int8 => make_null_array!(array::Int8Builder, num_rows),
        DataType::Int16 => make_null_array!(array::Int16Builder, num_rows),
        DataType::Int32 => make_null_array!(array::Int32Builder, num_rows),
        DataType::Int64 => make_null_array!(array::Int64Builder, num_rows),
        DataType::Float32 => make_null_array!(array::Float32Builder, num_rows),
        DataType::Float64 => make_null_array!(array::Float64Builder, num_rows),
        DataType::Utf8 => make_null_array!(array::StringBuilder, num_rows),
        other => Err(DataFusionError::Execution(format!(
            "CASE does not support '{:?}'",
            other
        ))),
    }
}

macro_rules! array_equals {
    ($TY:ty, $L:expr, $R:expr) => {{
        let when_value = $L
            .as_ref()
            .as_any()
            .downcast_ref::<$TY>()
            .expect("array_equals downcast failed");

        let base_value = $R
            .as_ref()
            .as_any()
            .downcast_ref::<$TY>()
            .expect("array_equals downcast failed");

        let mut builder = BooleanBuilder::new(when_value.len());
        for row in 0..when_value.len() {
            if when_value.is_valid(row) && base_value.is_valid(row) {
                builder.append_value(when_value.value(row) == base_value.value(row))?;
            } else {
                builder.append_null()?;
            }
        }
        Ok(builder.finish())
    }};
}

fn array_equals(
    data_type: &DataType,
    when_value: ArrayRef,
    base_value: ArrayRef,
) -> Result<BooleanArray> {
    match data_type {
        DataType::UInt8 => array_equals!(array::UInt8Array, when_value, base_value),
        DataType::UInt16 => array_equals!(array::UInt16Array, when_value, base_value),
        DataType::UInt32 => array_equals!(array::UInt32Array, when_value, base_value),
        DataType::UInt64 => array_equals!(array::UInt64Array, when_value, base_value),
        DataType::Int8 => array_equals!(array::Int8Array, when_value, base_value),
        DataType::Int16 => array_equals!(array::Int16Array, when_value, base_value),
        DataType::Int32 => array_equals!(array::Int32Array, when_value, base_value),
        DataType::Int64 => array_equals!(array::Int64Array, when_value, base_value),
        DataType::Float32 => array_equals!(array::Float32Array, when_value, base_value),
        DataType::Float64 => array_equals!(array::Float64Array, when_value, base_value),
        DataType::Utf8 => array_equals!(array::StringArray, when_value, base_value),
        other => Err(DataFusionError::Execution(format!(
            "CASE does not support '{:?}'",
            other
        ))),
    }
}

impl CaseExpr {
    /// This function evaluates the form of CASE that matches an expression to fixed values.
    ///
    /// CASE expression
    ///     WHEN value THEN result
    ///     [WHEN ...]
    ///     [ELSE result]
    /// END
    fn case_when_with_expr(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let return_type = self.when_then_expr[0].1.data_type(&batch.schema())?;
        let expr = self.expr.as_ref().unwrap();
        let base_value = expr.evaluate(batch)?;
        let base_type = expr.data_type(&batch.schema())?;
        let base_value = base_value.into_array(batch.num_rows());

        // start with the else condition, or nulls
        let mut current_value: Option<ArrayRef> = if let Some(e) = &self.else_expr {
            Some(e.evaluate(batch)?.into_array(batch.num_rows()))
        } else {
            Some(build_null_array(&return_type, batch.num_rows())?)
        };

        // walk backwards through the when/then expressions
        for i in (0..self.when_then_expr.len()).rev() {
            let i = i as usize;

            let when_value = self.when_then_expr[i].0.evaluate(batch)?;
            let when_value = when_value.into_array(batch.num_rows());

            let then_value = self.when_then_expr[i].1.evaluate(batch)?;
            let then_value = then_value.into_array(batch.num_rows());

            // build boolean array representing which rows match the "when" value
            let when_match = array_equals(&base_type, when_value, base_value.clone())?;

            current_value = Some(if_then_else(
                &when_match,
                then_value,
                current_value.unwrap(),
                &return_type,
            )?);
        }

        Ok(ColumnarValue::Array(current_value.unwrap()))
    }

    /// This function evaluates the form of CASE where each WHEN expression is a boolean
    /// expression.
    ///
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    fn case_when_no_expr(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let return_type = self.when_then_expr[0].1.data_type(&batch.schema())?;

        // start with the else condition, or nulls
        let mut current_value: Option<ArrayRef> = if let Some(e) = &self.else_expr {
            Some(e.evaluate(batch)?.into_array(batch.num_rows()))
        } else {
            Some(build_null_array(&return_type, batch.num_rows())?)
        };

        // walk backwards through the when/then expressions
        for i in (0..self.when_then_expr.len()).rev() {
            let i = i as usize;

            let when_value = self.when_then_expr[i].0.evaluate(batch)?;
            let when_value = when_value.into_array(batch.num_rows());
            let when_value = when_value
                .as_ref()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("WHEN expression did not return a BooleanArray");

            let then_value = self.when_then_expr[i].1.evaluate(batch)?;
            let then_value = then_value.into_array(batch.num_rows());

            current_value = Some(if_then_else(
                &when_value,
                then_value,
                current_value.unwrap(),
                &return_type,
            )?);
        }

        Ok(ColumnarValue::Array(current_value.unwrap()))
    }
}

impl PhysicalExpr for CaseExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.when_then_expr[0].1.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        // this expression is nullable if any of the input expressions are nullable
        let then_nullable = self
            .when_then_expr
            .iter()
            .map(|(_, t)| t.nullable(input_schema))
            .collect::<Result<Vec<_>>>()?;
        if then_nullable.contains(&true) {
            Ok(true)
        } else if let Some(e) = &self.else_expr {
            e.nullable(input_schema)
        } else {
            Ok(false)
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if self.expr.is_some() {
            // this use case evaluates "expr" and then compares the values with the "when"
            // values
            self.case_when_with_expr(batch)
        } else {
            // The "when" conditions all evaluate to boolean in this use case and can be
            // arbitrary expressions
            self.case_when_no_expr(batch)
        }
    }
}

/// CAST expression casts an expression to a specific data type
#[derive(Debug)]
pub struct CastExpr {
    /// The expression to cast
    expr: Arc<dyn PhysicalExpr>,
    /// The data type to cast to
    cast_type: DataType,
}

/// Determine if a DataType is signed numeric or not
pub fn is_signed_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}

/// Determine if a DataType is numeric or not
pub fn is_numeric(dt: &DataType) -> bool {
    is_signed_numeric(dt)
        || match dt {
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                true
            }
            _ => false,
        }
}

impl fmt::Display for CastExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CAST({} AS {:?})", self.expr, self.cast_type)
    }
}

impl PhysicalExpr for CastExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        match value {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(kernels::cast::cast(
                &array,
                &self.cast_type,
            )?)),
            ColumnarValue::Scalar(scalar) => {
                let scalar_array = scalar.to_array();
                let cast_array = kernels::cast::cast(&scalar_array, &self.cast_type)?;
                let cast_scalar = ScalarValue::try_from_array(&cast_array, 0)?;
                Ok(ColumnarValue::Scalar(cast_scalar))
            }
        }
    }
}

/// Return a PhysicalExpression representing `expr` casted to
/// `cast_type`, if any casting is needed.
///
/// Note that such casts may lose type information
pub fn cast(
    expr: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    cast_type: DataType,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr_type = expr.data_type(input_schema)?;
    if expr_type == cast_type {
        Ok(expr.clone())
    } else if can_cast_types(&expr_type, &cast_type) {
        Ok(Arc::new(CastExpr { expr, cast_type }))
    } else {
        Err(DataFusionError::Internal(format!(
            "Unsupported CAST from {:?} to {:?}",
            expr_type, cast_type
        )))
    }
}

/// Represents a non-null literal value
#[derive(Debug)]
pub struct Literal {
    value: ScalarValue,
}

impl Literal {
    /// Create a literal value expression
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl PhysicalExpr for Literal {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.value.get_datatype())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.value.is_null())
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(self.value.clone()))
    }
}

/// Create a literal expression
pub fn lit(value: ScalarValue) -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(value))
}

/// Represents Sort operation for a column in a RecordBatch
#[derive(Clone, Debug)]
pub struct PhysicalSortExpr {
    /// Physical expression representing the column to sort
    pub expr: Arc<dyn PhysicalExpr>,
    /// Option to specify how the given column should be sorted
    pub options: SortOptions,
}

impl PhysicalSortExpr {
    /// evaluate the sort expression into SortColumn that can be passed into arrow sort kernel
    pub fn evaluate_to_sort_column(&self, batch: &RecordBatch) -> Result<SortColumn> {
        let value_to_sort = self.expr.evaluate(batch)?;
        let array_to_sort = match value_to_sort {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => {
                return Err(DataFusionError::Internal(format!(
                    "Sort operation is not applicable to scalar value {}",
                    scalar
                )));
            }
        };
        Ok(SortColumn {
            values: array_to_sort,
            options: Some(self.options),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use arrow::datatypes::*;
    use arrow::{
        array::{
            LargeStringArray, PrimitiveArray, PrimitiveBuilder, StringArray,
            StringDictionaryBuilder, Time64NanosecondArray,
        },
        util::display::array_value_to_string,
    };

    // Create a binary expression without coercion. Used here when we do not want to coerce the expressions
    // to valid types. Usage can result in an execution (after plan) error.
    fn binary_simple(
        l: Arc<dyn PhysicalExpr>,
        op: Operator,
        r: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(l, op, r))
    }

    #[test]
    fn binary_comparison() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![1, 2, 4, 8, 16]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])?;

        // expression: "a < b"
        let lt = binary_simple(col("a"), Operator::Lt, col("b"));
        let result = lt.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.len(), 5);

        let expected = vec![false, false, true, true, true];
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");
        for i in 0..5 {
            assert_eq!(result.value(i), expected[i]);
        }

        Ok(())
    }

    #[test]
    fn binary_nested() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from(vec![2, 4, 6, 8, 10]);
        let b = Int32Array::from(vec![2, 5, 4, 8, 8]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])?;

        // expression: "a < b OR a == b"
        let expr = binary_simple(
            binary_simple(col("a"), Operator::Lt, col("b")),
            Operator::Or,
            binary_simple(col("a"), Operator::Eq, col("b")),
        );
        assert_eq!("a < b OR a = b", format!("{}", expr));

        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.len(), 5);

        let expected = vec![true, true, false, true, false];
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");
        for i in 0..5 {
            assert_eq!(result.value(i), expected[i]);
        }

        Ok(())
    }

    #[test]
    fn literal_i32() -> Result<()> {
        // create an arbitrary record bacth
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a = Int32Array::from(vec![Some(1), None, Some(3), Some(4), Some(5)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // create and evaluate a literal expression
        let literal_expr = lit(ScalarValue::from(42i32));
        assert_eq!("42", format!("{}", literal_expr));

        let literal_array = literal_expr.evaluate(&batch)?.into_array(batch.num_rows());
        let literal_array = literal_array.as_any().downcast_ref::<Int32Array>().unwrap();

        // note that the contents of the literal array are unrelated to the batch contents except for the length of the array
        assert_eq!(literal_array.len(), 5); // 5 rows in the batch
        for i in 0..literal_array.len() {
            assert_eq!(literal_array.value(i), 42);
        }

        Ok(())
    }

    // runs an end-to-end test of physical type coercion:
    // 1. construct a record batch with two columns of type A and B
    //  (*_ARRAY is the Rust Arrow array type, and *_TYPE is the DataType of the elements)
    // 2. construct a physical expression of A OP B
    // 3. evaluate the expression
    // 4. verify that the resulting expression is of type C
    // 5. verify that the results of evaluation are $VEC
    macro_rules! test_coercion {
        ($A_ARRAY:ident, $A_TYPE:expr, $A_VEC:expr, $B_ARRAY:ident, $B_TYPE:expr, $B_VEC:expr, $OP:expr, $C_ARRAY:ident, $C_TYPE:expr, $VEC:expr) => {{
            let schema = Schema::new(vec![
                Field::new("a", $A_TYPE, false),
                Field::new("b", $B_TYPE, false),
            ]);
            let a = $A_ARRAY::from($A_VEC);
            let b = $B_ARRAY::from($B_VEC);
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new(a), Arc::new(b)],
            )?;

            // verify that we can construct the expression
            let expression = binary(col("a"), $OP, col("b"), &schema)?;

            // verify that the expression's type is correct
            assert_eq!(expression.data_type(&schema)?, $C_TYPE);

            // compute
            let result = expression.evaluate(&batch)?.into_array(batch.num_rows());

            // verify that the array's data_type is correct
            assert_eq!(*result.data_type(), $C_TYPE);

            // verify that the data itself is downcastable
            let result = result
                .as_any()
                .downcast_ref::<$C_ARRAY>()
                .expect("failed to downcast");
            // verify that the result itself is correct
            for (i, x) in $VEC.iter().enumerate() {
                assert_eq!(result.value(i), *x);
            }
        }};
    }

    #[test]
    fn test_type_coersion() -> Result<()> {
        test_coercion!(
            Int32Array,
            DataType::Int32,
            vec![1i32, 2i32],
            UInt32Array,
            DataType::UInt32,
            vec![1u32, 2u32],
            Operator::Plus,
            Int32Array,
            DataType::Int32,
            vec![2i32, 4i32]
        );
        test_coercion!(
            Int32Array,
            DataType::Int32,
            vec![1i32],
            UInt16Array,
            DataType::UInt16,
            vec![1u16],
            Operator::Plus,
            Int32Array,
            DataType::Int32,
            vec![2i32]
        );
        test_coercion!(
            Float32Array,
            DataType::Float32,
            vec![1f32],
            UInt16Array,
            DataType::UInt16,
            vec![1u16],
            Operator::Plus,
            Float32Array,
            DataType::Float32,
            vec![2f32]
        );
        test_coercion!(
            Float32Array,
            DataType::Float32,
            vec![2f32],
            UInt16Array,
            DataType::UInt16,
            vec![1u16],
            Operator::Multiply,
            Float32Array,
            DataType::Float32,
            vec![2f32]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["hello world", "world"],
            StringArray,
            DataType::Utf8,
            vec!["%hello%", "%hello%"],
            Operator::Like,
            BooleanArray,
            DataType::Boolean,
            vec![true, false]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["1994-12-13", "1995-01-26"],
            Date32Array,
            DataType::Date32(DateUnit::Day),
            vec![9112, 9156],
            Operator::Eq,
            BooleanArray,
            DataType::Boolean,
            vec![true, true]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["1994-12-13", "1995-01-26"],
            Date32Array,
            DataType::Date32(DateUnit::Day),
            vec![9113, 9154],
            Operator::Lt,
            BooleanArray,
            DataType::Boolean,
            vec![true, false]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["1994-12-13T12:34:56", "1995-01-26T01:23:45"],
            Date64Array,
            DataType::Date64(DateUnit::Millisecond),
            vec![787322096000, 791083425000],
            Operator::Eq,
            BooleanArray,
            DataType::Boolean,
            vec![true, true]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["1994-12-13T12:34:56", "1995-01-26T01:23:45"],
            Date64Array,
            DataType::Date64(DateUnit::Millisecond),
            vec![787322096001, 791083424999],
            Operator::Lt,
            BooleanArray,
            DataType::Boolean,
            vec![true, false]
        );
        Ok(())
    }

    #[test]
    fn test_dictionary_type_coersion() -> Result<()> {
        use DataType::*;

        // TODO: In the future, this would ideally return Dictionary types and avoid unpacking
        let lhs_type = Dictionary(Box::new(Int8), Box::new(Int32));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type), Some(Int32));

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type), None);

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Utf8;
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type), Some(Utf8));

        let lhs_type = Utf8;
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type), Some(Utf8));

        Ok(())
    }

    // Note it would be nice to use the same test_coercion macro as
    // above, but sadly the type of the values of the dictionary are
    // not encoded in the rust type of the DictionaryArray. Thus there
    // is no way at the time of this writing to create a dictionary
    // array using the `From` trait
    #[test]
    fn test_dictionary_type_to_array_coersion() -> Result<()> {
        // Test string  a string dictionary
        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let string_type = DataType::Utf8;

        // build dictionary
        let keys_builder = PrimitiveBuilder::<Int32Type>::new(10);
        let values_builder = arrow::array::StringBuilder::new(10);
        let mut dict_builder = StringDictionaryBuilder::new(keys_builder, values_builder);

        dict_builder.append("one")?;
        dict_builder.append_null()?;
        dict_builder.append("three")?;
        dict_builder.append("four")?;
        let dict_array = dict_builder.finish();

        let str_array =
            StringArray::from(vec![Some("not one"), Some("two"), None, Some("four")]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("dict", dict_type, true),
            Field::new("str", string_type, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(dict_array), Arc::new(str_array)],
        )?;

        let expected = "false\n\n\ntrue";

        // Test 1: dict = str

        // verify that we can construct the expression
        let expression = binary(col("dict"), Operator::Eq, col("str"), &schema)?;
        assert_eq!(expression.data_type(&schema)?, DataType::Boolean);

        // evaluate and verify the result type matched
        let result = expression.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.data_type(), &DataType::Boolean);

        // verify that the result itself is correct
        assert_eq!(expected, array_to_string(&result)?);

        // Test 2: now test the other direction
        // str = dict

        // verify that we can construct the expression
        let expression = binary(col("str"), Operator::Eq, col("dict"), &schema)?;
        assert_eq!(expression.data_type(&schema)?, DataType::Boolean);

        // evaluate and verify the result type matched
        let result = expression.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.data_type(), &DataType::Boolean);

        // verify that the result itself is correct
        assert_eq!(expected, array_to_string(&result)?);

        Ok(())
    }

    // Convert the array to a newline delimited string of pretty printed values
    fn array_to_string(array: &ArrayRef) -> Result<String> {
        let s = (0..array.len())
            .map(|i| array_value_to_string(array, i))
            .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?
            .join("\n");
        Ok(s)
    }

    #[test]
    fn test_coersion_error() -> Result<()> {
        let expr =
            common_binary_type(&DataType::Float32, &Operator::Plus, &DataType::Utf8);

        if let Err(DataFusionError::Plan(e)) = expr {
            assert_eq!(e, "'Float32 + Utf8' can't be evaluated because there isn't a common type to coerce the types to");
            Ok(())
        } else {
            Err(DataFusionError::Internal(
                "Coercion should have returned an DataFusionError::Internal".to_string(),
            ))
        }
    }

    // runs an end-to-end test of physical type cast
    // 1. construct a record batch with a column "a" of type A
    // 2. construct a physical expression of CAST(a AS B)
    // 3. evaluate the expression
    // 4. verify that the resulting expression is of type B
    // 5. verify that the resulting values are downcastable and correct
    macro_rules! generic_test_cast {
        ($A_ARRAY:ident, $A_TYPE:expr, $A_VEC:expr, $TYPEARRAY:ident, $TYPE:expr, $VEC:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $A_TYPE, false)]);
            let a = $A_ARRAY::from($A_VEC);
            let batch =
                RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

            // verify that we can construct the expression
            let expression = cast(col("a"), &schema, $TYPE)?;

            // verify that its display is correct
            assert_eq!(format!("CAST(a AS {:?})", $TYPE), format!("{}", expression));

            // verify that the expression's type is correct
            assert_eq!(expression.data_type(&schema)?, $TYPE);

            // compute
            let result = expression.evaluate(&batch)?.into_array(batch.num_rows());

            // verify that the array's data_type is correct
            assert_eq!(*result.data_type(), $TYPE);

            // verify that the len is correct
            assert_eq!(result.len(), $A_VEC.len());

            // verify that the data itself is downcastable
            let result = result
                .as_any()
                .downcast_ref::<$TYPEARRAY>()
                .expect("failed to downcast");

            // verify that the result itself is correct
            for (i, x) in $VEC.iter().enumerate() {
                assert_eq!(result.value(i), *x);
            }
        }};
    }

    #[test]
    fn test_cast_i32_u32() -> Result<()> {
        generic_test_cast!(
            Int32Array,
            DataType::Int32,
            vec![1, 2, 3, 4, 5],
            UInt32Array,
            DataType::UInt32,
            vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]
        );
        Ok(())
    }

    #[test]
    fn test_cast_i32_utf8() -> Result<()> {
        generic_test_cast!(
            Int32Array,
            DataType::Int32,
            vec![1, 2, 3, 4, 5],
            StringArray,
            DataType::Utf8,
            vec!["1", "2", "3", "4", "5"]
        );
        Ok(())
    }
    #[allow(clippy::redundant_clone)]
    #[test]
    fn test_cast_i64_t64() -> Result<()> {
        let original = vec![1, 2, 3, 4, 5];
        let expected: Vec<i64> = original
            .iter()
            .map(|i| Time64NanosecondArray::from(vec![*i]).value(0))
            .collect();
        generic_test_cast!(
            Int64Array,
            DataType::Int64,
            original.clone(),
            TimestampNanosecondArray,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            expected
        );
        Ok(())
    }

    #[test]
    fn invalid_cast() -> Result<()> {
        // Ensure a useful error happens at plan time if invalid casts are used
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let result = cast(col("a"), &schema, DataType::LargeBinary);
        result.expect_err("expected Invalid CAST");
        Ok(())
    }

    /// macro to perform an aggregation and verify the result.
    macro_rules! generic_test_op {
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr, $EXPECTED_DATATYPE:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $DATATYPE, false)]);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![$ARRAY])?;

            let agg =
                Arc::new(<$OP>::new(col("a"), "bla".to_string(), $EXPECTED_DATATYPE));
            let actual = aggregate(&batch, agg)?;
            let expected = ScalarValue::from($EXPECTED);

            assert_eq!(expected, actual);

            Ok(())
        }};
    }

    #[test]
    fn sum_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Sum,
            ScalarValue::from(15i64),
            DataType::Int64
        )
    }

    #[test]
    fn avg_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Avg,
            ScalarValue::from(3_f64),
            DataType::Float64
        )
    }

    #[test]
    fn max_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Max,
            ScalarValue::from(5i32),
            DataType::Int32
        )
    }

    #[test]
    fn min_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Min,
            ScalarValue::from(1i32),
            DataType::Int32
        )
    }

    #[test]
    fn max_utf8() -> Result<()> {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["d", "a", "c", "b"]));
        generic_test_op!(
            a,
            DataType::Utf8,
            Max,
            ScalarValue::Utf8(Some("d".to_string())),
            DataType::Utf8
        )
    }

    #[test]
    fn max_large_utf8() -> Result<()> {
        let a: ArrayRef = Arc::new(LargeStringArray::from(vec!["d", "a", "c", "b"]));
        generic_test_op!(
            a,
            DataType::LargeUtf8,
            Max,
            ScalarValue::LargeUtf8(Some("d".to_string())),
            DataType::LargeUtf8
        )
    }

    #[test]
    fn min_utf8() -> Result<()> {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["d", "a", "c", "b"]));
        generic_test_op!(
            a,
            DataType::Utf8,
            Min,
            ScalarValue::Utf8(Some("a".to_string())),
            DataType::Utf8
        )
    }

    #[test]
    fn min_large_utf8() -> Result<()> {
        let a: ArrayRef = Arc::new(LargeStringArray::from(vec!["d", "a", "c", "b"]));
        generic_test_op!(
            a,
            DataType::LargeUtf8,
            Min,
            ScalarValue::LargeUtf8(Some("a".to_string())),
            DataType::LargeUtf8
        )
    }

    #[test]
    fn sum_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            Sum,
            ScalarValue::from(13i64),
            DataType::Int64
        )
    }

    #[test]
    fn avg_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            Avg,
            ScalarValue::from(3.25f64),
            DataType::Float64
        )
    }

    #[test]
    fn max_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            Max,
            ScalarValue::from(5i32),
            DataType::Int32
        )
    }

    #[test]
    fn min_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            Min,
            ScalarValue::from(1i32),
            DataType::Int32
        )
    }

    #[test]
    fn sum_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(
            a,
            DataType::Int32,
            Sum,
            ScalarValue::Int64(None),
            DataType::Int64
        )
    }

    #[test]
    fn max_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(
            a,
            DataType::Int32,
            Max,
            ScalarValue::Int32(None),
            DataType::Int32
        )
    }

    #[test]
    fn min_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(
            a,
            DataType::Int32,
            Min,
            ScalarValue::Int32(None),
            DataType::Int32
        )
    }

    #[test]
    fn avg_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(
            a,
            DataType::Int32,
            Avg,
            ScalarValue::Float64(None),
            DataType::Float64
        )
    }

    #[test]
    fn sum_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(
            a,
            DataType::UInt32,
            Sum,
            ScalarValue::from(15u64),
            DataType::UInt64
        )
    }

    #[test]
    fn avg_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(
            a,
            DataType::UInt32,
            Avg,
            ScalarValue::from(3.0f64),
            DataType::Float64
        )
    }

    #[test]
    fn max_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(
            a,
            DataType::UInt32,
            Max,
            ScalarValue::from(5_u32),
            DataType::UInt32
        )
    }

    #[test]
    fn min_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(
            a,
            DataType::UInt32,
            Min,
            ScalarValue::from(1u32),
            DataType::UInt32
        )
    }

    #[test]
    fn sum_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(
            a,
            DataType::Float32,
            Sum,
            ScalarValue::from(15_f32),
            DataType::Float32
        )
    }

    #[test]
    fn avg_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(
            a,
            DataType::Float32,
            Avg,
            ScalarValue::from(3_f64),
            DataType::Float64
        )
    }

    #[test]
    fn max_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(
            a,
            DataType::Float32,
            Max,
            ScalarValue::from(5_f32),
            DataType::Float32
        )
    }

    #[test]
    fn min_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(
            a,
            DataType::Float32,
            Min,
            ScalarValue::from(1_f32),
            DataType::Float32
        )
    }

    #[test]
    fn sum_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Sum,
            ScalarValue::from(15_f64),
            DataType::Float64
        )
    }

    #[test]
    fn avg_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Avg,
            ScalarValue::from(3_f64),
            DataType::Float64
        )
    }

    #[test]
    fn max_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Max,
            ScalarValue::from(5_f64),
            DataType::Float64
        )
    }

    #[test]
    fn min_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Min,
            ScalarValue::from(1_f64),
            DataType::Float64
        )
    }

    #[test]
    fn count_elements() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Count,
            ScalarValue::from(5u64),
            DataType::UInt64
        )
    }

    #[test]
    fn count_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(3),
            None,
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            Count,
            ScalarValue::from(3u64),
            DataType::UInt64
        )
    }

    #[test]
    fn count_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![
            None, None, None, None, None, None, None, None,
        ]));
        generic_test_op!(
            a,
            DataType::Boolean,
            Count,
            ScalarValue::from(0u64),
            DataType::UInt64
        )
    }

    #[test]
    fn count_empty() -> Result<()> {
        let a: Vec<bool> = vec![];
        let a: ArrayRef = Arc::new(BooleanArray::from(a));
        generic_test_op!(
            a,
            DataType::Boolean,
            Count,
            ScalarValue::from(0u64),
            DataType::UInt64
        )
    }

    #[test]
    fn count_utf8() -> Result<()> {
        let a: ArrayRef =
            Arc::new(StringArray::from(vec!["a", "bb", "ccc", "dddd", "ad"]));
        generic_test_op!(
            a,
            DataType::Utf8,
            Count,
            ScalarValue::from(5u64),
            DataType::UInt64
        )
    }

    #[test]
    fn count_large_utf8() -> Result<()> {
        let a: ArrayRef =
            Arc::new(LargeStringArray::from(vec!["a", "bb", "ccc", "dddd", "ad"]));
        generic_test_op!(
            a,
            DataType::LargeUtf8,
            Count,
            ScalarValue::from(5u64),
            DataType::UInt64
        )
    }

    #[test]
    fn nullif_int32() -> Result<()> {
        let a = Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(3),
            None,
            None,
            Some(4),
            Some(5),
        ]);
        let a = Arc::new(a);
        let a_len = a.len();

        let lit_array = Arc::new(Int32Array::from(vec![2; a.len()]));

        let result = nullif_func(&[a, lit_array])?;

        assert_eq!(result.len(), a_len);

        let expected = Int32Array::from(vec![
            Some(1),
            None,
            None,
            None,
            Some(3),
            None,
            None,
            Some(4),
            Some(5),
        ]);
        assert_array_eq::<Int32Type>(expected, result);
        Ok(())
    }

    #[test]
    // Ensure that arrays with no nulls can also invoke NULLIF() correctly
    fn nullif_int32_nonulls() -> Result<()> {
        let a = Int32Array::from(vec![1, 3, 10, 7, 8, 1, 2, 4, 5]);
        let a = Arc::new(a);
        let a_len = a.len();

        let lit_array = Arc::new(Int32Array::from(vec![1; a.len()]));

        let result = nullif_func(&[a, lit_array])?;
        assert_eq!(result.len(), a_len);

        let expected = Int32Array::from(vec![
            None,
            Some(3),
            Some(10),
            Some(7),
            Some(8),
            None,
            Some(2),
            Some(4),
            Some(5),
        ]);
        assert_array_eq::<Int32Type>(expected, result);
        Ok(())
    }

    fn aggregate(
        batch: &RecordBatch,
        agg: Arc<dyn AggregateExpr>,
    ) -> Result<ScalarValue> {
        let mut accum = agg.create_accumulator()?;
        let expr = agg.expressions();
        let values = expr
            .iter()
            .map(|e| e.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;
        accum.update_batch(&values)?;
        accum.evaluate()
    }

    #[test]
    fn plus_op() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![1, 2, 4, 8, 16]);

        apply_arithmetic::<Int32Type>(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b)],
            Operator::Plus,
            Int32Array::from(vec![2, 4, 7, 12, 21]),
        )?;

        Ok(())
    }

    #[test]
    fn minus_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from(vec![1, 2, 4, 8, 16]));
        let b = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));

        apply_arithmetic::<Int32Type>(
            schema.clone(),
            vec![a.clone(), b.clone()],
            Operator::Minus,
            Int32Array::from(vec![0, 0, 1, 4, 11]),
        )?;

        // should handle have negative values in result (for signed)
        apply_arithmetic::<Int32Type>(
            schema,
            vec![b, a],
            Operator::Minus,
            Int32Array::from(vec![0, 0, -1, -4, -11]),
        )?;

        Ok(())
    }

    #[test]
    fn multiply_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from(vec![4, 8, 16, 32, 64]));
        let b = Arc::new(Int32Array::from(vec![2, 4, 8, 16, 32]));

        apply_arithmetic::<Int32Type>(
            schema,
            vec![a, b],
            Operator::Multiply,
            Int32Array::from(vec![8, 32, 128, 512, 2048]),
        )?;

        Ok(())
    }

    #[test]
    fn divide_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from(vec![8, 32, 128, 512, 2048]));
        let b = Arc::new(Int32Array::from(vec![2, 4, 8, 16, 32]));

        apply_arithmetic::<Int32Type>(
            schema,
            vec![a, b],
            Operator::Divide,
            Int32Array::from(vec![4, 8, 16, 32, 64]),
        )?;

        Ok(())
    }

    fn apply_arithmetic<T: ArrowNumericType>(
        schema: SchemaRef,
        data: Vec<ArrayRef>,
        op: Operator,
        expected: PrimitiveArray<T>,
    ) -> Result<()> {
        let arithmetic_op = binary_simple(col("a"), op, col("b"));
        let batch = RecordBatch::try_new(schema, data)?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());

        assert_array_eq::<T>(expected, result);

        Ok(())
    }

    fn assert_array_eq<T: ArrowNumericType>(
        expected: PrimitiveArray<T>,
        actual: ArrayRef,
    ) {
        let actual = actual
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .expect("Actual array should unwrap to type of expected array");

        for i in 0..expected.len() {
            if expected.is_null(i) {
                assert!(actual.is_null(i));
            } else {
                assert_eq!(expected.value(i), actual.value(i));
            }
        }
    }

    #[test]
    fn neg_op() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);

        let expr = not(col("a"), &schema)?;
        assert_eq!(expr.data_type(&schema)?, DataType::Boolean);
        assert_eq!(expr.nullable(&schema)?, true);

        let input = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let expected = &BooleanArray::from(vec![Some(false), None, Some(true)]);

        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(input)])?;

        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");
        assert_eq!(result, expected);

        Ok(())
    }

    /// verify that expression errors when the input expression is not a boolean.
    #[test]
    fn neg_op_not_null() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);

        let expr = not(col("a"), &schema);
        assert!(expr.is_err());

        Ok(())
    }

    #[test]
    fn is_null_op() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("foo"), None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a is null"
        let expr = is_null(col("a")).unwrap();
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");

        let expected = &BooleanArray::from(vec![false, true]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn is_not_null_op() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("foo"), None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a is not null"
        let expr = is_not_null(col("a")).unwrap();
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");

        let expected = &BooleanArray::from(vec![true, false]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr() -> Result<()> {
        let batch = case_test_batch()?;

        // CASE a WHEN 'foo' THEN 123 WHEN 'bar' THEN 456 END
        let when1 = lit(ScalarValue::Utf8(Some("foo".to_string())));
        let then1 = lit(ScalarValue::Int32(Some(123)));
        let when2 = lit(ScalarValue::Utf8(Some("bar".to_string())));
        let then2 = lit(ScalarValue::Int32(Some(456)));

        let expr = case(Some(col("a")), &[(when1, then1), (when2, then2)], None)?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("failed to downcast to Int32Array");

        let expected = &Int32Array::from(vec![Some(123), None, None, Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr_else() -> Result<()> {
        let batch = case_test_batch()?;

        // CASE a WHEN 'foo' THEN 123 WHEN 'bar' THEN 456 ELSE 999 END
        let when1 = lit(ScalarValue::Utf8(Some("foo".to_string())));
        let then1 = lit(ScalarValue::Int32(Some(123)));
        let when2 = lit(ScalarValue::Utf8(Some("bar".to_string())));
        let then2 = lit(ScalarValue::Int32(Some(456)));
        let else_value = lit(ScalarValue::Int32(Some(999)));

        let expr = case(
            Some(col("a")),
            &[(when1, then1), (when2, then2)],
            Some(else_value),
        )?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("failed to downcast to Int32Array");

        let expected =
            &Int32Array::from(vec![Some(123), Some(999), Some(999), Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_without_expr() -> Result<()> {
        let batch = case_test_batch()?;

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 END
        let when1 = binary(
            col("a"),
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("foo".to_string()))),
            &batch.schema(),
        )?;
        let then1 = lit(ScalarValue::Int32(Some(123)));
        let when2 = binary(
            col("a"),
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("bar".to_string()))),
            &batch.schema(),
        )?;
        let then2 = lit(ScalarValue::Int32(Some(456)));

        let expr = case(None, &[(when1, then1), (when2, then2)], None)?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("failed to downcast to Int32Array");

        let expected = &Int32Array::from(vec![Some(123), None, None, Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_without_expr_else() -> Result<()> {
        let batch = case_test_batch()?;

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 ELSE 999 END
        let when1 = binary(
            col("a"),
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("foo".to_string()))),
            &batch.schema(),
        )?;
        let then1 = lit(ScalarValue::Int32(Some(123)));
        let when2 = binary(
            col("a"),
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("bar".to_string()))),
            &batch.schema(),
        )?;
        let then2 = lit(ScalarValue::Int32(Some(456)));
        let else_value = lit(ScalarValue::Int32(Some(999)));

        let expr = case(None, &[(when1, then1), (when2, then2)], Some(else_value))?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("failed to downcast to Int32Array");

        let expected =
            &Int32Array::from(vec![Some(123), Some(999), Some(999), Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    fn case_test_batch() -> Result<RecordBatch> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("foo"), Some("baz"), None, Some("bar")]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;
        Ok(batch)
    }
}
