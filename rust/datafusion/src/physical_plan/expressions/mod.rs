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

use std::any::Any;
use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use super::ColumnarValue;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use crate::scalar::ScalarValue;
use arrow::array::{self, Array, BooleanBuilder, LargeStringArray};
use arrow::compute;
use arrow::compute::kernels::boolean::nullif;
use arrow::compute::kernels::comparison::{eq, eq_utf8};
use arrow::compute::kernels::sort::{SortColumn, SortOptions};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::{
    array::{
        ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
        TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::Field,
};

#[macro_use]
mod binary;
mod cast;
mod coercion;
mod column;
mod in_list;
mod negative;
mod not;
pub use binary::{binary, binary_operator_data_type, BinaryExpr};
pub use cast::{cast, CastExpr};
pub use column::{col, Column};
pub use in_list::{in_list, InListExpr};
pub use negative::{negative, NegativeExpr};
pub use not::{not, NotExpr};

/// returns the name of the state
pub fn format_state_name(name: &str, state_name: &str) -> String {
    format!("{}[{}]", name, state_name)
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

/// IS NULL expression
#[derive(Debug)]
pub struct IsNullExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
}

impl IsNullExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl fmt::Display for IsNullExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} IS NULL", self.arg)
    }
}
impl PhysicalExpr for IsNullExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

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
    /// The input expression
    arg: Arc<dyn PhysicalExpr>,
}

impl IsNotNullExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl fmt::Display for IsNotNullExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} IS NOT NULL", self.arg)
    }
}

impl PhysicalExpr for IsNotNullExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    /// Optional base expression that can be compared to literal values in the "when" expressions
    pub fn expr(&self) -> &Option<Arc<dyn PhysicalExpr>> {
        &self.expr
    }

    /// One or more when/then expressions
    pub fn when_then_expr(&self) -> &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)] {
        &self.when_then_expr
    }

    /// Optional "else" expression
    pub fn else_expr(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.else_expr.as_ref()
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
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    /// Get the scalar value
    pub fn value(&self) -> &ScalarValue {
        &self.value
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl PhysicalExpr for Literal {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

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
    use crate::{error::Result, logical_plan::Operator};
    use arrow::array::{LargeStringArray, PrimitiveArray, StringArray};
    use arrow::datatypes::*;

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
