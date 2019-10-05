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

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use crate::logicalplan::{Operator, ScalarValue};
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::array::{
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::compute::kernels::boolean::{and, or};
use arrow::compute::kernels::cast::cast;
use arrow::compute::kernels::comparison::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

/// Represents the column at a given index in a RecordBatch
pub struct Column {
    index: usize,
}

impl Column {
    /// Create a new column expression
    pub fn new(index: usize) -> Self {
        Self { index }
    }
}

impl PhysicalExpr for Column {
    /// Get the name to use in a schema to represent the result of this expression
    fn name(&self) -> String {
        format!("c{}", self.index)
    }

    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        Ok(input_schema.field(self.index).data_type().clone())
    }

    /// Evaluate the expression
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        Ok(batch.column(self.index).clone())
    }
}

/// Create a column expression
pub fn col(i: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(i))
}

/// SUM aggregate expression
pub struct Sum {
    expr: Arc<dyn PhysicalExpr>,
}

impl Sum {
    /// Create a new SUM aggregate function
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl AggregateExpr for Sum {
    fn name(&self) -> String {
        "SUM".to_string()
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        match self.expr.data_type(input_schema)? {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(DataType::Int64)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Ok(DataType::UInt64)
            }
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            other => Err(ExecutionError::General(format!(
                "SUM does not support {:?}",
                other
            ))),
        }
    }

    fn evaluate_input(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.expr.evaluate(batch)
    }

    fn create_accumulator(&self) -> Rc<RefCell<dyn Accumulator>> {
        Rc::new(RefCell::new(SumAccumulator {
            expr: self.expr.clone(),
            sum: None,
        }))
    }

    fn create_combiner(&self, column_index: usize) -> Arc<dyn AggregateExpr> {
        Arc::new(Sum::new(Arc::new(Column::new(column_index))))
    }
}

macro_rules! sum_accumulate {
    ($SELF:ident, $ARRAY:ident, $ROW_INDEX:expr, $ARRAY_TYPE:ident, $SCALAR_VARIANT:ident, $TY:ty) => {{
        if let Some(array) = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>() {
            if $ARRAY.is_valid($ROW_INDEX) {
                let value = array.value($ROW_INDEX);
                $SELF.sum = match $SELF.sum {
                    Some(ScalarValue::$SCALAR_VARIANT(n)) => {
                        Some(ScalarValue::$SCALAR_VARIANT(n + value as $TY))
                    }
                    Some(_) => {
                        return Err(ExecutionError::InternalError(
                            "Unexpected ScalarValue variant".to_string(),
                        ))
                    }
                    None => Some(ScalarValue::$SCALAR_VARIANT(value as $TY)),
                };
            }
            Ok(())
        } else {
            Err(ExecutionError::General(
                "Failed to downcast array".to_string(),
            ))
        }
    }};
}

struct SumAccumulator {
    expr: Arc<dyn PhysicalExpr>,
    sum: Option<ScalarValue>,
}

impl Accumulator for SumAccumulator {
    fn accumulate(
        &mut self,
        batch: &RecordBatch,
        array: &ArrayRef,
        row_index: usize,
    ) -> Result<()> {
        match self.expr.data_type(batch.schema())? {
            DataType::Int8 => {
                sum_accumulate!(self, array, row_index, Int8Array, Int64, i64)
            }
            DataType::Int16 => {
                sum_accumulate!(self, array, row_index, Int16Array, Int64, i64)
            }
            DataType::Int32 => {
                sum_accumulate!(self, array, row_index, Int32Array, Int64, i64)
            }
            DataType::Int64 => {
                sum_accumulate!(self, array, row_index, Int64Array, Int64, i64)
            }
            DataType::UInt8 => {
                sum_accumulate!(self, array, row_index, UInt8Array, UInt64, u64)
            }
            DataType::UInt16 => {
                sum_accumulate!(self, array, row_index, UInt16Array, UInt64, u64)
            }
            DataType::UInt32 => {
                sum_accumulate!(self, array, row_index, UInt32Array, UInt64, u64)
            }
            DataType::UInt64 => {
                sum_accumulate!(self, array, row_index, UInt64Array, UInt64, u64)
            }
            DataType::Float32 => {
                sum_accumulate!(self, array, row_index, Float32Array, Float32, f32)
            }
            DataType::Float64 => {
                sum_accumulate!(self, array, row_index, Float64Array, Float64, f64)
            }
            other => Err(ExecutionError::General(format!(
                "SUM does not support {:?}",
                other
            ))),
        }
    }

    fn get_value(&self) -> Result<Option<ScalarValue>> {
        Ok(self.sum.clone())
    }
}

/// Create a sum expression
pub fn sum(expr: Arc<dyn PhysicalExpr>) -> Arc<dyn AggregateExpr> {
    Arc::new(Sum::new(expr))
}

/// AVG aggregate expression
pub struct Avg {
    expr: Arc<dyn PhysicalExpr>,
}

impl Avg {
    /// Create a new AVG aggregate function
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl AggregateExpr for Avg {
    fn name(&self) -> String {
        "AVG".to_string()
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        match self.expr.data_type(input_schema)? {
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
            other => Err(ExecutionError::General(format!(
                "AVG does not support {:?}",
                other
            ))),
        }
    }

    fn evaluate_input(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.expr.evaluate(batch)
    }

    fn create_accumulator(&self) -> Rc<RefCell<dyn Accumulator>> {
        Rc::new(RefCell::new(AvgAccumulator {
            expr: self.expr.clone(),
            sum: None,
            count: None,
        }))
    }

    fn create_combiner(&self, column_index: usize) -> Arc<dyn AggregateExpr> {
        Arc::new(Avg::new(Arc::new(Column::new(column_index))))
    }
}

macro_rules! avg_accumulate {
    ($SELF:ident, $ARRAY:ident, $ROW_INDEX:expr, $ARRAY_TYPE:ident) => {{
        if let Some(array) = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>() {
            if $ARRAY.is_valid($ROW_INDEX) {
                let value = array.value($ROW_INDEX);
                match ($SELF.sum, $SELF.count) {
                    (Some(sum), Some(count)) => {
                        $SELF.sum = Some(sum + value as f64);
                        $SELF.count = Some(count + 1);
                    }
                    _ => {
                        $SELF.sum = Some(value as f64);
                        $SELF.count = Some(1);
                    }
                };
            }
            Ok(())
        } else {
            Err(ExecutionError::General(
                "Failed to downcast array".to_string(),
            ))
        }
    }};
}
struct AvgAccumulator {
    expr: Arc<dyn PhysicalExpr>,
    sum: Option<f64>,
    count: Option<i64>,
}

impl Accumulator for AvgAccumulator {
    fn accumulate(
        &mut self,
        batch: &RecordBatch,
        array: &ArrayRef,
        row_index: usize,
    ) -> Result<()> {
        match self.expr.data_type(batch.schema())? {
            DataType::Int8 => avg_accumulate!(self, array, row_index, Int8Array),
            DataType::Int16 => avg_accumulate!(self, array, row_index, Int16Array),
            DataType::Int32 => avg_accumulate!(self, array, row_index, Int32Array),
            DataType::Int64 => avg_accumulate!(self, array, row_index, Int64Array),
            DataType::UInt8 => avg_accumulate!(self, array, row_index, UInt8Array),
            DataType::UInt16 => avg_accumulate!(self, array, row_index, UInt16Array),
            DataType::UInt32 => avg_accumulate!(self, array, row_index, UInt32Array),
            DataType::UInt64 => avg_accumulate!(self, array, row_index, UInt64Array),
            DataType::Float32 => avg_accumulate!(self, array, row_index, Float32Array),
            DataType::Float64 => avg_accumulate!(self, array, row_index, Float64Array),
            other => Err(ExecutionError::General(format!(
                "AVG does not support {:?}",
                other
            ))),
        }
    }

    fn get_value(&self) -> Result<Option<ScalarValue>> {
        match (self.sum, self.count) {
            (Some(sum), Some(count)) => {
                Ok(Some(ScalarValue::Float64(sum / count as f64)))
            }
            _ => Ok(None),
        }
    }
}

/// Create a avg expression
pub fn avg(expr: Arc<dyn PhysicalExpr>) -> Arc<dyn AggregateExpr> {
    Arc::new(Avg::new(expr))
}

/// MAX aggregate expression
pub struct Max {
    expr: Arc<dyn PhysicalExpr>,
}

impl Max {
    /// Create a new MAX aggregate function
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl AggregateExpr for Max {
    fn name(&self) -> String {
        "MAX".to_string()
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        match self.expr.data_type(input_schema)? {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(DataType::Int64)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Ok(DataType::UInt64)
            }
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            other => Err(ExecutionError::General(format!(
                "MAX does not support {:?}",
                other
            ))),
        }
    }

    fn evaluate_input(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.expr.evaluate(batch)
    }

    fn create_accumulator(&self) -> Rc<RefCell<dyn Accumulator>> {
        Rc::new(RefCell::new(MaxAccumulator {
            expr: self.expr.clone(),
            max: None,
        }))
    }

    fn create_combiner(&self, column_index: usize) -> Arc<dyn AggregateExpr> {
        Arc::new(Max::new(Arc::new(Column::new(column_index))))
    }
}

macro_rules! max_accumulate {
    ($SELF:ident, $ARRAY:ident, $ROW_INDEX:expr, $ARRAY_TYPE:ident, $SCALAR_VARIANT:ident, $TY:ty) => {{
        if let Some(array) = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>() {
            if $ARRAY.is_valid($ROW_INDEX) {
                let value = array.value($ROW_INDEX);
                $SELF.max = match $SELF.max {
                    Some(ScalarValue::$SCALAR_VARIANT(n)) => {
                        if n > (value as $TY) {
                            Some(ScalarValue::$SCALAR_VARIANT(n))
                        } else {
                            Some(ScalarValue::$SCALAR_VARIANT(value as $TY))
                        }
                    }
                    Some(_) => {
                        return Err(ExecutionError::InternalError(
                            "Unexpected ScalarValue variant".to_string(),
                        ))
                    }
                    None => Some(ScalarValue::$SCALAR_VARIANT(value as $TY)),
                };
            }
            Ok(())
        } else {
            Err(ExecutionError::General(
                "Failed to downcast array".to_string(),
            ))
        }
    }};
}
struct MaxAccumulator {
    expr: Arc<dyn PhysicalExpr>,
    max: Option<ScalarValue>,
}

impl Accumulator for MaxAccumulator {
    fn accumulate(
        &mut self,
        batch: &RecordBatch,
        array: &ArrayRef,
        row_index: usize,
    ) -> Result<()> {
        match self.expr.data_type(batch.schema())? {
            DataType::Int8 => {
                max_accumulate!(self, array, row_index, Int8Array, Int64, i64)
            }
            DataType::Int16 => {
                max_accumulate!(self, array, row_index, Int16Array, Int64, i64)
            }
            DataType::Int32 => {
                max_accumulate!(self, array, row_index, Int32Array, Int64, i64)
            }
            DataType::Int64 => {
                max_accumulate!(self, array, row_index, Int64Array, Int64, i64)
            }
            DataType::UInt8 => {
                max_accumulate!(self, array, row_index, UInt8Array, UInt64, u64)
            }
            DataType::UInt16 => {
                max_accumulate!(self, array, row_index, UInt16Array, UInt64, u64)
            }
            DataType::UInt32 => {
                max_accumulate!(self, array, row_index, UInt32Array, UInt64, u64)
            }
            DataType::UInt64 => {
                max_accumulate!(self, array, row_index, UInt64Array, UInt64, u64)
            }
            DataType::Float32 => {
                max_accumulate!(self, array, row_index, Float32Array, Float32, f32)
            }
            DataType::Float64 => {
                max_accumulate!(self, array, row_index, Float64Array, Float64, f64)
            }
            other => Err(ExecutionError::General(format!(
                "MAX does not support {:?}",
                other
            ))),
        }
    }

    fn get_value(&self) -> Result<Option<ScalarValue>> {
        Ok(self.max.clone())
    }
}

/// Create a max expression
pub fn max(expr: Arc<dyn PhysicalExpr>) -> Arc<dyn AggregateExpr> {
    Arc::new(Max::new(expr))
}

/// MIN aggregate expression
pub struct Min {
    expr: Arc<dyn PhysicalExpr>,
}

impl Min {
    /// Create a new MIN aggregate function
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl AggregateExpr for Min {
    fn name(&self) -> String {
        "MIN".to_string()
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        match self.expr.data_type(input_schema)? {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(DataType::Int64)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Ok(DataType::UInt64)
            }
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            other => Err(ExecutionError::General(format!(
                "MIN does not support {:?}",
                other
            ))),
        }
    }

    fn evaluate_input(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.expr.evaluate(batch)
    }

    fn create_accumulator(&self) -> Rc<RefCell<dyn Accumulator>> {
        Rc::new(RefCell::new(MinAccumulator {
            expr: self.expr.clone(),
            min: None,
        }))
    }

    fn create_combiner(&self, column_index: usize) -> Arc<dyn AggregateExpr> {
        Arc::new(Min::new(Arc::new(Column::new(column_index))))
    }
}

macro_rules! min_accumulate {
    ($SELF:ident, $ARRAY:ident, $ROW_INDEX:expr, $ARRAY_TYPE:ident, $SCALAR_VARIANT:ident, $TY:ty) => {{
        if let Some(array) = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>() {
            if $ARRAY.is_valid($ROW_INDEX) {
                let value = array.value($ROW_INDEX);
                $SELF.min = match $SELF.min {
                    Some(ScalarValue::$SCALAR_VARIANT(n)) => {
                        if n < (value as $TY) {
                            Some(ScalarValue::$SCALAR_VARIANT(n))
                        } else {
                            Some(ScalarValue::$SCALAR_VARIANT(value as $TY))
                        }
                    }
                    Some(_) => {
                        return Err(ExecutionError::InternalError(
                            "Unexpected ScalarValue variant".to_string(),
                        ))
                    }
                    None => Some(ScalarValue::$SCALAR_VARIANT(value as $TY)),
                };
            }
            Ok(())
        } else {
            Err(ExecutionError::General(
                "Failed to downcast array".to_string(),
            ))
        }
    }};
}
struct MinAccumulator {
    expr: Arc<dyn PhysicalExpr>,
    min: Option<ScalarValue>,
}

impl Accumulator for MinAccumulator {
    fn accumulate(
        &mut self,
        batch: &RecordBatch,
        array: &ArrayRef,
        row_index: usize,
    ) -> Result<()> {
        match self.expr.data_type(batch.schema())? {
            DataType::Int8 => {
                min_accumulate!(self, array, row_index, Int8Array, Int64, i64)
            }
            DataType::Int16 => {
                min_accumulate!(self, array, row_index, Int16Array, Int64, i64)
            }
            DataType::Int32 => {
                min_accumulate!(self, array, row_index, Int32Array, Int64, i64)
            }
            DataType::Int64 => {
                min_accumulate!(self, array, row_index, Int64Array, Int64, i64)
            }
            DataType::UInt8 => {
                min_accumulate!(self, array, row_index, UInt8Array, UInt64, u64)
            }
            DataType::UInt16 => {
                min_accumulate!(self, array, row_index, UInt16Array, UInt64, u64)
            }
            DataType::UInt32 => {
                min_accumulate!(self, array, row_index, UInt32Array, UInt64, u64)
            }
            DataType::UInt64 => {
                min_accumulate!(self, array, row_index, UInt64Array, UInt64, u64)
            }
            DataType::Float32 => {
                min_accumulate!(self, array, row_index, Float32Array, Float32, f32)
            }
            DataType::Float64 => {
                min_accumulate!(self, array, row_index, Float64Array, Float64, f64)
            }
            other => Err(ExecutionError::General(format!(
                "MIN does not support {:?}",
                other
            ))),
        }
    }

    fn get_value(&self) -> Result<Option<ScalarValue>> {
        Ok(self.min.clone())
    }
}

/// Create a min expression
pub fn min(expr: Arc<dyn PhysicalExpr>) -> Arc<dyn AggregateExpr> {
    Arc::new(Min::new(expr))
}

/// COUNT aggregate expression
/// Returns the amount of non-null values of the given expression.
pub struct Count {
    expr: Arc<dyn PhysicalExpr>,
}

impl Count {
    /// Create a new COUNT aggregate function.
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr: expr }
    }
}

impl AggregateExpr for Count {
    fn name(&self) -> String {
        "COUNT".to_string()
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn evaluate_input(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.expr.evaluate(batch)
    }

    fn create_accumulator(&self) -> Rc<RefCell<dyn Accumulator>> {
        Rc::new(RefCell::new(CountAccumulator { count: 0 }))
    }

    fn create_combiner(&self, column_index: usize) -> Arc<dyn AggregateExpr> {
        Arc::new(Sum::new(Arc::new(Column::new(column_index))))
    }
}

struct CountAccumulator {
    count: u64,
}

impl Accumulator for CountAccumulator {
    fn accumulate(
        &mut self,
        _batch: &RecordBatch,
        array: &ArrayRef,
        row_index: usize,
    ) -> Result<()> {
        if array.is_valid(row_index) {
            self.count += 1;
        }
        Ok(())
    }

    fn get_value(&self) -> Result<Option<ScalarValue>> {
        Ok(Some(ScalarValue::UInt64(self.count)))
    }
}

/// Create a count expression
pub fn count(expr: Arc<dyn PhysicalExpr>) -> Arc<dyn AggregateExpr> {
    Arc::new(Count::new(expr))
}

/// Invoke a compute kernel on a pair of arrays
macro_rules! compute_op {
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
}

/// Invoke a compute kernel on a pair of arrays
macro_rules! comparison_op {
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
            other => Err(ExecutionError::General(format!(
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

impl PhysicalExpr for BinaryExpr {
    fn name(&self) -> String {
        format!("{:?}", self.op)
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.left.data_type(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let left = self.left.evaluate(batch)?;
        let right = self.right.evaluate(batch)?;
        if left.data_type() != right.data_type() {
            return Err(ExecutionError::General(format!(
                "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                self.op,
                left.data_type(),
                right.data_type()
            )));
        }
        match &self.op {
            Operator::Lt => comparison_op!(left, right, lt),
            Operator::LtEq => comparison_op!(left, right, lt_eq),
            Operator::Gt => comparison_op!(left, right, gt),
            Operator::GtEq => comparison_op!(left, right, gt_eq),
            Operator::Eq => comparison_op!(left, right, eq),
            Operator::NotEq => comparison_op!(left, right, neq),
            Operator::And => {
                if left.data_type() == &DataType::Boolean {
                    boolean_op!(left, right, and)
                } else {
                    return Err(ExecutionError::General(format!(
                        "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                        self.op,
                        left.data_type(),
                        right.data_type()
                    )));
                }
            }
            Operator::Or => {
                if left.data_type() == &DataType::Boolean {
                    boolean_op!(left, right, or)
                } else {
                    return Err(ExecutionError::General(format!(
                        "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                        self.op,
                        left.data_type(),
                        right.data_type()
                    )));
                }
            }
            _ => Err(ExecutionError::General("Unsupported operator".to_string())),
        }
    }
}

/// Create a binary expression
pub fn binary(
    l: Arc<dyn PhysicalExpr>,
    op: Operator,
    r: Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(l, op, r))
}

/// CAST expression casts an expression to a specific data type
pub struct CastExpr {
    /// The expression to cast
    expr: Arc<dyn PhysicalExpr>,
    /// The data type to cast to
    cast_type: DataType,
}

/// Determine if a DataType is numeric or not
fn is_numeric(dt: &DataType) -> bool {
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => true,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => true,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
        _ => false,
    }
}

impl CastExpr {
    /// Create a CAST expression
    pub fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
        cast_type: DataType,
    ) -> Result<Self> {
        let expr_type = expr.data_type(input_schema)?;
        // numbers can be cast to numbers and strings
        if is_numeric(&expr_type)
            && (is_numeric(&cast_type) || cast_type == DataType::Utf8)
        {
            Ok(Self { expr, cast_type })
        } else {
            Err(ExecutionError::General(format!(
                "Invalid CAST from {:?} to {:?}",
                expr_type, cast_type
            )))
        }
    }
}

impl PhysicalExpr for CastExpr {
    fn name(&self) -> String {
        "CAST".to_string()
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let value = self.expr.evaluate(batch)?;
        Ok(cast(&value, &self.cast_type)?)
    }
}

/// Represents a non-null literal value
pub struct Literal {
    value: ScalarValue,
}

impl Literal {
    /// Create a literal value expression
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

/// Build array containing the same literal value repeated. This is necessary because the Arrow
/// memory model does not have the concept of a scalar value currently.
macro_rules! build_literal_array {
    ($BATCH:ident, $BUILDER:ident, $VALUE:expr) => {{
        let mut builder = $BUILDER::new($BATCH.num_rows());
        for _ in 0..$BATCH.num_rows() {
            builder.append_value($VALUE)?;
        }
        Ok(Arc::new(builder.finish()))
    }};
}

impl PhysicalExpr for Literal {
    fn name(&self) -> String {
        "lit".to_string()
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.value.get_datatype())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        match &self.value {
            ScalarValue::Int8(value) => build_literal_array!(batch, Int8Builder, *value),
            ScalarValue::Int16(value) => {
                build_literal_array!(batch, Int16Builder, *value)
            }
            ScalarValue::Int32(value) => {
                build_literal_array!(batch, Int32Builder, *value)
            }
            ScalarValue::Int64(value) => {
                build_literal_array!(batch, Int64Builder, *value)
            }
            ScalarValue::UInt8(value) => {
                build_literal_array!(batch, UInt8Builder, *value)
            }
            ScalarValue::UInt16(value) => {
                build_literal_array!(batch, UInt16Builder, *value)
            }
            ScalarValue::UInt32(value) => {
                build_literal_array!(batch, UInt32Builder, *value)
            }
            ScalarValue::UInt64(value) => {
                build_literal_array!(batch, UInt64Builder, *value)
            }
            ScalarValue::Float32(value) => {
                build_literal_array!(batch, Float32Builder, *value)
            }
            ScalarValue::Float64(value) => {
                build_literal_array!(batch, Float64Builder, *value)
            }
            other => Err(ExecutionError::General(format!(
                "Unsupported literal type {:?}",
                other
            ))),
        }
    }
}

/// Create a literal expression
pub fn lit(value: ScalarValue) -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use arrow::array::BinaryArray;
    use arrow::datatypes::*;

    #[test]
    fn binary_comparison() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![1, 2, 4, 8, 16]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(a), Arc::new(b)],
        )?;

        // expression: "a < b"
        let lt = binary(col(0), Operator::Lt, col(1));
        let result = lt.evaluate(&batch)?;
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
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(a), Arc::new(b)],
        )?;

        // expression: "a < b OR a == b"
        let expr = binary(
            binary(col(0), Operator::Lt, col(1)),
            Operator::Or,
            binary(col(0), Operator::Eq, col(1)),
        );
        let result = expr.evaluate(&batch)?;
        assert_eq!(result.len(), 5);

        let expected = vec![true, true, false, true, false];
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");
        for i in 0..5 {
            print!("{}", i);
            assert_eq!(result.value(i), expected[i]);
        }

        Ok(())
    }

    #[test]
    fn literal_i32() -> Result<()> {
        // create an arbitrary record bacth
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a = Int32Array::from(vec![Some(1), None, Some(3), Some(4), Some(5)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // create and evaluate a literal expression
        let literal_expr = lit(ScalarValue::Int32(42));
        let literal_array = literal_expr.evaluate(&batch)?;
        let literal_array = literal_array.as_any().downcast_ref::<Int32Array>().unwrap();

        // note that the contents of the literal array are unrelated to the batch contents except for the length of the array
        assert_eq!(literal_array.len(), 5); // 5 rows in the batch
        for i in 0..literal_array.len() {
            assert_eq!(literal_array.value(i), 42);
        }

        Ok(())
    }

    #[test]
    fn cast_i32_to_u32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        let cast = CastExpr::try_new(col(0), &schema, DataType::UInt32)?;
        let result = cast.evaluate(&batch)?;
        assert_eq!(result.len(), 5);

        let result = result
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("failed to downcast to UInt32Array");
        assert_eq!(result.value(0), 1_u32);

        Ok(())
    }

    #[test]
    fn cast_i32_to_utf8() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        let cast = CastExpr::try_new(col(0), &schema, DataType::Utf8)?;
        let result = cast.evaluate(&batch)?;
        assert_eq!(result.len(), 5);

        let result = result
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("failed to downcast to BinaryArray");
        assert_eq!(result.value(0), "1".as_bytes());

        Ok(())
    }

    #[test]
    fn invalid_cast() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        match CastExpr::try_new(col(0), &schema, DataType::Int32) {
            Err(ExecutionError::General(ref str)) => {
                assert_eq!(str, "Invalid CAST from Utf8 to Int32");
                Ok(())
            }
            _ => panic!(),
        }
    }

    #[test]
    fn sum_contract() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let sum = sum(col(0));
        assert_eq!("SUM".to_string(), sum.name());
        assert_eq!(DataType::Int64, sum.data_type(&schema)?);

        let combiner = sum.create_combiner(0);
        assert_eq!("SUM".to_string(), combiner.name());
        assert_eq!(DataType::Int64, combiner.data_type(&schema)?);

        Ok(())
    }

    #[test]
    fn max_contract() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let max = max(col(0));
        assert_eq!("MAX".to_string(), max.name());
        assert_eq!(DataType::Int64, max.data_type(&schema)?);

        let combiner = max.create_combiner(0);
        assert_eq!("MAX".to_string(), combiner.name());
        assert_eq!(DataType::Int64, combiner.data_type(&schema)?);

        Ok(())
    }

    #[test]
    fn min_contract() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let min = min(col(0));
        assert_eq!("MIN".to_string(), min.name());
        assert_eq!(DataType::Int64, min.data_type(&schema)?);

        let combiner = min.create_combiner(0);
        assert_eq!("MIN".to_string(), combiner.name());
        assert_eq!(DataType::Int64, combiner.data_type(&schema)?);

        Ok(())
    }
    #[test]
    fn avg_contract() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let avg = avg(col(0));
        assert_eq!("AVG".to_string(), avg.name());
        assert_eq!(DataType::Float64, avg.data_type(&schema)?);

        let combiner = avg.create_combiner(0);
        assert_eq!("AVG".to_string(), combiner.name());
        assert_eq!(DataType::Float64, combiner.data_type(&schema)?);

        Ok(())
    }

    #[test]
    fn sum_i32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_sum(&batch)?, Some(ScalarValue::Int64(15)));

        Ok(())
    }

    #[test]
    fn avg_i32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_avg(&batch)?, Some(ScalarValue::Float64(3_f64)));

        Ok(())
    }

    #[test]
    fn max_i32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_max(&batch)?, Some(ScalarValue::Int64(5)));

        Ok(())
    }

    #[test]
    fn min_i32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_min(&batch)?, Some(ScalarValue::Int64(1)));

        Ok(())
    }

    #[test]
    fn sum_i32_with_nulls() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![Some(1), None, Some(3), Some(4), Some(5)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_sum(&batch)?, Some(ScalarValue::Int64(13)));

        Ok(())
    }

    #[test]
    fn avg_i32_with_nulls() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![Some(1), None, Some(3), Some(4), Some(5)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_avg(&batch)?, Some(ScalarValue::Float64(3.25)));

        Ok(())
    }

    #[test]
    fn max_i32_with_nulls() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![Some(1), None, Some(3), Some(4), Some(5)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_max(&batch)?, Some(ScalarValue::Int64(5)));

        Ok(())
    }

    #[test]
    fn min_i32_with_nulls() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![Some(1), None, Some(3), Some(4), Some(5)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_min(&batch)?, Some(ScalarValue::Int64(1)));

        Ok(())
    }

    #[test]
    fn sum_i32_all_nulls() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![None, None]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_sum(&batch)?, None);

        Ok(())
    }

    #[test]
    fn max_i32_all_nulls() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![None, None]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_max(&batch)?, None);

        Ok(())
    }

    #[test]
    fn min_i32_all_nulls() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![None, None]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_min(&batch)?, None);

        Ok(())
    }

    #[test]
    fn avg_i32_all_nulls() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![None, None]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_avg(&batch)?, None);

        Ok(())
    }

    #[test]
    fn sum_u32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);

        let a = UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_sum(&batch)?, Some(ScalarValue::UInt64(15_u64)));

        Ok(())
    }

    #[test]
    fn avg_u32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);

        let a = UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_avg(&batch)?, Some(ScalarValue::Float64(3_f64)));

        Ok(())
    }

    #[test]
    fn max_u32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);

        let a = UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_max(&batch)?, Some(ScalarValue::UInt64(5_u64)));

        Ok(())
    }

    #[test]
    fn min_u32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);

        let a = UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_min(&batch)?, Some(ScalarValue::UInt64(1_u64)));

        Ok(())
    }

    #[test]
    fn sum_f32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float32, false)]);

        let a = Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_sum(&batch)?, Some(ScalarValue::Float32(15_f32)));

        Ok(())
    }

    #[test]
    fn avg_f32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float32, false)]);

        let a = Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_avg(&batch)?, Some(ScalarValue::Float64(3_f64)));

        Ok(())
    }

    #[test]
    fn max_f32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float32, false)]);

        let a = Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_max(&batch)?, Some(ScalarValue::Float32(5_f32)));

        Ok(())
    }

    #[test]
    fn min_f32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float32, false)]);

        let a = Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_min(&batch)?, Some(ScalarValue::Float32(1_f32)));

        Ok(())
    }

    #[test]
    fn sum_f64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);

        let a = Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_sum(&batch)?, Some(ScalarValue::Float64(15_f64)));

        Ok(())
    }

    #[test]
    fn avg_f64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);

        let a = Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_avg(&batch)?, Some(ScalarValue::Float64(3_f64)));

        Ok(())
    }

    #[test]
    fn max_f64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);

        let a = Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_max(&batch)?, Some(ScalarValue::Float64(5_f64)));

        Ok(())
    }

    #[test]
    fn min_f64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);

        let a = Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        assert_eq!(do_min(&batch)?, Some(ScalarValue::Float64(1_f64)));

        Ok(())
    }

    #[test]
    fn count_elements() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;
        assert_eq!(do_count(&batch)?, Some(ScalarValue::UInt64(5)));
        Ok(())
    }

    #[test]
    fn count_with_nulls() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a = Int32Array::from(vec![Some(1), Some(2), None, None, Some(3), None]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;
        assert_eq!(do_count(&batch)?, Some(ScalarValue::UInt64(3)));
        Ok(())
    }

    #[test]
    fn count_all_nulls() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);
        let a = BooleanArray::from(vec![None, None, None, None, None, None, None, None]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;
        assert_eq!(do_count(&batch)?, Some(ScalarValue::UInt64(0)));
        Ok(())
    }

    #[test]
    fn count_empty() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);
        let a = BooleanArray::from(Vec::<bool>::new());
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;
        assert_eq!(do_count(&batch)?, Some(ScalarValue::UInt64(0)));
        Ok(())
    }

    fn do_sum(batch: &RecordBatch) -> Result<Option<ScalarValue>> {
        let sum = sum(col(0));
        let accum = sum.create_accumulator();
        let input = sum.evaluate_input(batch)?;
        let mut accum = accum.borrow_mut();
        for i in 0..batch.num_rows() {
            accum.accumulate(&batch, &input, i)?;
        }
        accum.get_value()
    }

    fn do_max(batch: &RecordBatch) -> Result<Option<ScalarValue>> {
        let max = max(col(0));
        let accum = max.create_accumulator();
        let input = max.evaluate_input(batch)?;
        let mut accum = accum.borrow_mut();
        for i in 0..batch.num_rows() {
            accum.accumulate(&batch, &input, i)?;
        }
        accum.get_value()
    }

    fn do_min(batch: &RecordBatch) -> Result<Option<ScalarValue>> {
        let min = min(col(0));
        let accum = min.create_accumulator();
        let input = min.evaluate_input(batch)?;
        let mut accum = accum.borrow_mut();
        for i in 0..batch.num_rows() {
            accum.accumulate(&batch, &input, i)?;
        }
        accum.get_value()
    }

    fn do_count(batch: &RecordBatch) -> Result<Option<ScalarValue>> {
        let count = count(col(0));
        let accum = count.create_accumulator();
        let input = count.evaluate_input(batch)?;
        let mut accum = accum.borrow_mut();
        for i in 0..batch.num_rows() {
            accum.accumulate(&batch, &input, i)?;
        }
        accum.get_value()
    }

    fn do_avg(batch: &RecordBatch) -> Result<Option<ScalarValue>> {
        let avg = avg(col(0));
        let accum = avg.create_accumulator();
        let input = avg.evaluate_input(batch)?;
        let mut accum = accum.borrow_mut();
        for i in 0..batch.num_rows() {
            accum.accumulate(&batch, &input, i)?;
        }
        accum.get_value()
    }
}
