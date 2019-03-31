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

//! Evaluation of expressions against RecordBatch instances.

use std::rc::Rc;
use std::sync::Arc;

use arrow::array::*;
use arrow::compute;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::{ExecutionError, Result};
use crate::execution::context::ExecutionContext;
use crate::logicalplan::{Expr, Operator, ScalarValue};

/// Function that accepts a RecordBatch and returns an ArrayRef
pub type ArrayFunction = Rc<Fn(&RecordBatch) -> Result<ArrayRef>>;

/// Function that accepts an ArrayRef and returns an ArrayRef
pub type CompiledCastFunction = Rc<Fn(&ArrayRef) -> Result<ArrayRef>>;

/// Enumeration of supported aggregate functions
#[allow(missing_docs)] // seems like these variants are self-evident
pub enum AggregateType {
    Min,
    Max,
    Sum,
    Count,
    CountDistinct,
    Avg,
}

/// Compiled expression that can be invoked against a RecordBatch to produce an Array
pub(super) struct CompiledExpr {
    name: String,
    f: ArrayFunction,
    t: DataType,
}

impl CompiledExpr {
    /// get the name of the expression
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the data type of the expression
    pub fn data_type(&self) -> &DataType {
        &self.t
    }

    /// invoke the compiled expression
    pub fn invoke(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        (self.f)(batch)
    }
}

/// Compiled aggregate expression
pub(super) struct CompiledAggregateExpression {
    /// Aggregate type
    aggr_type: AggregateType,
    /// Arguments to the aggregate expression
    args: Vec<CompiledExpr>,
    /// Return type of aggregate expression
    t: DataType,
}

impl CompiledAggregateExpression {
    /// get the aggregate type (Min, Max, Count, etc)
    pub fn aggr_type(&self) -> &AggregateType {
        &self.aggr_type
    }

    /// Get the data type of the expression
    pub fn data_type(&self) -> &DataType {
        &self.t
    }

    /// invoke the compiled expression
    pub fn invoke(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.args[0].invoke(batch)
    }
}

/// Compiles an aggregate expression into a closure
pub(super) fn compile_aggregate_expr(
    ctx: &ExecutionContext,
    expr: &Expr,
    input_schema: &Schema,
) -> Result<CompiledAggregateExpression> {
    match *expr {
        Expr::AggregateFunction {
            ref name,
            ref args,
            ref return_type,
        } => {
            assert_eq!(1, args.len());

            let compiled_args: Vec<CompiledExpr> = args
                .iter()
                .map(|e| compile_expr(&ctx, e, input_schema))
                .collect::<Result<Vec<_>>>()?;

            let func = match name.to_lowercase().as_ref() {
                "min" => Ok(AggregateType::Min),
                "max" => Ok(AggregateType::Max),
                "count" => Ok(AggregateType::Count),
                "sum" => Ok(AggregateType::Sum),
                _ => Err(ExecutionError::General(format!(
                    "Unsupported aggregate function '{}'",
                    name
                ))),
            };

            Ok(CompiledAggregateExpression {
                args: compiled_args,
                t: return_type.clone(),
                aggr_type: func?,
            })
        }
        _ => panic!(),
    }
}

macro_rules! binary_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT.as_any().downcast_ref::<$DT>().unwrap();
        let rr = $RIGHT.as_any().downcast_ref::<$DT>().unwrap();
        Ok(Arc::new(compute::$OP(&ll, &rr)?))
    }};
}

macro_rules! math_ops {
    ($LEFT:expr, $RIGHT:expr, $BATCH:expr, $OP:ident) => {{
        let left_values = $LEFT.invoke($BATCH)?;
        let right_values = $RIGHT.invoke($BATCH)?;
        match (left_values.data_type(), right_values.data_type()) {
            (DataType::Int8, DataType::Int8) => {
                binary_op!(left_values, right_values, $OP, Int8Array)
            }
            (DataType::Int16, DataType::Int16) => {
                binary_op!(left_values, right_values, $OP, Int16Array)
            }
            (DataType::Int32, DataType::Int32) => {
                binary_op!(left_values, right_values, $OP, Int32Array)
            }
            (DataType::Int64, DataType::Int64) => {
                binary_op!(left_values, right_values, $OP, Int64Array)
            }
            (DataType::UInt8, DataType::UInt8) => {
                binary_op!(left_values, right_values, $OP, UInt8Array)
            }
            (DataType::UInt16, DataType::UInt16) => {
                binary_op!(left_values, right_values, $OP, UInt16Array)
            }
            (DataType::UInt32, DataType::UInt32) => {
                binary_op!(left_values, right_values, $OP, UInt32Array)
            }
            (DataType::UInt64, DataType::UInt64) => {
                binary_op!(left_values, right_values, $OP, UInt64Array)
            }
            (DataType::Float32, DataType::Float32) => {
                binary_op!(left_values, right_values, $OP, Float32Array)
            }
            (DataType::Float64, DataType::Float64) => {
                binary_op!(left_values, right_values, $OP, Float64Array)
            }
            (l, r) => Err(ExecutionError::ExecutionError(format!(
                "Cannot perform math operation on {:?} and {:?}",
                l, r
            ))),
        }
    }};
}

macro_rules! comparison_ops {
    ($LEFT:expr, $RIGHT:expr, $BATCH:expr, $OP:ident) => {{
        let left_values = $LEFT.invoke($BATCH)?;
        let right_values = $RIGHT.invoke($BATCH)?;
        match (left_values.data_type(), right_values.data_type()) {
            (DataType::Int8, DataType::Int8) => {
                binary_op!(left_values, right_values, $OP, Int8Array)
            }
            (DataType::Int16, DataType::Int16) => {
                binary_op!(left_values, right_values, $OP, Int16Array)
            }
            (DataType::Int32, DataType::Int32) => {
                binary_op!(left_values, right_values, $OP, Int32Array)
            }
            (DataType::Int64, DataType::Int64) => {
                binary_op!(left_values, right_values, $OP, Int64Array)
            }
            (DataType::UInt8, DataType::UInt8) => {
                binary_op!(left_values, right_values, $OP, UInt8Array)
            }
            (DataType::UInt16, DataType::UInt16) => {
                binary_op!(left_values, right_values, $OP, UInt16Array)
            }
            (DataType::UInt32, DataType::UInt32) => {
                binary_op!(left_values, right_values, $OP, UInt32Array)
            }
            (DataType::UInt64, DataType::UInt64) => {
                binary_op!(left_values, right_values, $OP, UInt64Array)
            }
            (DataType::Float32, DataType::Float32) => {
                binary_op!(left_values, right_values, $OP, Float32Array)
            }
            (DataType::Float64, DataType::Float64) => {
                binary_op!(left_values, right_values, $OP, Float64Array)
            }
            (l, r) => Err(ExecutionError::ExecutionError(format!(
                "Cannot compare {:?} with {:?}",
                l, r
            ))),
        }
    }};
}

macro_rules! boolean_ops {
    ($LEFT:expr, $RIGHT:expr, $BATCH:expr, $OP:ident) => {{
        let left_values = $LEFT.invoke($BATCH)?;
        let right_values = $RIGHT.invoke($BATCH)?;
        Ok(Arc::new(compute::$OP(
            left_values.as_any().downcast_ref::<BooleanArray>().unwrap(),
            right_values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
        )?))
    }};
}

macro_rules! literal_array {
    ($VALUE:expr, $ARRAY_TYPE:ident, $TY:ident) => {{
        let nn = *$VALUE;
        Ok(CompiledExpr {
            name: format!("{}", nn),
            f: Rc::new(move |batch: &RecordBatch| {
                let capacity = batch.num_rows();
                let mut builder = $ARRAY_TYPE::builder(capacity);
                for _ in 0..capacity {
                    builder.append_value(nn)?;
                }
                let array = builder.finish();
                Ok(Arc::new(array) as ArrayRef)
            }),
            t: DataType::$TY,
        })
    }};
}

/// Compiles a scalar expression into a closure
pub(super) fn compile_expr(
    ctx: &ExecutionContext,
    expr: &Expr,
    input_schema: &Schema,
) -> Result<CompiledExpr> {
    match expr {
        &Expr::Literal(ref value) => match value {
            //NOTE: this is a temporary hack .. due to the way expressions like 'a > 1'
            // are evaluated, currently the left and right are evaluated
            // separately and must result in arrays and then the '>' operator
            // is evaluated against the two arrays. This works but is dumb ...
            // I intend to optimize this soon to add special handling for
            // binary expressions that involve literal values to avoid creating arrays of
            // literals filed as https://github.com/andygrove/datafusion/issues/191
            ScalarValue::Int8(n) => literal_array!(n, Int8Array, Int8),
            ScalarValue::Int16(n) => literal_array!(n, Int16Array, Int16),
            ScalarValue::Int32(n) => literal_array!(n, Int32Array, Int32),
            ScalarValue::Int64(n) => literal_array!(n, Int64Array, Int64),
            ScalarValue::UInt8(n) => literal_array!(n, UInt8Array, UInt8),
            ScalarValue::UInt16(n) => literal_array!(n, UInt16Array, UInt16),
            ScalarValue::UInt32(n) => literal_array!(n, UInt32Array, UInt32),
            ScalarValue::UInt64(n) => literal_array!(n, UInt64Array, UInt64),
            ScalarValue::Float32(n) => literal_array!(n, Float32Array, Float32),
            ScalarValue::Float64(n) => literal_array!(n, Float64Array, Float64),
            other => Err(ExecutionError::ExecutionError(format!(
                "Unsupported literal type {:?}",
                other
            ))),
        },
        &Expr::Column(index) => {
            if index < input_schema.fields().len() {
                Ok(CompiledExpr {
                    name: input_schema.field(index).name().clone(),
                    f: Rc::new(move |batch: &RecordBatch| {
                        Ok((*batch.column(index)).clone())
                    }),
                    t: input_schema.field(index).data_type().clone(),
                })
            } else {
                Err(ExecutionError::InvalidColumn(format!(
                    "Column index {} out of bounds",
                    index
                )))
            }
        }
        &Expr::Cast {
            ref expr,
            ref data_type,
        } => match expr.as_ref() {
            &Expr::Column(index) => {
                let col = input_schema.field(index);
                let dt = data_type.clone();
                Ok(CompiledExpr {
                    name: col.name().clone(),
                    t: col.data_type().clone(),
                    f: Rc::new(move |batch: &RecordBatch| {
                        compute::cast(batch.column(index), &dt)
                            .map_err(|e| ExecutionError::ArrowError(e))
                    }),
                })
            }
            other => {
                let compiled_expr = compile_expr(ctx, other, input_schema)?;
                let dt = data_type.clone();
                Ok(CompiledExpr {
                    name: "CAST".to_string(),
                    t: data_type.clone(),
                    f: Rc::new(move |batch: &RecordBatch| {
                        // evaluate the expression
                        let array = compiled_expr.invoke(batch)?;
                        // cast the result
                        compute::cast(&array, &dt)
                            .map_err(|e| ExecutionError::ArrowError(e))
                    }),
                })
            }
        },
        &Expr::BinaryExpr {
            ref left,
            ref op,
            ref right,
        } => {
            let left_expr = compile_expr(ctx, left, input_schema)?;
            let right_expr = compile_expr(ctx, right, input_schema)?;
            let name = format!("{:?} {:?} {:?}", left, op, right);
            let op_type = left_expr.data_type().clone();
            match op {
                &Operator::Eq => Ok(CompiledExpr {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        comparison_ops!(left_expr, right_expr, batch, eq)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::NotEq => Ok(CompiledExpr {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        comparison_ops!(left_expr, right_expr, batch, neq)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::Lt => Ok(CompiledExpr {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        comparison_ops!(left_expr, right_expr, batch, lt)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::LtEq => Ok(CompiledExpr {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        comparison_ops!(left_expr, right_expr, batch, lt_eq)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::Gt => Ok(CompiledExpr {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        comparison_ops!(left_expr, right_expr, batch, gt)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::GtEq => Ok(CompiledExpr {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        comparison_ops!(left_expr, right_expr, batch, gt_eq)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::And => Ok(CompiledExpr {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        boolean_ops!(left_expr, right_expr, batch, and)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::Or => Ok(CompiledExpr {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        boolean_ops!(left_expr, right_expr, batch, or)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::Plus => Ok(CompiledExpr {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        math_ops!(left_expr, right_expr, batch, add)
                    }),
                    t: op_type,
                }),
                &Operator::Minus => Ok(CompiledExpr {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        math_ops!(left_expr, right_expr, batch, subtract)
                    }),
                    t: op_type,
                }),
                &Operator::Multiply => Ok(CompiledExpr {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        math_ops!(left_expr, right_expr, batch, multiply)
                    }),
                    t: op_type,
                }),
                &Operator::Divide => Ok(CompiledExpr {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        math_ops!(left_expr, right_expr, batch, divide)
                    }),
                    t: op_type,
                }),
                other => Err(ExecutionError::NotImplemented(format!(
                    "Unsupported operator: {:?}",
                    other
                ))),
            }
        }
        other => Err(ExecutionError::NotImplemented(format!(
            "Unsupported expression {:?}",
            other
        ))),
    }
}
