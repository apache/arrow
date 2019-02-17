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

use std::rc::Rc;
use std::sync::Arc;

use arrow::array::*;
use arrow::compute;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use super::super::logicalplan::{Expr, Operator, ScalarValue};
use super::context::ExecutionContext;
use super::error::{ExecutionError, Result};

/// Compiled Expression (basically just a closure to evaluate the expression at runtime)
pub type CompiledExpr = Rc<Fn(&RecordBatch) -> Result<ArrayRef>>;

pub type CompiledCastFunction = Rc<Fn(&ArrayRef) -> Result<ArrayRef>>;

pub enum AggregateType {
    Min,
    Max,
    Sum,
    Count,
    CountDistinct,
    Avg,
}

/// Runtime expression
pub enum RuntimeExpr {
    Compiled {
        name: String,
        f: CompiledExpr,
        t: DataType,
    },
    AggregateFunction {
        name: String,
        f: AggregateType,
        args: Vec<CompiledExpr>,
        t: DataType,
    },
}

impl RuntimeExpr {
    pub fn get_func(&self) -> CompiledExpr {
        match self {
            &RuntimeExpr::Compiled { ref f, .. } => f.clone(),
            _ => panic!(),
        }
    }

    pub fn get_name(&self) -> &String {
        match self {
            &RuntimeExpr::Compiled { ref name, .. } => name,
            &RuntimeExpr::AggregateFunction { ref name, .. } => name,
        }
    }

    pub fn get_type(&self) -> DataType {
        match self {
            &RuntimeExpr::Compiled { ref t, .. } => t.clone(),
            &RuntimeExpr::AggregateFunction { ref t, .. } => t.clone(),
        }
    }
}

/// Compiles a scalar expression into a closure
pub fn compile_expr(
    ctx: &ExecutionContext,
    expr: &Expr,
    input_schema: &Schema,
) -> Result<RuntimeExpr> {
    match *expr {
        Expr::AggregateFunction {
            ref name,
            ref args,
            ref return_type,
        } => {
            assert_eq!(1, args.len());

            let compiled_args: Result<Vec<RuntimeExpr>> = args
                .iter()
                .map(|e| compile_scalar_expr(&ctx, e, input_schema))
                .collect();

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

            Ok(RuntimeExpr::AggregateFunction {
                name: name.to_string(),
                f: func?,
                args: compiled_args?
                    .iter()
                    .map(|e| e.get_func().clone())
                    .collect(),
                t: return_type.clone(),
            })
        }
        _ => Ok(compile_scalar_expr(&ctx, expr, input_schema)?),
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
        let left_values = $LEFT.get_func()($BATCH)?;
        let right_values = $RIGHT.get_func()($BATCH)?;
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
            _ => Err(ExecutionError::ExecutionError(format!("math_ops"))),
        }
    }};
}

macro_rules! comparison_ops {
    ($LEFT:expr, $RIGHT:expr, $BATCH:expr, $OP:ident) => {{
        let left_values = $LEFT.get_func()($BATCH)?;
        let right_values = $RIGHT.get_func()($BATCH)?;
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
            //TODO other types
            _ => Err(ExecutionError::ExecutionError(format!("comparison_ops"))),
        }
    }};
}

macro_rules! boolean_ops {
    ($LEFT:expr, $RIGHT:expr, $BATCH:expr, $OP:ident) => {{
        let left_values = $LEFT.get_func()($BATCH)?;
        let right_values = $RIGHT.get_func()($BATCH)?;
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
        Ok(RuntimeExpr::Compiled {
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

/// Casts a column to an array with a different data type
macro_rules! cast_column {
    ($INDEX:expr, $FROM_TYPE:ty, $TO_TYPE:ident, $DT:ty) => {{
        Rc::new(move |batch: &RecordBatch| {
            // get data and cast to known type
            match batch.column($INDEX).as_any().downcast_ref::<$FROM_TYPE>() {
                Some(array) => {
                    // create builder for desired type
                    let mut builder = $TO_TYPE::builder(batch.num_rows());
                    for i in 0..batch.num_rows() {
                        if array.is_null(i) {
                            builder.append_null()?;
                        } else {
                            builder.append_value(array.value(i) as $DT)?;
                        }
                    }
                    Ok(Arc::new(builder.finish()) as ArrayRef)
                }
                None => Err(ExecutionError::InternalError(format!(
                    "Column at index {} is not of expected type",
                    $INDEX
                ))),
            }
        })
    }};
}

macro_rules! cast_column_outer {
    ($INDEX:expr, $FROM_TYPE:ty, $TO_TYPE:expr) => {{
        match $TO_TYPE {
            DataType::UInt8 => cast_column!($INDEX, $FROM_TYPE, UInt8Array, u8),
            DataType::UInt16 => cast_column!($INDEX, $FROM_TYPE, UInt16Array, u16),
            DataType::UInt32 => cast_column!($INDEX, $FROM_TYPE, UInt32Array, u32),
            DataType::UInt64 => cast_column!($INDEX, $FROM_TYPE, UInt64Array, u64),
            DataType::Int8 => cast_column!($INDEX, $FROM_TYPE, Int8Array, i8),
            DataType::Int16 => cast_column!($INDEX, $FROM_TYPE, Int16Array, i16),
            DataType::Int32 => cast_column!($INDEX, $FROM_TYPE, Int32Array, i32),
            DataType::Int64 => cast_column!($INDEX, $FROM_TYPE, Int64Array, i64),
            DataType::Float32 => cast_column!($INDEX, $FROM_TYPE, Float32Array, f32),
            DataType::Float64 => cast_column!($INDEX, $FROM_TYPE, Float64Array, f64),
            _ => unimplemented!(),
        }
    }};
}

/// Compiles a scalar expression into a closure
pub fn compile_scalar_expr(
    ctx: &ExecutionContext,
    expr: &Expr,
    input_schema: &Schema,
) -> Result<RuntimeExpr> {
    match expr {
        &Expr::Literal(ref value) => match value {
            //NOTE: this is a temporary hack .. due to the way expressions like 'a > 1' are
            // evaluated, currently the left and right are evaluated separately and must result
            // in arrays and then the '>' operator is evaluated against the two arrays. This works
            // but is dumb ... I intend to optimize this soon to add special handling for
            // binary expressions that involve literal values to avoid creating arrays of literals
            // filed as https://github.com/andygrove/datafusion/issues/191
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
                "No support for literal type {:?}",
                other
            ))),
        },
        &Expr::Column(index) => Ok(RuntimeExpr::Compiled {
            name: input_schema.field(index).name().clone(),
            f: Rc::new(move |batch: &RecordBatch| Ok((*batch.column(index)).clone())),
            t: input_schema.field(index).data_type().clone(),
        }),
        &Expr::Cast {
            ref expr,
            ref data_type,
        } => match expr.as_ref() {
            &Expr::Column(index) => {
                let col = input_schema.field(index);
                Ok(RuntimeExpr::Compiled {
                    name: col.name().clone(),
                    t: col.data_type().clone(),
                    f: match col.data_type() {
                        DataType::Int8 => {
                            cast_column_outer!(index, Int8Array, &data_type)
                        }
                        DataType::Int16 => {
                            cast_column_outer!(index, Int16Array, &data_type)
                        }
                        DataType::Int32 => {
                            cast_column_outer!(index, Int32Array, &data_type)
                        }
                        DataType::Int64 => {
                            cast_column_outer!(index, Int64Array, &data_type)
                        }
                        DataType::UInt8 => {
                            cast_column_outer!(index, UInt8Array, &data_type)
                        }
                        DataType::UInt16 => {
                            cast_column_outer!(index, UInt16Array, &data_type)
                        }
                        DataType::UInt32 => {
                            cast_column_outer!(index, UInt32Array, &data_type)
                        }
                        DataType::UInt64 => {
                            cast_column_outer!(index, UInt64Array, &data_type)
                        }
                        DataType::Float32 => {
                            cast_column_outer!(index, Float32Array, &data_type)
                        }
                        DataType::Float64 => {
                            cast_column_outer!(index, Float64Array, &data_type)
                        }
                        _ => panic!("unsupported CAST operation"), /*TODO */
                                                                   /*Err(ExecutionError::NotImplemented(format!(
                                                                       "CAST column from {:?} to {:?}",
                                                                       col.data_type(),
                                                                       data_type
                                                                   )))*/
                    },
                })
            }
            &Expr::Literal(ref value) => {
                //NOTE this is all very inefficient and needs to be optimized - tracking
                // issue is https://github.com/andygrove/datafusion/issues/191
                match value {
                    ScalarValue::Int64(n) => {
                        let nn = *n;
                        match data_type {
                            DataType::Float64 => Ok(RuntimeExpr::Compiled {
                                name: "lit".to_string(),
                                f: Rc::new(move |batch: &RecordBatch| {
                                    let mut b = Float64Array::builder(batch.num_rows());
                                    for _ in 0..batch.num_rows() {
                                        b.append_value(nn as f64)?;
                                    }
                                    Ok(Arc::new(b.finish()) as ArrayRef)
                                }),
                                t: data_type.clone(),
                            }),
                            other => Err(ExecutionError::NotImplemented(format!(
                                "CAST from Int64 to {:?}",
                                other
                            ))),
                        }
                    }
                    other => Err(ExecutionError::NotImplemented(format!(
                        "CAST from {:?} to {:?}",
                        other, data_type
                    ))),
                }
            }
            other => Err(ExecutionError::General(format!(
                "CAST not implemented for expression {:?}",
                other
            ))),
        },
        &Expr::BinaryExpr {
            ref left,
            ref op,
            ref right,
        } => {
            let left_expr = compile_scalar_expr(ctx, left, input_schema)?;
            let right_expr = compile_scalar_expr(ctx, right, input_schema)?;
            let name = format!("{:?} {:?} {:?}", left, op, right);
            let op_type = left_expr.get_type().clone();
            match op {
                &Operator::Eq => Ok(RuntimeExpr::Compiled {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        comparison_ops!(left_expr, right_expr, batch, eq)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::NotEq => Ok(RuntimeExpr::Compiled {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        comparison_ops!(left_expr, right_expr, batch, neq)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::Lt => Ok(RuntimeExpr::Compiled {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        comparison_ops!(left_expr, right_expr, batch, lt)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::LtEq => Ok(RuntimeExpr::Compiled {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        comparison_ops!(left_expr, right_expr, batch, lt_eq)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::Gt => Ok(RuntimeExpr::Compiled {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        comparison_ops!(left_expr, right_expr, batch, gt)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::GtEq => Ok(RuntimeExpr::Compiled {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        comparison_ops!(left_expr, right_expr, batch, gt_eq)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::And => Ok(RuntimeExpr::Compiled {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        boolean_ops!(left_expr, right_expr, batch, and)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::Or => Ok(RuntimeExpr::Compiled {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        boolean_ops!(left_expr, right_expr, batch, or)
                    }),
                    t: DataType::Boolean,
                }),
                &Operator::Plus => Ok(RuntimeExpr::Compiled {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        math_ops!(left_expr, right_expr, batch, add)
                    }),
                    t: op_type,
                }),
                &Operator::Minus => Ok(RuntimeExpr::Compiled {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        math_ops!(left_expr, right_expr, batch, subtract)
                    }),
                    t: op_type,
                }),
                &Operator::Multiply => Ok(RuntimeExpr::Compiled {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        math_ops!(left_expr, right_expr, batch, multiply)
                    }),
                    t: op_type,
                }),
                &Operator::Divide => Ok(RuntimeExpr::Compiled {
                    name,
                    f: Rc::new(move |batch: &RecordBatch| {
                        math_ops!(left_expr, right_expr, batch, divide)
                    }),
                    t: op_type,
                }),
                other => Err(ExecutionError::ExecutionError(format!(
                    "operator: {:?}",
                    other
                ))),
            }
        }
        other => Err(ExecutionError::ExecutionError(format!(
            "expression {:?}",
            other
        ))),
    }
}
