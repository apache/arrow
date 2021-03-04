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

use std::sync::Arc;

use super::ColumnarValue;
use crate::error::{DataFusionError, Result};
use crate::scalar::ScalarValue;
use arrow::array::Array;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampNanosecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::kernels::boolean::nullif;
use arrow::compute::kernels::comparison::{eq, eq_scalar, eq_utf8, eq_utf8_scalar};
use arrow::datatypes::{DataType, TimeUnit};

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
        Ok(Arc::new($OP(&ll, &rr)?) as ArrayRef)
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

/// Implements NULLIF(expr1, expr2)
/// Args: 0 - left expr is any array
///       1 - if the left is equal to this expr2, then the result is NULL, otherwise left value is passed.
///
pub fn nullif_func(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "{:?} args were supplied but NULLIF takes exactly two args",
            args.len(),
        )));
    }

    let (lhs, rhs) = (&args[0], &args[1]);

    match (lhs, rhs) {
        (ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)) => {
            let cond_array = binary_array_op_scalar!(lhs, rhs.clone(), eq).unwrap()?;

            let array = primitive_bool_array_op!(lhs, *cond_array, nullif)?;

            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)) => {
            // Get args0 == args1 evaluated and produce a boolean array
            let cond_array = binary_array_op!(lhs, rhs, eq)?;

            // Now, invoke nullif on the result
            let array = primitive_bool_array_op!(lhs, *cond_array, nullif)?;
            Ok(ColumnarValue::Array(array))
        }
        _ => Err(DataFusionError::NotImplemented(
            "nullif does not support a literal as first argument".to_string(),
        )),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

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
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));

        let result = nullif_func(&[a, lit_array])?;
        let result = result.into_array(0);

        let expected = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            None,
            None,
            Some(3),
            None,
            None,
            Some(4),
            Some(5),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    // Ensure that arrays with no nulls can also invoke NULLIF() correctly
    fn nullif_int32_nonulls() -> Result<()> {
        let a = Int32Array::from(vec![1, 3, 10, 7, 8, 1, 2, 4, 5]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Int32(Some(1i32)));

        let result = nullif_func(&[a, lit_array])?;
        let result = result.into_array(0);

        let expected = Arc::new(Int32Array::from(vec![
            None,
            Some(3),
            Some(10),
            Some(7),
            Some(8),
            None,
            Some(2),
            Some(4),
            Some(5),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }
}
