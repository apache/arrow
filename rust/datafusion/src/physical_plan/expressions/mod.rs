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

use std::sync::Arc;

use super::ColumnarValue;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::PhysicalExpr;
use arrow::array::Array;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampNanosecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::kernels::boolean::nullif;
use arrow::compute::kernels::comparison::{eq, eq_utf8};
use arrow::compute::kernels::sort::{SortColumn, SortOptions};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

mod average;
#[macro_use]
mod binary;
mod case;
mod cast;
mod coercion;
mod column;
mod count;
mod in_list;
mod is_not_null;
mod is_null;
mod literal;
mod min_max;
mod negative;
mod not;
mod sum;

pub use average::{avg_return_type, Avg, AvgAccumulator};
pub use binary::{binary, binary_operator_data_type, BinaryExpr};
pub use case::{case, CaseExpr};
pub use cast::{cast, CastExpr};
pub use column::{col, Column};
pub use count::Count;
pub use in_list::{in_list, InListExpr};
pub use is_not_null::{is_not_null, IsNotNullExpr};
pub use is_null::{is_null, IsNullExpr};
pub use literal::{lit, Literal};
pub use min_max::{Max, Min};
pub use negative::{negative, NegativeExpr};
pub use not::{not, NotExpr};
pub use sum::{sum_return_type, Sum};

/// returns the name of the state
pub fn format_state_name(name: &str, state_name: &str) -> String {
    format!("{}[{}]", name, state_name)
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
    use crate::{error::Result, physical_plan::AggregateExpr, scalar::ScalarValue};
    use arrow::array::PrimitiveArray;
    use arrow::datatypes::*;

    /// macro to perform an aggregation and verify the result.
    #[macro_export]
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

    pub fn aggregate(
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
}
