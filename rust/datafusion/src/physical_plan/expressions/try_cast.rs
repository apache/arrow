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

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use super::ColumnarValue;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::PhysicalExpr;
use crate::scalar::ScalarValue;
use arrow::compute;
use arrow::compute::kernels;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use compute::can_cast_types;

/// TRY_CAST expression casts an expression to a specific data type and retuns NULL on invalid cast
#[derive(Debug)]
pub struct TryCastExpr {
    /// The expression to cast
    expr: Arc<dyn PhysicalExpr>,
    /// The data type to cast to
    cast_type: DataType,
}

impl TryCastExpr {
    /// Create a new CastExpr
    pub fn new(expr: Arc<dyn PhysicalExpr>, cast_type: DataType) -> Self {
        Self { expr, cast_type }
    }

    /// The expression to cast
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// The data type to cast to
    pub fn cast_type(&self) -> &DataType {
        &self.cast_type
    }
}

impl fmt::Display for TryCastExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CAST({} AS {:?})", self.expr, self.cast_type)
    }
}

impl PhysicalExpr for TryCastExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
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
pub fn try_cast(
    expr: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    cast_type: DataType,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr_type = expr.data_type(input_schema)?;
    if expr_type == cast_type {
        Ok(expr.clone())
    } else if can_cast_types(&expr_type, &cast_type) {
        Ok(Arc::new(TryCastExpr::new(expr, cast_type)))
    } else {
        Err(DataFusionError::Internal(format!(
            "Unsupported CAST from {:?} to {:?}",
            expr_type, cast_type
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::physical_plan::expressions::col;
    use arrow::array::{StringArray, Time64NanosecondArray};
    use arrow::{
        array::{Array, Int32Array, Int64Array, TimestampNanosecondArray, UInt32Array},
        datatypes::*,
    };

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
            let expression = try_cast(col("a"), &schema, $TYPE)?;

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
                match x {
                    Some(x) => assert_eq!(result.value(i), *x),
                    None => assert!(!result.is_valid(i)),
                }
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
            vec![
                Some(1_u32),
                Some(2_u32),
                Some(3_u32),
                Some(4_u32),
                Some(5_u32)
            ]
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
            vec![Some("1"), Some("2"), Some("3"), Some("4"), Some("5")]
        );
        Ok(())
    }

    #[test]
    fn test_try_cast_utf8_i32() -> Result<()> {
        generic_test_cast!(
            StringArray,
            DataType::Utf8,
            vec!["a", "2", "3", "b", "5"],
            Int32Array,
            DataType::Int32,
            vec![None, Some(2), Some(3), None, Some(5)]
        );
        Ok(())
    }

    #[allow(clippy::redundant_clone)]
    #[test]
    fn test_cast_i64_t64() -> Result<()> {
        let original = vec![1, 2, 3, 4, 5];
        let expected: Vec<Option<i64>> = original
            .iter()
            .map(|i| Some(Time64NanosecondArray::from(vec![*i]).value(0)))
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
    fn invalid_cast() {
        // Ensure a useful error happens at plan time if invalid casts are used
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let result = try_cast(col("a"), &schema, DataType::LargeBinary);
        result.expect_err("expected Invalid CAST");
    }
}
