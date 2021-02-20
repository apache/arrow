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

//! Not expression

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use super::ColumnarValue;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::PhysicalExpr;
use crate::scalar::ScalarValue;
use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

/// Not expression
#[derive(Debug)]
pub struct NotExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
}

impl NotExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl fmt::Display for NotExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NOT {}", self.arg)
    }
}

impl PhysicalExpr for NotExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::physical_plan::expressions::col;
    use arrow::datatypes::*;

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
    fn neg_op_not_null() {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);

        let expr = not(col("a"), &schema);
        assert!(expr.is_err());
    }
}
