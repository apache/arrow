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

//! IS NOT NULL expression

use std::{any::Any, sync::Arc};

use arrow::compute;
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};

use crate::physical_plan::{ColumnarValue, PhysicalExpr};
use crate::{error::Result, scalar::ScalarValue};

/// IS NOT NULL expression
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

impl std::fmt::Display for IsNotNullExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
                compute::is_not_null(array.as_ref())?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::expressions::col;
    use arrow::{
        array::{BooleanArray, StringArray},
        datatypes::*,
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

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
}
