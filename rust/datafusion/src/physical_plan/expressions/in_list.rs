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

//! InList expression

use std::any::Any;
use std::sync::Arc;

use arrow::array::GenericStringArray;
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringOffsetSizeTrait, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};

use crate::error::Result;
use crate::physical_plan::{ColumnarValue, PhysicalExpr};
use crate::scalar::ScalarValue;

/// InList
#[derive(Debug)]
pub struct InListExpr {
    expr: Arc<dyn PhysicalExpr>,
    list: Vec<Arc<dyn PhysicalExpr>>,
    negated: bool,
}

macro_rules! make_contains {
    ($ARRAY:expr, $LIST_VALUES:expr, $NEGATED:expr, $SCALAR_VALUE:ident, $ARRAY_TYPE:ident) => {{
        let array = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();

        let mut contains_null = false;
        let values = $LIST_VALUES
            .iter()
            .flat_map(|expr| match expr {
                ColumnarValue::Scalar(s) => match s {
                    ScalarValue::$SCALAR_VALUE(Some(v)) => Some(*v),
                    ScalarValue::$SCALAR_VALUE(None) => {
                        contains_null = true;
                        None
                    }
                    ScalarValue::Utf8(None) => {
                        contains_null = true;
                        None
                    }
                    datatype => unimplemented!("Unexpected type {} for InList", datatype),
                },
                ColumnarValue::Array(_) => {
                    unimplemented!("InList does not yet support nested columns.")
                }
            })
            .collect::<Vec<_>>();

        Ok(ColumnarValue::Array(Arc::new(
            array
                .iter()
                .map(|x| {
                    let contains = x.map(|x| values.contains(&x));
                    match contains {
                        Some(true) => {
                            if $NEGATED {
                                Some(false)
                            } else {
                                Some(true)
                            }
                        }
                        Some(false) => {
                            if contains_null {
                                None
                            } else if $NEGATED {
                                Some(true)
                            } else {
                                Some(false)
                            }
                        }
                        None => None,
                    }
                })
                .collect::<BooleanArray>(),
        )))
    }};
}

impl InListExpr {
    /// Create a new InList expression
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        list: Vec<Arc<dyn PhysicalExpr>>,
        negated: bool,
    ) -> Self {
        Self {
            expr,
            list,
            negated,
        }
    }

    /// Input expression
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// List to search in
    pub fn list(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.list
    }

    /// Is this negated e.g. NOT IN LIST
    pub fn negated(&self) -> bool {
        self.negated
    }

    /// Compare for specific utf8 types
    #[allow(clippy::unnecessary_wraps)]
    fn compare_utf8<T: StringOffsetSizeTrait>(
        &self,
        array: ArrayRef,
        list_values: Vec<ColumnarValue>,
        negated: bool,
    ) -> Result<ColumnarValue> {
        let array = array
            .as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .unwrap();

        let mut contains_null = false;
        let values = list_values
            .iter()
            .flat_map(|expr| match expr {
                ColumnarValue::Scalar(s) => match s {
                    ScalarValue::Utf8(Some(v)) => Some(v.as_str()),
                    ScalarValue::Utf8(None) => {
                        contains_null = true;
                        None
                    }
                    ScalarValue::LargeUtf8(Some(v)) => Some(v.as_str()),
                    ScalarValue::LargeUtf8(None) => {
                        contains_null = true;
                        None
                    }
                    datatype => unimplemented!("Unexpected type {} for InList", datatype),
                },
                ColumnarValue::Array(_) => {
                    unimplemented!("InList does not yet support nested columns.")
                }
            })
            .collect::<Vec<&str>>();

        Ok(ColumnarValue::Array(Arc::new(
            array
                .iter()
                .map(|x| {
                    let contains = x.map(|x| values.contains(&x));
                    match contains {
                        Some(true) => {
                            if negated {
                                Some(false)
                            } else {
                                Some(true)
                            }
                        }
                        Some(false) => {
                            if contains_null {
                                None
                            } else if negated {
                                Some(true)
                            } else {
                                Some(false)
                            }
                        }
                        None => None,
                    }
                })
                .collect::<BooleanArray>(),
        )))
    }
}

impl std::fmt::Display for InListExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.negated {
            write!(f, "{} NOT IN ({:?})", self.expr, self.list)
        } else {
            write!(f, "{} IN ({:?})", self.expr, self.list)
        }
    }
}

impl PhysicalExpr for InListExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        let value_data_type = value.data_type();
        let list_values = self
            .list
            .iter()
            .map(|expr| expr.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        let array = match value {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };

        match value_data_type {
            DataType::Float32 => {
                make_contains!(array, list_values, self.negated, Float32, Float32Array)
            }
            DataType::Float64 => {
                make_contains!(array, list_values, self.negated, Float64, Float64Array)
            }
            DataType::Int16 => {
                make_contains!(array, list_values, self.negated, Int16, Int16Array)
            }
            DataType::Int32 => {
                make_contains!(array, list_values, self.negated, Int32, Int32Array)
            }
            DataType::Int64 => {
                make_contains!(array, list_values, self.negated, Int64, Int64Array)
            }
            DataType::Int8 => {
                make_contains!(array, list_values, self.negated, Int8, Int8Array)
            }
            DataType::UInt16 => {
                make_contains!(array, list_values, self.negated, UInt16, UInt16Array)
            }
            DataType::UInt32 => {
                make_contains!(array, list_values, self.negated, UInt32, UInt32Array)
            }
            DataType::UInt64 => {
                make_contains!(array, list_values, self.negated, UInt64, UInt64Array)
            }
            DataType::UInt8 => {
                make_contains!(array, list_values, self.negated, UInt8, UInt8Array)
            }
            DataType::Boolean => {
                make_contains!(array, list_values, self.negated, Boolean, BooleanArray)
            }
            DataType::Utf8 => self.compare_utf8::<i32>(array, list_values, self.negated),
            DataType::LargeUtf8 => {
                self.compare_utf8::<i64>(array, list_values, self.negated)
            }
            datatype => {
                unimplemented!("InList does not support datatype {:?}.", datatype)
            }
        }
    }
}

/// Creates a unary expression InList
pub fn in_list(
    expr: Arc<dyn PhysicalExpr>,
    list: Vec<Arc<dyn PhysicalExpr>>,
    negated: &bool,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(InListExpr::new(expr, list, *negated)))
}

#[cfg(test)]
mod tests {
    use arrow::{array::StringArray, datatypes::Field};

    use super::*;
    use crate::error::Result;
    use crate::physical_plan::expressions::{col, lit};

    // applies the in_list expr to an input batch and list
    macro_rules! in_list {
        ($BATCH:expr, $LIST:expr, $NEGATED:expr, $EXPECTED:expr) => {{
            let expr = in_list(col("a"), $LIST, $NEGATED).unwrap();
            let result = expr.evaluate(&$BATCH)?.into_array($BATCH.num_rows());
            let result = result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("failed to downcast to BooleanArray");
            let expected = &BooleanArray::from($EXPECTED);
            assert_eq!(expected, result);
        }};
    }

    #[test]
    fn in_list_utf8() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("a"), Some("d"), None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a in ("a", "b")"
        let list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(Some("b".to_string()))),
        ];
        in_list!(batch, list, &false, vec![Some(true), Some(false), None]);

        // expression: "a not in ("a", "b")"
        let list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(Some("b".to_string()))),
        ];
        in_list!(batch, list, &true, vec![Some(false), Some(true), None]);

        // expression: "a not in ("a", "b")"
        let list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(Some("b".to_string()))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &false, vec![Some(true), None, None]);

        // expression: "a not in ("a", "b")"
        let list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(Some("b".to_string()))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &true, vec![Some(false), None, None]);

        Ok(())
    }

    #[test]
    fn in_list_int64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let a = Int64Array::from(vec![Some(0), Some(2), None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a in (0, 1)"
        let list = vec![
            lit(ScalarValue::Int64(Some(0))),
            lit(ScalarValue::Int64(Some(1))),
        ];
        in_list!(batch, list, &false, vec![Some(true), Some(false), None]);

        // expression: "a not in (0, 1)"
        let list = vec![
            lit(ScalarValue::Int64(Some(0))),
            lit(ScalarValue::Int64(Some(1))),
        ];
        in_list!(batch, list, &true, vec![Some(false), Some(true), None]);

        // expression: "a in (0, 1, NULL)"
        let list = vec![
            lit(ScalarValue::Int64(Some(0))),
            lit(ScalarValue::Int64(Some(1))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &false, vec![Some(true), None, None]);

        // expression: "a not in (0, 1, NULL)"
        let list = vec![
            lit(ScalarValue::Int64(Some(0))),
            lit(ScalarValue::Int64(Some(1))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &true, vec![Some(false), None, None]);

        Ok(())
    }

    #[test]
    fn in_list_float64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, true)]);
        let a = Float64Array::from(vec![Some(0.0), Some(0.2), None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a in (0.0, 0.2)"
        let list = vec![
            lit(ScalarValue::Float64(Some(0.0))),
            lit(ScalarValue::Float64(Some(0.1))),
        ];
        in_list!(batch, list, &false, vec![Some(true), Some(false), None]);

        // expression: "a not in (0.0, 0.2)"
        let list = vec![
            lit(ScalarValue::Float64(Some(0.0))),
            lit(ScalarValue::Float64(Some(0.1))),
        ];
        in_list!(batch, list, &true, vec![Some(false), Some(true), None]);

        // expression: "a in (0.0, 0.2, NULL)"
        let list = vec![
            lit(ScalarValue::Float64(Some(0.0))),
            lit(ScalarValue::Float64(Some(0.1))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &false, vec![Some(true), None, None]);

        // expression: "a not in (0.0, 0.2, NULL)"
        let list = vec![
            lit(ScalarValue::Float64(Some(0.0))),
            lit(ScalarValue::Float64(Some(0.1))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &true, vec![Some(false), None, None]);

        Ok(())
    }

    #[test]
    fn in_list_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let a = BooleanArray::from(vec![Some(true), None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a in (true)"
        let list = vec![lit(ScalarValue::Boolean(Some(true)))];
        in_list!(batch, list, &false, vec![Some(true), None]);

        // expression: "a not in (true)"
        let list = vec![lit(ScalarValue::Boolean(Some(true)))];
        in_list!(batch, list, &true, vec![Some(false), None]);

        // expression: "a in (true, NULL)"
        let list = vec![
            lit(ScalarValue::Boolean(Some(true))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &false, vec![Some(true), None]);

        // expression: "a not in (true, NULL)"
        let list = vec![
            lit(ScalarValue::Boolean(Some(true))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &true, vec![Some(false), None]);

        Ok(())
    }
}
