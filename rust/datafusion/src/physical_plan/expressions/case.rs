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

use std::{any::Any, sync::Arc};

use arrow::array::{self, *};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{ColumnarValue, PhysicalExpr};

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

impl std::fmt::Display for CaseExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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

/// Create a CASE expression
pub fn case(
    expr: Option<Arc<dyn PhysicalExpr>>,
    when_thens: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
    else_expr: Option<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(CaseExpr::try_new(expr, when_thens, else_expr)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::Result,
        logical_plan::Operator,
        physical_plan::expressions::{binary, col, lit},
        scalar::ScalarValue,
    };
    use arrow::array::StringArray;
    use arrow::datatypes::*;

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
