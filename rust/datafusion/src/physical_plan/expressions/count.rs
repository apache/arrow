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

use std::any::Any;
use std::sync::Arc;

use crate::error::Result;
use crate::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use crate::scalar::ScalarValue;
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::{
    array::{ArrayRef, UInt64Array},
    datatypes::Field,
};

use super::format_state_name;

/// COUNT aggregate expression
/// Returns the amount of non-null values of the given expression.
#[derive(Debug)]
pub struct Count {
    name: String,
    data_type: DataType,
    nullable: bool,
    expr: Arc<dyn PhysicalExpr>,
}

impl Count {
    /// Create a new COUNT aggregate function.
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: String, data_type: DataType) -> Self {
        Self {
            name,
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for Count {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "count"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountAccumulator::new()))
    }
}

#[derive(Debug)]
struct CountAccumulator {
    count: u64,
}

impl CountAccumulator {
    /// new count accumulator
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.count += (array.len() - array.data().null_count()) as u64;
        Ok(())
    }

    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        let value = &values[0];
        if !value.is_null() {
            self.count += 1;
        }
        Ok(())
    }

    fn merge(&mut self, states: &[ScalarValue]) -> Result<()> {
        let count = &states[0];
        if let ScalarValue::UInt64(Some(delta)) = count {
            self.count += *delta;
        } else {
            unreachable!()
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = states[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        let delta = &compute::sum(counts);
        if let Some(d) = delta {
            self.count += *d;
        }
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::UInt64(Some(self.count))])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(self.count)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::expressions::tests::aggregate;
    use crate::{error::Result, generic_test_op};
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};

    #[test]
    fn count_elements() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Count,
            ScalarValue::from(5u64),
            DataType::UInt64
        )
    }

    #[test]
    fn count_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(3),
            None,
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            Count,
            ScalarValue::from(3u64),
            DataType::UInt64
        )
    }

    #[test]
    fn count_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![
            None, None, None, None, None, None, None, None,
        ]));
        generic_test_op!(
            a,
            DataType::Boolean,
            Count,
            ScalarValue::from(0u64),
            DataType::UInt64
        )
    }

    #[test]
    fn count_empty() -> Result<()> {
        let a: Vec<bool> = vec![];
        let a: ArrayRef = Arc::new(BooleanArray::from(a));
        generic_test_op!(
            a,
            DataType::Boolean,
            Count,
            ScalarValue::from(0u64),
            DataType::UInt64
        )
    }

    #[test]
    fn count_utf8() -> Result<()> {
        let a: ArrayRef =
            Arc::new(StringArray::from(vec!["a", "bb", "ccc", "dddd", "ad"]));
        generic_test_op!(
            a,
            DataType::Utf8,
            Count,
            ScalarValue::from(5u64),
            DataType::UInt64
        )
    }

    #[test]
    fn count_large_utf8() -> Result<()> {
        let a: ArrayRef =
            Arc::new(LargeStringArray::from(vec!["a", "bb", "ccc", "dddd", "ad"]));
        generic_test_op!(
            a,
            DataType::LargeUtf8,
            Count,
            ScalarValue::from(5u64),
            DataType::UInt64
        )
    }
}
