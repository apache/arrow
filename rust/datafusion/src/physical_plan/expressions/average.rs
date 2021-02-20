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
use std::convert::TryFrom;
use std::sync::Arc;

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use crate::scalar::ScalarValue;
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::{
    array::{ArrayRef, UInt64Array},
    datatypes::Field,
};

use super::{format_state_name, sum};

/// AVG aggregate expression
#[derive(Debug)]
pub struct Avg {
    name: String,
    data_type: DataType,
    nullable: bool,
    expr: Arc<dyn PhysicalExpr>,
}

/// function return type of an average
pub fn avg_return_type(arg_type: &DataType) -> Result<DataType> {
    match arg_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => Ok(DataType::Float64),
        other => Err(DataFusionError::Plan(format!(
            "AVG does not support {:?}",
            other
        ))),
    }
}

impl Avg {
    /// Create a new AVG aggregate function
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: String, data_type: DataType) -> Self {
        Self {
            name,
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for Avg {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                &format_state_name(&self.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "sum"),
                DataType::Float64,
                true,
            ),
        ])
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(AvgAccumulator::try_new(
            // avg is f64
            &DataType::Float64,
        )?))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }
}

/// An accumulator to compute the average
#[derive(Debug)]
pub struct AvgAccumulator {
    // sum is used for null
    sum: ScalarValue,
    count: u64,
}

impl AvgAccumulator {
    /// Creates a new `AvgAccumulator`
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            sum: ScalarValue::try_from(datatype)?,
            count: 0,
        })
    }
}

impl Accumulator for AvgAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::from(self.count), self.sum.clone()])
    }

    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        let values = &values[0];

        self.count += (!values.is_null()) as u64;
        self.sum = sum::sum(&self.sum, values)?;

        Ok(())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];

        self.count += (values.len() - values.data().null_count()) as u64;
        self.sum = sum::sum(&self.sum, &sum::sum_batch(values)?)?;
        Ok(())
    }

    fn merge(&mut self, states: &[ScalarValue]) -> Result<()> {
        let count = &states[0];
        // counts are summed
        if let ScalarValue::UInt64(Some(c)) = count {
            self.count += c
        } else {
            unreachable!()
        };

        // sums are summed
        self.sum = sum::sum(&self.sum, &states[1])?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = states[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        // counts are summed
        self.count += compute::sum(counts).unwrap_or(0);

        // sums are summed
        self.sum = sum::sum(&self.sum, &sum::sum_batch(&states[1])?)?;
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match self.sum {
            ScalarValue::Float64(e) => {
                Ok(ScalarValue::Float64(e.map(|f| f / self.count as f64)))
            }
            _ => Err(DataFusionError::Internal(
                "Sum should be f64 on average".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::expressions::col;
    use crate::{error::Result, generic_test_op};
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};

    #[test]
    fn avg_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Avg,
            ScalarValue::from(3_f64),
            DataType::Float64
        )
    }

    #[test]
    fn avg_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            Avg,
            ScalarValue::from(3.25f64),
            DataType::Float64
        )
    }

    #[test]
    fn avg_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(
            a,
            DataType::Int32,
            Avg,
            ScalarValue::Float64(None),
            DataType::Float64
        )
    }

    #[test]
    fn avg_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(
            a,
            DataType::UInt32,
            Avg,
            ScalarValue::from(3.0f64),
            DataType::Float64
        )
    }

    #[test]
    fn avg_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(
            a,
            DataType::Float32,
            Avg,
            ScalarValue::from(3_f64),
            DataType::Float64
        )
    }

    #[test]
    fn avg_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Avg,
            ScalarValue::from(3_f64),
            DataType::Float64
        )
    }

    fn aggregate(
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
}
