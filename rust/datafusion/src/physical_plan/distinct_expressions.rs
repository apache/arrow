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

//! Implementations for DISTINCT expressions, e.g. `COUNT(DISTINCT c)`

use std::cell::RefCell;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::ListArray;
use arrow::datatypes::{DataType, Field};

use fnv::FnvHashSet;

use crate::error::{ExecutionError, Result};
use crate::physical_plan::group_scalar::GroupByScalar;
use crate::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use crate::scalar::ScalarValue;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct DistinctScalarValues(Vec<GroupByScalar>);

fn format_state_name(name: &str, state_name: &str) -> String {
    format!("{}[{}]", name, state_name)
}

/// Expression for a COUNT(DISTINCT) aggregation.
#[derive(Debug)]
pub struct DistinctCount {
    /// Column name
    name: String,
    /// The DataType for the final count
    data_type: DataType,
    /// The DataType for each input argument
    input_data_types: Vec<DataType>,
    /// The input arguments
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl DistinctCount {
    /// Create a new COUNT(DISTINCT) aggregate function.
    pub fn new(
        input_data_types: Vec<DataType>,
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        name: String,
        data_type: DataType,
    ) -> Self {
        Self {
            input_data_types,
            exprs,
            name,
            data_type,
        }
    }
}

impl AggregateExpr for DistinctCount {
    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), false))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(self
            .input_data_types
            .iter()
            .map(|data_type| {
                Field::new(
                    &format_state_name(&self.name, "count distinct"),
                    DataType::List(Box::new(data_type.clone())),
                    false,
                )
            })
            .collect::<Vec<_>>())
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.exprs.clone()
    }

    fn create_accumulator(&self) -> Result<Rc<RefCell<dyn Accumulator>>> {
        Ok(Rc::new(RefCell::new(DistinctCountAccumulator {
            values: FnvHashSet::default(),
            data_types: self.input_data_types.clone(),
            count_data_type: self.data_type.clone(),
        })))
    }
}

#[derive(Debug)]
struct DistinctCountAccumulator {
    values: FnvHashSet<DistinctScalarValues>,
    data_types: Vec<DataType>,
    count_data_type: DataType,
}

impl Accumulator for DistinctCountAccumulator {
    fn update_batch(&mut self, arrays: &Vec<ArrayRef>) -> Result<()> {
        for row in 0..arrays[0].len() {
            let row_values = arrays
                .iter()
                .map(|array| ScalarValue::try_from_array(array, row))
                .collect::<Result<Vec<_>>>()?;

            self.update(&row_values)?;
        }

        Ok(())
    }

    fn update(&mut self, values: &Vec<ScalarValue>) -> Result<()> {
        // If a row has a NULL, it is not included in the final count.
        if !values.iter().any(|v| v.is_null()) {
            self.values.insert(DistinctScalarValues(
                values
                    .iter()
                    .map(GroupByScalar::try_from)
                    .collect::<Result<Vec<_>>>()?,
            ));
        }

        Ok(())
    }

    fn merge(&mut self, states: &Vec<ScalarValue>) -> Result<()> {
        self.update(states)
    }

    fn merge_batch(&mut self, states: &Vec<ArrayRef>) -> Result<()> {
        let list_arrays = states
            .iter()
            .map(|state_array| {
                state_array.as_any().downcast_ref::<ListArray>().ok_or(
                    ExecutionError::InternalError(
                        "Failed to downcast ListArray".to_string(),
                    ),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let values_arrays = list_arrays
            .iter()
            .map(|list_array| list_array.values())
            .collect::<Vec<_>>();

        self.update_batch(&values_arrays)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut cols_out = self
            .data_types
            .iter()
            .map(|data_type| ScalarValue::List(Some(Vec::new()), data_type.clone()))
            .collect::<Vec<_>>();

        let mut cols_vec = cols_out
            .iter_mut()
            .map(|c| match c {
                ScalarValue::List(Some(ref mut v), _) => v,
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();

        self.values.iter().for_each(|distinct_values| {
            distinct_values.0.iter().enumerate().for_each(
                |(col_index, distinct_value)| {
                    cols_vec[col_index].push(ScalarValue::from(distinct_value));
                },
            )
        });

        Ok(cols_out)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match &self.count_data_type {
            DataType::UInt64 => Ok(ScalarValue::UInt64(Some(self.values.len() as u64))),
            t => {
                return Err(ExecutionError::InternalError(format!(
                    "Invalid data type {:?} for count distinct aggregation",
                    t
                )))
            }
        }
    }
}
