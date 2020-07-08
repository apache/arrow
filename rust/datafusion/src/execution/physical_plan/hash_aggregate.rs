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

//! Defines the execution plan for the hash aggregate operation

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::{
    Accumulator, AggregateExpr, ExecutionPlan, Partition, PhysicalExpr,
};

use arrow::array::ArrayRef;
use arrow::array::{Float32Builder, Float64Builder, Int64Builder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use crate::execution::physical_plan::common::get_scalar_value;
use crate::execution::physical_plan::expressions::col;
use crate::execution::physical_plan::hash::{
    create_key, create_key_array, AccumulatorSet, KeyScalar,
};
use crate::logicalplan::ScalarValue;
use fnv::FnvHashMap;

/// Hash aggregate execution plan
pub struct HashAggregateExec {
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl HashAggregateExec {
    /// Create a new hash aggregate execution plan
    pub fn try_new(
        group_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        aggr_expr: Vec<(Arc<dyn AggregateExpr>, String)>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let mut fields = Vec::with_capacity(group_expr.len() + aggr_expr.len());
        for (expr, name) in &group_expr {
            fields.push(Field::new(name, expr.data_type(&input_schema)?, true))
        }
        for (expr, name) in &aggr_expr {
            fields.push(Field::new(&name, expr.data_type(&input_schema)?, true))
        }
        let schema = Arc::new(Schema::new(fields));

        Ok(HashAggregateExec {
            group_expr: group_expr.iter().map(|x| x.0.clone()).collect(),
            aggr_expr: aggr_expr.iter().map(|x| x.0.clone()).collect(),
            input,
            schema,
        })
    }

    /// Create the final group and aggregate expressions from the initial group and aggregate
    /// expressions
    pub fn make_final_expr(
        &self,
        group_names: Vec<String>,
        agg_names: Vec<String>,
    ) -> (Vec<Arc<dyn PhysicalExpr>>, Vec<Arc<dyn AggregateExpr>>) {
        let final_group: Vec<Arc<dyn PhysicalExpr>> = (0..self.group_expr.len())
            .map(|i| col(&group_names[i]) as Arc<dyn PhysicalExpr>)
            .collect();

        let final_aggr: Vec<Arc<dyn AggregateExpr>> = (0..self.aggr_expr.len())
            .map(|i| self.aggr_expr[i].create_reducer(&agg_names[i]))
            .collect();

        (final_group, final_aggr)
    }
}

impl ExecutionPlan for HashAggregateExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        Ok(self
            .input
            .partitions()?
            .iter()
            .map(|p| {
                let aggregate: Arc<dyn Partition> =
                    Arc::new(HashAggregatePartition::new(
                        self.group_expr.clone(),
                        self.aggr_expr.clone(),
                        p.clone() as Arc<dyn Partition>,
                        self.schema.clone(),
                    ));

                aggregate
            })
            .collect::<Vec<Arc<dyn Partition>>>())
    }
}

struct HashAggregatePartition {
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    input: Arc<dyn Partition>,
    schema: SchemaRef,
}

impl HashAggregatePartition {
    /// Create a new HashAggregatePartition
    pub fn new(
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<dyn Partition>,
        schema: SchemaRef,
    ) -> Self {
        HashAggregatePartition {
            group_expr,
            aggr_expr,
            input,
            schema,
        }
    }
}

impl Partition for HashAggregatePartition {
    fn execute(&self) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        if self.group_expr.is_empty() {
            Ok(Arc::new(Mutex::new(HashAggregateIterator::new(
                self.schema.clone(),
                self.aggr_expr.clone(),
                self.input.execute()?,
            ))))
        } else {
            Ok(Arc::new(Mutex::new(GroupedHashAggregateIterator::new(
                self.schema.clone(),
                self.group_expr.clone(),
                self.aggr_expr.clone(),
                self.input.execute()?,
            ))))
        }
    }
}

/// Create array from `value` attribute in map entry (representing an aggregate scalar
/// value)
macro_rules! aggr_array_from_map_entries {
    ($BUILDER:ident, $TY:ident, $TY2:ty, $MAP:expr, $COL_INDEX:expr) => {{
        let mut builder = $BUILDER::new($MAP.len());
        let mut err = false;
        for v in $MAP.values() {
            match v[$COL_INDEX]
                .as_ref()
                .borrow()
                .get_value()
                .map_err(ExecutionError::into_arrow_external_error)?
            {
                Some(ScalarValue::$TY(n)) => builder.append_value(n as $TY2).unwrap(),
                None => builder.append_null().unwrap(),
                _ => err = true,
            }
        }
        if err {
            Err(ExecutionError::ExecutionError(
                "unexpected type when creating aggregate array from aggregate map"
                    .to_string(),
            ))
        } else {
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
    }};
}

/// Create array from single accumulator value
macro_rules! aggr_array_from_accumulator {
    ($BUILDER:ident, $TY:ident, $TY2:ty, $VALUE:expr) => {{
        let mut builder = $BUILDER::new(1);
        match $VALUE {
            Some(ScalarValue::$TY(n)) => {
                builder.append_value(n as $TY2)?;
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            None => {
                builder.append_null()?;
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            _ => Err(ExecutionError::ExecutionError(
                "unexpected type when creating aggregate array from aggregate map"
                    .to_string(),
            )),
        }
    }};
}

struct GroupedHashAggregateIterator {
    schema: SchemaRef,
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    input: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
    finished: bool,
}

impl GroupedHashAggregateIterator {
    /// Create a new HashAggregateIterator
    pub fn new(
        schema: SchemaRef,
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
    ) -> Self {
        GroupedHashAggregateIterator {
            schema,
            group_expr,
            aggr_expr,
            input,
            finished: false,
        }
    }
}

impl RecordBatchReader for GroupedHashAggregateIterator {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }

        self.finished = true;

        // create map to store accumulators for each unique grouping key
        let mut map: FnvHashMap<Vec<KeyScalar>, Rc<AccumulatorSet>> =
            FnvHashMap::default();

        // iterate over all input batches and update the accumulators
        let mut input = self.input.lock().unwrap();

        // iterate over input and perform aggregation
        while let Some(batch) = input.next_batch()? {
            // evaluate the grouping expressions for this batch
            let group_values = self
                .group_expr
                .iter()
                .map(|expr| {
                    expr.evaluate(&batch)
                        .map_err(ExecutionError::into_arrow_external_error)
                })
                .collect::<ArrowResult<Vec<_>>>()?;

            // evaluate the inputs to the aggregate expressions for this batch
            let aggr_input_values = self
                .aggr_expr
                .iter()
                .map(|expr| {
                    expr.evaluate_input(&batch)
                        .map_err(ExecutionError::into_arrow_external_error)
                })
                .collect::<ArrowResult<Vec<_>>>()?;

            // create vector to hold the grouping key
            let mut key = Vec::with_capacity(group_values.len());
            for _ in 0..group_values.len() {
                key.push(KeyScalar::UInt32(0));
            }

            // iterate over each row in the batch and create the accumulators for each grouping key
            for row in 0..batch.num_rows() {
                // create and assign the grouping key of this row
                for i in 0..group_values.len() {
                    key[i] = create_key(&group_values[i], row)
                        .map_err(ExecutionError::into_arrow_external_error)?;
                }

                // for each new key on the map, add an accumulatorSet to the map
                match map.get(&key) {
                    None => {
                        let accumulator_set: AccumulatorSet = self
                            .aggr_expr
                            .iter()
                            .map(|expr| expr.create_accumulator())
                            .collect();
                        map.insert(key.clone(), Rc::new(accumulator_set));
                    }
                    _ => (),
                };

                // iterate over each non-grouping column in the batch and update the accumulator
                // for each row
                for col in 0..aggr_input_values.len() {
                    let value = get_scalar_value(&aggr_input_values[col], row)
                        .map_err(ExecutionError::into_arrow_external_error)?;

                    match map.get(&key) {
                        None => panic!("This code cannot be reached."),
                        Some(accumulator_set) => {
                            let mut accum = accumulator_set[col].borrow_mut();
                            accum
                                .accumulate_scalar(value)
                                .map_err(ExecutionError::into_arrow_external_error)?;
                        }
                    }
                }
            }
        }

        let input_schema = input.schema();

        // build the result arrays
        let mut result_arrays: Vec<ArrayRef> =
            Vec::with_capacity(self.group_expr.len() + self.aggr_expr.len());

        // grouping values
        for i in 0..self.group_expr.len() {
            let data_type = self.group_expr[i]
                .data_type(&input_schema)
                .map_err(ExecutionError::into_arrow_external_error)?;
            let array = create_key_array(i, data_type, &map)
                .map_err(ExecutionError::into_arrow_external_error)?;
            result_arrays.push(array);

            // aggregate values
            for i in 0..self.aggr_expr.len() {
                let aggr_data_type = self.aggr_expr[i]
                    .data_type(&input_schema)
                    .map_err(ExecutionError::into_arrow_external_error)?;
                let array = match aggr_data_type {
                    DataType::UInt8 => {
                        aggr_array_from_map_entries!(UInt64Builder, UInt8, u64, map, i)
                    }
                    DataType::UInt16 => {
                        aggr_array_from_map_entries!(UInt64Builder, UInt16, u64, map, i)
                    }
                    DataType::UInt32 => {
                        aggr_array_from_map_entries!(UInt64Builder, UInt32, u64, map, i)
                    }
                    DataType::UInt64 => {
                        aggr_array_from_map_entries!(UInt64Builder, UInt64, u64, map, i)
                    }
                    DataType::Int8 => {
                        aggr_array_from_map_entries!(Int64Builder, Int8, i64, map, i)
                    }
                    DataType::Int16 => {
                        aggr_array_from_map_entries!(Int64Builder, Int16, i64, map, i)
                    }
                    DataType::Int32 => {
                        aggr_array_from_map_entries!(Int64Builder, Int32, i64, map, i)
                    }
                    DataType::Int64 => {
                        aggr_array_from_map_entries!(Int64Builder, Int64, i64, map, i)
                    }
                    DataType::Float32 => {
                        aggr_array_from_map_entries!(Float32Builder, Float32, f32, map, i)
                    }
                    DataType::Float64 => {
                        aggr_array_from_map_entries!(Float64Builder, Float64, f64, map, i)
                    }
                    _ => Err(ExecutionError::ExecutionError(
                        "Unsupported aggregate expr".to_string(),
                    )),
                };
                result_arrays
                    .push(array.map_err(ExecutionError::into_arrow_external_error)?);
            }
        }

        let batch = RecordBatch::try_new(self.schema.clone(), result_arrays)?;
        Ok(Some(batch))
    }
}

struct HashAggregateIterator {
    schema: SchemaRef,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    input: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
    finished: bool,
}

impl HashAggregateIterator {
    /// Create a new HashAggregateIterator
    pub fn new(
        schema: SchemaRef,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
    ) -> Self {
        HashAggregateIterator {
            schema,
            aggr_expr,
            input,
            finished: false,
        }
    }
}

impl RecordBatchReader for HashAggregateIterator {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }

        self.finished = true;

        let accumulators: Vec<Rc<RefCell<dyn Accumulator>>> = self
            .aggr_expr
            .iter()
            .map(|expr| expr.create_accumulator())
            .collect();

        // iterate over all input batches and update the accumulators
        let mut input = self.input.lock().unwrap();

        // iterate over input and perform aggregation
        while let Some(batch) = input.next_batch()? {
            // evaluate the inputs to the aggregate expressions for this batch
            let aggr_input_values = self
                .aggr_expr
                .iter()
                .map(|expr| {
                    expr.evaluate_input(&batch)
                        .map_err(ExecutionError::into_arrow_external_error)
                })
                .collect::<ArrowResult<Vec<_>>>()?;

            // iterate over each row in the batch
            let _ = accumulators
                .iter()
                .zip(aggr_input_values.iter())
                .map(|(accum, input)| {
                    accum
                        .borrow_mut()
                        .accumulate_batch(input)
                        .map_err(ExecutionError::into_arrow_external_error)
                })
                .collect::<ArrowResult<Vec<_>>>()?;
        }

        let input_schema = input.schema();

        // build the result arrays
        let mut result_arrays: Vec<ArrayRef> = Vec::with_capacity(self.aggr_expr.len());

        // aggregate values
        for i in 0..self.aggr_expr.len() {
            let aggr_data_type = self.aggr_expr[i]
                .data_type(&input_schema)
                .map_err(ExecutionError::into_arrow_external_error)?;
            let value = accumulators[i]
                .borrow_mut()
                .get_value()
                .map_err(ExecutionError::into_arrow_external_error)?;
            let array = match aggr_data_type {
                DataType::UInt8 => {
                    aggr_array_from_accumulator!(UInt64Builder, UInt8, u64, value)
                }
                DataType::UInt16 => {
                    aggr_array_from_accumulator!(UInt64Builder, UInt16, u64, value)
                }
                DataType::UInt32 => {
                    aggr_array_from_accumulator!(UInt64Builder, UInt32, u64, value)
                }
                DataType::UInt64 => {
                    aggr_array_from_accumulator!(UInt64Builder, UInt64, u64, value)
                }
                DataType::Int8 => {
                    aggr_array_from_accumulator!(Int64Builder, Int8, i64, value)
                }
                DataType::Int16 => {
                    aggr_array_from_accumulator!(Int64Builder, Int16, i64, value)
                }
                DataType::Int32 => {
                    aggr_array_from_accumulator!(Int64Builder, Int32, i64, value)
                }
                DataType::Int64 => {
                    aggr_array_from_accumulator!(Int64Builder, Int64, i64, value)
                }
                DataType::Float32 => {
                    aggr_array_from_accumulator!(Float32Builder, Float32, f32, value)
                }
                DataType::Float64 => {
                    aggr_array_from_accumulator!(Float64Builder, Float64, f64, value)
                }
                _ => Err(ExecutionError::ExecutionError(
                    "Unsupported aggregate expr".to_string(),
                )),
            };
            result_arrays.push(array.map_err(ExecutionError::into_arrow_external_error)?);
        }

        let batch = RecordBatch::try_new(self.schema.clone(), result_arrays)?;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::execution::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::execution::physical_plan::expressions::{col, sum};
    use crate::execution::physical_plan::merge::MergeExec;
    use crate::test;
    use arrow::array::{Int64Array, UInt32Array};

    #[test]
    fn aggregate() -> Result<()> {
        let schema = test::aggr_test_schema();

        let partitions = 4;
        let path = test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;

        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("c2"), "c2".to_string())];

        let aggregates: Vec<(Arc<dyn AggregateExpr>, String)> =
            vec![(sum(col("c4")), "SUM(c4)".to_string())];

        let partition_aggregate = HashAggregateExec::try_new(
            groups.clone(),
            aggregates.clone(),
            Arc::new(csv),
        )?;

        let schema = partition_aggregate.schema();
        let partitions = partition_aggregate.partitions()?;

        // construct the expressions for the final aggregation
        let (final_group, final_aggr) = partition_aggregate.make_final_expr(
            groups.iter().map(|x| x.1.clone()).collect(),
            aggregates.iter().map(|x| x.1.clone()).collect(),
        );

        let merge = Arc::new(MergeExec::new(schema.clone(), partitions));

        let merged_aggregate = HashAggregateExec::try_new(
            final_group
                .iter()
                .enumerate()
                .map(|(i, expr)| (expr.clone(), groups[i].1.clone()))
                .collect(),
            final_aggr
                .iter()
                .enumerate()
                .map(|(i, expr)| (expr.clone(), aggregates[i].1.clone()))
                .collect(),
            merge,
        )?;

        let result = test::execute(&merged_aggregate)?;
        assert_eq!(result.len(), 1);

        let batch = &result[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 5);

        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut group_values = vec![];
        for i in 0..a.len() {
            group_values.push(a.value(i))
        }

        let mut aggr_values = vec![];
        for i in 1..=5 {
            // find index of row with this value for the grouping column
            let index = group_values.iter().position(|&r| r == i).unwrap();
            aggr_values.push(b.value(index));
        }

        let expected: Vec<i64> = vec![88722, 90999, 80899, -120910, 92287];
        assert_eq!(aggr_values, expected);

        Ok(())
    }
}
