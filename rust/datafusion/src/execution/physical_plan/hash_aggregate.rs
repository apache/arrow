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

use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::{
    Accumulator, AggregateExpr, ExecutionPlan, Partition, PhysicalExpr,
};

use arrow::array::{self, ArrayRef};
use arrow::array::{
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder,
    UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use crate::execution::physical_plan::expressions::col;
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

/// Create array from `key` attribute in map entry (representing a grouping scalar value)
macro_rules! extract_group_values {
    ($BUILDER:ident, $TY:ident, $MAP:expr, $COL_INDEX:expr) => {{
        let mut builder = $BUILDER::new($MAP.len());
        let mut err = false;
        for k in $MAP.keys() {
            match k[$COL_INDEX] {
                GroupByScalar::$TY(n) => builder.append_value(n).unwrap(),
                _ => err = true,
            }
        }
        if err {
            Err(ExecutionError::ExecutionError(
                "unexpected type when creating grouping array from aggregate map"
                    .to_string(),
            ))
        } else {
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
    }};
}

/// Create array from `value` attribute in map entry (representing an aggregate scalar
/// value)
macro_rules! extract_aggr_values {
    ($BUILDER:ident, $TY:ident, $TY2:ty, $MAP:expr, $COL_INDEX:expr) => {{
        let mut builder = $BUILDER::new($MAP.len());
        let mut err = false;
        for v in $MAP.values() {
            match v[$COL_INDEX]
                .as_ref()
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

#[derive(Debug)]
struct MapEntry {
    k: Vec<GroupByScalar>,
    v: Vec<Option<ScalarValue>>,
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

type AccumulatorSet = Vec<Box<dyn Accumulator>>;

/// Cast an Arrow Array to its expected type
#[macro_export]
macro_rules! cast_array {
    ($SELF:ident, $ARRAY_TYPE:ident) => {{
        match $SELF.as_any().downcast_ref::<array::$ARRAY_TYPE>() {
            Some(array) => Ok(array),
            None => Err(ExecutionError::General(
                "Failed to cast array to expected type".to_owned(),
            )),
        }
    }};
}

macro_rules! update_accumulators {
    ($ARRAY:ident, $ARRAY_TY:ident, $SCALAR_TY:expr, $ROW:expr, $COL:expr, $ACCUM:expr) => {{
        let primitive_array = cast_array!($ARRAY, $ARRAY_TY)?;
        if $ARRAY.is_valid($ROW) {
            let value = $SCALAR_TY(primitive_array.value($ROW));
            $ACCUM[$COL].accumulate_scalar(Some(value))?;
        }
    }};
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
        let mut map: FnvHashMap<Vec<GroupByScalar>, AccumulatorSet> =
            FnvHashMap::default();

        // create vector large enough to hold the grouping key
        let mut key = Vec::with_capacity(self.group_expr.len());
        for _ in 0..self.group_expr.len() {
            key.push(GroupByScalar::UInt32(0));
        }

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

            // we now need to switch to row-based processing :-(
            for row in 0..batch.num_rows() {
                // create grouping key for this row
                create_key(&group_values, row, &mut key)
                    .map_err(ExecutionError::into_arrow_external_error)?;

                // lookup the accumulators for this grouping key
                match map.get_mut(&key) {
                    Some(mut accumulators) => {
                        accumulate(&aggr_input_values, &mut accumulators, row)
                            .map_err(ExecutionError::into_arrow_external_error)?;
                    }
                    None => {
                        let mut accumulators: AccumulatorSet = self
                            .aggr_expr
                            .iter()
                            .map(|expr| expr.create_accumulator())
                            .collect();

                        accumulate(&aggr_input_values, &mut accumulators, row)
                            .map_err(ExecutionError::into_arrow_external_error)?;

                        map.insert(key.clone(), accumulators);
                    }
                };
            }
        }

        let input_schema = input.schema();

        // build the result arrays
        let mut result_arrays: Vec<ArrayRef> =
            Vec::with_capacity(self.group_expr.len() + self.aggr_expr.len());

        // grouping values
        for i in 0..self.group_expr.len() {
            let array: Result<ArrayRef> = match self.group_expr[i]
                .data_type(&input_schema)
                .map_err(ExecutionError::into_arrow_external_error)?
            {
                DataType::UInt8 => extract_group_values!(UInt8Builder, UInt8, map, i),
                DataType::UInt16 => extract_group_values!(UInt16Builder, UInt16, map, i),
                DataType::UInt32 => extract_group_values!(UInt32Builder, UInt32, map, i),
                DataType::UInt64 => extract_group_values!(UInt64Builder, UInt64, map, i),
                DataType::Int8 => extract_group_values!(Int8Builder, Int8, map, i),
                DataType::Int16 => extract_group_values!(Int16Builder, Int16, map, i),
                DataType::Int32 => extract_group_values!(Int32Builder, Int32, map, i),
                DataType::Int64 => extract_group_values!(Int64Builder, Int64, map, i),
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new(1);
                    for k in map.keys() {
                        match &k[i] {
                            GroupByScalar::Utf8(s) => builder.append_value(&s).unwrap(),
                            _ => {
                                return Err(ExecutionError::ExecutionError(
                                    "Unexpected value for Utf8 group column".to_string(),
                                )
                                .into_arrow_external_error())
                            }
                        }
                    }
                    Ok(Arc::new(builder.finish()) as ArrayRef)
                }
                _ => Err(ExecutionError::ExecutionError(
                    "Unsupported group by expr".to_string(),
                )),
            };
            result_arrays.push(array.map_err(ExecutionError::into_arrow_external_error)?);

            // aggregate values
            for i in 0..self.aggr_expr.len() {
                let aggr_data_type = self.aggr_expr[i]
                    .data_type(&input_schema)
                    .map_err(ExecutionError::into_arrow_external_error)?;
                let array = match aggr_data_type {
                    DataType::UInt8 => {
                        extract_aggr_values!(UInt64Builder, UInt8, u64, map, i)
                    }
                    DataType::UInt16 => {
                        extract_aggr_values!(UInt64Builder, UInt16, u64, map, i)
                    }
                    DataType::UInt32 => {
                        extract_aggr_values!(UInt64Builder, UInt32, u64, map, i)
                    }
                    DataType::UInt64 => {
                        extract_aggr_values!(UInt64Builder, UInt64, u64, map, i)
                    }
                    DataType::Int8 => {
                        extract_aggr_values!(Int64Builder, Int8, i64, map, i)
                    }
                    DataType::Int16 => {
                        extract_aggr_values!(Int64Builder, Int16, i64, map, i)
                    }
                    DataType::Int32 => {
                        extract_aggr_values!(Int64Builder, Int32, i64, map, i)
                    }
                    DataType::Int64 => {
                        extract_aggr_values!(Int64Builder, Int64, i64, map, i)
                    }
                    DataType::Float32 => {
                        extract_aggr_values!(Float32Builder, Float32, f32, map, i)
                    }
                    DataType::Float64 => {
                        extract_aggr_values!(Float64Builder, Float64, f64, map, i)
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

fn accumulate(
    aggr_input_values: &[ArrayRef],
    accumulators: &mut AccumulatorSet,
    row: usize,
) -> Result<()> {
    for (col, array) in aggr_input_values.iter().enumerate() {
        match array.data_type() {
            DataType::Int8 => update_accumulators!(
                array,
                Int8Array,
                ScalarValue::Int8,
                row,
                col,
                accumulators
            ),
            DataType::Int16 => update_accumulators!(
                array,
                Int16Array,
                ScalarValue::Int16,
                row,
                col,
                accumulators
            ),
            DataType::Int32 => update_accumulators!(
                array,
                Int32Array,
                ScalarValue::Int32,
                row,
                col,
                accumulators
            ),
            DataType::Int64 => update_accumulators!(
                array,
                Int64Array,
                ScalarValue::Int64,
                row,
                col,
                accumulators
            ),
            DataType::UInt8 => update_accumulators!(
                array,
                UInt8Array,
                ScalarValue::UInt8,
                row,
                col,
                accumulators
            ),
            DataType::UInt16 => update_accumulators!(
                array,
                UInt16Array,
                ScalarValue::UInt16,
                row,
                col,
                accumulators
            ),
            DataType::UInt32 => update_accumulators!(
                array,
                UInt32Array,
                ScalarValue::UInt32,
                row,
                col,
                accumulators
            ),
            DataType::UInt64 => update_accumulators!(
                array,
                UInt64Array,
                ScalarValue::UInt64,
                row,
                col,
                accumulators
            ),
            DataType::Float32 => update_accumulators!(
                array,
                Float32Array,
                ScalarValue::Float32,
                row,
                col,
                accumulators
            ),
            DataType::Float64 => update_accumulators!(
                array,
                Float64Array,
                ScalarValue::Float64,
                row,
                col,
                accumulators
            ),
            other => {
                return Err(ExecutionError::General(format!(
                    "Unsupported data type {:?} for result of aggregate expression",
                    other
                )))
            }
        }
    }
    Ok(())
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

        let mut accumulators: AccumulatorSet = self
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
                .iter_mut()
                .zip(aggr_input_values.iter())
                .map(|(accum, input)| {
                    accum
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

/// Enumeration of types that can be used in a GROUP BY expression (all primitives except
/// for floating point numerics)
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum GroupByScalar {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Utf8(String),
}

/// Create a Vec<GroupByScalar> that can be used as a map key
#[inline]
fn create_key(
    group_by_keys: &[ArrayRef],
    row: usize,
    vec: &mut Vec<GroupByScalar>,
) -> Result<()> {
    for i in 0..group_by_keys.len() {
        let col = &group_by_keys[i];
        match col.data_type() {
            DataType::UInt8 => {
                let array = cast_array!(col, UInt8Array)?;
                vec[i] = GroupByScalar::UInt8(array.value(row))
            }
            DataType::UInt16 => {
                let array = cast_array!(col, UInt16Array)?;
                vec[i] = GroupByScalar::UInt16(array.value(row))
            }
            DataType::UInt32 => {
                let array = cast_array!(col, UInt32Array)?;
                vec[i] = GroupByScalar::UInt32(array.value(row))
            }
            DataType::UInt64 => {
                let array = cast_array!(col, UInt64Array)?;
                vec[i] = GroupByScalar::UInt64(array.value(row))
            }
            DataType::Int8 => {
                let array = cast_array!(col, Int8Array)?;
                vec[i] = GroupByScalar::Int8(array.value(row))
            }
            DataType::Int16 => {
                let array = cast_array!(col, Int16Array)?;
                vec[i] = GroupByScalar::Int16(array.value(row))
            }
            DataType::Int32 => {
                let array = cast_array!(col, Int32Array)?;
                vec[i] = GroupByScalar::Int32(array.value(row))
            }
            DataType::Int64 => {
                let array = cast_array!(col, Int64Array)?;
                vec[i] = GroupByScalar::Int64(array.value(row))
            }
            DataType::Utf8 => {
                let array = cast_array!(col, StringArray)?;
                vec[i] = GroupByScalar::Utf8(String::from(array.value(row)))
            }
            _ => {
                return Err(ExecutionError::ExecutionError(
                    "Unsupported GROUP BY data type".to_string(),
                ))
            }
        }
    }
    Ok(())
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
