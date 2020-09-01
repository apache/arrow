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
use crate::physical_plan::{
    Accumulator, AggregateExpr, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
};

use arrow::array::{
    ArrayBuilder, ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::array::{
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder,
    UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use crate::logical_plan::ScalarValue;
use crate::physical_plan::expressions::col;
use fnv::FnvHashMap;

/// Hash aggregate modes
#[derive(Debug, Copy, Clone)]
pub enum AggregateMode {
    /// Partial aggregate that can be applied in parallel across input partitions
    Partial,
    /// Final aggregate that produces a single partition of output
    Final,
}

/// Hash aggregate execution plan
#[derive(Debug)]
pub struct HashAggregateExec {
    mode: AggregateMode,
    group_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    aggr_expr: Vec<(Arc<dyn AggregateExpr>, String)>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl HashAggregateExec {
    /// Create a new hash aggregate execution plan
    pub fn try_new(
        mode: AggregateMode,
        group_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        aggr_expr: Vec<(Arc<dyn AggregateExpr>, String)>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let mut fields = Vec::with_capacity(group_expr.len() + aggr_expr.len());
        for (expr, name) in &group_expr {
            fields.push(Field::new(
                name,
                expr.data_type(&input_schema)?,
                expr.nullable(&input_schema)?,
            ))
        }
        for (expr, name) in &aggr_expr {
            fields.push(Field::new(
                &name,
                expr.data_type(&input_schema)?,
                expr.nullable(&input_schema)?,
            ))
        }
        let schema = Arc::new(Schema::new(fields));

        Ok(HashAggregateExec {
            mode,
            group_expr,
            aggr_expr,
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
            .map(|i| self.aggr_expr[i].0.create_reducer(&agg_names[i]))
            .collect();

        (final_group, final_aggr)
    }
}

impl ExecutionPlan for HashAggregateExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn required_child_distribution(&self) -> Distribution {
        match &self.mode {
            AggregateMode::Partial => Distribution::UnspecifiedDistribution,
            AggregateMode::Final => Distribution::SinglePartition,
        }
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        let input = self.input.execute(partition)?;
        let group_expr = self.group_expr.iter().map(|x| x.0.clone()).collect();
        let aggr_expr = self.aggr_expr.iter().map(|x| x.0.clone()).collect();
        if self.group_expr.is_empty() {
            Ok(Arc::new(Mutex::new(HashAggregateIterator::new(
                self.schema.clone(),
                aggr_expr,
                input,
            ))))
        } else {
            Ok(Arc::new(Mutex::new(GroupedHashAggregateIterator::new(
                self.schema.clone(),
                group_expr,
                aggr_expr,
                input,
            ))))
        }
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(HashAggregateExec::try_new(
                self.mode,
                self.group_expr.clone(),
                self.aggr_expr.clone(),
                children[0].clone(),
            )?)),
            _ => Err(ExecutionError::General(
                "HashAggregateExec wrong number of children".to_string(),
            )),
        }
    }
}

/// Create array from single accumulator value
macro_rules! accum_val {
    ($BUILDER:ident, $SCALAR_TY:ident, $VALUE:expr) => {{
        let mut builder = $BUILDER::new(1);
        match $VALUE {
            Some(ScalarValue::$SCALAR_TY(n)) => {
                builder.append_value(n)?;
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            None => {
                builder.append_null()?;
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            _ => Err(ExecutionError::ExecutionError(
                "unexpected type when creating aggregate array from no-group aggregate"
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

type AccumulatorSet = Vec<Rc<RefCell<dyn Accumulator>>>;

macro_rules! update_accum {
    ($ARRAY:ident, $ARRAY_TY:ident, $SCALAR_TY:expr, $COL:expr, $ACCUM:expr) => {{
        let primitive_array = $ARRAY.as_any().downcast_ref::<$ARRAY_TY>().unwrap();

        for row in 0..$ARRAY.len() {
            if $ARRAY.is_valid(row) {
                let value = Some($SCALAR_TY(primitive_array.value(row)));
                let mut accum = $ACCUM[row][$COL].borrow_mut();
                accum
                    .accumulate_scalar(value)
                    .map_err(ExecutionError::into_arrow_external_error)?;
            }
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
        let mut map: FnvHashMap<Vec<GroupByScalar>, Rc<AccumulatorSet>> =
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

            // create vector large enough to hold the grouping key
            let mut key = Vec::with_capacity(group_values.len());
            for _ in 0..group_values.len() {
                key.push(GroupByScalar::UInt32(0));
            }

            // iterate over each row in the batch and create the accumulators for each grouping key
            let mut accums: Vec<Rc<AccumulatorSet>> =
                Vec::with_capacity(batch.num_rows());

            for row in 0..batch.num_rows() {
                // create grouping key for this row
                create_key(&group_values, row, &mut key)
                    .map_err(ExecutionError::into_arrow_external_error)?;

                if let Some(accumulator_set) = map.get(&key) {
                    accums.push(accumulator_set.clone());
                } else {
                    let accumulator_set: AccumulatorSet = self
                        .aggr_expr
                        .iter()
                        .map(|expr| expr.create_accumulator())
                        .collect();

                    let accumulator_set = Rc::new(accumulator_set);

                    map.insert(key.clone(), accumulator_set.clone());
                    accums.push(accumulator_set);
                }
            }

            // iterate over each non-grouping column in the batch and update the accumulator
            // for each row
            for col in 0..aggr_input_values.len() {
                let array = &aggr_input_values[col];

                match array.data_type() {
                    DataType::Int8 => {
                        update_accum!(array, Int8Array, ScalarValue::Int8, col, accums)
                    }
                    DataType::Int16 => {
                        update_accum!(array, Int16Array, ScalarValue::Int16, col, accums)
                    }
                    DataType::Int32 => {
                        update_accum!(array, Int32Array, ScalarValue::Int32, col, accums)
                    }
                    DataType::Int64 => {
                        update_accum!(array, Int64Array, ScalarValue::Int64, col, accums)
                    }
                    DataType::UInt8 => {
                        update_accum!(array, UInt8Array, ScalarValue::UInt8, col, accums)
                    }
                    DataType::UInt16 => update_accum!(
                        array,
                        UInt16Array,
                        ScalarValue::UInt16,
                        col,
                        accums
                    ),
                    DataType::UInt32 => update_accum!(
                        array,
                        UInt32Array,
                        ScalarValue::UInt32,
                        col,
                        accums
                    ),
                    DataType::UInt64 => update_accum!(
                        array,
                        UInt64Array,
                        ScalarValue::UInt64,
                        col,
                        accums
                    ),
                    DataType::Float32 => update_accum!(
                        array,
                        Float32Array,
                        ScalarValue::Float32,
                        col,
                        accums
                    ),
                    DataType::Float64 => update_accum!(
                        array,
                        Float64Array,
                        ScalarValue::Float64,
                        col,
                        accums
                    ),
                    other => {
                        return Err(ExecutionError::ExecutionError(format!(
                            "Unsupported data type {:?} for result of aggregate expression",
                            other
                        )).into_arrow_external_error());
                    }
                };
            }
        }

        let batch = create_batch_from_map(
            &map,
            self.group_expr.len(),
            self.aggr_expr.len(),
            &self.schema,
        )
        .map_err(ExecutionError::into_arrow_external_error)?;

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
                DataType::UInt8 => accum_val!(UInt8Builder, UInt8, value),
                DataType::UInt16 => accum_val!(UInt16Builder, UInt16, value),
                DataType::UInt32 => accum_val!(UInt32Builder, UInt32, value),
                DataType::UInt64 => accum_val!(UInt64Builder, UInt64, value),
                DataType::Int8 => accum_val!(Int8Builder, Int8, value),
                DataType::Int16 => accum_val!(Int16Builder, Int16, value),
                DataType::Int32 => accum_val!(Int32Builder, Int32, value),
                DataType::Int64 => accum_val!(Int64Builder, Int64, value),
                DataType::Float32 => accum_val!(Float32Builder, Float32, value),
                DataType::Float64 => accum_val!(Float64Builder, Float64, value),
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

/// Append a grouping expression value to a builder
macro_rules! group_val {
    ($BUILDER:expr, $BUILDER_TY:ident, $VALUE:expr) => {{
        let builder = $BUILDER
            .downcast_mut::<$BUILDER_TY>()
            .expect("failed to downcast group value builder to expected type");
        builder.append_value($VALUE)?;
    }};
}

/// Append an aggregate expression value to a builder
macro_rules! aggr_val {
    ($BUILDER:expr, $BUILDER_TY:ident, $VALUE:expr, $SCALAR_TY:ident) => {{
        let builder = $BUILDER
            .downcast_mut::<$BUILDER_TY>()
            .expect("failed to downcast aggregate value builder to expected type");
        match $VALUE {
            Some(ScalarValue::$SCALAR_TY(n)) => builder.append_value(n)?,
            None => builder.append_null()?,
            Some(other) => {
                return Err(ExecutionError::General(format!(
                    "Unexpected data type {:?} for aggregate value",
                    other
                )))
            }
        }
    }};
}

/// Create a RecordBatch representing the accumulated results in a map
fn create_batch_from_map(
    map: &FnvHashMap<Vec<GroupByScalar>, Rc<AccumulatorSet>>,
    num_group_expr: usize,
    num_aggr_expr: usize,
    output_schema: &Schema,
) -> Result<RecordBatch> {
    // create builders based on the output schema data types
    let output_types: Vec<&DataType> = output_schema
        .fields()
        .iter()
        .map(|f| f.data_type())
        .collect();
    let mut builders: Vec<Box<dyn ArrayBuilder>> = vec![];
    for data_type in &output_types {
        let builder: Box<dyn ArrayBuilder> = match data_type {
            DataType::Int8 => Box::new(Int8Builder::new(map.len())),
            DataType::Int16 => Box::new(Int16Builder::new(map.len())),
            DataType::Int32 => Box::new(Int32Builder::new(map.len())),
            DataType::Int64 => Box::new(Int64Builder::new(map.len())),
            DataType::UInt8 => Box::new(UInt8Builder::new(map.len())),
            DataType::UInt16 => Box::new(UInt16Builder::new(map.len())),
            DataType::UInt32 => Box::new(UInt32Builder::new(map.len())),
            DataType::UInt64 => Box::new(UInt64Builder::new(map.len())),
            DataType::Float32 => Box::new(Float32Builder::new(map.len())),
            DataType::Float64 => Box::new(Float64Builder::new(map.len())),
            DataType::Utf8 => Box::new(StringBuilder::new(map.len())),
            _ => {
                return Err(ExecutionError::ExecutionError(
                    "Unsupported data type in final aggregate result".to_string(),
                ))
            }
        };
        builders.push(builder);
    }

    // iterate over the map
    for (k, v) in map.iter() {
        // add group values to builders
        for i in 0..num_group_expr {
            let builder = builders[i].as_any_mut();
            match &k[i] {
                GroupByScalar::Int8(n) => group_val!(builder, Int8Builder, *n),
                GroupByScalar::Int16(n) => group_val!(builder, Int16Builder, *n),
                GroupByScalar::Int32(n) => group_val!(builder, Int32Builder, *n),
                GroupByScalar::Int64(n) => group_val!(builder, Int64Builder, *n),
                GroupByScalar::UInt8(n) => group_val!(builder, UInt8Builder, *n),
                GroupByScalar::UInt16(n) => group_val!(builder, UInt16Builder, *n),
                GroupByScalar::UInt32(n) => group_val!(builder, UInt32Builder, *n),
                GroupByScalar::UInt64(n) => group_val!(builder, UInt64Builder, *n),
                GroupByScalar::Utf8(str) => group_val!(builder, StringBuilder, str),
            }
        }

        // add aggregate values to builders
        for i in 0..num_aggr_expr {
            let value = v[i].borrow().get_value()?;
            let index = num_group_expr + i;
            let builder = builders[index].as_any_mut();
            match output_types[index] {
                DataType::Int8 => aggr_val!(builder, Int8Builder, value, Int8),
                DataType::Int16 => aggr_val!(builder, Int16Builder, value, Int16),
                DataType::Int32 => aggr_val!(builder, Int32Builder, value, Int32),
                DataType::Int64 => aggr_val!(builder, Int64Builder, value, Int64),
                DataType::UInt8 => aggr_val!(builder, UInt8Builder, value, UInt8),
                DataType::UInt16 => aggr_val!(builder, UInt16Builder, value, UInt16),
                DataType::UInt32 => aggr_val!(builder, UInt32Builder, value, UInt32),
                DataType::UInt64 => aggr_val!(builder, UInt64Builder, value, UInt64),
                DataType::Float32 => aggr_val!(builder, Float32Builder, value, Float32),
                DataType::Float64 => aggr_val!(builder, Float64Builder, value, Float64),
                // The aggr_val! macro doesn't work for ScalarValue::Utf8 because it contains
                // String and the builder wants &str. In all other cases the scalar and builder
                // types are the same.
                DataType::Utf8 => {
                    let builder = builder
                        .downcast_mut::<StringBuilder>()
                        .expect("failed to downcast builder to expected type");
                    match value {
                        Some(ScalarValue::Utf8(str)) => builder.append_value(&str)?,
                        None => builder.append_null()?,
                        Some(_) => {
                            return Err(ExecutionError::ExecutionError(
                                "Invalid value for accumulator".to_string(),
                            ))
                        }
                    }
                }
                _ => {
                    return Err(ExecutionError::ExecutionError(
                        "Unsupported aggregate data type".to_string(),
                    ))
                }
            };
        }
    }

    let arrays: Vec<ArrayRef> = builders
        .iter_mut()
        .map(|builder| builder.finish())
        .collect();

    let batch = RecordBatch::try_new(Arc::new(output_schema.to_owned()), arrays)?;

    Ok(batch)
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
fn create_key(
    group_by_keys: &[ArrayRef],
    row: usize,
    vec: &mut Vec<GroupByScalar>,
) -> Result<()> {
    for i in 0..group_by_keys.len() {
        let col = &group_by_keys[i];
        match col.data_type() {
            DataType::UInt8 => {
                let array = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                vec[i] = GroupByScalar::UInt8(array.value(row))
            }
            DataType::UInt16 => {
                let array = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                vec[i] = GroupByScalar::UInt16(array.value(row))
            }
            DataType::UInt32 => {
                let array = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                vec[i] = GroupByScalar::UInt32(array.value(row))
            }
            DataType::UInt64 => {
                let array = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                vec[i] = GroupByScalar::UInt64(array.value(row))
            }
            DataType::Int8 => {
                let array = col.as_any().downcast_ref::<Int8Array>().unwrap();
                vec[i] = GroupByScalar::Int8(array.value(row))
            }
            DataType::Int16 => {
                let array = col.as_any().downcast_ref::<Int16Array>().unwrap();
                vec[i] = GroupByScalar::Int16(array.value(row))
            }
            DataType::Int32 => {
                let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                vec[i] = GroupByScalar::Int32(array.value(row))
            }
            DataType::Int64 => {
                let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                vec[i] = GroupByScalar::Int64(array.value(row))
            }
            DataType::Utf8 => {
                let array = col.as_any().downcast_ref::<StringArray>().unwrap();
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
    use crate::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::physical_plan::expressions::{col, sum};
    use crate::physical_plan::merge::MergeExec;
    use crate::test;

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

        let partial_aggregate = Arc::new(HashAggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            Arc::new(csv),
        )?);

        // construct the expressions for the final aggregation
        let (final_group, final_aggr) = partial_aggregate.make_final_expr(
            groups.iter().map(|x| x.1.clone()).collect(),
            aggregates.iter().map(|x| x.1.clone()).collect(),
        );

        let merge = Arc::new(MergeExec::new(partial_aggregate, 2));

        let merged_aggregate = Arc::new(HashAggregateExec::try_new(
            AggregateMode::Final,
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
        )?);

        let result = test::execute(merged_aggregate)?;
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
