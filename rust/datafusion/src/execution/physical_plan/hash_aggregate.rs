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
use std::str;
use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::{
    Accumulator, AggregateExpr, BatchIterator, ExecutionPlan, Partition, PhysicalExpr,
};

use arrow::array::{
    ArrayRef, BinaryArray, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::array::{
    BinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::execution::physical_plan::expressions::Column;
use crate::execution::physical_plan::merge::MergeExec;
use crate::logicalplan::ScalarValue;
use fnv::FnvHashMap;

/// Hash aggregate execution plan
pub struct HashAggregateExec {
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    input: Arc<dyn ExecutionPlan>,
    schema: Arc<Schema>,
}

impl HashAggregateExec {
    /// Create a new hash aggregate execution plan
    pub fn try_new(
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<dyn ExecutionPlan>,
        schema: Arc<Schema>,
    ) -> Result<Self> {
        Ok(HashAggregateExec {
            group_expr,
            aggr_expr,
            input,
            schema,
        })
    }
}

impl ExecutionPlan for HashAggregateExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        let partitions: Vec<Arc<dyn Partition>> = self
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
            .collect();

        // create partition to combine and aggregate the results
        let final_group: Vec<Arc<dyn PhysicalExpr>> = (0..self.group_expr.len())
            .map(|i| Arc::new(Column::new(i)) as Arc<dyn PhysicalExpr>)
            .collect();

        let final_aggr: Vec<Arc<dyn AggregateExpr>> = (0..self.aggr_expr.len())
            .map(|i| {
                let aggr = self.aggr_expr[i].create_combiner(i + self.group_expr.len());
                aggr as Arc<dyn AggregateExpr>
            })
            .collect();

        let mut fields = vec![];
        for expr in &final_group {
            let name = expr.name();
            fields.push(Field::new(&name, expr.data_type(&self.schema)?, true));
        }
        for expr in &final_aggr {
            let name = expr.name();
            fields.push(Field::new(&name, expr.data_type(&self.schema)?, true));
        }
        let schema = Arc::new(Schema::new(fields));

        let merge = MergeExec::new(schema.clone(), partitions);
        let merged: Vec<Arc<dyn Partition>> = merge.partitions()?;
        if merged.len() == 1 {
            Ok(vec![Arc::new(HashAggregatePartition::new(
                final_group,
                final_aggr,
                merged[0].clone(),
                schema,
            ))])
        } else {
            Err(ExecutionError::InternalError(format!(
                "MergeExec returned {} partitions",
                merged.len()
            )))
        }
    }
}

struct HashAggregatePartition {
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    input: Arc<dyn Partition>,
    schema: Arc<Schema>,
}

impl HashAggregatePartition {
    /// Create a new HashAggregatePartition
    pub fn new(
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<dyn Partition>,
        schema: Arc<Schema>,
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
    fn execute(&self) -> Result<Arc<Mutex<dyn BatchIterator>>> {
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
macro_rules! group_array_from_map_entries {
    ($BUILDER:ident, $TY:ident, $ENTRIES:expr, $COL_INDEX:expr) => {{
        let mut builder = $BUILDER::new($ENTRIES.len());
        let mut err = false;
        for j in 0..$ENTRIES.len() {
            match $ENTRIES[j].k[$COL_INDEX] {
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
macro_rules! aggr_array_from_map_entries {
    ($BUILDER:ident, $TY:ident, $TY2:ty, $ENTRIES:expr, $COL_INDEX:expr) => {{
        let mut builder = $BUILDER::new($ENTRIES.len());
        let mut err = false;
        for j in 0..$ENTRIES.len() {
            match $ENTRIES[j].v[$COL_INDEX] {
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

#[derive(Debug)]
struct MapEntry {
    k: Vec<GroupByScalar>,
    v: Vec<Option<ScalarValue>>,
}

struct GroupedHashAggregateIterator {
    schema: Arc<Schema>,
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    input: Arc<Mutex<dyn BatchIterator>>,
    finished: bool,
}

impl GroupedHashAggregateIterator {
    /// Create a new HashAggregateIterator
    pub fn new(
        schema: Arc<Schema>,
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<Mutex<dyn BatchIterator>>,
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

impl BatchIterator for GroupedHashAggregateIterator {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }

        self.finished = true;

        // create map to store accumulators for each unique grouping key
        let mut map: FnvHashMap<Vec<GroupByScalar>, Vec<Rc<RefCell<dyn Accumulator>>>> =
            FnvHashMap::default();

        // iterate over all input batches and update the accumulators
        let mut input = self.input.lock().unwrap();

        // iterate over input and perform aggregation
        while let Some(batch) = input.next()? {
            // evaluate the grouping expressions for this batch
            let group_values = self
                .group_expr
                .iter()
                .map(|expr| expr.evaluate(&batch))
                .collect::<Result<Vec<_>>>()?;

            // iterate over each row in the batch
            for row in 0..batch.num_rows() {
                // create grouping key for this row
                let key = create_key(&group_values, row)?;

                let updated: Result<bool> = match map.get(&key) {
                    Some(accumulators) => {
                        let _ = accumulators
                            .iter()
                            .map(|accum| accum.borrow_mut().accumulate(&batch, row))
                            .collect::<Result<Vec<_>>>()?;
                        Ok(true)
                    }
                    None => Ok(false),
                };

                if !updated? {
                    let accumulators: Vec<Rc<RefCell<dyn Accumulator>>> = self
                        .aggr_expr
                        .iter()
                        .map(|expr| expr.create_accumulator())
                        .collect();

                    let _ = accumulators
                        .iter()
                        .map(|accum| accum.borrow_mut().accumulate(&batch, row))
                        .collect::<Result<Vec<_>>>()?;

                    map.insert(key.clone(), accumulators);
                }
            }
        }
        let input_schema = input.schema();

        // convert the map to a vec to make it easier to build arrays
        let entries: Vec<MapEntry> = map
            .iter()
            .map(|(k, v)| {
                let aggr_values = v
                    .iter()
                    .map(|accum| {
                        let accum = accum.borrow_mut();
                        accum.get_value()
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(MapEntry {
                    k: k.clone(),
                    v: aggr_values,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // build the result arrays
        let mut result_arrays: Vec<ArrayRef> =
            Vec::with_capacity(self.group_expr.len() + self.aggr_expr.len());

        // grouping values
        for i in 0..self.group_expr.len() {
            let array: Result<ArrayRef> = match self.group_expr[i]
                .data_type(&input_schema)?
            {
                DataType::UInt8 => {
                    group_array_from_map_entries!(UInt8Builder, UInt8, entries, i)
                }
                DataType::UInt16 => {
                    group_array_from_map_entries!(UInt16Builder, UInt16, entries, i)
                }
                DataType::UInt32 => {
                    group_array_from_map_entries!(UInt32Builder, UInt32, entries, i)
                }
                DataType::UInt64 => {
                    group_array_from_map_entries!(UInt64Builder, UInt64, entries, i)
                }
                DataType::Int8 => {
                    group_array_from_map_entries!(Int8Builder, Int8, entries, i)
                }
                DataType::Int16 => {
                    group_array_from_map_entries!(Int16Builder, Int16, entries, i)
                }
                DataType::Int32 => {
                    group_array_from_map_entries!(Int32Builder, Int32, entries, i)
                }
                DataType::Int64 => {
                    group_array_from_map_entries!(Int64Builder, Int64, entries, i)
                }
                DataType::Utf8 => {
                    let mut builder = BinaryBuilder::new(1);
                    for j in 0..entries.len() {
                        match &entries[j].k[i] {
                            GroupByScalar::Utf8(s) => builder.append_string(&s).unwrap(),
                            _ => {}
                        }
                    }
                    Ok(Arc::new(builder.finish()) as ArrayRef)
                }
                _ => Err(ExecutionError::ExecutionError(
                    "Unsupported group by expr".to_string(),
                )),
            };
            result_arrays.push(array?);

            // aggregate values
            for i in 0..self.aggr_expr.len() {
                let aggr_data_type = self.aggr_expr[i].data_type(&input_schema)?;
                let array = match aggr_data_type {
                    DataType::UInt8 => aggr_array_from_map_entries!(
                        UInt64Builder,
                        UInt8,
                        u64,
                        entries,
                        i
                    ),
                    DataType::UInt16 => aggr_array_from_map_entries!(
                        UInt64Builder,
                        UInt16,
                        u64,
                        entries,
                        i
                    ),
                    DataType::UInt32 => aggr_array_from_map_entries!(
                        UInt64Builder,
                        UInt32,
                        u64,
                        entries,
                        i
                    ),
                    DataType::UInt64 => aggr_array_from_map_entries!(
                        UInt64Builder,
                        UInt64,
                        u64,
                        entries,
                        i
                    ),
                    DataType::Int8 => {
                        aggr_array_from_map_entries!(Int64Builder, Int8, i64, entries, i)
                    }
                    DataType::Int16 => {
                        aggr_array_from_map_entries!(Int64Builder, Int16, i64, entries, i)
                    }
                    DataType::Int32 => {
                        aggr_array_from_map_entries!(Int64Builder, Int32, i64, entries, i)
                    }
                    DataType::Int64 => {
                        aggr_array_from_map_entries!(Int64Builder, Int64, i64, entries, i)
                    }
                    DataType::Float32 => aggr_array_from_map_entries!(
                        Float32Builder,
                        Float32,
                        f32,
                        entries,
                        i
                    ),
                    DataType::Float64 => aggr_array_from_map_entries!(
                        Float64Builder,
                        Float64,
                        f64,
                        entries,
                        i
                    ),
                    _ => Err(ExecutionError::ExecutionError(
                        "Unsupported aggregate expr".to_string(),
                    )),
                };
                result_arrays.push(array?);
            }
        }

        let mut fields = vec![];
        for expr in &self.group_expr {
            let name = expr.name();
            fields.push(Field::new(&name, expr.data_type(&input_schema)?, true))
        }
        for expr in &self.aggr_expr {
            let name = expr.name();
            fields.push(Field::new(&name, expr.data_type(&input_schema)?, true))
        }
        let schema = Schema::new(fields);

        let batch = RecordBatch::try_new(Arc::new(schema), result_arrays)?;
        Ok(Some(batch))
    }
}

struct HashAggregateIterator {
    schema: Arc<Schema>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    input: Arc<Mutex<dyn BatchIterator>>,
    finished: bool,
}

impl HashAggregateIterator {
    /// Create a new HashAggregateIterator
    pub fn new(
        schema: Arc<Schema>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<Mutex<dyn BatchIterator>>,
    ) -> Self {
        HashAggregateIterator {
            schema,
            aggr_expr,
            input,
            finished: false,
        }
    }
}

impl BatchIterator for HashAggregateIterator {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
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
        while let Some(batch) = input.next()? {
            // iterate over each row in the batch
            for row in 0..batch.num_rows() {
                let _ = accumulators
                    .iter()
                    .map(|accum| accum.borrow_mut().accumulate(&batch, row))
                    .collect::<Result<Vec<_>>>()?;
            }
        }

        let input_schema = input.schema();

        // build the result arrays
        let mut result_arrays: Vec<ArrayRef> = Vec::with_capacity(self.aggr_expr.len());

        let entries = vec![MapEntry {
            k: vec![],
            v: accumulators
                .iter()
                .map(|accum| accum.borrow_mut().get_value())
                .collect::<Result<Vec<_>>>()?,
        }];

        // aggregate values
        for i in 0..self.aggr_expr.len() {
            let aggr_data_type = self.aggr_expr[i].data_type(&input_schema)?;
            let array = match aggr_data_type {
                DataType::UInt8 => {
                    aggr_array_from_map_entries!(UInt64Builder, UInt8, u64, entries, i)
                }
                DataType::UInt16 => {
                    aggr_array_from_map_entries!(UInt64Builder, UInt16, u64, entries, i)
                }
                DataType::UInt32 => {
                    aggr_array_from_map_entries!(UInt64Builder, UInt32, u64, entries, i)
                }
                DataType::UInt64 => {
                    aggr_array_from_map_entries!(UInt64Builder, UInt64, u64, entries, i)
                }
                DataType::Int8 => {
                    aggr_array_from_map_entries!(Int64Builder, Int8, i64, entries, i)
                }
                DataType::Int16 => {
                    aggr_array_from_map_entries!(Int64Builder, Int16, i64, entries, i)
                }
                DataType::Int32 => {
                    aggr_array_from_map_entries!(Int64Builder, Int32, i64, entries, i)
                }
                DataType::Int64 => {
                    aggr_array_from_map_entries!(Int64Builder, Int64, i64, entries, i)
                }
                DataType::Float32 => {
                    aggr_array_from_map_entries!(Float32Builder, Float32, f32, entries, i)
                }
                DataType::Float64 => {
                    aggr_array_from_map_entries!(Float64Builder, Float64, f64, entries, i)
                }
                _ => Err(ExecutionError::ExecutionError(
                    "Unsupported aggregate expr".to_string(),
                )),
            };
            result_arrays.push(array?);
        }

        let mut fields = vec![];
        for expr in &self.aggr_expr {
            let name = expr.name();
            fields.push(Field::new(&name, expr.data_type(&input_schema)?, true))
        }
        let schema = Schema::new(fields);

        let batch = RecordBatch::try_new(Arc::new(schema), result_arrays)?;
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
fn create_key(group_by_keys: &Vec<ArrayRef>, row: usize) -> Result<Vec<GroupByScalar>> {
    group_by_keys
        .iter()
        .map(|col| match col.data_type() {
            DataType::UInt8 => {
                let array = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                Ok(GroupByScalar::UInt8(array.value(row)))
            }
            DataType::UInt16 => {
                let array = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                Ok(GroupByScalar::UInt16(array.value(row)))
            }
            DataType::UInt32 => {
                let array = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                Ok(GroupByScalar::UInt32(array.value(row)))
            }
            DataType::UInt64 => {
                let array = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(GroupByScalar::UInt64(array.value(row)))
            }
            DataType::Int8 => {
                let array = col.as_any().downcast_ref::<Int8Array>().unwrap();
                Ok(GroupByScalar::Int8(array.value(row)))
            }
            DataType::Int16 => {
                let array = col.as_any().downcast_ref::<Int16Array>().unwrap();
                Ok(GroupByScalar::Int16(array.value(row)))
            }
            DataType::Int32 => {
                let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(GroupByScalar::Int32(array.value(row)))
            }
            DataType::Int64 => {
                let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(GroupByScalar::Int64(array.value(row)))
            }
            DataType::Utf8 => {
                let array = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                Ok(GroupByScalar::Utf8(String::from(
                    str::from_utf8(array.value(row)).unwrap(),
                )))
            }
            _ => Err(ExecutionError::ExecutionError(
                "Unsupported GROUP BY data type".to_string(),
            )),
        })
        .collect::<Result<Vec<GroupByScalar>>>()
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::execution::physical_plan::csv::CsvExec;
    use crate::execution::physical_plan::expressions::{col, sum};
    use crate::test;
    use arrow::datatypes::Field;

    #[test]
    fn aggregate() -> Result<()> {
        let schema = test::aggr_test_schema();

        let partitions = 4;
        let path = test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;

        let csv = CsvExec::try_new(&path, schema, true, None, 1024)?;

        let group_expr: Vec<Arc<dyn PhysicalExpr>> = vec![col(1)];

        let aggr_expr: Vec<Arc<dyn AggregateExpr>> = vec![sum(col(3))];

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Int64, true),
        ]));

        let partition_aggregate = HashAggregateExec::try_new(
            group_expr.clone(),
            aggr_expr.clone(),
            Arc::new(csv),
            schema.clone(),
        )?;

        let result = test::execute(&partition_aggregate)?;
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
