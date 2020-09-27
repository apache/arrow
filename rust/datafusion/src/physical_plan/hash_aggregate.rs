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

use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::physical_plan::{Accumulator, AggregateExpr};
use crate::physical_plan::{Distribution, ExecutionPlan, Partitioning, PhysicalExpr};

use crate::arrow::array::PrimitiveArrayOps;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::{
    array::{
        ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    compute,
};

use fnv::FnvHashMap;

use super::{common, expressions::Column};

use async_trait::async_trait;

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
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

fn create_schema(
    input_schema: &Schema,
    group_expr: &Vec<(Arc<dyn PhysicalExpr>, String)>,
    aggr_expr: &Vec<Arc<dyn AggregateExpr>>,
    mode: AggregateMode,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(group_expr.len() + aggr_expr.len());
    for (expr, name) in group_expr {
        fields.push(Field::new(
            name,
            expr.data_type(&input_schema)?,
            expr.nullable(&input_schema)?,
        ))
    }

    match mode {
        AggregateMode::Partial => {
            // in partial mode, the fields of the accumulator's state
            for expr in aggr_expr {
                fields.extend(expr.state_fields()?.iter().cloned())
            }
        }
        AggregateMode::Final => {
            // in final mode, the field with the final result of the accumulator
            for expr in aggr_expr {
                fields.push(expr.field()?)
            }
        }
    }

    Ok(Schema::new(fields))
}

impl HashAggregateExec {
    /// Create a new hash aggregate execution plan
    pub fn try_new(
        mode: AggregateMode,
        group_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &group_expr, &aggr_expr, mode)?;

        let schema = Arc::new(schema);

        Ok(HashAggregateExec {
            mode,
            group_expr,
            aggr_expr,
            input,
            schema,
        })
    }
}

#[async_trait]
impl ExecutionPlan for HashAggregateExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        let input = self.input.execute(partition).await?;
        let group_expr = self.group_expr.iter().map(|x| x.0.clone()).collect();

        if self.group_expr.is_empty() {
            Ok(Arc::new(Mutex::new(HashAggregateIterator::new(
                self.mode,
                self.schema.clone(),
                self.aggr_expr.clone(),
                input,
            ))))
        } else {
            Ok(Arc::new(Mutex::new(GroupedHashAggregateIterator::new(
                self.mode.clone(),
                self.schema.clone(),
                group_expr,
                self.aggr_expr.clone(),
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

/*
The architecture is the following:

1. An accumulator has state that is updated on each batch.
2. At the end of the aggregation (e.g. end of batches in a partition), the accumulator converts its state to a RecordBatch of a single row
3. The RecordBatches of all accumulators are merged (`concatenate` in `rust/arrow`) together to a single RecordBatch.
4. The state's RecordBatch is `merge`d to a new state
5. The state is mapped to the final value

Why:

* Accumulators' state can be statically typed, but it is more efficient to transmit data from the accumulators via `Array`
* The `merge` operation must have access to the state of the aggregators because it uses it to correctly merge
* It uses Arrow's native dynamically typed object, `Array`.
* Arrow shines in batch operations and both `merge` and `concatenate` of uniform types are very performant.

Example: average

* the state is `n: u32` and `sum: f64`
* For every batch, we update them accordingly.
* At the end of the accumulation (of a partition), we convert `n` and `sum` to a RecordBatch of 1 row and two columns: `[n, sum]`
* The RecordBatch is (sent back / transmitted over network)
* Once all N record batches arrive, `merge` is performed, which builds a RecordBatch with N rows and 2 columns.
* Finally, `get_value` returns an array with one entry computed from the state
*/
struct GroupedHashAggregateIterator {
    mode: AggregateMode,
    schema: SchemaRef,
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    input: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
    finished: bool,
}

impl GroupedHashAggregateIterator {
    /// Create a new HashAggregateIterator
    pub fn new(
        mode: AggregateMode,
        schema: SchemaRef,
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
    ) -> Self {
        GroupedHashAggregateIterator {
            mode,
            schema,
            group_expr,
            aggr_expr,
            input,
            finished: false,
        }
    }
}

type AccumulatorSet = Vec<Rc<RefCell<dyn Accumulator>>>;

impl RecordBatchReader for GroupedHashAggregateIterator {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }

        // return single batch
        self.finished = true;

        // the expressions to evaluate the batch, one vec of expressions per aggregation
        let aggregate_expressions = aggregate_expressions(&self.aggr_expr, &self.mode)
            .map_err(ExecutionError::into_arrow_external_error)?;

        // mapping key -> (set of accumulators, indices of the key in the batch)
        // * the indexes are updated at each row
        // * the accumulators are updated at the end of each batch
        // * the indexes are `clear`ed at the end of each batch
        let mut accumulators: FnvHashMap<
            Vec<GroupByScalar>,
            (AccumulatorSet, Box<Vec<u32>>),
        > = FnvHashMap::default();

        // iterate over all input batches and update the accumulators
        let mut input = self.input.lock().unwrap();

        // iterate over input and perform aggregation
        while let Some(batch) = &input.next_batch()? {
            // evaluate the grouping expressions
            let group_values = evaluate(&self.group_expr, batch)
                .map_err(ExecutionError::into_arrow_external_error)?;

            // evaluate the aggregation expressions.
            // We could evaluate them after the `take`, but since we need to evaluate all
            // of them anyways, it is more performant to do it while they are together.
            let aggr_input_values = evaluate_many(&aggregate_expressions, &batch)
                .map_err(ExecutionError::into_arrow_external_error)?;

            // create vector large enough to hold the grouping key
            // this is an optimization to avoid allocating `key` on every row.
            // it will be overwritten on every iteration of the loop below
            let mut key = Vec::with_capacity(group_values.len());
            for _ in 0..group_values.len() {
                key.push(GroupByScalar::UInt32(0));
            }

            // 1.1 construct the key from the group values
            // 1.2 construct the mapping key if it does not exist
            // 1.3 add the row' index to `indices`
            for row in 0..batch.num_rows() {
                // 1.1
                create_key(&group_values, row, &mut key)
                    .map_err(ExecutionError::into_arrow_external_error)?;

                match accumulators.get_mut(&key) {
                    // 1.2
                    None => {
                        let accumulator_set = create_accumulators(&self.aggr_expr)
                            .map_err(ExecutionError::into_arrow_external_error)?;

                        accumulators.insert(
                            key.clone(),
                            (accumulator_set, Box::new(vec![row as u32])),
                        );
                    }
                    // 1.3
                    Some((_, v)) => v.push(row as u32),
                }
            }

            // 2.1 for each key
            // 2.2 for each aggregation
            // 2.3 `take` from each of its arrays the keys' values
            // 2.4 update / merge the accumulator with the values
            // 2.5 clear indices
            accumulators
                .iter_mut()
                // 2.1
                .map(|(_, (accumulator_set, indices))| {
                    // 2.2
                    accumulator_set
                        .iter()
                        .zip(&aggr_input_values)
                        .into_iter()
                        .map(|(accumulator, aggr_array)| {
                            (
                                accumulator,
                                aggr_array
                                    .iter()
                                    .map(|array| {
                                        // 2.3
                                        compute::take(
                                            array,
                                            &UInt32Array::from(*indices.clone()),
                                            None, // None: no index check
                                        )
                                        .unwrap()
                                    })
                                    .collect::<Vec<ArrayRef>>(),
                            )
                        })
                        // 2.4
                        .map(|(accumulator, values)| match self.mode {
                            AggregateMode::Partial => {
                                accumulator.borrow_mut().update_batch(&values)
                            }
                            AggregateMode::Final => {
                                // note: the aggregation here is over states, not values, thus the merge
                                accumulator.borrow_mut().merge_batch(&values)
                            }
                        })
                        .collect::<Result<()>>()
                        // 2.5
                        .and(Ok(indices.clear()))
                })
                .collect::<Result<()>>()
                .map_err(ExecutionError::into_arrow_external_error)?;
        }

        let batch = create_batch_from_map(
            &self.mode,
            &accumulators,
            self.group_expr.len(),
            &self.schema,
        )
        .map_err(ExecutionError::into_arrow_external_error)?;

        Ok(Some(batch))
    }
}

/// Evaluates expressions against a record batch.
fn evaluate(
    expr: &Vec<Arc<dyn PhysicalExpr>>,
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    expr.iter()
        .map(|expr| expr.evaluate(&batch))
        .collect::<Result<Vec<_>>>()
}

/// Evaluates expressions against a record batch.
fn evaluate_many(
    expr: &Vec<Vec<Arc<dyn PhysicalExpr>>>,
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    expr.iter()
        .map(|expr| evaluate(expr, batch))
        .collect::<Result<Vec<_>>>()
}

/// uses `state_fields` to build a vec of expressions required to merge the AggregateExpr' accumulator's state.
fn merge_expressions(
    expr: &Arc<dyn AggregateExpr>,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    Ok(expr
        .state_fields()?
        .iter()
        .map(|f| Arc::new(Column::new(f.name())) as Arc<dyn PhysicalExpr>)
        .collect::<Vec<_>>())
}

/// returns physical expressions to evaluate against a batch
/// The expressions are different depending on `mode`:
/// * Partial: AggregateExpr::expressions
/// * Final: columns of `AggregateExpr::state_fields()`
/// The return value is to be understood as:
/// * index 0 is the aggregation
/// * index 1 is the expression i of the aggregation
fn aggregate_expressions(
    aggr_expr: &[Arc<dyn AggregateExpr>],
    mode: &AggregateMode,
) -> Result<Vec<Vec<Arc<dyn PhysicalExpr>>>> {
    match mode {
        AggregateMode::Partial => {
            Ok(aggr_expr.iter().map(|agg| agg.expressions()).collect())
        }
        // in this mode, we build the merge expressions of the aggregation
        AggregateMode::Final => Ok(aggr_expr
            .iter()
            .map(|agg| merge_expressions(agg))
            .collect::<Result<Vec<_>>>()?),
    }
}

struct HashAggregateIterator {
    mode: AggregateMode,
    schema: SchemaRef,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    input: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
    finished: bool,
}

impl HashAggregateIterator {
    /// Create a new HashAggregateIterator
    pub fn new(
        mode: AggregateMode,
        schema: SchemaRef,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
    ) -> Self {
        HashAggregateIterator {
            mode,
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

        // return single batch
        self.finished = true;

        let accumulators = create_accumulators(&self.aggr_expr)
            .map_err(ExecutionError::into_arrow_external_error)?;

        let expressions = aggregate_expressions(&self.aggr_expr, &self.mode)
            .map_err(ExecutionError::into_arrow_external_error)?;

        let mut input = self.input.lock().unwrap();

        // 1 for each batch:
        // 1.1 iterate accumulators and respective expressions together
        // 1.2 evaluate expressions
        // 1.3 update / merge accumulators with the expressions' values
        // 2 convert values to a record batch
        while let Some(batch) = input.next_batch()? {
            // 1.1
            accumulators
                .iter()
                .zip(&expressions)
                .map(|(accum, expr)| {
                    // 1.2
                    let values = &expr
                        .iter()
                        .map(|e| e.evaluate(&batch))
                        .collect::<Result<Vec<_>>>()?;

                    // 1.3
                    match self.mode {
                        AggregateMode::Partial => accum.borrow_mut().update_batch(values),
                        AggregateMode::Final => accum.borrow_mut().merge_batch(values),
                    }
                })
                .collect::<Result<()>>()
                .map_err(ExecutionError::into_arrow_external_error)?;
        }

        // 2
        let columns = finalize_aggregation(&accumulators, &self.mode)
            .map_err(ExecutionError::into_arrow_external_error)?;

        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(Some(batch))
    }
}

/// Given Vec<Vec<ArrayRef>>, concatenates the inners `Vec<ArrayRef>` into `ArrayRef`, returning `Vec<ArrayRef>`
/// This assumes that `arrays` is not empty.
fn concatenate(arrays: Vec<Vec<ArrayRef>>) -> ArrowResult<Vec<ArrayRef>> {
    (0..arrays[0].len())
        .map(|column| {
            let array_list = arrays.iter().map(|a| a[column].clone()).collect::<Vec<_>>();
            compute::concat(&array_list)
        })
        .collect::<ArrowResult<Vec<_>>>()
}

/// Create a RecordBatch with all group keys and accumulator' states or values.
fn create_batch_from_map(
    mode: &AggregateMode,
    accumulators: &FnvHashMap<Vec<GroupByScalar>, (AccumulatorSet, Box<Vec<u32>>)>,
    num_group_expr: usize,
    output_schema: &Schema,
) -> Result<RecordBatch> {
    // 1. for each key
    // 2. create single-row ArrayRef with all group expressions
    // 3. create single-row ArrayRef with all aggregate states or values
    // 4. collect all in a vector per key of vec<ArrayRef>, vec[i][j]
    // 5. concatenate the arrays over the second index [j] into a single vec<ArrayRef>.
    let arrays = accumulators
        .iter()
        .map(|(k, (accumulator_set, _))| {
            // 2.
            let mut groups = (0..num_group_expr)
                .map(|i| match &k[i] {
                    GroupByScalar::Int8(n) => {
                        Arc::new(Int8Array::from(vec![*n])) as ArrayRef
                    }
                    GroupByScalar::Int16(n) => Arc::new(Int16Array::from(vec![*n])),
                    GroupByScalar::Int32(n) => Arc::new(Int32Array::from(vec![*n])),
                    GroupByScalar::Int64(n) => Arc::new(Int64Array::from(vec![*n])),
                    GroupByScalar::UInt8(n) => Arc::new(UInt8Array::from(vec![*n])),
                    GroupByScalar::UInt16(n) => Arc::new(UInt16Array::from(vec![*n])),
                    GroupByScalar::UInt32(n) => Arc::new(UInt32Array::from(vec![*n])),
                    GroupByScalar::UInt64(n) => Arc::new(UInt64Array::from(vec![*n])),
                    GroupByScalar::Utf8(str) => Arc::new(StringArray::from(vec![&**str])),
                })
                .collect::<Vec<ArrayRef>>();

            // 3.
            groups.extend(
                finalize_aggregation(accumulator_set, mode)
                    .map_err(ExecutionError::into_arrow_external_error)?,
            );

            Ok(groups)
        })
        // 4.
        .collect::<Result<Vec<Vec<ArrayRef>>>>()?;

    let batch = if arrays.len() != 0 {
        // 5.
        let columns = concatenate(arrays)?;
        RecordBatch::try_new(Arc::new(output_schema.to_owned()), columns)?
    } else {
        common::create_batch_empty(output_schema)?
    };
    Ok(batch)
}

fn create_accumulators(
    aggr_expr: &Vec<Arc<dyn AggregateExpr>>,
) -> Result<AccumulatorSet> {
    aggr_expr
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect::<Result<Vec<_>>>()
}

/// returns a vector of ArrayRefs, where each entry corresponds to either the
/// final value (mode = Final) or states (mode = Partial)
fn finalize_aggregation(
    accumulators: &AccumulatorSet,
    mode: &AggregateMode,
) -> Result<Vec<ArrayRef>> {
    match mode {
        AggregateMode::Partial => {
            // build the vector of states
            let a = accumulators
                .iter()
                .map(|accumulator| accumulator.borrow_mut().state())
                .map(|value| {
                    value.and_then(|e| {
                        Ok(e.iter().map(|v| v.to_array()).collect::<Vec<ArrayRef>>())
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(a.iter().flatten().cloned().collect::<Vec<_>>())
        }
        AggregateMode::Final => {
            // merge the state to the final value
            accumulators
                .iter()
                .map(|accumulator| {
                    accumulator
                        .borrow_mut()
                        .evaluate()
                        .and_then(|v| Ok(v.to_array()))
                })
                .collect::<Result<Vec<ArrayRef>>>()
        }
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

    use arrow::array::Float64Array;

    use super::*;
    use crate::physical_plan::expressions::{col, Avg};
    use crate::physical_plan::merge::MergeExec;
    use crate::physical_plan::{common, memory::MemoryExec};

    fn some_data() -> ArrowResult<(Arc<Schema>, Vec<RecordBatch>)> {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        // define data.
        Ok((
            schema.clone(),
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt32Array::from(vec![2, 3, 4, 4])),
                        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                    ],
                )?,
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt32Array::from(vec![2, 3, 3, 4])),
                        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                    ],
                )?,
            ],
        ))
    }

    #[tokio::test]
    async fn aggregate() -> Result<()> {
        let (schema, batches) = some_data().unwrap();

        let input: Arc<dyn ExecutionPlan> = Arc::new(
            MemoryExec::try_new(&vec![batches.clone(), batches], schema, None).unwrap(),
        );

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("a"), "a".to_string())];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b"),
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        let partial_aggregate = Arc::new(HashAggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            input,
        )?);

        let result = common::collect(partial_aggregate.execute(0).await?)?;

        let keys = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(*keys, UInt32Array::from(vec![2, 3, 4]));

        let ns = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(*ns, UInt64Array::from(vec![2, 3, 3]));

        let sums = result[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(*sums, Float64Array::from(vec![2.0, 7.0, 11.0]));

        let merge = Arc::new(MergeExec::new(partial_aggregate, 2));

        let final_group: Vec<Arc<dyn PhysicalExpr>> =
            (0..groups.len()).map(|i| col(&groups[i].1)).collect();

        let merged_aggregate = Arc::new(HashAggregateExec::try_new(
            AggregateMode::Final,
            final_group
                .iter()
                .enumerate()
                .map(|(i, expr)| (expr.clone(), groups[i].1.clone()))
                .collect(),
            aggregates,
            merge,
        )?);

        let result = common::collect(merged_aggregate.execute(0).await?)?;
        assert_eq!(result.len(), 1);

        let batch = &result[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(*a, UInt32Array::from(vec![2, 3, 4]));
        assert_eq!(
            *b,
            Float64Array::from(vec![
                1.0,
                (2.0 + 3.0 + 2.0) / 3.0,
                (3.0 + 4.0 + 4.0) / 3.0
            ])
        );

        Ok(())
    }
}
