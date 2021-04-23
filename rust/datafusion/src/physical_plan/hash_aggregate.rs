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
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use ahash::RandomState;
use futures::{
    stream::{Stream, StreamExt},
    Future,
};

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{Accumulator, AggregateExpr, SQLMetric};
use crate::physical_plan::{Distribution, ExecutionPlan, Partitioning, PhysicalExpr};

use arrow::{
    array::{Array, UInt32Builder},
    error::{ArrowError, Result as ArrowResult},
};
use arrow::{
    array::{
        ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    compute,
};
use arrow::{
    array::{BooleanArray, Date32Array, DictionaryArray},
    compute::cast,
    datatypes::{
        ArrowDictionaryKeyType, ArrowNativeType, Int16Type, Int32Type, Int64Type,
        Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};
use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use hashbrown::HashMap;
use ordered_float::OrderedFloat;
use pin_project_lite::pin_project;

use arrow::array::{
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
};
use async_trait::async_trait;

use super::{
    expressions::Column, group_scalar::GroupByScalar, RecordBatchStream,
    SendableRecordBatchStream,
};

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
    /// Aggregation mode (full, partial)
    mode: AggregateMode,
    /// Grouping expressions
    group_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// Aggregate expressions
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    /// Input plan, could be a partial aggregate or the input to the aggregate
    input: Arc<dyn ExecutionPlan>,
    /// Schema after the aggregate is applied
    schema: SchemaRef,
    /// Input schema before any aggregation is applied. For partial aggregate this will be the
    /// same as input.schema() but for the final aggregate it will be the same as the input
    /// to the partial aggregate
    input_schema: SchemaRef,
    /// Metric to track number of output rows
    output_rows: Arc<Mutex<SQLMetric>>,
}

fn create_schema(
    input_schema: &Schema,
    group_expr: &[(Arc<dyn PhysicalExpr>, String)],
    aggr_expr: &[Arc<dyn AggregateExpr>],
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
        input_schema: SchemaRef,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &group_expr, &aggr_expr, mode)?;

        let schema = Arc::new(schema);

        let output_rows = SQLMetric::counter("outputRows");

        Ok(HashAggregateExec {
            mode,
            group_expr,
            aggr_expr,
            input,
            schema,
            input_schema,
            output_rows,
        })
    }

    /// Aggregation mode (full, partial)
    pub fn mode(&self) -> &AggregateMode {
        &self.mode
    }

    /// Grouping expressions
    pub fn group_expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.group_expr
    }

    /// Aggregate expressions
    pub fn aggr_expr(&self) -> &[Arc<dyn AggregateExpr>] {
        &self.aggr_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any aggregates are applied
    pub fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
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

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition).await?;
        let group_expr = self.group_expr.iter().map(|x| x.0.clone()).collect();

        if self.group_expr.is_empty() {
            Ok(Box::pin(HashAggregateStream::new(
                self.mode,
                self.schema.clone(),
                self.aggr_expr.clone(),
                input,
            )))
        } else {
            Ok(Box::pin(GroupedHashAggregateStream::new(
                self.mode,
                self.schema.clone(),
                group_expr,
                self.aggr_expr.clone(),
                input,
                self.output_rows.clone(),
            )))
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
                self.input_schema.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "HashAggregateExec wrong number of children".to_string(),
            )),
        }
    }

    fn metrics(&self) -> HashMap<String, SQLMetric> {
        let mut metrics = HashMap::new();
        metrics.insert(
            "outputRows".to_owned(),
            self.output_rows.lock().unwrap().clone(),
        );
        metrics
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
pin_project! {
    struct GroupedHashAggregateStream {
        schema: SchemaRef,
        #[pin]
        output: futures::channel::oneshot::Receiver<ArrowResult<RecordBatch>>,
        finished: bool,
        output_rows: Arc<Mutex<SQLMetric>>,
    }
}

fn group_aggregate_batch(
    mode: &AggregateMode,
    group_expr: &[Arc<dyn PhysicalExpr>],
    aggr_expr: &[Arc<dyn AggregateExpr>],
    batch: RecordBatch,
    mut accumulators: Accumulators,
    aggregate_expressions: &[Vec<Arc<dyn PhysicalExpr>>],
) -> Result<Accumulators> {
    // evaluate the grouping expressions
    let group_values = evaluate(group_expr, &batch)?;

    // evaluate the aggregation expressions.
    // We could evaluate them after the `take`, but since we need to evaluate all
    // of them anyways, it is more performant to do it while they are together.
    let aggr_input_values = evaluate_many(aggregate_expressions, &batch)?;

    // create vector large enough to hold the grouping key
    // this is an optimization to avoid allocating `key` on every row.
    // it will be overwritten on every iteration of the loop below
    let mut group_by_values = Vec::with_capacity(group_values.len());
    for _ in 0..group_values.len() {
        group_by_values.push(GroupByScalar::UInt32(0));
    }

    let mut group_by_values = group_by_values.into_boxed_slice();

    let mut key = Vec::with_capacity(group_values.len());

    // 1.1 construct the key from the group values
    // 1.2 construct the mapping key if it does not exist
    // 1.3 add the row' index to `indices`

    // Make sure we can create the accumulators or otherwise return an error
    create_accumulators(aggr_expr).map_err(DataFusionError::into_arrow_external_error)?;

    // Keys received in this batch
    let mut batch_keys = vec![];

    for row in 0..batch.num_rows() {
        // 1.1
        create_key(&group_values, row, &mut key)
            .map_err(DataFusionError::into_arrow_external_error)?;

        accumulators
            .raw_entry_mut()
            .from_key(&key)
            // 1.3
            .and_modify(|_, (_, _, v)| {
                if v.is_empty() {
                    batch_keys.push(key.clone())
                };
                v.push(row as u32)
            })
            // 1.2
            .or_insert_with(|| {
                // We can safely unwrap here as we checked we can create an accumulator before
                let accumulator_set = create_accumulators(aggr_expr).unwrap();
                batch_keys.push(key.clone());
                let _ = create_group_by_values(&group_values, row, &mut group_by_values);
                (
                    key.clone(),
                    (group_by_values.clone(), accumulator_set, vec![row as u32]),
                )
            });
    }

    // Collect all indices + offsets based on keys in this vec
    let mut batch_indices: UInt32Builder = UInt32Builder::new(0);
    let mut offsets = vec![0];
    let mut offset_so_far = 0;
    for key in batch_keys.iter() {
        let (_, _, indices) = accumulators.get_mut(key).unwrap();
        batch_indices.append_slice(&indices)?;
        offset_so_far += indices.len();
        offsets.push(offset_so_far);
    }
    let batch_indices = batch_indices.finish();

    // `Take` all values based on indices into Arrays
    let values: Vec<Vec<Arc<dyn Array>>> = aggr_input_values
        .iter()
        .map(|array| {
            array
                .iter()
                .map(|array| {
                    compute::take(
                        array.as_ref(),
                        &batch_indices,
                        None, // None: no index check
                    )
                    .unwrap()
                })
                .collect()
            // 2.3
        })
        .collect();

    // 2.1 for each key in this batch
    // 2.2 for each aggregation
    // 2.3 `slice` from each of its arrays the keys' values
    // 2.4 update / merge the accumulator with the values
    // 2.5 clear indices
    batch_keys
        .iter_mut()
        .zip(offsets.windows(2))
        .try_for_each(|(key, offsets)| {
            let (_, accumulator_set, indices) = accumulators.get_mut(key).unwrap();
            // 2.2
            accumulator_set
                .iter_mut()
                .zip(values.iter())
                .map(|(accumulator, aggr_array)| {
                    (
                        accumulator,
                        aggr_array
                            .iter()
                            .map(|array| {
                                // 2.3
                                array.slice(offsets[0], offsets[1] - offsets[0])
                            })
                            .collect::<Vec<ArrayRef>>(),
                    )
                })
                .try_for_each(|(accumulator, values)| match mode {
                    AggregateMode::Partial => accumulator.update_batch(&values),
                    AggregateMode::Final => {
                        // note: the aggregation here is over states, not values, thus the merge
                        accumulator.merge_batch(&values)
                    }
                })
                // 2.5
                .and({
                    indices.clear();
                    Ok(())
                })
        })?;
    Ok(accumulators)
}

/// Appends a sequence of [u8] bytes for the value in `col[row]` to
/// `vec` to be used as a key into the hash map for a dictionary type
///
/// Note that ideally, for dictionary encoded columns, we would be
/// able to simply use the dictionary idicies themselves (no need to
/// look up values) or possibly simply build the hash table entirely
/// on the dictionary indexes.
///
/// This aproach would likely work (very) well for the common case,
/// but it also has to to handle the case where the dictionary itself
/// is not the same across all record batches (and thus indexes in one
/// record batch may not correspond to the same index in another)
fn dictionary_create_key_for_col<K: ArrowDictionaryKeyType>(
    col: &ArrayRef,
    row: usize,
    vec: &mut Vec<u8>,
) -> Result<()> {
    let dict_col = col.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    // look up the index in the values dictionary
    let keys_col = dict_col.keys_array();
    let values_index = keys_col.value(row).to_usize().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Can not convert index to usize in dictionary of type creating group by value {:?}",
            keys_col.data_type()
        ))
    })?;

    create_key_for_col(&dict_col.values(), values_index, vec)
}

/// Appends a sequence of [u8] bytes for the value in `col[row]` to
/// `vec` to be used as a key into the hash map
fn create_key_for_col(col: &ArrayRef, row: usize, vec: &mut Vec<u8>) -> Result<()> {
    match col.data_type() {
        DataType::Boolean => {
            let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            vec.extend_from_slice(&[array.value(row) as u8]);
        }
        DataType::Float32 => {
            let array = col.as_any().downcast_ref::<Float32Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Float64 => {
            let array = col.as_any().downcast_ref::<Float64Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::UInt8 => {
            let array = col.as_any().downcast_ref::<UInt8Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::UInt16 => {
            let array = col.as_any().downcast_ref::<UInt16Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::UInt32 => {
            let array = col.as_any().downcast_ref::<UInt32Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::UInt64 => {
            let array = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Int8 => {
            let array = col.as_any().downcast_ref::<Int8Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Int16 => {
            let array = col.as_any().downcast_ref::<Int16Array>().unwrap();
            vec.extend(array.value(row).to_le_bytes().iter());
        }
        DataType::Int32 => {
            let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Int64 => {
            let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            let array = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let array = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let array = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Utf8 => {
            let array = col.as_any().downcast_ref::<StringArray>().unwrap();
            let value = array.value(row);
            // store the size
            vec.extend_from_slice(&value.len().to_le_bytes());
            // store the string value
            vec.extend_from_slice(value.as_bytes());
        }
        DataType::Date32 => {
            let array = col.as_any().downcast_ref::<Date32Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Dictionary(index_type, _) => match **index_type {
            DataType::Int8 => {
                dictionary_create_key_for_col::<Int8Type>(col, row, vec)?;
            }
            DataType::Int16 => {
                dictionary_create_key_for_col::<Int16Type>(col, row, vec)?;
            }
            DataType::Int32 => {
                dictionary_create_key_for_col::<Int32Type>(col, row, vec)?;
            }
            DataType::Int64 => {
                dictionary_create_key_for_col::<Int64Type>(col, row, vec)?;
            }
            DataType::UInt8 => {
                dictionary_create_key_for_col::<UInt8Type>(col, row, vec)?;
            }
            DataType::UInt16 => {
                dictionary_create_key_for_col::<UInt16Type>(col, row, vec)?;
            }
            DataType::UInt32 => {
                dictionary_create_key_for_col::<UInt32Type>(col, row, vec)?;
            }
            DataType::UInt64 => {
                dictionary_create_key_for_col::<UInt64Type>(col, row, vec)?;
            }
            _ => return Err(DataFusionError::Internal(format!(
                "Unsupported GROUP BY type (dictionary index type not supported creating key) {}",
                col.data_type(),
            ))),
        },
        _ => {
            // This is internal because we should have caught this before.
            return Err(DataFusionError::Internal(format!(
                "Unsupported GROUP BY type creating key {}",
                col.data_type(),
            )));
        }
    }
    Ok(())
}

/// Create a key `Vec<u8>` that is used as key for the hashmap
pub(crate) fn create_key(
    group_by_keys: &[ArrayRef],
    row: usize,
    vec: &mut Vec<u8>,
) -> Result<()> {
    vec.clear();
    for col in group_by_keys {
        create_key_for_col(col, row, vec)?
    }
    Ok(())
}

async fn compute_grouped_hash_aggregate(
    mode: AggregateMode,
    schema: SchemaRef,
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    mut input: SendableRecordBatchStream,
) -> ArrowResult<RecordBatch> {
    // the expressions to evaluate the batch, one vec of expressions per aggregation
    let aggregate_expressions = aggregate_expressions(&aggr_expr, &mode)
        .map_err(DataFusionError::into_arrow_external_error)?;

    // mapping key -> (set of accumulators, indices of the key in the batch)
    // * the indexes are updated at each row
    // * the accumulators are updated at the end of each batch
    // * the indexes are `clear`ed at the end of each batch
    //let mut accumulators: Accumulators = FnvHashMap::default();

    // iterate over all input batches and update the accumulators
    let mut accumulators = Accumulators::default();
    while let Some(batch) = input.next().await {
        let batch = batch?;
        accumulators = group_aggregate_batch(
            &mode,
            &group_expr,
            &aggr_expr,
            batch,
            accumulators,
            &aggregate_expressions,
        )
        .map_err(DataFusionError::into_arrow_external_error)?;
    }

    create_batch_from_map(&mode, &accumulators, group_expr.len(), &schema)
}

impl GroupedHashAggregateStream {
    /// Create a new HashAggregateStream
    pub fn new(
        mode: AggregateMode,
        schema: SchemaRef,
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: SendableRecordBatchStream,
        output_rows: Arc<Mutex<SQLMetric>>,
    ) -> Self {
        let (tx, rx) = futures::channel::oneshot::channel();

        let schema_clone = schema.clone();
        tokio::spawn(async move {
            let result = compute_grouped_hash_aggregate(
                mode,
                schema_clone,
                group_expr,
                aggr_expr,
                input,
            )
            .await;
            tx.send(result)
        });

        GroupedHashAggregateStream {
            schema,
            output: rx,
            finished: false,
            output_rows,
        }
    }
}

type AccumulatorItem = Box<dyn Accumulator>;
type Accumulators =
    HashMap<Vec<u8>, (Box<[GroupByScalar]>, Vec<AccumulatorItem>, Vec<u32>), RandomState>;

impl Stream for GroupedHashAggregateStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let output_rows = self.output_rows.clone();

        // is the output ready?
        let this = self.project();
        let output_poll = this.output.poll(cx);

        match output_poll {
            Poll::Ready(result) => {
                *this.finished = true;

                // check for error in receiving channel and unwrap actual result
                let result = match result {
                    Err(e) => Err(ArrowError::ExternalError(Box::new(e))), // error receiving
                    Ok(result) => result,
                };

                if let Ok(batch) = &result {
                    let mut output_rows = output_rows.lock().unwrap();
                    output_rows.add(batch.num_rows())
                }

                Poll::Ready(Some(result))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for GroupedHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Evaluates expressions against a record batch.
fn evaluate(
    expr: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    expr.iter()
        .map(|expr| expr.evaluate(&batch))
        .map(|r| r.map(|v| v.into_array(batch.num_rows())))
        .collect::<Result<Vec<_>>>()
}

/// Evaluates expressions against a record batch.
fn evaluate_many(
    expr: &[Vec<Arc<dyn PhysicalExpr>>],
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

pin_project! {
    struct HashAggregateStream {
        schema: SchemaRef,
        #[pin]
        output: futures::channel::oneshot::Receiver<ArrowResult<RecordBatch>>,
        finished: bool,
    }
}

async fn compute_hash_aggregate(
    mode: AggregateMode,
    schema: SchemaRef,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    mut input: SendableRecordBatchStream,
) -> ArrowResult<RecordBatch> {
    let mut accumulators = create_accumulators(&aggr_expr)
        .map_err(DataFusionError::into_arrow_external_error)?;

    let expressions = aggregate_expressions(&aggr_expr, &mode)
        .map_err(DataFusionError::into_arrow_external_error)?;

    let expressions = Arc::new(expressions);

    // 1 for each batch, update / merge accumulators with the expressions' values
    // future is ready when all batches are computed
    while let Some(batch) = input.next().await {
        let batch = batch?;
        aggregate_batch(&mode, &batch, &mut accumulators, &expressions)
            .map_err(DataFusionError::into_arrow_external_error)?;
    }

    // 2. convert values to a record batch
    finalize_aggregation(&accumulators, &mode)
        .map(|columns| RecordBatch::try_new(schema.clone(), columns))
        .map_err(DataFusionError::into_arrow_external_error)?
}

impl HashAggregateStream {
    /// Create a new HashAggregateStream
    pub fn new(
        mode: AggregateMode,
        schema: SchemaRef,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: SendableRecordBatchStream,
    ) -> Self {
        let (tx, rx) = futures::channel::oneshot::channel();

        let schema_clone = schema.clone();
        tokio::spawn(async move {
            let result =
                compute_hash_aggregate(mode, schema_clone, aggr_expr, input).await;
            tx.send(result)
        });

        HashAggregateStream {
            schema,
            output: rx,
            finished: false,
        }
    }
}

fn aggregate_batch(
    mode: &AggregateMode,
    batch: &RecordBatch,
    accumulators: &mut [AccumulatorItem],
    expressions: &[Vec<Arc<dyn PhysicalExpr>>],
) -> Result<()> {
    // 1.1 iterate accumulators and respective expressions together
    // 1.2 evaluate expressions
    // 1.3 update / merge accumulators with the expressions' values

    // 1.1
    accumulators
        .iter_mut()
        .zip(expressions)
        .try_for_each(|(accum, expr)| {
            // 1.2
            let values = &expr
                .iter()
                .map(|e| e.evaluate(batch))
                .map(|r| r.map(|v| v.into_array(batch.num_rows())))
                .collect::<Result<Vec<_>>>()?;

            // 1.3
            match mode {
                AggregateMode::Partial => accum.update_batch(values),
                AggregateMode::Final => accum.merge_batch(values),
            }
        })
}

impl Stream for HashAggregateStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        // is the output ready?
        let this = self.project();
        let output_poll = this.output.poll(cx);

        match output_poll {
            Poll::Ready(result) => {
                *this.finished = true;

                // check for error in receiving channel and unwrap actual result
                let result = match result {
                    Err(e) => Err(ArrowError::ExternalError(Box::new(e))), // error receiving
                    Ok(result) => result,
                };

                Poll::Ready(Some(result))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for HashAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Given Vec<Vec<ArrayRef>>, concatenates the inners `Vec<ArrayRef>` into `ArrayRef`, returning `Vec<ArrayRef>`
/// This assumes that `arrays` is not empty.
fn concatenate(arrays: Vec<Vec<ArrayRef>>) -> ArrowResult<Vec<ArrayRef>> {
    (0..arrays[0].len())
        .map(|column| {
            let array_list = arrays
                .iter()
                .map(|a| a[column].as_ref())
                .collect::<Vec<_>>();
            compute::concat(&array_list)
        })
        .collect::<ArrowResult<Vec<_>>>()
}

/// Create a RecordBatch with all group keys and accumulator' states or values.
fn create_batch_from_map(
    mode: &AggregateMode,
    accumulators: &Accumulators,
    num_group_expr: usize,
    output_schema: &Schema,
) -> ArrowResult<RecordBatch> {
    // 1. for each key
    // 2. create single-row ArrayRef with all group expressions
    // 3. create single-row ArrayRef with all aggregate states or values
    // 4. collect all in a vector per key of vec<ArrayRef>, vec[i][j]
    // 5. concatenate the arrays over the second index [j] into a single vec<ArrayRef>.
    let arrays = accumulators
        .iter()
        .map(|(_, (group_by_values, accumulator_set, _))| {
            // 2.
            let mut groups = (0..num_group_expr)
                .map(|i| match &group_by_values[i] {
                    GroupByScalar::Float32(n) => {
                        Arc::new(Float32Array::from(vec![(*n).into()] as Vec<f32>))
                            as ArrayRef
                    }
                    GroupByScalar::Float64(n) => {
                        Arc::new(Float64Array::from(vec![(*n).into()] as Vec<f64>))
                            as ArrayRef
                    }
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
                    GroupByScalar::Utf8(str) => {
                        Arc::new(StringArray::from(vec![&***str]))
                    }
                    GroupByScalar::Boolean(b) => Arc::new(BooleanArray::from(vec![*b])),
                    GroupByScalar::TimeMillisecond(n) => {
                        Arc::new(TimestampMillisecondArray::from(vec![*n]))
                    }
                    GroupByScalar::TimeMicrosecond(n) => {
                        Arc::new(TimestampMicrosecondArray::from(vec![*n]))
                    }
                    GroupByScalar::TimeNanosecond(n) => {
                        Arc::new(TimestampNanosecondArray::from_vec(vec![*n], None))
                    }
                    GroupByScalar::Date32(n) => Arc::new(Date32Array::from(vec![*n])),
                })
                .collect::<Vec<ArrayRef>>();

            // 3.
            groups.extend(
                finalize_aggregation(accumulator_set, mode)
                    .map_err(DataFusionError::into_arrow_external_error)?,
            );

            Ok(groups)
        })
        // 4.
        .collect::<ArrowResult<Vec<Vec<ArrayRef>>>>()?;

    let batch = if !arrays.is_empty() {
        // 5.
        let columns = concatenate(arrays)?;

        // cast output if needed (e.g. for types like Dictionary where
        // the intermediate GroupByScalar type was not the same as the
        // output
        let columns = columns
            .iter()
            .zip(output_schema.fields().iter())
            .map(|(col, desired_field)| cast(col, desired_field.data_type()))
            .collect::<ArrowResult<Vec<_>>>()?;

        RecordBatch::try_new(Arc::new(output_schema.to_owned()), columns)?
    } else {
        RecordBatch::new_empty(Arc::new(output_schema.to_owned()))
    };
    Ok(batch)
}

fn create_accumulators(
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> Result<Vec<AccumulatorItem>> {
    aggr_expr
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect::<Result<Vec<_>>>()
}

/// returns a vector of ArrayRefs, where each entry corresponds to either the
/// final value (mode = Final) or states (mode = Partial)
fn finalize_aggregation(
    accumulators: &[AccumulatorItem],
    mode: &AggregateMode,
) -> Result<Vec<ArrayRef>> {
    match mode {
        AggregateMode::Partial => {
            // build the vector of states
            let a = accumulators
                .iter()
                .map(|accumulator| accumulator.state())
                .map(|value| {
                    value.map(|e| {
                        e.iter().map(|v| v.to_array()).collect::<Vec<ArrayRef>>()
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(a.iter().flatten().cloned().collect::<Vec<_>>())
        }
        AggregateMode::Final => {
            // merge the state to the final value
            accumulators
                .iter()
                .map(|accumulator| accumulator.evaluate().map(|v| v.to_array()))
                .collect::<Result<Vec<ArrayRef>>>()
        }
    }
}

/// Extract the value in `col[row]` from a dictionary a GroupByScalar
fn dictionary_create_group_by_value<K: ArrowDictionaryKeyType>(
    col: &ArrayRef,
    row: usize,
) -> Result<GroupByScalar> {
    let dict_col = col.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    // look up the index in the values dictionary
    let keys_col = dict_col.keys_array();
    let values_index = keys_col.value(row).to_usize().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Can not convert index to usize in dictionary of type creating group by value {:?}",
            keys_col.data_type()
        ))
    })?;

    create_group_by_value(&dict_col.values(), values_index)
}

/// Extract the value in `col[row]` as a GroupByScalar
fn create_group_by_value(col: &ArrayRef, row: usize) -> Result<GroupByScalar> {
    match col.data_type() {
        DataType::Float32 => {
            let array = col.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(GroupByScalar::Float32(OrderedFloat::from(array.value(row))))
        }
        DataType::Float64 => {
            let array = col.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(GroupByScalar::Float64(OrderedFloat::from(array.value(row))))
        }
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
            let array = col.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(GroupByScalar::Utf8(Box::new(array.value(row).into())))
        }
        DataType::Boolean => {
            let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(GroupByScalar::Boolean(array.value(row)))
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            let array = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(GroupByScalar::TimeMillisecond(array.value(row)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let array = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(GroupByScalar::TimeMicrosecond(array.value(row)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let array = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(GroupByScalar::TimeNanosecond(array.value(row)))
        }
        DataType::Date32 => {
            let array = col.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(GroupByScalar::Date32(array.value(row)))
        }
        DataType::Dictionary(index_type, _) => match **index_type {
            DataType::Int8 => dictionary_create_group_by_value::<Int8Type>(col, row),
            DataType::Int16 => dictionary_create_group_by_value::<Int16Type>(col, row),
            DataType::Int32 => dictionary_create_group_by_value::<Int32Type>(col, row),
            DataType::Int64 => dictionary_create_group_by_value::<Int64Type>(col, row),
            DataType::UInt8 => dictionary_create_group_by_value::<UInt8Type>(col, row),
            DataType::UInt16 => dictionary_create_group_by_value::<UInt16Type>(col, row),
            DataType::UInt32 => dictionary_create_group_by_value::<UInt32Type>(col, row),
            DataType::UInt64 => dictionary_create_group_by_value::<UInt64Type>(col, row),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported GROUP BY type (dictionary index type not supported) {}",
                col.data_type(),
            ))),
        },
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported GROUP BY type {}",
            col.data_type(),
        ))),
    }
}

/// Extract the values in `group_by_keys` arrow arrays into the target vector
/// as GroupByScalar values
pub(crate) fn create_group_by_values(
    group_by_keys: &[ArrayRef],
    row: usize,
    vec: &mut Box<[GroupByScalar]>,
) -> Result<()> {
    for (i, col) in group_by_keys.iter().enumerate() {
        vec[i] = create_group_by_value(col, row)?
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use arrow::array::Float64Array;

    use super::*;
    use crate::physical_plan::expressions::{col, Avg};
    use crate::{assert_batches_sorted_eq, physical_plan::common};

    use crate::physical_plan::merge::MergeExec;

    /// some mock data to aggregates
    fn some_data() -> (Arc<Schema>, Vec<RecordBatch>) {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        // define data.
        (
            schema.clone(),
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt32Array::from(vec![2, 3, 4, 4])),
                        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(UInt32Array::from(vec![2, 3, 3, 4])),
                        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                    ],
                )
                .unwrap(),
            ],
        )
    }

    /// build the aggregates on the data from some_data() and check the results
    async fn check_aggregates(input: Arc<dyn ExecutionPlan>) -> Result<()> {
        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("a"), "a".to_string())];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b"),
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        let input_schema = input.schema();
        let partial_aggregate = Arc::new(HashAggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            input,
            input_schema.clone(),
        )?);

        let result = common::collect(partial_aggregate.execute(0).await?).await?;

        let expected = vec![
            "+---+---------------+-------------+",
            "| a | AVG(b)[count] | AVG(b)[sum] |",
            "+---+---------------+-------------+",
            "| 2 | 2             | 2           |",
            "| 3 | 3             | 7           |",
            "| 4 | 3             | 11          |",
            "+---+---------------+-------------+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        let merge = Arc::new(MergeExec::new(partial_aggregate));

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
            input_schema,
        )?);

        let result = common::collect(merged_aggregate.execute(0).await?).await?;
        assert_eq!(result.len(), 1);

        let batch = &result[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let expected = vec![
            "+---+--------------------+",
            "| a | AVG(b)             |",
            "+---+--------------------+",
            "| 2 | 1                  |",
            "| 3 | 2.3333333333333335 |", // 3, (2 + 3 + 2) / 3
            "| 4 | 3.6666666666666665 |", // 4, (3 + 4 + 4) / 3
            "+---+--------------------+",
        ];

        assert_batches_sorted_eq!(&expected, &result);

        let metrics = merged_aggregate.metrics();
        let output_rows = metrics.get("outputRows").unwrap();
        assert_eq!(3, output_rows.value());

        Ok(())
    }

    /// Define a test source that can yield back to runtime before returning its first item ///

    #[derive(Debug)]
    struct TestYieldingExec {
        /// True if this exec should yield back to runtime the first time it is polled
        pub yield_first: bool,
    }

    #[async_trait]
    impl ExecutionPlan for TestYieldingExec {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            some_data().0
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(1)
        }

        fn with_new_children(
            &self,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }

        async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
            let stream;
            if self.yield_first {
                stream = TestYieldingStream::New;
            } else {
                stream = TestYieldingStream::Yielded;
            }
            Ok(Box::pin(stream))
        }
    }

    /// A stream using the demo data. If inited as new, it will first yield to runtime before returning records
    enum TestYieldingStream {
        New,
        Yielded,
        ReturnedBatch1,
        ReturnedBatch2,
    }

    impl Stream for TestYieldingStream {
        type Item = ArrowResult<RecordBatch>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            match &*self {
                TestYieldingStream::New => {
                    *(self.as_mut()) = TestYieldingStream::Yielded;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                TestYieldingStream::Yielded => {
                    *(self.as_mut()) = TestYieldingStream::ReturnedBatch1;
                    Poll::Ready(Some(Ok(some_data().1[0].clone())))
                }
                TestYieldingStream::ReturnedBatch1 => {
                    *(self.as_mut()) = TestYieldingStream::ReturnedBatch2;
                    Poll::Ready(Some(Ok(some_data().1[1].clone())))
                }
                TestYieldingStream::ReturnedBatch2 => Poll::Ready(None),
            }
        }
    }

    impl RecordBatchStream for TestYieldingStream {
        fn schema(&self) -> SchemaRef {
            some_data().0
        }
    }

    //// Tests ////

    #[tokio::test]
    async fn aggregate_source_not_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: false });

        check_aggregates(input).await
    }

    #[tokio::test]
    async fn aggregate_source_with_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: true });

        check_aggregates(input).await
    }
}
