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

//! Defines the SORT plan

use std::any::Any;
use std::sync::{Arc, Mutex};

use arrow::array::ArrayRef;
pub use arrow::compute::SortOptions;
use arrow::compute::{concat, lexsort_to_indices, take, SortColumn, TakeOptions};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use crate::error::{ExecutionError, Result};
use crate::physical_plan::common::RecordBatchIterator;
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::{common, Distribution, ExecutionPlan, Partitioning};

use async_trait::async_trait;

/// Sort execution plan
#[derive(Debug)]
pub struct SortExec {
    /// Input schema
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    /// Number of threads to execute input partitions on before combining into a single partition
    concurrency: usize,
}

impl SortExec {
    /// Create a new sort execution plan
    pub fn try_new(
        expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        concurrency: usize,
    ) -> Result<Self> {
        Ok(Self {
            expr,
            input,
            concurrency,
        })
    }
}

#[async_trait]
impl ExecutionPlan for SortExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema().clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::SinglePartition
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(SortExec::try_new(
                self.expr.clone(),
                children[0].clone(),
                self.concurrency,
            )?)),
            _ => Err(ExecutionError::General(
                "SortExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        if 0 != partition {
            return Err(ExecutionError::General(format!(
                "SortExec invalid partition {}",
                partition
            )));
        }

        // sort needs to operate on a single partition currently
        if 1 != self.input.output_partitioning().partition_count() {
            return Err(ExecutionError::General(
                "SortExec requires a single input partition".to_owned(),
            ));
        }
        let it = self.input.execute(0).await?;
        let batches = common::collect(it)?;

        // combine all record batches into one for each column
        let combined_batch = RecordBatch::try_new(
            self.schema(),
            self.schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _)| -> Result<ArrayRef> {
                    Ok(concat(
                        &batches
                            .iter()
                            .map(|batch| batch.columns()[i].clone())
                            .collect::<Vec<ArrayRef>>(),
                    )?)
                })
                .collect::<Result<Vec<ArrayRef>>>()?,
        )?;

        // sort combined record batch
        let indices = lexsort_to_indices(
            &self
                .expr
                .iter()
                .map(|e| e.evaluate_to_sort_column(&combined_batch))
                .collect::<Result<Vec<SortColumn>>>()?,
        )?;

        // reorder all rows based on sorted indices
        let sorted_batch = RecordBatch::try_new(
            self.schema(),
            combined_batch
                .columns()
                .iter()
                .map(|column| -> Result<ArrayRef> {
                    Ok(take(
                        column,
                        &indices,
                        // disable bound check overhead since indices are already generated from
                        // the same record batch
                        Some(TakeOptions {
                            check_bounds: false,
                        }),
                    )?)
                })
                .collect::<Result<Vec<ArrayRef>>>()?,
        )?;

        Ok(Arc::new(Mutex::new(RecordBatchIterator::new(
            self.schema(),
            vec![Arc::new(sorted_batch)],
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::merge::MergeExec;
    use crate::test;
    use arrow::array::*;
    use arrow::datatypes::*;

    #[tokio::test]
    async fn test_sort() -> Result<()> {
        let schema = test::aggr_test_schema();
        let partitions = 4;
        let path = test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;
        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;

        let sort_exec = Arc::new(SortExec::try_new(
            vec![
                // c1 string column
                PhysicalSortExpr {
                    expr: col("c1"),
                    options: SortOptions::default(),
                },
                // c2 uin32 column
                PhysicalSortExpr {
                    expr: col("c2"),
                    options: SortOptions::default(),
                },
                // c7 uin8 column
                PhysicalSortExpr {
                    expr: col("c7"),
                    options: SortOptions::default(),
                },
            ],
            Arc::new(MergeExec::new(Arc::new(csv), 2)),
            2,
        )?);

        let result: Vec<RecordBatch> = test::execute(sort_exec).await?;
        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        let c1 = as_string_array(&columns[0]);
        assert_eq!(c1.value(0), "a");
        assert_eq!(c1.value(c1.len() - 1), "e");

        let c2 = as_primitive_array::<UInt32Type>(&columns[1]);
        assert_eq!(c2.value(0), 1);
        assert_eq!(c2.value(c2.len() - 1), 5,);

        let c7 = as_primitive_array::<UInt8Type>(&columns[6]);
        assert_eq!(c7.value(0), 15);
        assert_eq!(c7.value(c7.len() - 1), 254,);

        Ok(())
    }

    #[tokio::test]
    async fn test_lex_sort_by_float() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float64, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(vec![
                    Some(f32::NAN),
                    None,
                    None,
                    Some(f32::NAN),
                    Some(1.0_f32),
                    Some(1.0_f32),
                    Some(2.0_f32),
                    Some(3.0_f32),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(200.0_f64),
                    Some(20.0_f64),
                    Some(10.0_f64),
                    Some(100.0_f64),
                    Some(f64::NAN),
                    None,
                    None,
                    Some(f64::NAN),
                ])),
            ],
        )?;

        let sort_exec = Arc::new(SortExec::try_new(
            vec![
                PhysicalSortExpr {
                    expr: col("a"),
                    options: SortOptions {
                        descending: true,
                        nulls_first: true,
                    },
                },
                PhysicalSortExpr {
                    expr: col("b"),
                    options: SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                },
            ],
            Arc::new(MemoryExec::try_new(&vec![vec![batch]], schema, None)?),
            2,
        )?);

        assert_eq!(DataType::Float32, *sort_exec.schema().field(0).data_type());
        assert_eq!(DataType::Float64, *sort_exec.schema().field(1).data_type());

        let result: Vec<RecordBatch> = test::execute(sort_exec).await?;
        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        assert_eq!(DataType::Float32, *columns[0].data_type());
        assert_eq!(DataType::Float64, *columns[1].data_type());

        let a = as_primitive_array::<Float32Type>(&columns[0]);
        let b = as_primitive_array::<Float64Type>(&columns[1]);

        // convert result to strings to allow comparing to expected result containing NaN
        let result: Vec<(Option<String>, Option<String>)> = (0..result[0].num_rows())
            .map(|i| {
                let aval = if a.is_valid(i) {
                    Some(a.value(i).to_string())
                } else {
                    None
                };
                let bval = if b.is_valid(i) {
                    Some(b.value(i).to_string())
                } else {
                    None
                };
                (aval, bval)
            })
            .collect();

        let expected: Vec<(Option<String>, Option<String>)> = vec![
            (None, Some("10".to_owned())),
            (None, Some("20".to_owned())),
            (Some("NaN".to_owned()), Some("100".to_owned())),
            (Some("NaN".to_owned()), Some("200".to_owned())),
            (Some("3".to_owned()), Some("NaN".to_owned())),
            (Some("2".to_owned()), None),
            (Some("1".to_owned()), Some("NaN".to_owned())),
            (Some("1".to_owned()), None),
        ];

        assert_eq!(expected, result);

        Ok(())
    }
}
