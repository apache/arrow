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

//! In-memory data source for presenting a Vec<RecordBatch> as a data source that can be
//! queried by DataFusion. This allows data to be pre-loaded into memory and then
//! repeatedly queried without incurring additional file I/O overhead.

use futures::StreamExt;
use log::debug;
use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::datasource::TableProvider;
use crate::error::{DataFusionError, Result};
use crate::logical_plan::Expr;
use crate::physical_plan::common;
use crate::physical_plan::memory::MemoryExec;
use crate::physical_plan::ExecutionPlan;
use crate::{
    datasource::datasource::Statistics,
    physical_plan::{repartition::RepartitionExec, Partitioning},
};

use super::datasource::ColumnStatistics;

/// In-memory table
pub struct MemTable {
    schema: SchemaRef,
    batches: Vec<Vec<RecordBatch>>,
    statistics: Statistics,
}

// Calculates statistics based on partitions
fn calculate_statistics(
    schema: &SchemaRef,
    partitions: &[Vec<RecordBatch>],
) -> Statistics {
    let num_rows: usize = partitions
        .iter()
        .flat_map(|batches| batches.iter().map(RecordBatch::num_rows))
        .sum();

    let mut null_count: Vec<usize> = vec![0; schema.fields().len()];
    for partition in partitions.iter() {
        for batch in partition {
            for (i, array) in batch.columns().iter().enumerate() {
                null_count[i] += array.null_count();
            }
        }
    }

    let column_statistics = Some(
        null_count
            .iter()
            .map(|null_count| ColumnStatistics {
                null_count: Some(*null_count),
                distinct_count: None,
                max_value: None,
                min_value: None,
            })
            .collect(),
    );

    Statistics {
        num_rows: Some(num_rows),
        total_byte_size: None,
        column_statistics,
    }
}

impl MemTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn try_new(schema: SchemaRef, partitions: Vec<Vec<RecordBatch>>) -> Result<Self> {
        if partitions
            .iter()
            .flatten()
            .all(|batches| schema.contains(&batches.schema()))
        {
            let statistics = calculate_statistics(&schema, &partitions);
            debug!("MemTable statistics: {:?}", statistics);

            Ok(Self {
                schema,
                batches: partitions,
                statistics,
            })
        } else {
            Err(DataFusionError::Plan(
                "Mismatch between schema and batches".to_string(),
            ))
        }
    }

    /// Create a mem table by reading from another data source
    pub async fn load(
        t: Arc<dyn TableProvider>,
        batch_size: usize,
        output_partitions: Option<usize>,
    ) -> Result<Self> {
        let schema = t.schema();
        let exec = t.scan(&None, batch_size, &[], None)?;
        let partition_count = exec.output_partitioning().partition_count();

        let tasks = (0..partition_count)
            .map(|part_i| {
                let exec = exec.clone();
                tokio::spawn(async move {
                    let stream = exec.execute(part_i).await?;
                    common::collect(stream).await
                })
            })
            // this collect *is needed* so that the join below can
            // switch between tasks
            .collect::<Vec<_>>();

        let mut data: Vec<Vec<RecordBatch>> =
            Vec::with_capacity(exec.output_partitioning().partition_count());
        for task in tasks {
            let result = task.await.expect("MemTable::load could not join task")?;
            data.push(result);
        }

        let exec = MemoryExec::try_new(&data, schema.clone(), None)?;

        if let Some(num_partitions) = output_partitions {
            let exec = RepartitionExec::try_new(
                Arc::new(exec),
                Partitioning::RoundRobinBatch(num_partitions),
            )?;

            // execute and collect results
            let mut output_partitions = vec![];
            for i in 0..exec.output_partitioning().partition_count() {
                // execute this *output* partition and collect all batches
                let mut stream = exec.execute(i).await?;
                let mut batches = vec![];
                while let Some(result) = stream.next().await {
                    batches.push(result?);
                }
                output_partitions.push(batches);
            }

            return MemTable::try_new(schema.clone(), output_partitions);
        }
        MemTable::try_new(schema.clone(), data)
    }
}

impl TableProvider for MemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let columns: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => {
                let l = self.schema.fields().len();
                let mut v = Vec::with_capacity(l);
                for i in 0..l {
                    v.push(i);
                }
                v
            }
        };

        let projected_columns: Result<Vec<Field>> = columns
            .iter()
            .map(|i| {
                if *i < self.schema.fields().len() {
                    Ok(self.schema.field(*i).clone())
                } else {
                    Err(DataFusionError::Internal(
                        "Projection index out of range".to_string(),
                    ))
                }
            })
            .collect();

        let projected_schema = Arc::new(Schema::new(projected_columns?));

        Ok(Arc::new(MemoryExec::try_new(
            &self.batches.clone(),
            projected_schema,
            projection.clone(),
        )?))
    }

    fn statistics(&self) -> Statistics {
        self.statistics.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_with_projection() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
                Arc::new(Int32Array::from(vec![None, None, Some(9)])),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![batch]])?;

        assert_eq!(provider.statistics().num_rows, Some(3));
        assert_eq!(
            provider.statistics().column_statistics,
            Some(vec![
                ColumnStatistics {
                    null_count: Some(0),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
                ColumnStatistics {
                    null_count: Some(0),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
                ColumnStatistics {
                    null_count: Some(0),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
                ColumnStatistics {
                    null_count: Some(2),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
            ])
        );

        // scan with projection
        let exec = provider.scan(&Some(vec![2, 1]), 1024, &[], None)?;
        let mut it = exec.execute(0).await?;
        let batch2 = it.next().await.unwrap()?;
        assert_eq!(2, batch2.schema().fields().len());
        assert_eq!("c", batch2.schema().field(0).name());
        assert_eq!("b", batch2.schema().field(1).name());
        assert_eq!(2, batch2.num_columns());

        Ok(())
    }

    #[tokio::test]
    async fn test_without_projection() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![batch]])?;

        let exec = provider.scan(&None, 1024, &[], None)?;
        let mut it = exec.execute(0).await?;
        let batch1 = it.next().await.unwrap()?;
        assert_eq!(3, batch1.schema().fields().len());
        assert_eq!(3, batch1.num_columns());

        Ok(())
    }

    #[test]
    fn test_invalid_projection() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![batch]])?;

        let projection: Vec<usize> = vec![0, 4];

        match provider.scan(&Some(projection), 1024, &[], None) {
            Err(DataFusionError::Internal(e)) => {
                assert_eq!("\"Projection index out of range\"", format!("{:?}", e))
            }
            _ => panic!("Scan should failed on invalid projection"),
        };

        Ok(())
    }

    #[test]
    fn test_schema_validation_incompatible_column() -> Result<()> {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema1,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        match MemTable::try_new(schema2, vec![vec![batch]]) {
            Err(DataFusionError::Plan(e)) => assert_eq!(
                "\"Mismatch between schema and batches\"",
                format!("{:?}", e)
            ),
            _ => panic!("MemTable::new should have failed due to schema mismatch"),
        }

        Ok(())
    }

    #[test]
    fn test_schema_validation_different_column_count() -> Result<()> {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema1,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![7, 5, 9])),
            ],
        )?;

        match MemTable::try_new(schema2, vec![vec![batch]]) {
            Err(DataFusionError::Plan(e)) => assert_eq!(
                "\"Mismatch between schema and batches\"",
                format!("{:?}", e)
            ),
            _ => panic!("MemTable::new should have failed due to schema mismatch"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_merged_schema() -> Result<()> {
        let mut metadata = HashMap::new();
        metadata.insert("foo".to_string(), "bar".to_string());

        let schema1 = Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ],
            // test for comparing metadata
            metadata,
        );

        let schema2 = Schema::new(vec![
            // test for comparing nullability
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let merged_schema = Schema::try_merge(vec![schema1.clone(), schema2.clone()])?;

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let provider =
            MemTable::try_new(Arc::new(merged_schema), vec![vec![batch1, batch2]])?;

        let exec = provider.scan(&None, 1024, &[], None)?;
        let mut it = exec.execute(0).await?;
        let batch1 = it.next().await.unwrap()?;
        assert_eq!(3, batch1.schema().fields().len());
        assert_eq!(3, batch1.num_columns());
        assert_eq!(provider.statistics().num_rows, Some(6));

        Ok(())
    }
}
