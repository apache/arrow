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

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::datasource::TableProvider;
use crate::error::{ExecutionError, Result};
use crate::physical_plan::memory::MemoryExec;
use crate::physical_plan::ExecutionPlan;

/// In-memory table
pub struct MemTable {
    schema: SchemaRef,
    batches: Vec<Vec<RecordBatch>>,
}

impl MemTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn new(schema: SchemaRef, partitions: Vec<Vec<RecordBatch>>) -> Result<Self> {
        if partitions.iter().all(|partition| {
            partition
                .iter()
                .all(|batches| batches.schema().as_ref() == schema.as_ref())
        }) {
            Ok(Self {
                schema,
                batches: partitions,
            })
        } else {
            Err(ExecutionError::General(
                "Mismatch between schema and batches".to_string(),
            ))
        }
    }

    /// Create a mem table by reading from another data source
    pub async fn load(t: &dyn TableProvider) -> Result<Self> {
        let schema = t.schema();
        let exec = t.scan(&None, 1024 * 1024)?;

        let mut data: Vec<Vec<RecordBatch>> =
            Vec::with_capacity(exec.output_partitioning().partition_count());
        for partition in 0..exec.output_partitioning().partition_count() {
            let it = exec.execute(partition).await?;
            let mut it = it.lock().unwrap();
            let mut partition_batches = vec![];
            while let Ok(Some(batch)) = it.next_batch() {
                partition_batches.push(batch);
            }
            data.push(partition_batches);
        }

        MemTable::new(schema.clone(), data)
    }
}

impl TableProvider for MemTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
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
                    Err(ExecutionError::General(
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn test_with_projection() -> Result<()> {
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

        let provider = MemTable::new(schema, vec![vec![batch]])?;

        // scan with projection
        let exec = provider.scan(&Some(vec![2, 1]), 1024)?;
        let it = exec.execute(0).await?;
        let batch2 = it.lock().expect("mutex lock").next_batch()?.unwrap();
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

        let provider = MemTable::new(schema, vec![vec![batch]])?;

        let exec = provider.scan(&None, 1024)?;
        let it = exec.execute(0).await?;
        let batch1 = it.lock().expect("mutex lock").next_batch()?.unwrap();
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

        let provider = MemTable::new(schema, vec![vec![batch]])?;

        let projection: Vec<usize> = vec![0, 4];

        match provider.scan(&Some(projection), 1024) {
            Err(ExecutionError::General(e)) => {
                assert_eq!("\"Projection index out of range\"", format!("{:?}", e))
            }
            _ => assert!(false, "Scan should failed on invalid projection"),
        };

        Ok(())
    }

    #[test]
    fn test_schema_validation() -> Result<()> {
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
            schema1.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        match MemTable::new(schema2, vec![vec![batch]]) {
            Err(ExecutionError::General(e)) => assert_eq!(
                "\"Mismatch between schema and batches\"",
                format!("{:?}", e)
            ),
            _ => assert!(
                false,
                "MemTable::new should have failed due to schema mismatch"
            ),
        }

        Ok(())
    }
}
