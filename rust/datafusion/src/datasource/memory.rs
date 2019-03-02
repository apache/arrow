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
//! queried by DataFusion. This allows data to be pre-loaded into memory and then repeatedly
//! queried without incurring additional file I/O overhead.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::datasource::{RecordBatchIterator, ScanResult, Table};
use crate::execution::error::{ExecutionError, Result};

/// In-memory table
pub struct MemTable {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
}

impl MemTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Result<Self> {
        if batches
            .iter()
            .all(|batch| batch.schema().as_ref() == schema.as_ref())
        {
            Ok(Self { schema, batches })
        } else {
            Err(ExecutionError::General(
                "Mismatch between schema and batches".to_string(),
            ))
        }
    }

    /// Create a mem table by reading from another data source
    pub fn load(t: &Table) -> Result<Self> {
        let schema = t.schema();
        let it = t.scan(&None, 1024 * 1024)?;
        let mut it_mut = it.borrow_mut();

        let mut data: Vec<RecordBatch> = vec![];

        while let Ok(Some(batch)) = it_mut.next() {
            data.push(batch);
        }
        MemTable::new(schema.clone(), data)
    }
}

impl Table for MemTable {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
    ) -> Result<ScanResult> {
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

        Ok(Rc::new(RefCell::new(MemBatchIterator {
            schema: projected_schema.clone(),
            index: 0,
            batches: self
                .batches
                .iter()
                .map(|batch| {
                    RecordBatch::new(
                        projected_schema.clone(),
                        columns.iter().map(|i| batch.column(*i).clone()).collect(),
                    )
                })
                .collect(),
        })))
    }
}

/// Iterator over an in-memory table
pub struct MemBatchIterator {
    schema: Arc<Schema>,
    index: usize,
    batches: Vec<RecordBatch>,
}

impl RecordBatchIterator for MemBatchIterator {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.index < self.batches.len() {
            self.index += 1;
            Ok(Some(self.batches[self.index - 1].clone()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_with_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        );

        let provider = MemTable::new(schema, vec![batch]).unwrap();

        // scan with projection
        let scan2 = provider.scan(&Some(vec![2, 1]), 1024).unwrap();
        let batch2 = scan2.borrow_mut().next().unwrap().unwrap();
        assert_eq!(2, batch2.schema().fields().len());
        assert_eq!("c", batch2.schema().field(0).name());
        assert_eq!("b", batch2.schema().field(1).name());
        assert_eq!(2, batch2.num_columns());
    }

    #[test]
    fn test_without_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        );

        let provider = MemTable::new(schema, vec![batch]).unwrap();

        let scan1 = provider.scan(&None, 1024).unwrap();
        let batch1 = scan1.borrow_mut().next().unwrap().unwrap();
        assert_eq!(3, batch1.schema().fields().len());
        assert_eq!(3, batch1.num_columns());
    }

    #[test]
    fn test_invalid_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        );

        let provider = MemTable::new(schema, vec![batch]).unwrap();

        let projection: Vec<usize> = vec![0, 4];

        match provider.scan(&Some(projection), 1024) {
            Err(ExecutionError::General(e)) => {
                assert_eq!("\"Projection index out of range\"", format!("{:?}", e))
            }
            _ => panic!("Scan should failed on invalid projection"),
        };
    }

    #[test]
    fn test_schema_validation() {
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

        let batch = RecordBatch::new(
            schema1.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        );

        match MemTable::new(schema2, vec![batch]) {
            Err(ExecutionError::General(e)) => assert_eq!(
                "\"Mismatch between schema and batches\"",
                format!("{:?}", e)
            ),
            _ => panic!("MemTable::new should have failed due to schema mismatch"),
        }
    }
}
