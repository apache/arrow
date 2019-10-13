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

//! Execution plan for reading Parquet files

use std::fs::File;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::common;
use crate::execution::physical_plan::{BatchIterator, ExecutionPlan, Partition};
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::array_reader::*;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crossbeam::channel::{unbounded, Receiver, Sender};
use parquet::arrow::parquet_to_arrow_schema;

/// Execution plan for scanning a Parquet file
pub struct ParquetExec {
    /// Path to directory containing partitioned Parquet files with the same schema
    filenames: Vec<String>,
    /// Schema after projection is applied
    schema: Arc<Schema>,
    /// Projection for which columns to load
    projection: Vec<usize>,
    /// Batch size
    batch_size: usize,
}

impl ParquetExec {
    pub fn try_new(
        path: &str,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let mut filenames: Vec<String> = vec![];
        common::build_file_list(path, &mut filenames, ".parquet")?;
        if filenames.is_empty() {
            Err(ExecutionError::General("No files found".to_string()))
        } else {
            let file = File::open(&filenames[0])?;
            let reader = SerializedFileReader::new(file)?;
            let metadata = reader.metadata();
            let schema =
                parquet_to_arrow_schema(metadata.file_metadata().schema_descr_ptr())?;

            let projection = match projection {
                Some(p) => p,
                None => vec![], //TODO select all
            };

            let projected_schema = Schema::new(
                projection
                    .iter()
                    .map(|i| schema.field(*i).clone())
                    .collect(),
            );

            println!("projection: {:?}", projection);
            println!("projected schema: {:?}", projected_schema);

            Ok(Self {
                filenames,
                schema: Arc::new(projected_schema),
                projection,
                batch_size,
            })
        }
    }
}

impl ExecutionPlan for ParquetExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        let partitions = self
            .filenames
            .iter()
            .map(|filename| {
                Arc::new(ParquetPartition::new(
                    &filename,
                    self.projection.clone(),
                    self.schema.clone(),
                    self.batch_size,
                )) as Arc<dyn Partition>
            })
            .collect();
        Ok(partitions)
    }
}

struct ParquetPartition {
    iterator: Arc<Mutex<dyn BatchIterator>>,
}

impl ParquetPartition {
    /// Create a new Parquet partition
    pub fn new(
        filename: &str,
        projection: Vec<usize>,
        schema: Arc<Schema>,
        batch_size: usize,
    ) -> Self {
        // because the parquet implementation is not thread-safe, it is necessary to execute
        // on a thread and communicate with channels
        let (request_tx, request_rx): (Sender<()>, Receiver<()>) = unbounded();
        let (response_tx, response_rx): (
            Sender<Result<Option<RecordBatch>>>,
            Receiver<Result<Option<RecordBatch>>>,
        ) = unbounded();
        let filename = filename.to_string();

        let projected_schema = schema.clone();
        thread::spawn(move || {
            //TODO error handling, remove unwraps

            // open file
            let file = File::open(&filename).unwrap();
            let file_reader = Rc::new(SerializedFileReader::new(file).unwrap());
            let parquet_schema =
                file_reader.metadata().file_metadata().schema_descr_ptr();

            // create array readers
            let mut array_readers = Vec::with_capacity(projection.len());
            for i in projection {
                println!("create array reader");

                let array_reader = build_array_reader(
                    parquet_schema.clone(),
                    vec![i].into_iter(),
                    file_reader.clone(),
                )
                .unwrap();
                array_readers.push(array_reader);
            }

            while let Ok(_) = request_rx.recv() {
                let arrays: Vec<ArrayRef> = array_readers
                    .iter_mut()
                    .map(|r| {
                        println!("read array batch");
                        r.next_batch(batch_size).unwrap()
                    })
                    .collect();

                println!("read {} arrays", arrays.len());
                //println!("schema has {} fields", projected_schema.fields().len());

                let batch =
                    RecordBatch::try_new(projected_schema.clone(), arrays).unwrap();
                response_tx.send(Ok(Some(batch))).unwrap();
            }
        });

        let iterator = Arc::new(Mutex::new(ParquetIterator {
            schema,
            request_tx,
            response_rx,
        }));

        Self { iterator }
    }
}

impl Partition for ParquetPartition {
    fn execute(&self) -> Result<Arc<Mutex<dyn BatchIterator>>> {
        Ok(self.iterator.clone())
    }
}

struct ParquetIterator {
    schema: Arc<Schema>,
    request_tx: Sender<()>,
    response_rx: Receiver<Result<Option<RecordBatch>>>,
}

impl BatchIterator for ParquetIterator {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.request_tx.send(()) {
            Ok(_) => match self.response_rx.recv() {
                Ok(batch) => batch,
                Err(e) => Err(ExecutionError::General(format!(
                    "Error receiving batch: {:?}",
                    e
                ))),
            },
            _ => Err(ExecutionError::General(
                "Error sending request for next batch".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;
    use std::env;

    #[test]
    fn test() -> Result<()> {
        let testdata =
            env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");
        let filename = format!("{}/alltypes_plain.parquet", testdata);
        let parquet_exec = ParquetExec::try_new(&filename, Some(vec![0, 1, 2]), 1024)?;
        let partitions = parquet_exec.partitions()?;
        assert_eq!(partitions.len(), 1);

        let results = partitions[0].execute()?;
        let mut results = results.lock().unwrap();
        while let Some(batch) = results.next()? {
            println!(
                "got batch with {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
        }

        //TODO assertions

        Ok(())
    }
}
