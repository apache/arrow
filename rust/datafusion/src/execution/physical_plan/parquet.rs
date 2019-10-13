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

use crate::error::Result;
use crate::execution::physical_plan::common;
use crate::execution::physical_plan::{BatchIterator, ExecutionPlan, Partition};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::array_reader::*;
use parquet::file::reader::{FileReader, SerializedFileReader};

/// Execution plan for scanning a Parquet file
pub struct ParquetExec {
    /// Path to directory containing partitioned Parquet files with the same schema
    path: String,
    /// Projection for which columns to load
    projection: Vec<usize>,
    /// Batch size
    batch_size: usize,
}

impl ParquetExec {
    pub fn new(path: &str, projection: Option<Vec<usize>>, batch_size: usize) -> Self {
        //TODO load first file to determine schema
        //TODO handle projection=None (map to projection of all columns)
        Self { path: path.to_string(), projection: projection.unwrap(), batch_size }
    }
}

impl ExecutionPlan for ParquetExec {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        let mut filenames: Vec<String> = vec![];
        common::build_file_list(&self.path, &mut filenames, ".parquet")?;
        let partitions = filenames
            .iter()
            .map(|filename| {
                Arc::new(ParquetPartition::new(
                    &filename,
                    self.projection.clone(),
                    self.batch_size,
                )) as Arc<dyn Partition>
            })
            .collect();
        Ok(partitions)
    }
}

struct ParquetPartition {
    /// Parquet filename
    filename: String,
    /// Projection for which columns to load
    projection: Vec<usize>,
    /// Batch size
    batch_size: usize,
}

impl ParquetPartition {
    /// Create a new Parquet partition
    pub fn new(filename: &str, projection: Vec<usize>, batch_size: usize) -> Self {
        Self { filename: filename.to_string(), projection, batch_size }
    }
}

impl Partition for ParquetPartition {
    fn execute(&self) -> Result<Arc<Mutex<dyn BatchIterator>>> {

        let file = File::open(&self.filename)?;
        let file_reader = Rc::new(SerializedFileReader::new(file)?);
        let parquet_schema = file_reader.metadata().file_metadata().schema_descr_ptr();

        let mut array_readers = vec![];
        for i in &self.projection {

            //TODO this method returns Box<> which is not thread-safe so we'll need to introduce channels
            let array_reader = build_array_reader(
                parquet_schema.clone(),
                vec![*i].into_iter(), //TODO I don't understand the usage of `vec![i]` .. presumably for primitive types this is always a single column and for structs it can be more?
                file_reader.clone(),
            )?;

            array_readers.push(array_reader);
        }


        unimplemented!()
        //Ok(Arc::new(Mutex::new(ParquetIterator::new(array_readers))))
    }
}

//struct ParquetIterator {
//    //readers: Vec<Box<dyn ArrayReader>>
//}
//
//impl ParquetIterator {
//    pub fn new(readers: Vec<Box<dyn ArrayReader>>) -> Self {
//        Self { readers }
//    }
//}
//
//impl BatchIterator for ParquetIterator {
//    fn schema(&self) -> Arc<Schema> {
//        unimplemented!()
//    }
//
//    fn next(&mut self) -> Result<Option<RecordBatch>> {
//        unimplemented!()
//    }
//}


#[cfg(test)]
mod tests {
    use std::env;
    use super::*;
    use crate::execution::physical_plan::csv::CsvExec;
    use crate::execution::physical_plan::expressions::{col, sum};
    use crate::test;

    #[test]
    fn test() -> Result<()> {

        let testdata =
            env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");
        let filename = format!("{}/alltypes_plain.parquet", testdata);
        let parquet_exec = ParquetExec::new(&filename, Some(vec![0]), 1024);
        let partitions = parquet_exec.partitions()?;
        assert_eq!(partitions.len(), 1);

        let results = partitions[0].execute()?;
        let mut results = results.lock().unwrap();
        while let Some(batch) = results.next()? {
            println!("got batch");
        }

        Ok(())
    }

}
