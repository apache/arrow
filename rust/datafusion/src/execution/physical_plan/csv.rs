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

//! Execution plan for reading CSV files

use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::common;
use crate::execution::physical_plan::{BatchIterator, ExecutionPlan, Partition};
use arrow::csv;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

/// Execution plan for scanning a CSV file
pub struct CsvExec {
    /// Path to directory containing partitioned CSV files with the same schema
    path: String,
    /// Schema representing the CSV files after the optional projection is applied
    schema: Arc<Schema>,
    /// Does the CSV file have a header?
    has_header: bool,
    /// An optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Batch size
    batch_size: usize,
}

impl ExecutionPlan for CsvExec {
    /// Get the schema for this execution plan
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Get the partitions for this execution plan. Each partition can be executed in parallel.
    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        let mut filenames: Vec<String> = vec![];
        common::build_file_list(&self.path, &mut filenames, ".csv")?;
        let partitions = filenames
            .iter()
            .map(|filename| {
                Arc::new(CsvPartition::new(
                    &filename,
                    self.schema.clone(),
                    self.has_header,
                    self.delimiter,
                    self.projection.clone(),
                    self.batch_size,
                )) as Arc<dyn Partition>
            })
            .collect();
        Ok(partitions)
    }
}

impl CsvExec {
    /// Create a new execution plan for reading a set of CSV files
    pub fn try_new(
        path: &str,
        schema: Option<Arc<Schema>>,
        has_header: bool,
        delimiter: Option<u8>,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let schema = match schema {
            Some(s) => s,
            None => {
                let mut filenames: Vec<String> = vec![];
                common::build_file_list(path, &mut filenames, ".csv")?;
                if filenames.is_empty() {
                    return Err(ExecutionError::General("No files found".to_string()));
                }

                let f = File::open(&filenames[0])?;
                Arc::new(csv::infer_file_schema(
                    &mut BufReader::new(f),
                    delimiter.unwrap_or(b','),
                    Some(1000),
                    has_header,
                )?)
            }
        };
        Ok(Self {
            path: path.to_string(),
            schema,
            has_header,
            delimiter,
            projection,
            batch_size,
        })
    }
}

/// CSV Partition
struct CsvPartition {
    /// Path to the CSV File
    path: String,
    /// Schema representing the CSV file
    schema: Arc<Schema>,
    /// Does the CSV file have a header?
    has_header: bool,
    /// An optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Batch size
    batch_size: usize,
}

impl CsvPartition {
    fn new(
        path: &str,
        schema: Arc<Schema>,
        has_header: bool,
        delimiter: Option<u8>,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        Self {
            path: path.to_string(),
            schema,
            has_header,
            delimiter,
            projection,
            batch_size,
        }
    }
}

impl Partition for CsvPartition {
    /// Execute this partition and return an iterator over RecordBatch
    fn execute(&self) -> Result<Arc<Mutex<dyn BatchIterator>>> {
        Ok(Arc::new(Mutex::new(CsvIterator::try_new(
            &self.path,
            self.schema.clone(),
            self.has_header,
            self.delimiter,
            &self.projection,
            self.batch_size,
        )?)))
    }
}

/// Iterator over batches
struct CsvIterator {
    /// Arrow CSV reader
    reader: csv::Reader<File>,
}

impl CsvIterator {
    /// Create an iterator for a CSV file
    pub fn try_new(
        filename: &str,
        schema: Arc<Schema>,
        has_header: bool,
        delimiter: Option<u8>,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let file = File::open(filename)?;
        let reader = csv::Reader::new(
            file,
            schema.clone(),
            has_header,
            delimiter,
            batch_size,
            projection.clone(),
        );

        Ok(Self { reader })
    }
}

impl BatchIterator for CsvIterator {
    /// Get the schema
    fn schema(&self) -> Arc<Schema> {
        self.reader.schema()
    }

    /// Get the next RecordBatch
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.reader.next()?)
    }
}
