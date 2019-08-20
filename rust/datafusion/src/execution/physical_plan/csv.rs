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

use std::fs;
use std::fs::File;
use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::{BatchIterator, ExecutionPlan, Partition};
use arrow::csv;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

/// Execution plan for scanning a CSV file
pub struct CsvExec {
    /// Path to directory containing partitioned CSV files with the same schema
    path: String,
    /// Schema representing the CSV files after the optional projection is applied
    schema: Arc<Schema>,
    /// Does the CSV file have a header?
    has_header: bool,
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
        self.build_file_list(&self.path, &mut filenames)?;
        let partitions = filenames
            .iter()
            .map(|filename| {
                Arc::new(CsvPartition::new(
                    &filename,
                    self.schema.clone(),
                    self.has_header,
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
        schema: Arc<Schema>,
        has_header: bool,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let projected_schema = match &projection {
            Some(p) => {
                let projected_fields: Vec<Field> =
                    p.iter().map(|i| schema.fields()[*i].clone()).collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => schema,
        };

        Ok(Self {
            path: path.to_string(),
            schema: projected_schema,
            has_header,
            projection,
            batch_size,
        })
    }

    /// Recursively build a list of csv files in a directory
    fn build_file_list(&self, dir: &str, filenames: &mut Vec<String>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(path_name) = path.to_str() {
                if path.is_dir() {
                    self.build_file_list(path_name, filenames)?;
                } else {
                    if path_name.ends_with(".csv") {
                        filenames.push(path_name.to_string());
                    }
                }
            } else {
                return Err(ExecutionError::General("Invalid path".to_string()));
            }
        }
        Ok(())
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
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        Self {
            path: path.to_string(),
            schema,
            has_header,
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
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let file = File::open(filename)?;
        let reader = csv::Reader::new(
            file,
            schema.clone(),
            has_header,
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
