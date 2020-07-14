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
use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::common;
use crate::execution::physical_plan::{ExecutionPlan, Partition};
use arrow::csv;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

/// CSV file read option
#[derive(Copy, Clone)]
pub struct CsvReadOptions<'a> {
    /// Does the CSV file have a header?
    ///
    /// If schema inference is run on a file with no headers, default column names
    /// are created.
    pub has_header: bool,
    /// An optional column delimiter. Defaults to `b','`.
    pub delimiter: u8,
    /// An optional schema representing the CSV files. If None, CSV reader will try to infer it
    /// based on data in file.
    pub schema: Option<&'a Schema>,
    /// Max number of rows to read from CSV files for schema inference if needed. Defaults to 1000.
    pub schema_infer_max_records: usize,
}

impl<'a> CsvReadOptions<'a> {
    /// Create a CSV read option with default presets
    pub fn new() -> Self {
        Self {
            has_header: true,
            schema: None,
            schema_infer_max_records: 1000,
            delimiter: b',',
        }
    }

    /// Configure has_header setting
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Specify delimiter to use for CSV read
    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Configure delimiter setting with Option, None value will be ignored
    pub fn delimiter_option(mut self, delimiter: Option<u8>) -> Self {
        match delimiter {
            Some(d) => {
                self.delimiter = d;
            }
            _ => (),
        }
        self
    }

    /// Specify schema to use for CSV read
    pub fn schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Configure number of max records to read for schema inference
    pub fn schema_infer_max_records(mut self, max_records: usize) -> Self {
        self.schema_infer_max_records = max_records;
        self
    }
}

/// Execution plan for scanning a CSV file
pub struct CsvExec {
    /// Path to directory containing partitioned CSV files with the same schema
    path: String,
    /// Schema representing the CSV file
    schema: SchemaRef,
    /// Does the CSV file have a header?
    has_header: bool,
    /// An optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Schema after the projection has been applied
    projected_schema: SchemaRef,
    /// Batch size
    batch_size: usize,
}

impl CsvExec {
    /// Create a new execution plan for reading a set of CSV files
    pub fn try_new(
        path: &str,
        options: CsvReadOptions,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let schema = match options.schema {
            Some(s) => s.clone(),
            None => CsvExec::try_infer_schema(path, &options)?,
        };

        let projected_schema = match &projection {
            None => schema.clone(),
            Some(p) => Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()),
        };

        Ok(Self {
            path: path.to_string(),
            schema: Arc::new(schema),
            has_header: options.has_header,
            delimiter: Some(options.delimiter),
            projection,
            projected_schema: Arc::new(projected_schema),
            batch_size,
        })
    }

    /// Infer schema for given CSV dataset
    pub fn try_infer_schema(path: &str, options: &CsvReadOptions) -> Result<Schema> {
        let mut filenames: Vec<String> = vec![];
        common::build_file_list(path, &mut filenames, ".csv")?;
        if filenames.is_empty() {
            return Err(ExecutionError::General("No files found".to_string()));
        }

        Ok(csv::infer_schema_from_files(
            &filenames,
            options.delimiter,
            Some(options.schema_infer_max_records),
            options.has_header,
        )?)
    }
}

impl ExecutionPlan for CsvExec {
    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
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

/// CSV Partition
struct CsvPartition {
    /// Path to the CSV File
    path: String,
    /// Schema representing the CSV file
    schema: SchemaRef,
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
        schema: SchemaRef,
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
    fn execute(&self) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
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
        schema: SchemaRef,
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

impl RecordBatchReader for CsvIterator {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }

    /// Get the next RecordBatch
    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        Ok(self.reader.next()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{aggr_test_schema, arrow_testdata_path};

    #[test]
    fn csv_exec_with_projection() -> Result<()> {
        let schema = aggr_test_schema();
        let testdata = arrow_testdata_path();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let csv = CsvExec::try_new(
            &path,
            CsvReadOptions::new().schema(&schema),
            Some(vec![0, 2, 4]),
            1024,
        )?;
        assert_eq!(13, csv.schema.fields().len());
        assert_eq!(3, csv.projected_schema.fields().len());
        assert_eq!(3, csv.schema().fields().len());
        let partitions = csv.partitions()?;
        let results = partitions[0].execute()?;
        let mut it = results.lock().unwrap();
        let batch = it.next_batch()?.unwrap();
        assert_eq!(3, batch.num_columns());
        let batch_schema = batch.schema();
        assert_eq!(3, batch_schema.fields().len());
        assert_eq!("c1", batch_schema.field(0).name());
        assert_eq!("c3", batch_schema.field(1).name());
        assert_eq!("c5", batch_schema.field(2).name());
        Ok(())
    }

    #[test]
    fn csv_exec_without_projection() -> Result<()> {
        let schema = aggr_test_schema();
        let testdata = arrow_testdata_path();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;
        assert_eq!(13, csv.schema.fields().len());
        assert_eq!(13, csv.projected_schema.fields().len());
        assert_eq!(13, csv.schema().fields().len());
        let partitions = csv.partitions()?;
        let results = partitions[0].execute()?;
        let mut it = results.lock().unwrap();
        let batch = it.next_batch()?.unwrap();
        assert_eq!(13, batch.num_columns());
        let batch_schema = batch.schema();
        assert_eq!(13, batch_schema.fields().len());
        assert_eq!("c1", batch_schema.field(0).name());
        assert_eq!("c2", batch_schema.field(1).name());
        assert_eq!("c3", batch_schema.field(2).name());
        Ok(())
    }
}
