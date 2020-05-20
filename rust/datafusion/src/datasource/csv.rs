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

//! CSV Data source

use std::fs::File;

use arrow::csv;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::string::String;
use std::sync::Arc;

use crate::datasource::{ScanResult, TableProvider};
use crate::error::Result;
use crate::execution::physical_plan::csv::CsvExec;
use crate::execution::physical_plan::{BatchIterator, ExecutionPlan};

/// Represents a CSV file with a provided schema
// TODO: usage example (rather than documenting `new()`)
pub struct CsvFile {
    filename: String,
    schema: Arc<Schema>,
    has_header: bool,
    delimiter: Option<u8>,
}

impl CsvFile {
    #[allow(missing_docs)]
    pub fn new(
        filename: &str,
        schema: &Schema,
        has_header: bool,
        delimiter: Option<u8>,
    ) -> Self {
        Self {
            filename: String::from(filename),
            schema: Arc::new(schema.clone()),
            has_header,
            delimiter,
        }
    }

    /// Attempt to initialize a new `CsvFile` from a file path
    pub fn try_new(
        filename: &str,
        schema: Option<&Schema>,
        has_header: bool,
        delimiter: Option<u8>,
    ) -> Result<Self> {
        let schema = match schema {
            Some(s) => Arc::new(s.clone()),
            None => {
                let schema_infer_batch_size = 1024;
                let csv_exec = CsvExec::try_new(
                    filename,
                    None,
                    has_header,
                    delimiter,
                    None,
                    schema_infer_batch_size,
                )?;
                csv_exec.schema()
            }
        };
        Ok(Self {
            filename: String::from(filename),
            schema: schema,
            has_header,
            delimiter,
        })
    }
}

impl TableProvider for CsvFile {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Vec<ScanResult>> {
        let exec = CsvExec::try_new(
            &self.filename,
            Some(self.schema.clone()),
            self.has_header,
            self.delimiter,
            projection.clone(),
            batch_size,
        )?;
        let partitions = exec.partitions()?;
        let iterators = partitions
            .iter()
            .map(|p| p.execute())
            .collect::<Result<Vec<_>>>()?;
        Ok(iterators)
    }
}

/// Iterator over CSV batches
// TODO: usage example (rather than documenting `new()`)
pub struct CsvBatchIterator {
    schema: Arc<Schema>,
    reader: csv::Reader<File>,
}

impl CsvBatchIterator {
    #[allow(missing_docs)]
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

        let projected_schema = match projection {
            Some(p) => {
                let projected_fields: Vec<Field> =
                    p.iter().map(|i| schema.fields()[*i].clone()).collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => schema,
        };

        Ok(Self {
            schema: projected_schema,
            reader,
        })
    }
}

impl BatchIterator for CsvBatchIterator {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.reader.next()?)
    }
}
