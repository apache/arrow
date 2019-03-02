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

use std::cell::RefCell;
use std::fs::File;
use std::rc::Rc;
use std::string::String;
use std::sync::Arc;

use arrow::csv;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::datasource::{RecordBatchIterator, ScanResult, Table};
use crate::execution::error::Result;

/// Represents a CSV file with a provided schema
pub struct CsvFile {
    filename: String,
    schema: Arc<Schema>,
    has_header: bool,
}

impl CsvFile {
    pub fn new(filename: &str, schema: &Schema, has_header: bool) -> Self {
        Self {
            filename: String::from(filename),
            schema: Arc::new(schema.clone()),
            has_header,
        }
    }
}

impl Table for CsvFile {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<ScanResult> {
        Ok(Rc::new(RefCell::new(CsvBatchIterator::new(
            &self.filename,
            self.schema.clone(),
            self.has_header,
            projection,
            batch_size,
        ))))
    }
}

/// Iterator over CSV batches
pub struct CsvBatchIterator {
    schema: Arc<Schema>,
    reader: csv::Reader<File>,
}

impl CsvBatchIterator {
    pub fn new(
        filename: &str,
        schema: Arc<Schema>,
        has_header: bool,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        let file = File::open(filename).unwrap();
        let reader = csv::Reader::new(
            file,
            schema.clone(),
            has_header,
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

        Self {
            schema: projected_schema,
            reader,
        }
    }
}

impl RecordBatchIterator for CsvBatchIterator {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.reader.next()?)
    }
}
