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

//! Data sources

use std::cell::RefCell;
use std::fs::File;
use std::rc::Rc;
use std::string::String;
use std::sync::Arc;

use arrow::csv;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use super::error::Result;

pub trait DataSource {
    fn schema(&self) -> &Arc<Schema>;
    fn next(&mut self) -> Result<Option<RecordBatch>>;
}

/// CSV data source
pub struct CsvDataSource {
    schema: Arc<Schema>,
    reader: csv::Reader<File>,
}

impl CsvDataSource {
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

impl DataSource for CsvDataSource {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.reader.next()?)
    }
}

pub trait DataSourceProvider {
    fn schema(&self) -> &Arc<Schema>;
    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Rc<RefCell<DataSource>>;
}

/// Represents a CSV file with a provided schema
pub struct CsvProvider {
    filename: String,
    schema: Arc<Schema>,
    has_header: bool,
}

impl CsvProvider {
    pub fn new(filename: &str, schema: &Schema, has_header: bool) -> Self {
        Self {
            filename: String::from(filename),
            schema: Arc::new(schema.clone()),
            has_header,
        }
    }
}

impl DataSourceProvider for CsvProvider {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Rc<RefCell<DataSource>> {
        Rc::new(RefCell::new(CsvDataSource::new(
            &self.filename,
            self.schema.clone(),
            self.has_header,
            projection,
            batch_size,
        )))
    }
}
