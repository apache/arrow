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

use std::fs::File;
use std::rc::Rc;
use std::sync::Arc;

use arrow::csv;
use arrow::datatypes::Schema;
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
    pub fn new(filename: &str, schema: Arc<Schema>, batch_size: usize) -> Self {
        let file = File::open(filename).unwrap();
        let reader = csv::Reader::new(file, schema.clone(), true, batch_size, None);
        Self { schema, reader }
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

#[derive(Serialize, Deserialize, Clone)]
pub enum DataSourceMeta {
    /// Represents a CSV file with a provided schema
    CsvFile {
        filename: String,
        schema: Rc<Schema>,
        has_header: bool,
        projection: Option<Vec<usize>>,
    },
    /// Represents a Parquet file that contains schema information
    ParquetFile {
        filename: String,
        schema: Rc<Schema>,
        projection: Option<Vec<usize>>,
    },
}
