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

//! CSV data source
//!
//! This CSV data source allows CSV files to be used as input for queries.
//!
//! Example:
//!
//! ```
//! use datafusion::datasource::TableProvider;
//! use datafusion::datasource::csv::{CsvFile, CsvReadOptions};
//!
//! let testdata = arrow::util::test_util::arrow_test_data();
//! let csvdata = CsvFile::try_new(
//!     &format!("{}/csv/aggregate_test_100.csv", testdata),
//!     CsvReadOptions::new().delimiter(b'|'),
//! ).unwrap();
//! let schema = csvdata.schema();
//! ```

use arrow::datatypes::SchemaRef;
use std::any::Any;
use std::string::String;
use std::sync::Arc;

use crate::datasource::datasource::Statistics;
use crate::datasource::TableProvider;
use crate::error::{DataFusionError, Result};
use crate::logical_plan::Expr;
use crate::physical_plan::csv::CsvExec;
pub use crate::physical_plan::csv::CsvReadOptions;
use crate::physical_plan::{common, ExecutionPlan};

/// Represents a CSV file with a provided schema
pub struct CsvFile {
    /// Path to a single CSV file or a directory containing one of more CSV files
    path: String,
    schema: SchemaRef,
    has_header: bool,
    delimiter: u8,
    file_extension: String,
    statistics: Statistics,
}

impl CsvFile {
    /// Attempt to initialize a new `CsvFile` from a file path
    pub fn try_new(path: &str, options: CsvReadOptions) -> Result<Self> {
        let schema = Arc::new(match options.schema {
            Some(s) => s.clone(),
            None => {
                let mut filenames: Vec<String> = vec![];
                common::build_file_list(path, &mut filenames, options.file_extension)?;
                if filenames.is_empty() {
                    return Err(DataFusionError::Plan(format!(
                        "No files found at {path} with file extension {file_extension}",
                        path = path,
                        file_extension = options.file_extension
                    )));
                }
                CsvExec::try_infer_schema(&filenames, &options)?
            }
        });

        Ok(Self {
            path: String::from(path),
            schema,
            has_header: options.has_header,
            delimiter: options.delimiter,
            file_extension: String::from(options.file_extension),
            statistics: Statistics::default(),
        })
    }

    /// Get the path for the CSV file(s) represented by this CsvFile instance
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Determine whether the CSV file(s) represented by this CsvFile instance have a header row
    pub fn has_header(&self) -> bool {
        self.has_header
    }

    /// Get the delimiter for the CSV file(s) represented by this CsvFile instance
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    /// Get the file extension for the CSV file(s) represented by this CsvFile instance
    pub fn file_extension(&self) -> &str {
        &self.file_extension
    }
}

impl TableProvider for CsvFile {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CsvExec::try_new(
            &self.path,
            CsvReadOptions::new()
                .schema(&self.schema)
                .has_header(self.has_header)
                .delimiter(self.delimiter)
                .file_extension(self.file_extension.as_str()),
            projection.clone(),
            limit
                .map(|l| std::cmp::min(l, batch_size))
                .unwrap_or(batch_size),
            limit,
        )?))
    }

    fn statistics(&self) -> Statistics {
        self.statistics.clone()
    }
}
