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
use std::io::{Read, Seek};
use std::string::String;
use std::sync::{Arc, Mutex};

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
/// Loads CSV data from a reader
pub struct CsvRead<R: Read> {
    reader: Mutex<Option<R>>,
    schema: SchemaRef,
    has_header: bool,
    delimiter: u8,
    statistics: Statistics,
}

impl<R: Read> CsvRead<R> {
    /// Attempt to initialize a `CsvRead` from a reader. The schema MUST be provided in options.
    pub fn try_new(reader: R, options: CsvReadOptions) -> Result<Self> {
        let schema = Arc::new(match options.schema {
            Some(s) => s.clone(),
            None => {
                return Err(DataFusionError::Execution(
                    "Schema must be provided".to_string(),
                ));
            }
        });

        Ok(Self {
            reader: Mutex::new(Some(reader)),
            schema,
            has_header: options.has_header,
            delimiter: options.delimiter,
            statistics: Statistics::default(),
        })
    }
    /// Attempt to initialize a `CsvRead` from a reader impls `Seek`. The schema can be inferred automatically.
    pub fn try_new_infer_schema(mut reader: R, options: CsvReadOptions) -> Result<Self>
    where
        R: Seek,
    {
        let schema = Arc::new(match options.schema {
            Some(s) => s.clone(),
            None => {
                let (schema, _) = arrow::csv::reader::infer_file_schema(
                    &mut reader,
                    options.delimiter,
                    Some(options.schema_infer_max_records),
                    options.has_header,
                )?;
                schema
            }
        });

        Ok(Self {
            reader: Mutex::new(Some(reader)),
            schema,
            has_header: options.has_header,
            delimiter: options.delimiter,
            statistics: Statistics::default(),
        })
    }

    /// Determine whether the CSV file(s) represented by this CsvFile instance have a header row
    pub fn has_header(&self) -> bool {
        self.has_header
    }

    /// Get the delimiter for the CSV file(s) represented by this CsvFile instance
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }
}

impl<R: Read + Send + Sync + 'static> TableProvider for CsvRead<R> {
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
        if let Some(rdr) = self.reader.lock().unwrap().take() {
            Ok(Arc::new(CsvExec::try_new_from_reader(
                rdr,
                CsvReadOptions::new()
                    .schema(&self.schema)
                    .has_header(self.has_header)
                    .delimiter(self.delimiter),
                projection.clone(),
                limit
                    .map(|l| std::cmp::min(l, batch_size))
                    .unwrap_or(batch_size),
                limit,
            )?))
        } else {
            Err(DataFusionError::Execution(
                "You can only read once if the data comes from a reader".to_string(),
            ))
        }
    }

    fn statistics(&self) -> Statistics {
        self.statistics.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    #[tokio::test]
    async fn csv_read() -> Result<()> {
        let testdata = arrow::util::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let buf = std::fs::read(path).unwrap();
        let rdr = std::io::Cursor::new(buf);
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "aggregate_test",
            Arc::new(CsvRead::try_new_infer_schema(rdr, CsvReadOptions::new())?),
        )?;
        let df = ctx.sql("select max(c2) from aggregate_test")?;
        let batches = df.collect().await?;
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            5
        );
        Ok(())
    }
}
