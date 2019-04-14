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

//! Data source traits

use std::sync::{Arc, Mutex};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::error::Result;

/// Returned by implementors of `Table#scan`, this `RecordBatchIterator` is wrapped with
/// an `Arc` and `Mutex` so that it can be shared across threads as it is used.
pub type ScanResult = Arc<Mutex<RecordBatchIterator>>;

/// Source table
pub trait TableProvider {
    /// Get a reference to the schema for this table
    fn schema(&self) -> &Arc<Schema>;

    /// Perform a scan of a table and return a sequence of iterators over the data (one
    /// iterator per partition)
    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Vec<ScanResult>>;
}

/// Iterator for reading a series of record batches with a known schema
pub trait RecordBatchIterator {
    /// Get the schema of this iterator
    fn schema(&self) -> &Arc<Schema>;

    /// Get the next batch in this iterator
    fn next(&mut self) -> Result<Option<RecordBatch>>;
}
