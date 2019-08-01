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

use crate::error::Result;
use crate::execution::physical_plan::{BatchIterator, ExecutionPlan, Partition};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub struct CsvExec {
    /// Path to directory containing partitioned CSV files with the same schema
    path: String,
    /// Schema representing the CSV files
    schema: Arc<Schema>,
}

impl ExecutionPlan for CsvExec {
    /// Get the schema for this execution plan
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Get the partitions for this execution plan. Each partition can be executed in parallel.
    fn partitions(&self) -> Result<Vec<Arc<Partition>>> {
        // TODO get list of files in the directory and create a partition for each one
        unimplemented!()
    }
}

impl CsvExec {
    /// Create a new execution plan for reading a set of CSV files
    pub fn try_new(path: &str, schema: Arc<Schema>) -> Result<Self> {
        //TODO
        Ok(Self {
            path: path.to_string(),
            schema: schema.clone(),
        })
    }
}

/// CSV Partition
struct CsvPartition {
    /// Path to the CSV File
    path: String,
    /// Schema representing the CSV file
    schema: Arc<Schema>,
}

impl Partition for CsvPartition {
    /// Execute this partition and return an iterator over RecordBatch
    fn execute(&self) -> Result<Arc<dyn BatchIterator>> {
        unimplemented!()
    }
}

/// Iterator over batches
struct CsvIterator {}

impl BatchIterator for CsvIterator {
    /// Get the next RecordBatch
    fn next(&self) -> Result<Option<RecordBatch>> {
        unimplemented!()
    }
}
