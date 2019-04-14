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

//! Data source relation

use std::sync::{Arc, Mutex};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::datasource::RecordBatchIterator;
use crate::error::Result;
use crate::execution::relation::Relation;

/// Implementation of a relation that represents a DataFusion data source
pub(super) struct DataSourceRelation {
    schema: Arc<Schema>,
    ds: Arc<Mutex<RecordBatchIterator>>,
}

impl DataSourceRelation {
    pub fn new(ds: Arc<Mutex<RecordBatchIterator>>) -> Self {
        let schema = ds.lock().unwrap().schema().clone();
        Self { ds, schema }
    }
}

impl Relation for DataSourceRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        self.ds.lock().unwrap().next()
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
