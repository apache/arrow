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

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::datasource::DataSource;
use super::error::Result;

/// trait for all relations (a relation is essentially just an iterator over rows with
/// a known schema)
pub trait Relation {
    fn next(&mut self) -> Result<Option<RecordBatch>>;

    /// get the schema for this relation
    fn schema(&self) -> &Arc<Schema>;
}

pub struct DataSourceRelation {
    schema: Arc<Schema>,
    ds: Rc<RefCell<DataSource>>,
}

impl DataSourceRelation {
    pub fn new(ds: Rc<RefCell<DataSource>>) -> Self {
        let schema = ds.borrow().schema().clone();
        Self { ds, schema }
    }
}

impl Relation for DataSourceRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        self.ds.borrow_mut().next()
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
