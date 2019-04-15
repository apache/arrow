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

//! Scalar relation, emit one fixed scalar value.

use crate::error::Result;
use crate::execution::relation::Relation;
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// A relation emit single scalar array;
pub(super) struct ScalarRelation {
    /// The schema for the limit relation, which is always the same as the schema of the input relation
    schema: Arc<Schema>,

    value: Vec<ArrayRef>,

    /// The number of rows that have been returned so far
    emitted: bool,
}

impl ScalarRelation {
    pub fn new(schema: Arc<Schema>, value: Vec<ArrayRef>) -> Self {
        Self {
            schema,
            value,
            emitted: false,
        }
    }
}

impl Relation for ScalarRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }

        self.emitted = true;

        Ok(Some(RecordBatch::try_new(
            self.schema().clone(),
            self.value.clone(),
        )?))
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
