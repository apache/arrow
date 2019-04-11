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

use arrow::datatypes::{Schema, Field};
use crate::execution::relation::Relation;
use crate::error::Result;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use arrow::array::ArrayRef;
use arrow::builder::BooleanBuilder;
use arrow::datatypes::DataType;

/// Implementation of a LIMIT relation
pub(super) struct CreateTableRelation {
    /// The schema for the limit relation, which is always the same as the schema of the input relation
    schema: Arc<Schema>,

    /// The number of rows that have been returned so far
    emit: bool,
}

impl CreateTableRelation {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            emit: false,
        }
    }
}

impl Relation for CreateTableRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.emit {
            return Ok(None);
        }

        self.emit = true;
        let mut builder = BooleanBuilder::new(1);
        builder.append_value(true)?;

        let columns = vec![Arc::new(builder.finish()) as ArrayRef];
        Ok(Some(RecordBatch::try_new(Arc::new(Schema::new(vec![
            Field::new("result", DataType::Boolean, false)
        ])), columns)?))
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}