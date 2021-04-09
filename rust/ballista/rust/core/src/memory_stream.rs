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

//! This is copied from DataFusion because it is declared as `pub(crate)`. See
//! https://issues.apache.org/jira/browse/ARROW-11276.

use std::task::{Context, Poll};

use arrow::{datatypes::SchemaRef, error::Result, record_batch::RecordBatch};
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;

/// Iterator over batches

pub struct MemoryStream {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Schema representing the data
    schema: SchemaRef,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Index into the data
    index: usize,
}

impl MemoryStream {
    /// Create an iterator for a vector of record batches

    pub fn try_new(
        data: Vec<RecordBatch>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            data,
            schema,
            projection,
            index: 0,
        })
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            self.index += 1;

            let batch = &self.data[self.index - 1];

            // apply projection
            match &self.projection {
                Some(columns) => Some(RecordBatch::try_new(
                    self.schema.clone(),
                    columns.iter().map(|i| batch.column(*i).clone()).collect(),
                )),
                None => Some(Ok(batch.clone())),
            }
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl RecordBatchStream for MemoryStream {
    /// Get the schema

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
