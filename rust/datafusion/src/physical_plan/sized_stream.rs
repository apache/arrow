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

//! SizedRecordBatchStream is a stream of [arrow::record_batch::RecordBatch] that knows its schema.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use arrow::record_batch::RecordBatch;
use arrow::{datatypes::SchemaRef, error::Result as ArrowResult};
use futures::stream::{Stream, StreamExt};

use super::RecordBatchStream;

/// A schema-aware [Stream] of [`RecordBatch`]es.
pub struct SizedRecordBatchStream<S: Stream<Item = ArrowResult<RecordBatch>>> {
    stream: Pin<Box<S>>,
    schema: SchemaRef,
}

impl<S> SizedRecordBatchStream<S>
where
    S: Stream<Item = ArrowResult<RecordBatch>>,
{
    /// Create a new [SizedRecordBatchStream]
    pub fn new(stream: S, schema: SchemaRef) -> Self {
        Self {
            stream: Box::pin(stream),
            schema,
        }
    }
}

impl<S> Stream for SizedRecordBatchStream<S>
where
    S: Stream<Item = ArrowResult<RecordBatch>>,
{
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<S> RecordBatchStream for SizedRecordBatchStream<S>
where
    S: Stream<Item = ArrowResult<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
