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

use std::{collections::HashSet, fmt, sync::Arc};

use crate::arrow::datatypes::SchemaRef;
use crate::error::Result;
use crate::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use async_trait::async_trait;

/// Source table
pub trait TableProvider {
    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef;

    /// Create an ExecutionPlan that will scan the table.
    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

#[async_trait]
/// A scanner implementation that can be used by datafusion
pub trait SourceScanner: Send + Sync + fmt::Debug {
    /// reference to the schema of the data as it will be read by this scanner
    fn projected_schema(&self) -> &SchemaRef;

    /// string display of this scanner
    fn format(&self) -> &str;

    /// apply projection on this scanner
    fn project(
        &self,
        required_columns: &HashSet<String>,
        has_projection: bool,
    ) -> Result<Arc<dyn SourceScanner>>;

    /// get scanner partitioning
    fn output_partitioning(&self) -> Partitioning;

    /// get iterator for a given partition
    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream>;
}
