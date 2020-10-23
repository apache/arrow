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

//! Custom data source
//!
//! A data source trait that can be implemented to plug in custom sources into datafusion

use crate::error::Result;
use crate::physical_plan::Partitioning;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatchReader;
use async_trait::async_trait;
use std::{collections::HashSet, fmt, sync::Arc};

#[async_trait]
/// A user implemented scanner that can be used by datafusion
pub trait CustomScanner: Send + Sync + fmt::Debug {
  /// reference to the schema of the data as it will be read by this scanner
  fn projected_schema(&self) -> &SchemaRef;
  /// string display of this scanner
  fn format(&self) -> &str;
  /// apply projection on this scanner
  fn project(
    &self,
    required_columns: &HashSet<String>,
    has_projection: bool,
  ) -> Result<Arc<dyn CustomScanner>>;
  /// get scanner partitioning
  fn output_partitioning(&self) -> Partitioning;
  /// get iterator for a given partition
  async fn execute(&self, partition: usize) -> Result<Box<dyn RecordBatchReader + Send>>;
}
