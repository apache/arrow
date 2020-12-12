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

use std::any::Any;
use std::sync::Arc;

use crate::arrow::datatypes::SchemaRef;
use crate::error::Result;
use crate::physical_plan::ExecutionPlan;

/// This table statistics are estimates.
/// It can not be used directly in the precise compute
#[derive(Clone, Default)]
pub struct Statistics {
    /// The number of table rows
    pub num_rows: Option<usize>,
    /// total byte of the table rows
    pub total_byte_size: Option<usize>,
}

/// Source table
pub trait TableProvider {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef;

    /// Create an ExecutionPlan that will scan the table.
    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Returns the table Statistics
    /// Statistics should be optional because not all data sources can provide statistics.
    fn statistics(&self) -> Statistics;
}
