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

//! ExecutionPlan implementation for DataFusion data sources

use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use crate::error::Result;
use crate::execution::physical_plan::{ExecutionPlan, Partition};
use arrow::datatypes::SchemaRef;

/// Datasource execution plan
pub struct DatasourceExec {
    schema: SchemaRef,
    partitions: Vec<Arc<dyn Partition>>,
}

impl Debug for DatasourceExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataSourceExec")
            .field("schema", &self.schema)
            .field("partitions.len", &self.partitions.len())
            .finish()
    }
}

impl DatasourceExec {
    /// Create a new data source execution plan
    pub fn new(schema: SchemaRef, partitions: Vec<Arc<dyn Partition>>) -> Self {
        Self { schema, partitions }
    }
}

impl ExecutionPlan for DatasourceExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        Ok(self.partitions.clone())
    }
}
