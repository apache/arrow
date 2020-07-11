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

use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::execution::physical_plan::{ExecutionPlan, Partition};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatchReader;

/// Datasource execution plan
pub struct DatasourceExec {
    schema: SchemaRef,
    partitions: Vec<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>>,
}

impl DatasourceExec {
    /// Create a new data source execution plan
    pub fn new(
        schema: SchemaRef,
        partitions: Vec<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>>,
    ) -> Self {
        Self { schema, partitions }
    }
}

impl ExecutionPlan for DatasourceExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        Ok(self
            .partitions
            .iter()
            .map(|it| {
                Arc::new(DatasourcePartition::new(it.clone())) as Arc<dyn Partition>
            })
            .collect::<Vec<_>>())
    }
}

/// Wrapper to convert a `SendableRecordBatchReader` into a `Partition`.
pub struct DatasourcePartition {
    batch_iter: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
}

impl DatasourcePartition {
    fn new(batch_iter: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>) -> Self {
        Self { batch_iter }
    }
}

impl Partition for DatasourcePartition {
    fn execute(&self) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        Ok(self.batch_iter.clone())
    }
}
