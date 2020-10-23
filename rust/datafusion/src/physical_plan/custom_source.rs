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

//! Execution plan for reading Parquet files

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::datasource::custom::CustomScanner;
use crate::error::{ExecutionError, Result};
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Partitioning;
use arrow::datatypes::SchemaRef;

use fmt::Debug;

use super::SendableRecordBatchReader;
use async_trait::async_trait;

/// Execution plan for scanning a Parquet file
#[derive(Debug, Clone)]
pub struct CustomSourceExec {
  scanner: Arc<dyn CustomScanner>,
}

impl CustomSourceExec {
  /// Create a new Parquet reader execution plan
  pub fn try_new(scanner: &Arc<dyn CustomScanner>) -> Result<Self> {
    Ok(Self {
      scanner: scanner.clone(),
    })
  }
}

#[async_trait]
impl ExecutionPlan for CustomSourceExec {
  /// Return a reference to Any that can be used for downcasting
  fn as_any(&self) -> &dyn Any {
    self
  }

  fn schema(&self) -> SchemaRef {
    self.scanner.projected_schema().clone()
  }

  fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
    // this is a leaf node and has no children
    vec![]
  }

  /// Get the output partitioning of this plan
  fn output_partitioning(&self) -> Partitioning {
    self.scanner.output_partitioning()
  }

  fn with_new_children(
    &self,
    children: Vec<Arc<dyn ExecutionPlan>>,
  ) -> Result<Arc<dyn ExecutionPlan>> {
    if children.is_empty() {
      Ok(Arc::new(self.clone()))
    } else {
      Err(ExecutionError::General(format!(
        "Children cannot be replaced in {:?}",
        self
      )))
    }
  }

  async fn execute(&self, partition: usize) -> Result<SendableRecordBatchReader> {
    self.execute(partition).await
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn test() -> Result<()> {
    assert!(true);
    Ok(())
  }
}
