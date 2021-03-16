// Copyright 2021 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::{any::Any, pin::Pin};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::{error::Result, physical_plan::RecordBatchStream};
use uuid::Uuid;

/// QueryStageExec represents a section of a query plan that has consistent partitioning and
/// can be executed as one unit with each partition being executed in parallel. The output of
/// a query stage either forms the input of another query stage or can be the final result of
/// a query.
#[derive(Debug, Clone)]
pub struct QueryStageExec {
    /// Unique ID for the job (query) that this stage is a part of
    pub job_id: String,
    /// Unique query stage ID within the job
    pub stage_id: usize,
    /// Physical execution plan for this query stage
    pub child: Arc<dyn ExecutionPlan>,
}

impl QueryStageExec {
    /// Create a new query stage
    pub fn try_new(job_id: String, stage_id: usize, child: Arc<dyn ExecutionPlan>) -> Result<Self> {
        Ok(Self {
            job_id,
            stage_id,
            child,
        })
    }
}

#[async_trait]
impl ExecutionPlan for QueryStageExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.child.output_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.child.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.len() == 1);
        Ok(Arc::new(QueryStageExec::try_new(
            self.job_id.clone(),
            self.stage_id,
            children[0].clone(),
        )?))
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
        self.child.execute(partition).await
    }
}
