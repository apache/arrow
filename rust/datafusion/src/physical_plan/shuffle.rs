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

use std::any::Any;
use std::sync::{Arc, Mutex};

use crate::arrow::datatypes::Schema;
use crate::arrow::datatypes::SchemaRef;
use crate::arrow::record_batch::RecordBatchReader;
use crate::error::Result;
use crate::physical_plan::{ExecutionPlan, Partitioning};
use crate::scheduler::ShuffleId;

#[derive(Debug, Clone)]
pub struct ShuffleExchangeExec {
    pub(crate) child: Arc<dyn ExecutionPlan>,
    output_partitioning: Partitioning,
}

impl ExecutionPlan for ShuffleExchangeExec {
    fn as_any(&self) -> &dyn Any {
        unimplemented!()
    }

    fn schema(&self) -> SchemaRef {
        unimplemented!()
    }

    fn output_partitioning(&self) -> Partitioning {
        unimplemented!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    schema: Arc<Schema>,
    pub(crate) shuffle_id: Vec<ShuffleId>,
}

impl ShuffleReaderExec {
    pub fn new(schema: Arc<Schema>, shuffle_id: Vec<ShuffleId>) -> Self {
        Self { schema, shuffle_id }
    }
}

impl ExecutionPlan for ShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        unimplemented!()
    }

    fn schema(&self) -> SchemaRef {
        unimplemented!()
    }

    fn output_partitioning(&self) -> Partitioning {
        unimplemented!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        unimplemented!()
    }
}
