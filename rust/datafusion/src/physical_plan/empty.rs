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

//! EmptyRelation execution plan

use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::physical_plan::memory::MemoryIterator;
use crate::physical_plan::{Distribution, ExecutionPlan, Partitioning};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatchReader;

/// Execution plan for empty relation (produces no rows)
#[derive(Debug)]
pub struct EmptyExec {
    schema: SchemaRef,
}

impl EmptyExec {
    /// Create a new EmptyExec
    pub fn new(schema: SchemaRef) -> Self {
        EmptyExec { schema }
    }
}

impl ExecutionPlan for EmptyExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            0 => Ok(Arc::new(EmptyExec::new(self.schema.clone()))),
            _ => Err(ExecutionError::General(
                "EmptyExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        // GlobalLimitExec has a single output partition
        if 0 != partition {
            return Err(ExecutionError::General(format!(
                "EmptyExec invalid partition {} (expected 0)",
                partition
            )));
        }

        let data = vec![];
        Ok(Arc::new(Mutex::new(MemoryIterator::try_new(
            data,
            self.schema.clone(),
            None,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::common;
    use crate::test;

    #[test]
    fn empty() -> Result<()> {
        let schema = test::aggr_test_schema();

        let empty = EmptyExec::new(schema.clone());
        assert_eq!(empty.schema(), schema);

        // we should have no results
        let iter = empty.execute(0)?;
        let batches = common::collect(iter)?;
        assert!(batches.is_empty());

        Ok(())
    }

    #[test]
    fn with_new_children() -> Result<()> {
        let schema = test::aggr_test_schema();
        let empty = EmptyExec::new(schema.clone());

        let empty2 = empty.with_new_children(vec![])?;
        assert_eq!(empty.schema(), empty2.schema());

        let too_many_kids = vec![empty2];
        assert!(
            empty.with_new_children(too_many_kids).is_err(),
            "expected error when providing list of kids"
        );
        Ok(())
    }

    #[test]
    fn invalid_execute() -> Result<()> {
        let schema = test::aggr_test_schema();
        let empty = EmptyExec::new(schema.clone());

        // ask for the wrong partition
        assert!(empty.execute(1).is_err());
        assert!(empty.execute(20).is_err());
        Ok(())
    }
}
