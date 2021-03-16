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

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::{
    datasource::{datasource::Statistics, TableProvider},
    logical_plan::{Expr, LogicalPlan},
    physical_plan::ExecutionPlan,
};

/// This ugly adapter is needed because we use DataFusion's logical plan when building queries
/// and when we register tables with DataFusion's `ExecutionContext` we need to provide a
/// TableProvider which is effectively a wrapper around a physical plan. We need to be able to
/// register tables so that we can create logical plans from SQL statements that reference these
/// tables.
pub struct DFTableAdapter {
    /// DataFusion logical plan
    pub logical_plan: LogicalPlan,
    /// DataFusion execution plan
    plan: Arc<dyn ExecutionPlan>,
}

impl DFTableAdapter {
    pub fn new(logical_plan: LogicalPlan, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { logical_plan, plan }
    }
}

impl TableProvider for DFTableAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self.plan.clone())
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
        }
    }
}
