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

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

use datafusion::error::{DataFusionError, Result};
use datafusion::{
    datasource::{datasource::Statistics, TableProvider},
    physical_plan::collect,
};

use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{col, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};

use futures::stream::Stream;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;

//// Custom source dataframe tests ////

struct CustomTableProvider;
#[derive(Debug, Clone)]
struct CustomExecutionPlan {
    projection: Option<Vec<usize>>,
}
struct TestCustomRecordBatchStream {
    /// the nb of batches of TEST_CUSTOM_RECORD_BATCH generated
    nb_batch: i32,
}
macro_rules! TEST_CUSTOM_SCHEMA_REF {
    () => {
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]))
    };
}
macro_rules! TEST_CUSTOM_RECORD_BATCH {
    () => {
        RecordBatch::try_new(
            TEST_CUSTOM_SCHEMA_REF!(),
            vec![
                Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
                Arc::new(Int32Array::from(vec![2, 12, 12, 120])),
            ],
        )
    };
}

impl RecordBatchStream for TestCustomRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        TEST_CUSTOM_SCHEMA_REF!()
    }
}

impl Stream for TestCustomRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.nb_batch > 0 {
            self.get_mut().nb_batch -= 1;
            Poll::Ready(Some(TEST_CUSTOM_RECORD_BATCH!()))
        } else {
            Poll::Ready(None)
        }
    }
}

#[async_trait]
impl ExecutionPlan for CustomExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        let schema = TEST_CUSTOM_SCHEMA_REF!();
        match &self.projection {
            None => schema,
            Some(p) => Arc::new(Schema::new(
                p.iter().map(|i| schema.field(*i).clone()).collect(),
            )),
        }
    }
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(
                "Children cannot be replaced in CustomExecutionPlan".to_owned(),
            ))
        }
    }
    async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(TestCustomRecordBatchStream { nb_batch: 1 }))
    }
}

impl TableProvider for CustomTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        TEST_CUSTOM_SCHEMA_REF!()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExecutionPlan {
            projection: projection.clone(),
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[tokio::test]
async fn custom_source_dataframe() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let table = ctx.read_table(Arc::new(CustomTableProvider))?;
    let logical_plan = LogicalPlanBuilder::from(&table.to_logical_plan())
        .project(vec![col("c2")])?
        .build()?;

    let optimized_plan = ctx.optimize(&logical_plan)?;
    match &optimized_plan {
        LogicalPlan::Projection { input, .. } => match &**input {
            LogicalPlan::TableScan {
                source,
                projected_schema,
                ..
            } => {
                assert_eq!(source.schema().fields().len(), 2);
                assert_eq!(projected_schema.fields().len(), 1);
            }
            _ => panic!("input to projection should be TableScan"),
        },
        _ => panic!("expect optimized_plan to be projection"),
    }

    let expected = "Projection: #c2\
        \n  TableScan: projection=Some([1])";
    assert_eq!(format!("{:?}", optimized_plan), expected);

    let physical_plan = ctx.create_physical_plan(&optimized_plan)?;

    assert_eq!(1, physical_plan.schema().fields().len());
    assert_eq!("c2", physical_plan.schema().field(0).name().as_str());

    let batches = collect(physical_plan).await?;
    let origin_rec_batch = TEST_CUSTOM_RECORD_BATCH!()?;
    assert_eq!(1, batches.len());
    assert_eq!(1, batches[0].num_columns());
    assert_eq!(origin_rec_batch.num_rows(), batches[0].num_rows());

    Ok(())
}
