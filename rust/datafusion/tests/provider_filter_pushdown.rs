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

use arrow::array::{as_primitive_array, Int32Builder, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::datasource::{
    Statistics, TableProvider, TableProviderFilterPushDown,
};
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::common::SizedRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

fn create_batch(value: i32, num_rows: usize) -> Result<RecordBatch> {
    let mut builder = Int32Builder::new(num_rows);
    for _ in 0..num_rows {
        builder.append_value(value)?;
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(builder.finish())],
    )?)
}

#[derive(Debug)]
struct CustomPlan {
    schema: SchemaRef,
    batches: Vec<Arc<RecordBatch>>,
}

#[async_trait]
impl ExecutionPlan for CustomPlan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!()
    }

    async fn execute(&self, _: usize) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(SizedRecordBatchStream::new(
            self.schema(),
            self.batches.clone(),
        )))
    }
}

#[derive(Clone)]
struct CustomProvider {
    zero_batch: RecordBatch,
    one_batch: RecordBatch,
}

impl TableProvider for CustomProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.zero_batch.schema()
    }

    fn scan(
        &self,
        _: &Option<Vec<usize>>,
        _: usize,
        filters: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match &filters[0] {
            Expr::BinaryExpr { right, .. } => {
                let int_value = match &**right {
                    Expr::Literal(ScalarValue::Int64(i)) => i.unwrap(),
                    _ => unimplemented!(),
                };

                Ok(Arc::new(CustomPlan {
                    schema: self.zero_batch.schema(),
                    batches: match int_value {
                        0 => vec![Arc::new(self.zero_batch.clone())],
                        1 => vec![Arc::new(self.one_batch.clone())],
                        _ => vec![],
                    },
                }))
            }
            _ => Ok(Arc::new(CustomPlan {
                schema: self.zero_batch.schema(),
                batches: vec![],
            })),
        }
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn supports_filter_pushdown(&self, _: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Exact)
    }
}

async fn assert_provider_row_count(value: i64, expected_count: u64) -> Result<()> {
    let provider = CustomProvider {
        zero_batch: create_batch(0, 10)?,
        one_batch: create_batch(1, 5)?,
    };

    let mut ctx = ExecutionContext::new();
    let df = ctx
        .read_table(Arc::new(provider.clone()))?
        .filter(col("flag").eq(lit(value)))?
        .aggregate(vec![], vec![count(col("flag"))])?;

    let results = df.collect().await?;
    let result_col: &UInt64Array = as_primitive_array(results[0].column(0));
    assert_eq!(result_col.value(0), expected_count);

    ctx.register_table("data", Arc::new(provider))?;
    let sql_results = ctx
        .sql(&format!("select count(*) from data where flag = {}", value))?
        .collect()
        .await?;

    let sql_result_col: &UInt64Array = as_primitive_array(sql_results[0].column(0));
    assert_eq!(sql_result_col.value(0), expected_count);

    Ok(())
}

#[tokio::test]
async fn test_filter_pushdown_results() -> Result<()> {
    assert_provider_row_count(0, 10).await?;
    assert_provider_row_count(1, 5).await?;
    assert_provider_row_count(2, 0).await?;
    Ok(())
}
