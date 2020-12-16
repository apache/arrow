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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::{
    array::{Int32Array, StringArray},
    record_batch::RecordBatch,
};

use datafusion::error::Result;
use datafusion::{datasource::MemTable, prelude::JoinType};

use datafusion::execution::context::ExecutionContext;

#[tokio::test]
async fn join() -> Result<()> {
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
    ]));

    // define data.
    let batch1 = RecordBatch::try_new(
        schema1.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;
    // define data.
    let batch2 = RecordBatch::try_new(
        schema2.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    let mut ctx = ExecutionContext::new();

    let table1 = MemTable::try_new(schema1, vec![vec![batch1]])?;
    let table2 = MemTable::try_new(schema2, vec![vec![batch2]])?;

    ctx.register_table("aa", Box::new(table1));

    let df1 = ctx.table("aa")?;

    ctx.register_table("aaa", Box::new(table2));

    let df2 = ctx.table("aaa")?;

    let a = df1.join(df2, JoinType::Inner, &["a"], &["a"])?;

    let batches = a.collect().await?;
    assert_eq!(batches.len(), 1);

    Ok(())
}
