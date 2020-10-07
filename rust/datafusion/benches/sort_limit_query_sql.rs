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

#[macro_use]
extern crate criterion;
use criterion::Criterion;

use std::env;
use std::sync::{Arc, Mutex};

extern crate arrow;
extern crate datafusion;

use arrow::datatypes::{DataType, Field, Schema};

use datafusion::datasource::{CsvFile, CsvReadOptions, MemTable};
use datafusion::execution::context::ExecutionContext;

use tokio::runtime::Runtime;

async fn run_query(ctx: Arc<Mutex<ExecutionContext>>, sql: &str) {
    // execute the query
    let df = ctx.lock().unwrap().sql(&sql).unwrap();
    let results = df.collect().await.unwrap();

    // display the relation
    for _batch in results {}
}

fn create_context() -> Arc<Mutex<ExecutionContext>> {
    // define schema for data source (csv file)
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::UInt32, false),
        Field::new("c3", DataType::Int8, false),
        Field::new("c4", DataType::Int16, false),
        Field::new("c5", DataType::Int32, false),
        Field::new("c6", DataType::Int64, false),
        Field::new("c7", DataType::UInt8, false),
        Field::new("c8", DataType::UInt16, false),
        Field::new("c9", DataType::UInt32, false),
        Field::new("c10", DataType::UInt64, false),
        Field::new("c11", DataType::Float32, false),
        Field::new("c12", DataType::Float64, false),
        Field::new("c13", DataType::Utf8, false),
    ]));

    let testdata = env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined");

    // create CSV data source
    let csv = CsvFile::try_new(
        &format!("{}/csv/aggregate_test_100.csv", testdata),
        CsvReadOptions::new().schema(&schema),
    )
    .unwrap();

    let mut rt = Runtime::new().unwrap();

    let ctx_holder: Arc<Mutex<Vec<Arc<Mutex<ExecutionContext>>>>> =
        Arc::new(Mutex::new(vec![]));
    rt.block_on(async {
        let mem_table = MemTable::load(&csv).await.unwrap();

        // create local execution context
        let mut ctx = ExecutionContext::new();
        ctx.state.config.concurrency = 1;
        ctx.register_table("aggregate_test_100", Box::new(mem_table));
        ctx_holder.lock().unwrap().push(Arc::new(Mutex::new(ctx)))
    });

    let ctx = ctx_holder.lock().unwrap().get(0).unwrap().clone();
    ctx
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("sort_and_limit_by_int", |b| {
        let ctx = create_context();
        b.iter(|| {
            run_query(
                ctx.clone(),
                "SELECT c1, c13, c6, c10 \
                 FROM aggregate_test_100 \
                 ORDER BY 3
                 LIMIT 10",
            )
        })
    });

    c.bench_function("sort_and_limit_by_float", |b| {
        let ctx = create_context();
        b.iter(|| {
            run_query(
                ctx.clone(),
                "SELECT c1, c13, c12 \
                 FROM aggregate_test_100 \
                 ORDER BY 2
                 LIMIT 10",
            )
        })
    });

    c.bench_function("sort_and_limit_lex_by_int", |b| {
        let ctx = create_context();
        b.iter(|| {
            run_query(
                ctx.clone(),
                "SELECT c1, c13, c6, c10 \
                 FROM aggregate_test_100 \
                 ORDER BY 3 DESC, 4 DESC
                 LIMIT 10",
            )
        })
    });

    c.bench_function("sort_and_limit_lex_by_string", |b| {
        let ctx = create_context();
        b.iter(|| {
            run_query(
                ctx.clone(),
                "SELECT c1, c13, c6, c10 \
                 FROM aggregate_test_100 \
                 ORDER BY 1, 2
                 LIMIT 10",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
