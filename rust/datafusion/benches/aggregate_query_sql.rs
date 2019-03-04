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

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

extern crate arrow;
extern crate datafusion;

use arrow::datatypes::{DataType, Field, Schema};

use datafusion::datasource::{CsvFile, MemTable};
use datafusion::execution::context::ExecutionContext;

fn aggregate_query(ctx: &Rc<RefCell<ExecutionContext>>, sql: &str) {
    // execute the query
    let mut mut_ctx = ctx.borrow_mut();
    let relation = mut_ctx.sql(&sql, 1024 * 1024).unwrap();

    // display the relation
    let mut results = relation.borrow_mut();

    while let Some(_) = results.next().unwrap() {}
}

fn create_context() -> Rc<RefCell<ExecutionContext>> {
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

    // create CSV data source
    let csv = CsvFile::new(
        "../../testing/data/csv/aggregate_test_100.csv",
        &schema,
        true,
    );

    let mem_table = MemTable::load(&csv).unwrap();

    // create local execution context
    let ctx = Rc::new(RefCell::new(ExecutionContext::new()));

    let mut mut_ctx = ctx.borrow_mut();

    mut_ctx.register_table("aggregate_test_100", Rc::new(mem_table));

    ctx.clone()
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("aggregate_query_no_group_by", |b| {
        let ctx = create_context();
        b.iter(|| {
            aggregate_query(
                &ctx,
                "SELECT MIN(c12), MAX(c12) \
                 FROM aggregate_test_100",
            )
        })
    });

    c.bench_function("aggregate_query_group_by", |b| {
        let ctx = create_context();
        b.iter(|| {
            aggregate_query(
                &ctx,
                "SELECT c1, MIN(c12), MAX(c12) \
                 FROM aggregate_test_100 GROUP BY c1",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_with_filter", |b| {
        let ctx = create_context();
        b.iter(|| {
            aggregate_query(
                &ctx,
                "SELECT c1, MIN(c12), MAX(c12) \
                 FROM aggregate_test_100 \
                 WHERE c11 > 0.1 AND c11 < 0.9 GROUP BY c1",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
