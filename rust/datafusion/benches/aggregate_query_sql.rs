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

use rand::seq::SliceRandom;
use rand::Rng;
use std::sync::{Arc, Mutex};

extern crate arrow;
extern crate datafusion;

use arrow::{
    array::Float32Array,
    array::Float64Array,
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;

async fn query(ctx: Arc<Mutex<ExecutionContext>>, sql: &str) {
    // execute the query
    let df = ctx.lock().unwrap().sql(&sql).unwrap();
    let results = df.collect().await.unwrap();

    // display the relation
    for _batch in results {}
}

fn create_data(size: usize, null_density: f64) -> Vec<Option<f64>> {
    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = rand::thread_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f64>() > null_density {
                None
            } else {
                Some(rng.gen::<f64>())
            }
        })
        .collect()
}

fn create_context(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<Mutex<ExecutionContext>>> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("utf8", DataType::Utf8, false),
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, false),
    ]));

    // define data.
    let partitions = (0..partitions_len)
        .map(|_| {
            (0..array_len / batch_size / partitions_len)
                .map(|i| {
                    // the 4 here is the number of different keys.
                    // a higher number increase sparseness
                    let vs = vec![0, 1, 2, 3];
                    let keys: Vec<String> = (0..batch_size)
                        .map(
                            // use random numbers to avoid spurious compiler optimizations wrt to branching
                            |_| format!("hi{:?}", vs.choose(&mut rand::thread_rng())),
                        )
                        .collect();
                    let keys: Vec<&str> = keys.iter().map(|e| &**e).collect();

                    let values = create_data(batch_size, 0.5);

                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(StringArray::from(keys)),
                            Arc::new(Float32Array::from(vec![i as f32; batch_size])),
                            Arc::new(Float64Array::from(values)),
                        ],
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let mut ctx = ExecutionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::new(schema, partitions)?;
    ctx.register_table("t", Box::new(provider));

    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark(c: &mut Criterion) {
    let partitions_len = 4;
    let array_len = 32768; // 2^15
    let batch_size = 2048; // 2^11
    let ctx = create_context(partitions_len, array_len, batch_size).unwrap();

    c.bench_function("aggregate_query_no_group_by 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_group_by 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT utf8, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_with_filter 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT utf8, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t \
                 WHERE f32 > 10 AND f32 < 20 GROUP BY utf8",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
