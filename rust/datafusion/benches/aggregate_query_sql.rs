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

use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

extern crate arrow;
extern crate datafusion;

use arrow::{
    array::Float32Array,
    array::Float64Array,
    array::StringArray,
    array::UInt64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;

pub fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

fn query(ctx: Arc<Mutex<ExecutionContext>>, sql: &str) {
    let rt = Runtime::new().unwrap();

    // execute the query
    let df = ctx.lock().unwrap().sql(&sql).unwrap();
    rt.block_on(df.collect()).unwrap();
}

fn create_data(size: usize, null_density: f64) -> Vec<Option<f64>> {
    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();

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

fn create_integer_data(size: usize, value_density: f64) -> Vec<Option<u64>> {
    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f64>() > value_density {
                None
            } else {
                Some(rng.gen::<u64>())
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
        // This field will contain integers randomly selected from a large
        // range of values, i.e. [0, u64::MAX], such that there are none (or
        // very few) repeated values.
        Field::new("u64_wide", DataType::UInt64, false),
        // This field will contain integers randomly selected from a narrow
        // range of values such that there are a few distinct values, but they
        // are repeated often.
        Field::new("u64_narrow", DataType::UInt64, false),
    ]));

    let mut rng = seedable_rng();

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
                            |_| format!("hi{:?}", vs.choose(&mut rng)),
                        )
                        .collect();
                    let keys: Vec<&str> = keys.iter().map(|e| &**e).collect();

                    let values = create_data(batch_size, 0.5);

                    // Integer values between [0, u64::MAX].
                    let integer_values_wide = create_integer_data(batch_size, 9.0);

                    // Integer values between [0, 9].
                    let integer_values_narrow_choices = (0..10).collect::<Vec<u64>>();
                    let integer_values_narrow = (0..batch_size)
                        .map(|_| *integer_values_narrow_choices.choose(&mut rng).unwrap())
                        .collect::<Vec<u64>>();

                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(StringArray::from(keys)),
                            Arc::new(Float32Array::from(vec![i as f32; batch_size])),
                            Arc::new(Float64Array::from(values)),
                            Arc::new(UInt64Array::from(integer_values_wide)),
                            Arc::new(UInt64Array::from(integer_values_narrow)),
                        ],
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let mut ctx = ExecutionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, partitions)?;
    ctx.register_table("t", Box::new(provider));

    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark(c: &mut Criterion) {
    let partitions_len = 8;
    let array_len = 32768 * 2; // 2^16
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

    c.bench_function("aggregate_query_no_group_by_min_max_f64", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT MIN(f64), MAX(f64) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_count_distinct_wide", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT COUNT(DISTINCT u64_wide) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_count_distinct_narrow", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT COUNT(DISTINCT u64_narrow) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_group_by", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT utf8, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_with_filter", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT utf8, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t \
                 WHERE f32 > 10 AND f32 < 20 GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_u64 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT u64_narrow, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_with_filter_u64 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT u64_narrow, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t \
                 WHERE f32 > 10 AND f32 < 20 GROUP BY u64_narrow",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
