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

// Allowed because we use `arr == arr` in benchmarks
#![allow(clippy::eq_op)]

#[macro_use]
extern crate criterion;
use criterion::Criterion;

use rand::distributions::Alphanumeric;
use rand::Rng;
use std::sync::Arc;

extern crate arrow;

use arrow::array::*;
use arrow::util::test_util::seedable_rng;

fn create_string_array(size: usize, with_nulls: bool) -> ArrayRef {
    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();
    let mut builder = StringBuilder::new(size);

    for _ in 0..size {
        if with_nulls && rng.gen::<f32>() > 0.5 {
            builder.append_null().unwrap();
        } else {
            let string = seedable_rng()
                .sample_iter(&Alphanumeric)
                .take(10)
                .collect::<String>();
            builder.append_value(&string).unwrap();
        }
    }
    Arc::new(builder.finish())
}

fn create_array(size: usize, with_nulls: bool) -> ArrayRef {
    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();
    let mut builder = Float32Builder::new(size);

    for _ in 0..size {
        if with_nulls && rng.gen::<f32>() > 0.5 {
            builder.append_null().unwrap();
        } else {
            builder.append_value(rng.gen()).unwrap();
        }
    }
    Arc::new(builder.finish())
}

fn bench_equal(arr_a: &ArrayRef) {
    criterion::black_box(arr_a == arr_a);
}

fn add_benchmark(c: &mut Criterion) {
    let arr_a = create_array(512, false);
    c.bench_function("equal_512", |b| b.iter(|| bench_equal(&arr_a)));

    let arr_a_nulls = create_array(512, true);
    c.bench_function("equal_nulls_512", |b| b.iter(|| bench_equal(&arr_a_nulls)));

    let arr_a = create_string_array(512, false);
    c.bench_function("equal_string_512", |b| b.iter(|| bench_equal(&arr_a)));

    let arr_a_nulls = create_string_array(512, true);
    c.bench_function("equal_string_nulls_512", |b| {
        b.iter(|| bench_equal(&arr_a_nulls))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
