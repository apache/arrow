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

use rand::Rng;
use std::sync::Arc;

extern crate arrow;

use arrow::array::*;
use arrow::compute::kernels::sort::{lexsort, SortColumn};
use arrow::util::test_util::seedable_rng;

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

fn bench_sort(arr_a: &ArrayRef, array_b: &ArrayRef) {
    let columns = vec![
        SortColumn {
            values: arr_a.clone(),
            options: None,
        },
        SortColumn {
            values: array_b.clone(),
            options: None,
        },
    ];

    criterion::black_box(lexsort(&columns).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let arr_a = create_array(2u64.pow(10) as usize, false);
    let arr_b = create_array(2u64.pow(10) as usize, false);

    c.bench_function("sort 2^10", |b| b.iter(|| bench_sort(&arr_a, &arr_b)));

    let arr_a = create_array(2u64.pow(12) as usize, false);
    let arr_b = create_array(2u64.pow(12) as usize, false);

    c.bench_function("sort 2^12", |b| b.iter(|| bench_sort(&arr_a, &arr_b)));

    let arr_a = create_array(2u64.pow(10) as usize, true);
    let arr_b = create_array(2u64.pow(10) as usize, true);

    c.bench_function("sort nulls 2^10", |b| b.iter(|| bench_sort(&arr_a, &arr_b)));

    let arr_a = create_array(2u64.pow(12) as usize, true);
    let arr_b = create_array(2u64.pow(12) as usize, true);

    c.bench_function("sort nulls 2^12", |b| b.iter(|| bench_sort(&arr_a, &arr_b)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
