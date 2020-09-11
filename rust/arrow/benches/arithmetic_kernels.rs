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

use std::sync::Arc;

extern crate arrow;

use arrow::array::*;
use arrow::compute::kernels::arithmetic::*;
use arrow::compute::kernels::limit::*;

fn create_array(size: usize) -> ArrayRef {
    let mut builder = Float32Builder::new(size);
    for i in 0..size {
        builder.append_value(1.0 + 1.0 * i as f32).unwrap();
    }
    Arc::new(builder.finish())
}

fn bench_add(arr_a: &ArrayRef, arr_b: &ArrayRef) {
    let arr_a = arr_a.as_any().downcast_ref::<Float32Array>().unwrap();
    let arr_b = arr_b.as_any().downcast_ref::<Float32Array>().unwrap();
    criterion::black_box(add(arr_a, arr_b).unwrap());
}

fn bench_subtract(arr_a: &ArrayRef, arr_b: &ArrayRef) {
    let arr_a = arr_a.as_any().downcast_ref::<Float32Array>().unwrap();
    let arr_b = arr_b.as_any().downcast_ref::<Float32Array>().unwrap();
    criterion::black_box(subtract(&arr_a, &arr_b).unwrap());
}

fn bench_multiply(arr_a: &ArrayRef, arr_b: &ArrayRef) {
    let arr_a = arr_a.as_any().downcast_ref::<Float32Array>().unwrap();
    let arr_b = arr_b.as_any().downcast_ref::<Float32Array>().unwrap();
    criterion::black_box(multiply(&arr_a, &arr_b).unwrap());
}

fn bench_divide(arr_a: &ArrayRef, arr_b: &ArrayRef) {
    let arr_a = arr_a.as_any().downcast_ref::<Float32Array>().unwrap();
    let arr_b = arr_b.as_any().downcast_ref::<Float32Array>().unwrap();
    criterion::black_box(divide(&arr_a, &arr_b).unwrap());
}

fn bench_limit(arr_a: &ArrayRef, max: usize) {
    criterion::black_box(limit(arr_a, max).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let arr_a = create_array(512);
    let arr_b = create_array(512);

    c.bench_function("add 512", |b| b.iter(|| bench_add(&arr_a, &arr_b)));
    c.bench_function("subtract 512", |b| {
        b.iter(|| bench_subtract(&arr_a, &arr_b))
    });
    c.bench_function("multiply 512", |b| {
        b.iter(|| bench_multiply(&arr_a, &arr_b))
    });
    c.bench_function("divide 512", |b| b.iter(|| bench_divide(&arr_a, &arr_b)));
    c.bench_function("limit 512, 512", |b| b.iter(|| bench_limit(&arr_a, 512)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
