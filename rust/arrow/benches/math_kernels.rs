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

extern crate arrow;

use arrow::datatypes::Float32Type;
use arrow::util::bench_util::*;
use arrow::{array::*, compute::*};

fn bench_powf(array: &Float32Array, raise: f32) {
    criterion::black_box(powf_scalar(array, raise));
}

fn bench_powi(array: &Float32Array, raise: i32) {
    criterion::black_box(powi(array, raise));
}

fn add_benchmark(c: &mut Criterion) {
    let array = create_primitive_array::<Float32Type>(512, 0.0);
    let array_nulls = create_primitive_array::<Float32Type>(512, 0.5);

    c.bench_function("powf(2.0) 512", |b| b.iter(|| bench_powf(&array, 2.0)));
    c.bench_function("powf(4.0) 512", |b| b.iter(|| bench_powf(&array, 4.0)));
    c.bench_function("powf(32.0) 512", |b| b.iter(|| bench_powf(&array, 32.0)));
    c.bench_function("powf(2.0) nulls 512", |b| {
        b.iter(|| bench_powf(&array_nulls, 2.0))
    });
    c.bench_function("powf(4.0) nulls 512", |b| {
        b.iter(|| bench_powf(&array_nulls, 4.0))
    });
    c.bench_function("powf(32.0) nulls 512", |b| {
        b.iter(|| bench_powf(&array_nulls, 32.0))
    });

    c.bench_function("powi(2) 512", |b| b.iter(|| bench_powi(&array, 2)));
    c.bench_function("powi(4) 512", |b| b.iter(|| bench_powi(&array, 4)));
    c.bench_function("powi(32) 512", |b| b.iter(|| bench_powi(&array, 32)));
    c.bench_function("powi(2) nulls 512", |b| {
        b.iter(|| bench_powi(&array_nulls, 2))
    });
    c.bench_function("powi(4) nulls 512", |b| {
        b.iter(|| bench_powi(&array_nulls, 4))
    });
    c.bench_function("powi(32) nulls 512", |b| {
        b.iter(|| bench_powi(&array_nulls, 32))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
