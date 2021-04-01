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

use arrow::compute::kernels::aggregate::*;
use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type};

fn bench_sum(arr_a: &Float32Array) {
    criterion::black_box(sum(&arr_a).unwrap());
}

fn bench_min(arr_a: &Float32Array) {
    criterion::black_box(min(&arr_a).unwrap());
}

fn bench_max(arr_a: &Float32Array) {
    criterion::black_box(max(&arr_a).unwrap());
}

fn bench_min_string(arr_a: &StringArray) {
    criterion::black_box(min_string(&arr_a).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let arr_a = create_primitive_array::<Float32Type>(512, 0.0);

    c.bench_function("sum 512", |b| b.iter(|| bench_sum(&arr_a)));
    c.bench_function("min 512", |b| b.iter(|| bench_min(&arr_a)));
    c.bench_function("max 512", |b| b.iter(|| bench_max(&arr_a)));

    let arr_a = create_primitive_array::<Float32Type>(512, 0.5);

    c.bench_function("sum nulls 512", |b| b.iter(|| bench_sum(&arr_a)));
    c.bench_function("min nulls 512", |b| b.iter(|| bench_min(&arr_a)));
    c.bench_function("max nulls 512", |b| b.iter(|| bench_max(&arr_a)));

    let arr_b = create_string_array::<i32>(512, 0.0);
    c.bench_function("min string 512", |b| b.iter(|| bench_min_string(&arr_b)));

    let arr_b = create_string_array::<i32>(512, 0.5);
    c.bench_function("min nulls string 512", |b| {
        b.iter(|| bench_min_string(&arr_b))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
