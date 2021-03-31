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

use arrow::array::*;
use arrow::compute::concat;
use arrow::datatypes::*;
use arrow::util::bench_util::*;

fn bench_concat(v1: &dyn Array, v2: &dyn Array) {
    criterion::black_box(concat(&[v1, v2]).unwrap());
}

fn bench_concat_arrays(arrays: &[&dyn Array]) {
    criterion::black_box(concat(arrays).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let v1 = create_primitive_array::<Int32Type>(1024, 0.0);
    let v2 = create_primitive_array::<Int32Type>(1024, 0.0);
    c.bench_function("concat i32 1024", |b| b.iter(|| bench_concat(&v1, &v2)));

    let v1 = create_primitive_array::<Int32Type>(1024, 0.5);
    let v2 = create_primitive_array::<Int32Type>(1024, 0.5);
    c.bench_function("concat i32 nulls 1024", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });

    let small_array = create_primitive_array::<Int32Type>(4, 0.0);
    let arrays: Vec<_> = (0..1024).map(|_| &small_array as &dyn Array).collect();
    c.bench_function("concat 1024 arrays i32 4", |b| {
        b.iter(|| bench_concat_arrays(&arrays))
    });

    let v1 = create_string_array::<i32>(1024, 0.0);
    let v2 = create_string_array::<i32>(1024, 0.0);
    c.bench_function("concat str 1024", |b| b.iter(|| bench_concat(&v1, &v2)));

    let v1 = create_string_array::<i32>(1024, 0.5);
    let v2 = create_string_array::<i32>(1024, 0.5);
    c.bench_function("concat str nulls 1024", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
