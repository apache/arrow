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

use arrow::compute::array_ops::*;
use arrow::compute::arithmetic_kernels::{add_sse, add_avx2};
use arrow::builder::*;
use arrow::array::*;


fn create_array(size: usize) -> Float32Array {
    let mut builder = Float32Builder::new(size);
    for _i in 0..size {
        builder.append_value(1.0).unwrap();
    }
    builder.finish()
}

fn primitive_array_add(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(add(&arr_a, &arr_b).unwrap());
}

fn primitive_array_add_sse(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(unsafe{add_sse(&arr_a, &arr_b).unwrap()});
}

fn primitive_array_add_avx2(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(unsafe{add_avx2(&arr_a, &arr_b).unwrap()});
}

fn add_benchmark(c: &mut Criterion) {
    c.bench_function("add 128", |b| b.iter(|| primitive_array_add(128)));
    c.bench_function("add 128 sse", |b| b.iter(|| primitive_array_add_sse(128)));
    c.bench_function("add 128 avx2", |b| b.iter(|| primitive_array_add_avx2(128)));
    c.bench_function("add 256", |b| b.iter(|| primitive_array_add(256)));
    c.bench_function("add 256 sse", |b| b.iter(|| primitive_array_add_sse(256)));
    c.bench_function("add 256 avx2", |b| b.iter(|| primitive_array_add_avx2(256)));
    c.bench_function("add 512", |b| b.iter(|| primitive_array_add(512)));
    c.bench_function("add 512 sse", |b| b.iter(|| primitive_array_add_sse(512)));
    c.bench_function("add 512 avx2", |b| b.iter(|| primitive_array_add_avx2(512)));
    c.bench_function("add 1024", |b| b.iter(|| primitive_array_add(1024)));
    c.bench_function("add 1024 sse", |b| b.iter(|| primitive_array_add_sse(1024)));
    c.bench_function("add 1024 avx2", |b| b.iter(|| primitive_array_add_avx2(1024)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
