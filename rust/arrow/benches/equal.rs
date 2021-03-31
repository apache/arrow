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

extern crate arrow;

use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type};

fn bench_equal<A: Array + PartialEq<A>>(arr_a: &A) {
    criterion::black_box(arr_a == arr_a);
}

fn add_benchmark(c: &mut Criterion) {
    let arr_a = create_primitive_array::<Float32Type>(512, 0.0);
    c.bench_function("equal_512", |b| b.iter(|| bench_equal(&arr_a)));

    let arr_a_nulls = create_primitive_array::<Float32Type>(512, 0.5);
    c.bench_function("equal_nulls_512", |b| b.iter(|| bench_equal(&arr_a_nulls)));

    let arr_a = create_string_array::<i32>(512, 0.0);
    c.bench_function("equal_string_512", |b| b.iter(|| bench_equal(&arr_a)));

    let arr_a_nulls = create_string_array::<i32>(512, 0.5);
    c.bench_function("equal_string_nulls_512", |b| {
        b.iter(|| bench_equal(&arr_a_nulls))
    });

    let arr_a = create_boolean_array(512, 0.0, 0.5);
    c.bench_function("equal_bool_512", |b| b.iter(|| bench_equal(&arr_a)));

    let arr_a = create_boolean_array(513, 0.0, 0.5);
    c.bench_function("equal_bool_513", |b| b.iter(|| bench_equal(&arr_a)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
