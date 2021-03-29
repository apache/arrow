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

use arrow::compute::kernels::limit::*;
use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type};
use arrow::{compute::kernels::arithmetic::*, util::test_util::seedable_rng};

fn create_array(size: usize, with_nulls: bool) -> ArrayRef {
    let null_density = if with_nulls { 0.5 } else { 0.0 };
    let array = create_primitive_array::<Float32Type>(size, null_density);
    Arc::new(array)
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

fn bench_divide_scalar(array: &ArrayRef, divisor: f32) {
    let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
    criterion::black_box(divide_scalar(&array, divisor).unwrap());
}

fn bench_limit(arr_a: &ArrayRef, max: usize) {
    criterion::black_box(limit(arr_a, max));
}

fn add_benchmark(c: &mut Criterion) {
    let arr_a = create_array(512, false);
    let arr_b = create_array(512, false);
    let scalar = seedable_rng().gen();

    c.bench_function("add 512", |b| b.iter(|| bench_add(&arr_a, &arr_b)));
    c.bench_function("subtract 512", |b| {
        b.iter(|| bench_subtract(&arr_a, &arr_b))
    });
    c.bench_function("multiply 512", |b| {
        b.iter(|| bench_multiply(&arr_a, &arr_b))
    });
    c.bench_function("divide 512", |b| b.iter(|| bench_divide(&arr_a, &arr_b)));
    c.bench_function("divide_scalar 512", |b| {
        b.iter(|| bench_divide_scalar(&arr_a, scalar))
    });
    c.bench_function("limit 512, 512", |b| b.iter(|| bench_limit(&arr_a, 512)));

    let arr_a_nulls = create_array(512, false);
    let arr_b_nulls = create_array(512, false);
    c.bench_function("add_nulls_512", |b| {
        b.iter(|| bench_add(&arr_a_nulls, &arr_b_nulls))
    });
    c.bench_function("divide_nulls_512", |b| {
        b.iter(|| bench_divide(&arr_a_nulls, &arr_b_nulls))
    });
    c.bench_function("divide_scalar_nulls_512", |b| {
        b.iter(|| bench_divide_scalar(&arr_a_nulls, scalar))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
