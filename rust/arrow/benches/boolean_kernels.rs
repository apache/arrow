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
use arrow::builder::*;
use arrow::compute::boolean_kernels;
use arrow::error::{ArrowError, Result};

///  Helper function to create arrays
fn create_boolean_array(size: usize) -> BooleanArray {
    let mut builder = BooleanBuilder::new(size);
    for i in 0..size {
        if i % 2 == 0 {
            builder.append_value(true).unwrap();
        } else {
            builder.append_value(false).unwrap();
        }
    }
    builder.finish()
}

/// Helper function to implement `AND` and `OR` without SIMD
pub fn bin_op_no_simd<F>(
    left: &BooleanArray,
    right: &BooleanArray,
    op: F,
) -> Result<BooleanArray>
where
    F: Fn(bool, bool) -> bool,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform boolean operation on arrays of different length".to_string(),
        ));
    }
    let mut b = BooleanArray::builder(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            b.append_null()?;
        } else {
            b.append_value(op(left.value(i), right.value(i)))?;
        }
    }
    Ok(b.finish())
}

/// Benchmark for `AND` and `OR` with no SIMD
fn bench_bin_op_no_simd<F>(size: usize, op: F)
where
    F: Fn(bool, bool) -> bool,
{
    let array_a = create_boolean_array(size);
    let array_b = create_boolean_array(size);

    criterion::black_box(bin_op_no_simd(&array_a, &array_b, &op).unwrap());
}

/// Benchmark for `NOT` with no SIMD
fn bench_not_no_simd(size: usize) {
    let array = create_boolean_array(size);

    criterion::black_box({
        let mut b = BooleanArray::builder(array.len());
        for i in 0..array.len() {
            if array.is_null(i) {
                b.append_null().unwrap();
            } else {
                b.append_value(!array.value(i)).unwrap();
            }
        }
        b.finish()
    });
}

/// Benchmark for `AND` with SIMD
fn bench_and_simd(size: usize) {
    let buffer_a = create_boolean_array(size);
    let buffer_b = create_boolean_array(size);
    criterion::black_box(boolean_kernels::and(&buffer_a, &buffer_b).unwrap());
}

/// Benchmark for `OR` with SIMD
fn bench_or_simd(size: usize) {
    let buffer_a = create_boolean_array(size);
    let buffer_b = create_boolean_array(size);
    criterion::black_box(boolean_kernels::or(&buffer_a, &buffer_b).unwrap());
}

/// Benchmark for `NOT` with SIMD
fn bench_not_simd(size: usize) {
    let buffer = create_boolean_array(size);
    criterion::black_box(boolean_kernels::not(&buffer).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    c.bench_function("and", |b| {
        b.iter(|| bench_bin_op_no_simd(512, |a, b| a && b))
    });
    c.bench_function("and simd", |b| b.iter(|| bench_and_simd(512)));
    c.bench_function("or", |b| {
        b.iter(|| bench_bin_op_no_simd(512, |a, b| a || b))
    });
    c.bench_function("or simd", |b| b.iter(|| bench_or_simd(512)));
    c.bench_function("not", |b| b.iter(|| bench_not_no_simd(512)));
    c.bench_function("not simd", |b| b.iter(|| bench_not_simd(512)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
