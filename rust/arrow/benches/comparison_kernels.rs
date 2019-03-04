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
use arrow::compute::*;

fn create_array(size: usize) -> Float32Array {
    let mut builder = Float32Builder::new(size);
    for i in 0..size {
        if i % 2 == 0 {
            builder.append_value(1.0).unwrap();
        } else {
            builder.append_value(0.0).unwrap();
        }
    }
    builder.finish()
}

pub fn eq_no_simd(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(compare_op(&arr_a, &arr_b, |a, b| a == b).unwrap());
}

pub fn neq_no_simd(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(compare_op(&arr_a, &arr_b, |a, b| a != b).unwrap());
}

pub fn lt_no_simd(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(
        compare_op(&arr_a, &arr_b, |a, b| match (a, b) {
            (None, None) => false,
            (None, _) => true,
            (_, None) => false,
            (Some(aa), Some(bb)) => aa < bb,
        })
        .unwrap(),
    );
}

fn lt_eq_no_simd(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(
        compare_op(&arr_a, &arr_b, |a, b| match (a, b) {
            (None, None) => true,
            (None, _) => true,
            (_, None) => false,
            (Some(aa), Some(bb)) => aa <= bb,
        })
        .unwrap(),
    );
}

pub fn gt_no_simd(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(
        compare_op(&arr_a, &arr_b, |a, b| match (a, b) {
            (None, None) => false,
            (None, _) => false,
            (_, None) => true,
            (Some(aa), Some(bb)) => aa > bb,
        })
        .unwrap(),
    );
}

fn gt_eq_no_simd(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(
        compare_op(&arr_a, &arr_b, |a, b| match (a, b) {
            (None, None) => true,
            (None, _) => false,
            (_, None) => true,
            (Some(aa), Some(bb)) => aa >= bb,
        })
        .unwrap(),
    );
}

fn eq_simd(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(eq(&arr_a, &arr_b).unwrap());
}

fn neq_simd(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(neq(&arr_a, &arr_b).unwrap());
}

fn lt_simd(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(lt(&arr_a, &arr_b).unwrap());
}

fn lt_eq_simd(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(lt_eq(&arr_a, &arr_b).unwrap());
}

fn gt_simd(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(gt(&arr_a, &arr_b).unwrap());
}

fn gt_eq_simd(size: usize) {
    let arr_a = create_array(size);
    let arr_b = create_array(size);
    criterion::black_box(gt_eq(&arr_a, &arr_b).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    c.bench_function("eq 512", |b| b.iter(|| eq_no_simd(512)));
    c.bench_function("eq 512 simd", |b| b.iter(|| eq_simd(512)));
    c.bench_function("neq 512", |b| b.iter(|| neq_no_simd(512)));
    c.bench_function("neq 512 simd", |b| b.iter(|| neq_simd(512)));
    c.bench_function("lt 512", |b| b.iter(|| lt_no_simd(512)));
    c.bench_function("lt 512 simd", |b| b.iter(|| lt_simd(512)));
    c.bench_function("lt_eq 512", |b| b.iter(|| lt_eq_no_simd(512)));
    c.bench_function("lt_eq 512 simd", |b| b.iter(|| lt_eq_simd(512)));
    c.bench_function("gt 512", |b| b.iter(|| gt_no_simd(512)));
    c.bench_function("gt 512 simd", |b| b.iter(|| gt_simd(512)));
    c.bench_function("gt_eq 512", |b| b.iter(|| gt_eq_no_simd(512)));
    c.bench_function("gt_eq 512 simd", |b| b.iter(|| gt_eq_simd(512)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
