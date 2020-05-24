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
use arrow::compute::*;
use arrow::datatypes::ArrowNumericType;

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

pub fn eq_no_simd<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    no_simd_compare_op(
        criterion::black_box(arr_a),
        criterion::black_box(arr_b),
        |a, b| a == b,
    )
    .unwrap();
}

pub fn eq_no_simd_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    no_simd_compare_op_scalar(
        criterion::black_box(arr_a),
        criterion::black_box(value_b),
        |a, b| a == b,
    )
    .unwrap();
}

pub fn neq_no_simd<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    no_simd_compare_op(
        criterion::black_box(arr_a),
        criterion::black_box(arr_b),
        |a, b| a != b,
    )
    .unwrap();
}

pub fn neq_no_simd_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    no_simd_compare_op_scalar(
        criterion::black_box(arr_a),
        criterion::black_box(value_b),
        |a, b| a != b,
    )
    .unwrap();
}

pub fn lt_no_simd<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    no_simd_compare_op(
        criterion::black_box(arr_a),
        criterion::black_box(arr_b),
        |a, b| a < b,
    )
    .unwrap();
}

pub fn lt_no_simd_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    no_simd_compare_op_scalar(
        criterion::black_box(arr_a),
        criterion::black_box(value_b),
        |a, b| a < b,
    )
    .unwrap();
}

fn lt_eq_no_simd<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    no_simd_compare_op(
        criterion::black_box(arr_a),
        criterion::black_box(arr_b),
        |a, b| a <= b,
    )
    .unwrap();
}

fn lt_eq_no_simd_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    no_simd_compare_op_scalar(
        criterion::black_box(arr_a),
        criterion::black_box(value_b),
        |a, b| a <= b,
    )
    .unwrap();
}

pub fn gt_no_simd<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    no_simd_compare_op(
        criterion::black_box(arr_a),
        criterion::black_box(arr_b),
        |a, b| a > b,
    )
    .unwrap();
}

pub fn gt_no_simd_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    no_simd_compare_op_scalar(
        criterion::black_box(arr_a),
        criterion::black_box(value_b),
        |a, b| a > b,
    )
    .unwrap();
}

fn gt_eq_no_simd<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    no_simd_compare_op(
        criterion::black_box(arr_a),
        criterion::black_box(arr_b),
        |a, b| a >= b,
    )
    .unwrap();
}

fn gt_eq_no_simd_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    no_simd_compare_op_scalar(
        criterion::black_box(arr_a),
        criterion::black_box(value_b),
        |a, b| a >= b,
    )
    .unwrap();
}

fn eq_simd<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    eq(criterion::black_box(arr_a), criterion::black_box(arr_b)).unwrap();
}

fn eq_simd_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    eq_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn neq_simd<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    neq(criterion::black_box(arr_a), criterion::black_box(arr_b)).unwrap();
}

fn neq_simd_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    neq_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn lt_simd<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    lt(criterion::black_box(arr_a), criterion::black_box(arr_b)).unwrap();
}

fn lt_simd_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    lt_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn lt_eq_simd<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    lt_eq(criterion::black_box(arr_a), criterion::black_box(arr_b)).unwrap();
}

fn lt_eq_simd_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    lt_eq_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn gt_simd<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    gt(criterion::black_box(arr_a), criterion::black_box(arr_b)).unwrap();
}

fn gt_simd_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    gt_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn gt_eq_simd<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
{
    gt_eq(criterion::black_box(arr_a), criterion::black_box(arr_b)).unwrap();
}

fn gt_eq_simd_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T::Native)
where
    T: ArrowNumericType,
{
    gt_eq_scalar(criterion::black_box(arr_a), criterion::black_box(value_b)).unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let arr_a = create_array(size);
    let arr_b = create_array(size);

    c.bench_function("eq Float32", |b| b.iter(|| eq_no_simd(&arr_a, &arr_b)));
    c.bench_function("eq scalar Float32", |b| {
        b.iter(|| eq_no_simd_scalar(&arr_a, 1.0))
    });
    c.bench_function("eq Float32 simd", |b| b.iter(|| eq_simd(&arr_a, &arr_b)));
    c.bench_function("eq scalar Float32 simd", |b| {
        b.iter(|| eq_simd_scalar(&arr_a, 1.0))
    });

    c.bench_function("neq Float32", |b| b.iter(|| neq_no_simd(&arr_a, &arr_b)));
    c.bench_function("neq scalar Float32", |b| {
        b.iter(|| neq_no_simd_scalar(&arr_a, 1.0))
    });
    c.bench_function("neq Float32 simd", |b| b.iter(|| neq_simd(&arr_a, &arr_b)));
    c.bench_function("neq scalar Float32 simd", |b| {
        b.iter(|| neq_simd_scalar(&arr_a, 1.0))
    });

    c.bench_function("lt Float32", |b| b.iter(|| lt_no_simd(&arr_a, &arr_b)));
    c.bench_function("lt scalar Float32", |b| {
        b.iter(|| lt_no_simd_scalar(&arr_a, 1.0))
    });
    c.bench_function("lt Float32 simd", |b| b.iter(|| lt_simd(&arr_a, &arr_b)));
    c.bench_function("lt scalar Float32 simd", |b| {
        b.iter(|| lt_simd_scalar(&arr_a, 1.0))
    });

    c.bench_function("lt_eq Float32", |b| {
        b.iter(|| lt_eq_no_simd(&arr_a, &arr_b))
    });
    c.bench_function("lt_eq scalar Float32", |b| {
        b.iter(|| lt_eq_no_simd_scalar(&arr_a, 1.0))
    });
    c.bench_function("lt_eq Float32 simd", |b| {
        b.iter(|| lt_eq_simd(&arr_a, &arr_b))
    });
    c.bench_function("lt_eq scalar Float32 simd", |b| {
        b.iter(|| lt_eq_simd_scalar(&arr_a, 1.0))
    });

    c.bench_function("gt Float32", |b| b.iter(|| gt_no_simd(&arr_a, &arr_b)));
    c.bench_function("gt scalar Float32", |b| {
        b.iter(|| gt_no_simd_scalar(&arr_a, 1.0))
    });
    c.bench_function("gt Float32 simd", |b| b.iter(|| gt_simd(&arr_a, &arr_b)));
    c.bench_function("gt scalar Float32 simd", |b| {
        b.iter(|| gt_simd_scalar(&arr_a, 1.0))
    });

    c.bench_function("gt_eq Float32", |b| {
        b.iter(|| gt_eq_no_simd(&arr_a, &arr_b))
    });
    c.bench_function("gt_eq scalar Float32", |b| {
        b.iter(|| gt_eq_no_simd_scalar(&arr_a, 1.0))
    });
    c.bench_function("gt_eq Float32 simd", |b| {
        b.iter(|| gt_eq_simd(&arr_a, &arr_b))
    });
    c.bench_function("gt_eq scalar Float32 simd", |b| {
        b.iter(|| gt_eq_simd_scalar(&arr_a, 1.0))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
