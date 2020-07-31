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

use arrow::array::*;
use arrow::compute::{filter, FilterContext};
use arrow::datatypes::ArrowNumericType;
use criterion::{criterion_group, criterion_main, Criterion};

fn create_primitive_array<T, F>(size: usize, value_fn: F) -> PrimitiveArray<T>
where
    T: ArrowNumericType,
    F: Fn(usize) -> T::Native,
{
    let mut builder = PrimitiveArray::<T>::builder(size);
    for i in 0..size {
        builder.append_value(value_fn(i)).unwrap();
    }
    builder.finish()
}

fn create_u8_array_with_nulls(size: usize) -> UInt8Array {
    let mut builder = UInt8Builder::new(size);
    for i in 0..size {
        if i % 2 == 0 {
            builder.append_value(1).unwrap();
        } else {
            builder.append_null().unwrap();
        }
    }
    builder.finish()
}

fn create_bool_array<F>(size: usize, value_fn: F) -> BooleanArray
where
    F: Fn(usize) -> bool,
{
    let mut builder = BooleanBuilder::new(size);
    for i in 0..size {
        builder.append_value(value_fn(i)).unwrap();
    }
    builder.finish()
}

fn bench_filter_u8(data_array: &UInt8Array, filter_array: &BooleanArray) {
    filter(
        criterion::black_box(data_array),
        criterion::black_box(filter_array),
    )
    .unwrap();
}

// fn bench_filter_f32(data_array: &Float32Array, filter_array: &BooleanArray) {
//     filter(criterion::black_box(data_array), criterion::black_box(filter_array)).unwrap();
// }

fn bench_filter_context_u8(data_array: &UInt8Array, filter_context: &FilterContext) {
    filter_context
        .filter(criterion::black_box(data_array))
        .unwrap();
}

fn bench_filter_context_f32(data_array: &Float32Array, filter_context: &FilterContext) {
    filter_context
        .filter(criterion::black_box(data_array))
        .unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let filter_array = create_bool_array(size, |i| match i % 2 {
        0 => true,
        _ => false,
    });
    let sparse_filter_array = create_bool_array(size, |i| match i % 8000 {
        0 => true,
        _ => false,
    });
    let dense_filter_array = create_bool_array(size, |i| match i % 8000 {
        0 => false,
        _ => true,
    });

    let filter_context = FilterContext::new(&filter_array).unwrap();
    let sparse_filter_context = FilterContext::new(&sparse_filter_array).unwrap();
    let dense_filter_context = FilterContext::new(&dense_filter_array).unwrap();

    let data_array = create_primitive_array(size, |i| match i % 2 {
        0 => 1,
        _ => 0,
    });
    c.bench_function("filter u8 low selectivity", |b| {
        b.iter(|| bench_filter_u8(&data_array, &filter_array))
    });
    c.bench_function("filter u8 high selectivity", |b| {
        b.iter(|| bench_filter_u8(&data_array, &sparse_filter_array))
    });
    c.bench_function("filter u8 very low selectivity", |b| {
        b.iter(|| bench_filter_u8(&data_array, &dense_filter_array))
    });

    c.bench_function("filter context u8 low selectivity", |b| {
        b.iter(|| bench_filter_context_u8(&data_array, &filter_context))
    });
    c.bench_function("filter context u8 high selectivity", |b| {
        b.iter(|| bench_filter_context_u8(&data_array, &sparse_filter_context))
    });
    c.bench_function("filter context u8 very low selectivity", |b| {
        b.iter(|| bench_filter_context_u8(&data_array, &dense_filter_context))
    });

    let data_array = create_u8_array_with_nulls(size);
    c.bench_function("filter context u8 w NULLs low selectivity", |b| {
        b.iter(|| bench_filter_context_u8(&data_array, &filter_context))
    });
    c.bench_function("filter context u8 w NULLs high selectivity", |b| {
        b.iter(|| bench_filter_context_u8(&data_array, &sparse_filter_context))
    });
    c.bench_function("filter context u8 w NULLs very low selectivity", |b| {
        b.iter(|| bench_filter_context_u8(&data_array, &dense_filter_context))
    });

    let data_array = create_primitive_array(size, |i| match i % 2 {
        0 => 1.0,
        _ => 0.0,
    });
    c.bench_function("filter context f32 low selectivity", |b| {
        b.iter(|| bench_filter_context_f32(&data_array, &filter_context))
    });
    c.bench_function("filter context f32 high selectivity", |b| {
        b.iter(|| bench_filter_context_f32(&data_array, &sparse_filter_context))
    });
    c.bench_function("filter context f32 very low selectivity", |b| {
        b.iter(|| bench_filter_context_f32(&data_array, &dense_filter_context))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
