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
use rand::prelude::random;

use std::sync::Arc;

extern crate arrow;

use arrow::array::*;
use arrow::compute::cast;
use arrow::datatypes::{DataType, DateUnit, TimeUnit};

fn cast_date64_to_date32(size: usize) {
    let arr_a = Arc::new(Date64Array::from(vec![random::<i64>(); size])) as ArrayRef;
    criterion::black_box(cast(&arr_a, &DataType::Date32(DateUnit::Day)).unwrap());
}

fn cast_date32_to_date64(size: usize) {
    let arr_a = Arc::new(Date32Array::from(vec![random::<i32>(); size])) as ArrayRef;
    criterion::black_box(cast(&arr_a, &DataType::Date64(DateUnit::Millisecond)).unwrap());
}

fn cast_time32_s_to_time32_ms(size: usize) {
    let arr_a =
        Arc::new(Time32SecondArray::from(vec![random::<i32>(); size])) as ArrayRef;
    criterion::black_box(cast(&arr_a, &DataType::Time32(TimeUnit::Millisecond)).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    c.bench_function("cast date64 to date32 512", |b| {
        b.iter(|| cast_date64_to_date32(512))
    });
    c.bench_function("cast date32 to date64 512", |b| {
        b.iter(|| cast_date32_to_date64(512))
    });
    c.bench_function("cast time32s to time32ms 512", |b| {
        b.iter(|| cast_time32_s_to_time32_ms(512))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
