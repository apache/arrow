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
use std::sync::Arc;

fn create_array_slice(array: &ArrayRef, length: usize) -> ArrayRef {
    array.slice(0, length)
}

fn create_array_with_nulls(size: usize) -> ArrayRef {
    let array: Float64Array = (0..size)
        .map(|i| if i % 2 == 0 { Some(1.0) } else { None })
        .collect();
    Arc::new(array)
}

fn array_slice_benchmark(c: &mut Criterion) {
    let array = create_array_with_nulls(4096);
    c.bench_function("array_slice 128", |b| {
        b.iter(|| create_array_slice(&array, 128))
    });
    c.bench_function("array_slice 512", |b| {
        b.iter(|| create_array_slice(&array, 512))
    });
    c.bench_function("array_slice 2048", |b| {
        b.iter(|| create_array_slice(&array, 2048))
    });
}

criterion_group!(benches, array_slice_benchmark);
criterion_main!(benches);
