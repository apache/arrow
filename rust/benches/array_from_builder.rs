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

fn array_from_builder(n: usize) {
    let mut v: Builder<i32> = Builder::with_capacity(n);
    for i in 0..n {
        v.push(i as i32);
    }
    Array::from(v.finish());
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("array_from_builder 128", |b| {
        b.iter(|| array_from_builder(128))
    });
    c.bench_function("array_from_builder 256", |b| {
        b.iter(|| array_from_builder(256))
    });
    c.bench_function("array_from_builder 512", |b| {
        b.iter(|| array_from_builder(512))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
