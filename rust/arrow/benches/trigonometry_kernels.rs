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

use arrow::{
    array::*, compute::*, datatypes::Float32Type,
    util::bench_util::create_primitive_array,
};

fn bench_haversine(
    lat_a: &Float32Array,
    lng_a: &Float32Array,
    lat_b: &Float32Array,
    lng_b: &Float32Array,
) {
    criterion::black_box(haversine(lat_a, lng_a, lat_b, lng_b, 6371000.0).unwrap());
}
fn bench_sin(array: &Float32Array) {
    criterion::black_box(sin(array));
}

fn bench_cos(array: &Float32Array) {
    criterion::black_box(cos(array));
}

fn bench_tan(array: &Float32Array) {
    criterion::black_box(tan(array));
}

fn add_benchmark(c: &mut Criterion) {
    let lat_a = create_primitive_array(512, 0.0);
    let lng_a = create_primitive_array(512, 0.0);
    let lat_b = create_primitive_array(512, 0.0);
    let lng_b = create_primitive_array(512, 0.0);

    c.bench_function("haversine_unary 512", |b| {
        b.iter(|| bench_haversine(&lat_a, &lng_a, &lat_b, &lng_b))
    });

    let lat_a = create_primitive_array(512, 0.5);
    let lng_a = create_primitive_array(512, 0.5);
    let lat_b = create_primitive_array(512, 0.5);
    let lng_b = create_primitive_array(512, 0.5);

    c.bench_function("haversine_unary_nulls 512", |b| {
        b.iter(|| bench_haversine(&lat_a, &lng_a, &lat_b, &lng_b))
    });

    let array = create_primitive_array::<Float32Type>(512, 0.0);
    let array_nulls = create_primitive_array::<Float32Type>(512, 0.5);

    c.bench_function("sin 512", |b| b.iter(|| bench_sin(&array)));
    c.bench_function("cos 512", |b| b.iter(|| bench_cos(&array)));
    c.bench_function("tan 512", |b| b.iter(|| bench_tan(&array)));
    c.bench_function("sin nulls 512", |b| b.iter(|| bench_sin(&array_nulls)));
    c.bench_function("cos nulls 512", |b| b.iter(|| bench_cos(&array_nulls)));
    c.bench_function("tan nulls 512", |b| b.iter(|| bench_tan(&array_nulls)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
