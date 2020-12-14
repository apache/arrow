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

use rand::distributions::{Alphanumeric, Distribution, Standard};
use rand::Rng;

use std::sync::Arc;

extern crate arrow;

use arrow::array::*;
use arrow::compute::concat;
use arrow::datatypes::*;
use arrow::util::test_util::seedable_rng;

// cast array from specified primitive array type to desired data type
fn create_primitive<T>(size: usize, null_density: f32) -> ArrayRef
where
    T: ArrowPrimitiveType,
    Standard: Distribution<T::Native>,
    PrimitiveArray<T>: std::convert::From<Vec<T::Native>>,
{
    let mut rng = seedable_rng();

    let array: PrimitiveArray<T> = seedable_rng()
        .sample_iter(&Standard)
        .take(size)
        .map(|value| {
            let x = rng.gen::<f32>();
            if x < null_density {
                Some(value)
            } else {
                None
            }
        })
        .collect();

    Arc::new(array) as ArrayRef
}

fn create_strings(size: usize, null_density: f32) -> ArrayRef {
    let rng = &mut seedable_rng();

    let mut builder = StringBuilder::new(size);
    for _ in 0..size {
        let x = rng.gen::<f32>();
        if x < null_density {
            let value = rng.sample_iter(&Alphanumeric).take(4).collect::<String>();
            builder.append_value(&value).unwrap();
        } else {
            builder.append_null().unwrap()
        }
    }
    Arc::new(builder.finish())
}

fn bench_concat(v1: &ArrayRef, v2: &ArrayRef) {
    criterion::black_box(concat(&[v1.as_ref(), v2.as_ref()]).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let v1 = create_primitive::<Int32Type>(1024, 0.0);
    let v2 = create_primitive::<Int32Type>(1024, 0.0);
    c.bench_function("concat i32 1024", |b| b.iter(|| bench_concat(&v1, &v2)));

    let v1 = create_primitive::<Int32Type>(1024, 0.5);
    let v2 = create_primitive::<Int32Type>(1024, 0.5);
    c.bench_function("concat i32 nulls 1024", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });

    let v1 = create_strings(1024, 0.0);
    let v2 = create_strings(1024, 0.0);
    c.bench_function("concat str 1024", |b| b.iter(|| bench_concat(&v1, &v2)));

    let v1 = create_strings(1024, 0.5);
    let v2 = create_strings(1024, 0.5);
    c.bench_function("concat str nulls 1024", |b| {
        b.iter(|| bench_concat(&v1, &v2))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
