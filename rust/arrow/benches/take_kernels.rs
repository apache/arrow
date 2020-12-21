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

extern crate arrow;

use arrow::array::*;
use arrow::compute::take;
use arrow::datatypes::*;
use arrow::util::test_util::seedable_rng;

// cast array from specified primitive array type to desired data type
fn create_primitive<T>(size: usize) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    Standard: Distribution<T::Native>,
    PrimitiveArray<T>: std::convert::From<Vec<T::Native>>,
{
    seedable_rng()
        .sample_iter(&Standard)
        .take(size)
        .map(Some)
        .collect()
}

// cast array from specified primitive array type to desired data type
fn create_boolean(size: usize) -> BooleanArray
where
    Standard: Distribution<bool>,
{
    seedable_rng()
        .sample_iter(&Standard)
        .take(size)
        .map(Some)
        .collect()
}

fn create_strings(size: usize, null_density: f32) -> StringArray {
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
    builder.finish()
}

fn create_random_index(size: usize, null_density: f32) -> UInt32Array {
    let mut rng = seedable_rng();
    let mut builder = UInt32Builder::new(size);
    for _ in 0..size {
        if rng.gen::<f32>() < null_density {
            let value = rng.gen_range::<u32, _, _>(0u32, size as u32);
            builder.append_value(value).unwrap();
        } else {
            builder.append_null().unwrap()
        }
    }
    builder.finish()
}

fn bench_take(values: &dyn Array, indices: &UInt32Array) {
    criterion::black_box(take(values, &indices, None).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let values = create_primitive::<Int32Type>(512);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take i32 512", |b| b.iter(|| bench_take(&values, &indices)));
    let values = create_primitive::<Int32Type>(1024);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take i32 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let indices = create_random_index(512, 0.5);
    c.bench_function("take i32 nulls 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_primitive::<Int32Type>(1024);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take i32 nulls 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_boolean(512);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take bool 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_boolean(1024);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take bool 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_boolean(512);
    let indices = create_random_index(512, 0.5);
    c.bench_function("take bool nulls 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_boolean(1024);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take bool nulls 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_strings(512, 0.0);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take str 512", |b| b.iter(|| bench_take(&values, &indices)));

    let values = create_strings(1024, 0.0);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take str 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_strings(512, 0.0);
    let indices = create_random_index(512, 0.5);
    c.bench_function("take str null indices 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_strings(1024, 0.0);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take str null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_strings(1024, 0.5);

    let indices = create_random_index(1024, 0.0);
    c.bench_function("take str null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_strings(1024, 0.5);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take str null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
