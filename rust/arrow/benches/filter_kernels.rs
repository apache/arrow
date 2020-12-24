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
extern crate arrow;

use arrow::{compute::Filter, util::test_util::seedable_rng};
use rand::{
    distributions::{Alphanumeric, Standard},
    prelude::Distribution,
    Rng,
};

use arrow::array::*;
use arrow::compute::{build_filter, filter};
use arrow::datatypes::ArrowNumericType;
use arrow::datatypes::{Float32Type, UInt8Type};

use criterion::{criterion_group, criterion_main, Criterion};

fn create_primitive_array<T>(size: usize, null_density: f32) -> PrimitiveArray<T>
where
    T: ArrowNumericType,
    Standard: Distribution<T::Native>,
{
    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();
    let mut builder = PrimitiveArray::<T>::builder(size);

    for _ in 0..size {
        if rng.gen::<f32>() < null_density {
            builder.append_null().unwrap();
        } else {
            builder.append_value(rng.gen()).unwrap();
        }
    }
    builder.finish()
}

fn create_string_array(size: usize, null_density: f32) -> StringArray {
    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();
    let mut builder = StringBuilder::new(size);

    for _ in 0..size {
        if rng.gen::<f32>() < null_density {
            builder.append_null().unwrap();
        } else {
            let value = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(10)
                .collect::<String>();
            builder.append_value(&value).unwrap();
        }
    }
    builder.finish()
}

fn create_bool_array(size: usize, trues_density: f32) -> BooleanArray {
    let mut rng = seedable_rng();
    let mut builder = BooleanBuilder::new(size);
    for _ in 0..size {
        let value = rng.gen::<f32>() < trues_density;
        builder.append_value(value).unwrap();
    }
    builder.finish()
}

fn bench_filter(data_array: &dyn Array, filter_array: &BooleanArray) {
    criterion::black_box(filter(data_array, filter_array).unwrap());
}

fn bench_built_filter<'a>(filter: &Filter<'a>, data: &impl Array) {
    criterion::black_box(filter(&data.data()));
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let filter_array = create_bool_array(size, 0.5);
    let dense_filter_array = create_bool_array(size, 1.0 - 1.0 / 1024.0);
    let sparse_filter_array = create_bool_array(size, 1.0 / 1024.0);

    let filter = build_filter(&filter_array).unwrap();
    let dense_filter = build_filter(&dense_filter_array).unwrap();
    let sparse_filter = build_filter(&sparse_filter_array).unwrap();

    let data_array = create_primitive_array::<UInt8Type>(size, 0.0);

    c.bench_function("filter u8", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter u8 high selectivity", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });
    c.bench_function("filter u8 low selectivity", |b| {
        b.iter(|| bench_filter(&data_array, &sparse_filter_array))
    });

    c.bench_function("filter context u8", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context u8 high selectivity", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context u8 low selectivity", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<UInt8Type>(size, 0.5);
    c.bench_function("filter context u8 w NULLs", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context u8 w NULLs high selectivity", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context u8 w NULLs low selectivity", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<Float32Type>(size, 0.5);
    c.bench_function("filter f32", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter context f32", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context f32 high selectivity", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context f32 low selectivity", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_string_array(size, 0.5);
    c.bench_function("filter context string", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context string high selectivity", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context string low selectivity", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
