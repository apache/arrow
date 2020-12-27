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
use rand::distributions::{Distribution, Standard, Uniform};
use rand::prelude::random;
use rand::Rng;

use std::sync::Arc;

extern crate arrow;

use arrow::array::*;
use arrow::compute::cast;
use arrow::datatypes::*;
use arrow::util::test_util::seedable_rng;

fn build_array<FROM>(size: usize) -> ArrayRef
where
    FROM: ArrowNumericType,
    Standard: Distribution<FROM::Native>,
    PrimitiveArray<FROM>: std::convert::From<Vec<Option<FROM::Native>>>,
{
    let values = (0..size)
        .map(|_| {
            // 10% nulls, i.e. dense.
            if random::<f64>() < 0.1 {
                None
            } else {
                Some(random::<FROM::Native>())
            }
        })
        .collect();

    Arc::new(PrimitiveArray::<FROM>::from(values))
}

fn build_timestamp_array<FROM>(size: usize) -> ArrayRef
where
    FROM: ArrowTimestampType,
    Standard: Distribution<FROM::Native>,
{
    let values = (0..size)
        .map(|_| {
            if random::<f64>() < 0.5 {
                None
            } else {
                Some(random::<i64>())
            }
        })
        .collect::<Vec<Option<i64>>>();

    Arc::new(PrimitiveArray::<FROM>::from_opt_vec(values, None))
}

fn build_utf8_date_array(size: usize, with_nulls: bool) -> ArrayRef {
    use chrono::NaiveDate;

    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();
    let mut builder = StringBuilder::new(size);
    let range = Uniform::new(0, 737776);

    for _ in 0..size {
        if with_nulls && rng.gen::<f32>() > 0.8 {
            builder.append_null().unwrap();
        } else {
            let string = NaiveDate::from_num_days_from_ce(rng.sample(range))
                .format("%Y-%m-%d")
                .to_string();
            builder.append_value(&string).unwrap();
        }
    }
    Arc::new(builder.finish())
}

fn build_utf8_date_time_array(size: usize, with_nulls: bool) -> ArrayRef {
    use chrono::NaiveDateTime;

    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();
    let mut builder = StringBuilder::new(size);
    let range = Uniform::new(0, 1608071414123);

    for _ in 0..size {
        if with_nulls && rng.gen::<f32>() > 0.8 {
            builder.append_null().unwrap();
        } else {
            let string = NaiveDateTime::from_timestamp(rng.sample(range), 0)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            builder.append_value(&string).unwrap();
        }
    }
    Arc::new(builder.finish())
}

// cast array from specified primitive array type to desired data type
fn cast_array(array: &ArrayRef, to_type: DataType) {
    criterion::black_box(cast(array, &to_type).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let i32_array = build_array::<Int32Type>(512);
    let i64_array = build_array::<Int64Type>(512);
    let f32_array = build_array::<Float32Type>(512);
    let f32_utf8_array = cast(&build_array::<Float32Type>(512), &DataType::Utf8).unwrap();

    let f64_array = build_array::<Float64Type>(512);
    let date64_array = build_array::<Date64Type>(512);
    let date32_array = build_array::<Date32Type>(512);
    let time32s_array = build_array::<Time32SecondType>(512);
    let time64ns_array = build_array::<Time64NanosecondType>(512);
    let time_ns_array = build_timestamp_array::<TimestampNanosecondType>(512);
    let time_ms_array = build_timestamp_array::<TimestampMillisecondType>(512);
    let utf8_date_array = build_utf8_date_array(512, true);
    let utf8_date_time_array = build_utf8_date_time_array(512, true);

    c.bench_function("cast int32 to int32 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::Int32))
    });
    c.bench_function("cast int32 to uint32 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::UInt32))
    });
    c.bench_function("cast int32 to float32 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::Float32))
    });
    c.bench_function("cast int32 to float64 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::Float64))
    });
    c.bench_function("cast int32 to int64 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::Int64))
    });
    c.bench_function("cast float32 to int32 512", |b| {
        b.iter(|| cast_array(&f32_array, DataType::Int32))
    });
    c.bench_function("cast float64 to float32 512", |b| {
        b.iter(|| cast_array(&f64_array, DataType::Float32))
    });
    c.bench_function("cast float64 to uint64 512", |b| {
        b.iter(|| cast_array(&f64_array, DataType::UInt64))
    });
    c.bench_function("cast int64 to int32 512", |b| {
        b.iter(|| cast_array(&i64_array, DataType::Int32))
    });
    c.bench_function("cast date64 to date32 512", |b| {
        b.iter(|| cast_array(&date64_array, DataType::Date32(DateUnit::Day)))
    });
    c.bench_function("cast date32 to date64 512", |b| {
        b.iter(|| cast_array(&date32_array, DataType::Date64(DateUnit::Millisecond)))
    });
    c.bench_function("cast time32s to time32ms 512", |b| {
        b.iter(|| cast_array(&time32s_array, DataType::Time32(TimeUnit::Millisecond)))
    });
    c.bench_function("cast time32s to time64us 512", |b| {
        b.iter(|| cast_array(&time32s_array, DataType::Time64(TimeUnit::Microsecond)))
    });
    c.bench_function("cast time64ns to time32s 512", |b| {
        b.iter(|| cast_array(&time64ns_array, DataType::Time32(TimeUnit::Second)))
    });
    c.bench_function("cast timestamp_ns to timestamp_s 512", |b| {
        b.iter(|| {
            cast_array(
                &time_ns_array,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )
        })
    });
    c.bench_function("cast timestamp_ms to timestamp_ns 512", |b| {
        b.iter(|| {
            cast_array(
                &time_ms_array,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )
        })
    });
    c.bench_function("cast utf8 to f32", |b| {
        b.iter(|| cast_array(&f32_utf8_array, DataType::Float32))
    });

    c.bench_function("cast timestamp_ms to i64 512", |b| {
        b.iter(|| cast_array(&time_ms_array, DataType::Int64))
    });
    c.bench_function("cast utf8 to date32 512", |b| {
        b.iter(|| cast_array(&utf8_date_array, DataType::Date32(DateUnit::Day)))
    });
    c.bench_function("cast utf8 to date64 512", |b| {
        b.iter(|| {
            cast_array(
                &utf8_date_time_array,
                DataType::Date64(DateUnit::Millisecond),
            )
        })
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
