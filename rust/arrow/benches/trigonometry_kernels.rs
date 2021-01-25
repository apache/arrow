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

use num::Zero;
use rand::Rng;
use std::{ops::Div, sync::Arc};

extern crate arrow;

use arrow::error::{ArrowError, Result};
use arrow::util::test_util::seedable_rng;
use arrow::{
    array::*,
    compute::*,
    datatypes::{ArrowNumericType, ArrowPrimitiveType},
};

/// This computes the Haversine formula without using unary kernels
fn haversine_no_unary<T>(
    lat_a: &PrimitiveArray<T>,
    lng_a: &PrimitiveArray<T>,
    lat_b: &PrimitiveArray<T>,
    lng_b: &PrimitiveArray<T>,
    radius: impl num::traits::Float,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType + ArrowNumericType,
    T::Native: num::traits::Float + Div<Output = T::Native> + Zero + num::NumCast,
{
    // Check array lengths, must all equal
    let len = lat_a.len();
    if lat_b.len() != len || lng_a.len() != len || lng_b.len() != len {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    // there doesn't seem to be a way to satify the type system without branching
    let (one, two, radius) = match T::DATA_TYPE {
        arrow::datatypes::DataType::Float32 => {
            let one = Float32Array::from(vec![1.0; len]);
            let two = Float32Array::from(vec![2.0; len]);
            let radius =
                Float32Array::from(vec![num::cast::<_, f32>(radius).unwrap(); len]);
            // cast to array then back to T
            (
                Arc::new(one) as ArrayRef,
                Arc::new(two) as ArrayRef,
                Arc::new(radius) as ArrayRef,
            )
        }
        arrow::datatypes::DataType::Float64 => {
            let one = Float64Array::from(vec![1.0; len]);
            let two = Float64Array::from(vec![2.0; len]);
            let radius =
                Float64Array::from(vec![num::cast::<_, f64>(radius).unwrap(); len]);
            // cast to array then back to T
            (
                Arc::new(one) as ArrayRef,
                Arc::new(two) as ArrayRef,
                Arc::new(radius) as ArrayRef,
            )
        }
        _ => unreachable!("This function should only be callable from floats"),
    };
    let one = one.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let two = two.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let radius = radius.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    let lat_delta = to_radians(&subtract(lat_b, lat_a)?);
    let lng_delta = to_radians(&subtract(lng_b, lng_a)?);
    let lat_a_rad = to_radians(lat_a);
    let lat_b_rad = to_radians(lat_b);

    let v1 = &sin(&divide(&lat_delta, &two)?);
    let v2 = sin(&divide(&lng_delta, &two)?);

    let a = add(
        &multiply(&v1, &v1)?,
        &multiply(
            &multiply(&v2, &v2)?,
            &multiply(&cos(&lat_a_rad), &cos(&lat_b_rad))?,
        )?,
    )?;
    let c: PrimitiveArray<T> =
        multiply(&atan2(&sqrt(&a), &sqrt(&subtract(&one, &a)?))?, &two)?;

    multiply(&c, &radius)
}

fn create_array(size: usize, with_nulls: bool) -> Float32Array {
    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();
    let mut builder = Float32Builder::new(size);

    for _ in 0..size {
        if with_nulls && rng.gen::<f32>() > 0.5 {
            builder.append_null().unwrap();
        } else {
            builder.append_value(rng.gen()).unwrap();
        }
    }
    builder.finish()
}

fn bench_haversine_no_unary(
    lat_a: &Float32Array,
    lng_a: &Float32Array,
    lat_b: &Float32Array,
    lng_b: &Float32Array,
) {
    criterion::black_box(
        haversine_no_unary(lat_a, lng_a, lat_b, lng_b, 6371000.0).unwrap(),
    );
}

fn bench_haversine(
    lat_a: &Float32Array,
    lng_a: &Float32Array,
    lat_b: &Float32Array,
    lng_b: &Float32Array,
) {
    criterion::black_box(haversine(lat_a, lng_a, lat_b, lng_b, 6371000.0).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let lat_a = create_array(512, false);
    let lng_a = create_array(512, false);
    let lat_b = create_array(512, false);
    let lng_b = create_array(512, false);

    c.bench_function("haversine_no_unary 512", |b| {
        b.iter(|| bench_haversine_no_unary(&lat_a, &lng_a, &lat_b, &lng_b))
    });
    c.bench_function("haversine_unary 512", |b| {
        b.iter(|| bench_haversine(&lat_a, &lng_a, &lat_b, &lng_b))
    });

    let lat_a = create_array(512, true);
    let lng_a = create_array(512, true);
    let lat_b = create_array(512, true);
    let lng_b = create_array(512, true);

    c.bench_function("haversine_no_unary_nulls 512", |b| {
        b.iter(|| bench_haversine_no_unary(&lat_a, &lng_a, &lat_b, &lng_b))
    });
    c.bench_function("haversine_unary_nulls 512", |b| {
        b.iter(|| bench_haversine(&lat_a, &lng_a, &lat_b, &lng_b))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
