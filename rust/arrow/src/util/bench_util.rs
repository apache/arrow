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

//! Utils to make benchmarking easier

use rand::distributions::{Alphanumeric, Distribution, Standard};
use rand::Rng;

use crate::array::*;
use crate::datatypes::*;
use crate::util::test_util::seedable_rng;

/// Creates an random (but fixed-seeded) array of a given size and null density
pub fn create_primitive_array<T>(size: usize, null_density: f32) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    Standard: Distribution<T::Native>,
{
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(rng.gen())
            }
        })
        .collect()
}

/// Creates an random (but fixed-seeded) array of a given size and null density
pub fn create_boolean_array(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> BooleanArray
where
    Standard: Distribution<bool>,
{
    let mut rng = seedable_rng();
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let value = rng.gen::<f32>() < true_density;
                Some(value)
            }
        })
        .collect()
}

/// Creates an random (but fixed-seeded) array of a given size and null density
pub fn create_string_array(size: usize, null_density: f32) -> StringArray {
    let rng = &mut seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let value = rng.sample_iter(&Alphanumeric).take(4).collect::<String>();
                Some(value)
            }
        })
        .collect()
}
