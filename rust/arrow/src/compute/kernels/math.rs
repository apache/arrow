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

//! Defines basic math kernels for `PrimitiveArrays`.

use num::Float;

use crate::datatypes::ArrowNumericType;
use crate::{array::*, float_unary};

use super::arity::unary;

float_unary!(sqrt);

pub fn powf<T>(array: &PrimitiveArray<T>, n: T::Native) -> PrimitiveArray<T>
where
    T: ArrowNumericType,
    T::Native: num::traits::Float,
{
    unary(array, |x| x.powf(n))
}

pub fn powi<T>(array: &PrimitiveArray<T>, n: i32) -> PrimitiveArray<T>
where
    T: ArrowNumericType,
    T::Native: num::traits::Float,
{
    unary(array, |x| x.powi(n))
}

// pub fn pow<T>(array: &PrimitiveArray<T>, n: usize) -> PrimitiveArray<T>
// where
//     T: ArrowPrimitiveType,
//     T::Native: num::Num
// {
//     unary::<_, _, T::Native>(array, |x| x.pow(n))
// }
