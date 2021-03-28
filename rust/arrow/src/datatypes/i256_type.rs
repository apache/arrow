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

use crate::datatypes::{ArrowDecimalNativeType, ArrowNativeType, JsonSerializable};
use num::Zero;
use serde_json::Value;
use std::cmp::Ordering;
use std::fmt;
use std::num::ParseIntError;
use std::ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign};
use std::str::FromStr;

/// Number of bits a signed 256-bit number occupies.
pub const BITS: usize = 256;

/// Number of bytes a signed 256-bit number occupies.
pub const BYTES: usize = 32;

/// An signed 256-bit number. Rust language does not provide i256, for now It is a polyfill.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
pub struct i256 {
    #[cfg(target_endian = "little")]
    lo: u128,

    hi: i128,

    #[cfg(target_endian = "big")]
    lo: u128,
}

impl i256 {
    pub fn from_le_bytes(_bytes: [u8; 32]) -> Self {
        unimplemented!();
    }

    pub fn to_le_bytes(self) -> [u8; 32] {
        unimplemented!();
    }

    pub fn to_be_bytes(self) -> [u8; 32] {
        unimplemented!();
    }

    pub fn abs(self) -> Self {
        unimplemented!();
    }
}

impl Default for i256 {
    fn default() -> Self {
        i256::zero()
    }
}

impl fmt::Display for i256 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "not working")
    }
}

impl Ord for i256 {
    fn cmp(&self, _other: &Self) -> Ordering {
        unimplemented!()
    }

    fn max(self, _other: Self) -> Self
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn min(self, _other: Self) -> Self
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn clamp(self, _min: Self, _max: Self) -> Self
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

impl PartialOrd for i256 {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        unimplemented!()
    }

    fn lt(&self, _other: &Self) -> bool {
        unimplemented!()
    }

    fn le(&self, _other: &Self) -> bool {
        unimplemented!()
    }

    fn gt(&self, _other: &Self) -> bool {
        unimplemented!()
    }

    fn ge(&self, _other: &Self) -> bool {
        unimplemented!()
    }
}

impl FromStr for i256 {
    type Err = ParseIntError;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        unimplemented!()
    }
}

impl Zero for i256 {
    fn zero() -> Self {
        i256 { lo: 0, hi: 0 }
    }

    fn is_zero(&self) -> bool {
        self.lo == 0 && self.hi == 0
    }
}

impl Add for i256 {
    type Output = i256;

    fn add(self, _rhs: Self) -> Self::Output {
        unimplemented!()
    }
}

impl AddAssign for i256 {
    fn add_assign(&mut self, _rhs: Self) {
        unimplemented!()
    }
}

impl Sub for i256 {
    type Output = i256;

    fn sub(self, _rhs: Self) -> Self::Output {
        unimplemented!()
    }
}

impl SubAssign for i256 {
    fn sub_assign(&mut self, _rhs: Self) {
        unimplemented!()
    }
}

impl Mul for i256 {
    type Output = i256;

    fn mul(self, _rhs: Self) -> Self::Output {
        unimplemented!()
    }
}

impl MulAssign for i256 {
    fn mul_assign(&mut self, _rhs: Self) {
        unimplemented!()
    }
}

impl Div for i256 {
    type Output = i256;

    fn div(self, _rhs: Self) -> Self::Output {
        unimplemented!()
    }
}

impl DivAssign for i256 {
    fn div_assign(&mut self, _rhs: Self) {
        unimplemented!()
    }
}

// Start From * to i256
impl From<i64> for i256 {
    fn from(_: i64) -> Self {
        unimplemented!()
    }
}

impl From<i128> for i256 {
    fn from(_: i128) -> Self {
        unimplemented!()
    }
}
// End From * to i256

// Start From i256 to *
impl From<i256> for usize {
    fn from(_: i256) -> Self {
        unimplemented!()
    }
}

impl From<i256> for i8 {
    fn from(_: i256) -> Self {
        unimplemented!()
    }
}

impl From<i256> for u8 {
    fn from(_: i256) -> Self {
        unimplemented!()
    }
}

impl From<i256> for i16 {
    fn from(_: i256) -> Self {
        unimplemented!()
    }
}

impl From<i256> for u16 {
    fn from(_: i256) -> Self {
        unimplemented!()
    }
}

impl From<i256> for i32 {
    fn from(_: i256) -> Self {
        unimplemented!()
    }
}

impl From<i256> for u32 {
    fn from(_: i256) -> Self {
        unimplemented!()
    }
}

impl From<i256> for i64 {
    fn from(_: i256) -> Self {
        unimplemented!()
    }
}

impl From<i256> for u64 {
    fn from(_: i256) -> Self {
        unimplemented!()
    }
}

impl From<i256> for i128 {
    fn from(_: i256) -> Self {
        unimplemented!()
    }
}

impl From<i256> for u128 {
    fn from(_: i256) -> Self {
        unimplemented!()
    }
}
// End From i256 to *

impl JsonSerializable for i256 {
    fn into_json_value(self) -> Option<Value> {
        unimplemented!("into_json_value")
    }
}

impl ArrowNativeType for i256 {
    #[inline]
    fn from_usize(_v: usize) -> Option<Self> {
        unimplemented!()
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        unimplemented!()
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        unimplemented!()
    }
}

impl ArrowDecimalNativeType for i256 {
    fn pow_to(_mul: u32) -> i256 {
        unimplemented!()
    }

    #[inline]
    fn as_string(self, scale: usize) -> String {
        // Skip sign, because we split string to lead, trail
        let as_str = self.abs().to_string();
        let len = as_str.len();

        let (lead, trail) = (&as_str[..(len - scale)], &as_str[(len - scale)..]);

        let mut result = String::new();

        if self.lt(&i256::zero()) {
            result.push_str("-")
        }

        if lead == "" {
            result.push_str(&"0");
        } else {
            result.push_str(&lead);
        }

        if !trail.is_empty() {
            result.push_str(&".");
            result.push_str(&trail);
        }

        result
    }
}
