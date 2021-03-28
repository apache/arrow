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

use super::*;

use crate::alloc::NativeType;
use num::{NumCast, ToPrimitive, Zero};
use serde_json::Value;
use std::hash::{Hash, Hasher};
use std::{
    cmp::Ordering,
    convert::TryInto,
    fmt,
    mem::size_of,
    num::ParseIntError,
    ops::{Add, AddAssign, Sub, SubAssign},
    str::FromStr,
};

#[derive(Debug, PartialEq)]
pub enum ParseDecimalError {
    ParseIntError(ParseIntError),
    Other(String),
}

impl From<ParseIntError> for ParseDecimalError {
    fn from(err: ParseIntError) -> ParseDecimalError {
        ParseDecimalError::ParseIntError(err)
    }
}

// Decimal (precision, scale) = Decimal(1, 2) = 1.00
pub trait ArrowDecimalType: fmt::Debug + Send + Sync + FromStr + PartialEq {
    const MAX_DIGITS: usize;

    /// Corresponding Rust native type for the primitive type.
    type Native: ArrowDecimalNativeType;

    fn get_byte_width_for_precision_scale(precision: usize, scale: usize) -> usize;

    // Rescale scale part
    fn rescale(&mut self, scale: usize);

    fn rescale_to_new(self, scale: usize) -> Self;

    // Try to parse string with precision, scale
    fn from_i128(
        n: i128,
        precision: usize,
        scale: usize,
    ) -> std::result::Result<Self, ParseDecimalError>;

    fn from_i64(
        n: i64,
        precision: usize,
        scale: usize,
    ) -> std::result::Result<Self, ParseDecimalError>;

    fn from_f64(
        n: f64,
        precision: usize,
        scale: usize,
    ) -> std::result::Result<Self, ParseDecimalError>;

    // Try to parse string with precision, scale
    fn parse(
        string: &str,
        precision: usize,
        scale: usize,
    ) -> std::result::Result<Self, ParseDecimalError>;

    fn to_byte_slice(&self) -> Vec<u8>;

    fn get_signed_lead_part(&self) -> Self::Native;

    fn from_bytes_with_precision_scale(
        bytes: &[u8],
        precision: usize,
        scale: usize,
    ) -> Self;
}

#[inline]
pub fn numeric_to_decimal<T: ToString, U: ArrowDecimalType>(
    n: T,
    p: usize,
    s: usize,
) -> Option<U> {
    Some(U::parse(n.to_string().as_str(), p, s).unwrap_or_else(|_e| {
        panic!("unable to represent");
    }))
}

#[inline]
pub fn decimal_to_numeric<U: DecimalCast, T: NumCast>(n: U) -> Option<T> {
    T::from(n)
}

pub trait DecimalCast: Sized + ArrowDecimalType + ToPrimitive {
    fn from<T: ToPrimitive>(n: T, p: usize, s: usize) -> Option<Self>;
}

macro_rules! make_type {
    ($name:ident, $native_ty:ty, $max_digits:expr, $bytes_size:expr) => {
        #[derive(Copy, Clone, Eq)]
        pub struct $name {
            pub digits: $native_ty,
            pub precision: usize,
            pub scale: usize,
        }

        impl $name {
            pub fn new(digits: $native_ty, precision: usize, scale: usize) -> $name {
                assert!(
                    (precision + scale) <= $max_digits,
                    "Unable to use {} to represent Decimal({}, {}), max digits reached ({}).",
                    stringify!($name),
                    precision,
                    scale,
                    stringify!($max_digits),
                );

                $name {
                    digits,
                    precision,
                    scale,
                }
            }
        }

        impl ArrowDecimalType for $name {
            const MAX_DIGITS: usize = $max_digits;

            type Native = $native_ty;

            /// Returns the byte width of this primitive type.
            fn get_byte_width_for_precision_scale(
                _precision: usize,
                _scale: usize,
            ) -> usize {
                size_of::<$native_ty>()
            }

            #[inline(always)]
            fn rescale(&mut self, scale: usize) {
                if self.digits.is_zero() {
                    self.scale = scale;
                } else {
                    match self.scale.cmp(&scale) {
                        Ordering::Greater => {
                            self.digits /= <$native_ty>::pow_to((self.scale - scale) as u32);
                            self.scale = scale;
                        }
                        Ordering::Less => {
                            self.digits *= <$native_ty>::pow_to((scale - self.scale) as u32);
                            self.scale = scale;
                        }
                        Ordering::Equal => {}
                    };
                }
            }

            #[inline(always)]
            fn get_signed_lead_part(&self) -> Self::Native {
                self.rescale_to_new(0).digits
            }

            #[inline(always)]
            fn rescale_to_new(self, scale: usize) -> $name {
                if self.digits.is_zero() {
                    return $name::new(<$native_ty>::zero(), 0, scale);
                }

                let digits = match self.scale.cmp(&scale) {
                    Ordering::Greater => {
                        self.digits / <$native_ty>::pow_to((self.scale - scale) as u32)
                    }
                    Ordering::Less => {
                        self.digits * <$native_ty>::pow_to((scale - self.scale) as u32)
                    }
                    Ordering::Equal => self.digits,
                };

                $name::new(digits, self.precision, scale)
            }

            fn from_i128(
                n: i128,
                precision: usize,
                scale: usize,
            ) -> Result<Self, ParseDecimalError> {
                let mut as_decimal = $name::new(From::from(n), precision, 0);

                if as_decimal.scale != scale {
                    as_decimal.rescale(scale)
                }

                as_decimal.precision = precision;

                Ok(as_decimal)
            }

            fn from_i64(
                n: i64,
                precision: usize,
                scale: usize,
            ) -> Result<Self, ParseDecimalError> {
                let mut as_decimal = $name::new(From::from(n), precision, 0);

                if as_decimal.scale != scale {
                    as_decimal.rescale(scale)
                }

                as_decimal.precision = precision;

                Ok(as_decimal)
            }

            fn from_f64(
                n: f64,
                precision: usize,
                scale: usize,
            ) -> Result<Self, ParseDecimalError> {
                $name::parse(n.to_string().as_str(), precision, scale)
            }

            fn parse(
                string: &str,
                precision: usize,
                scale: usize,
            ) -> Result<Self, ParseDecimalError> {
                let mut as_decimal = $name::from_str(string)?;

                if as_decimal.scale != scale {
                    as_decimal.rescale(scale)
                }

                as_decimal.precision = precision;

                Ok(as_decimal)
            }

            fn to_byte_slice(&self) -> Vec<u8> {
                self.digits.to_le_bytes().to_vec()
            }

            fn from_bytes_with_precision_scale(
                bytes: &[u8],
                precision: usize,
                scale: usize,
            ) -> $name {
                let as_array = bytes.try_into();
                match as_array {
                    Ok(v) if bytes.len() == 16 => $name {
                        digits: <$native_ty>::from_le_bytes(v),
                        precision,
                        scale,
                    },
                    Err(e) => panic!(
                        "Unable to load Decimal from bytes slice ({}): {}",
                        bytes.len(),
                        e
                    ),
                    _ => panic!(
                        "Unable to load Decimal from bytes slice with length {}",
                        bytes.len()
                    ),
                }
            }
        }

        impl ToPrimitive for $name {
            fn to_isize(&self) -> Option<isize> {
                unimplemented!("Unimplemented to_isize for {}", stringify!($name))
            }

            fn to_usize(&self) -> Option<usize> {
                unimplemented!("Unimplemented to_usize for {}", stringify!($name))
            }

            fn to_i8(&self) -> Option<i8> {
                Some(self.get_signed_lead_part().try_into().unwrap())
            }

            fn to_i16(&self) -> Option<i16> {
                Some(self.get_signed_lead_part().try_into().unwrap())
            }

            fn to_i32(&self) -> Option<i32> {
                Some(self.get_signed_lead_part().try_into().unwrap())
            }

            fn to_i64(&self) -> Option<i64> {
                Some(self.get_signed_lead_part().try_into().unwrap())
            }

            fn to_i128(&self) -> Option<i128> {
                Some(self.get_signed_lead_part().try_into().unwrap())
            }

            fn to_u8(&self) -> Option<u8> {
                Some(self.get_signed_lead_part().try_into().unwrap())
            }

            fn to_u16(&self) -> Option<u16> {
                Some(self.get_signed_lead_part().try_into().unwrap())
            }

            fn to_u32(&self) -> Option<u32> {
                Some(self.get_signed_lead_part().try_into().unwrap())
            }

            fn to_u64(&self) -> Option<u64> {
                Some(self.get_signed_lead_part().try_into().unwrap())
            }

            fn to_u128(&self) -> Option<u128> {
                Some(self.get_signed_lead_part().try_into().unwrap())
            }

            fn to_f32(&self) -> Option<f32> {
                // @todo Optimize this
                Some(self.to_string().parse::<f32>().unwrap())
            }

            fn to_f64(&self) -> Option<f64> {
                // @todo Optimize this
                Some(self.to_string().parse::<f64>().unwrap())
            }
        }

        impl ToString for $name {
            fn to_string(&self) -> String {
               self.digits.as_string(self.scale)
            }
        }

        impl Default for $name {
            #[inline]
            fn default() -> $name {
                Zero::zero()
            }
        }

        impl Zero for $name {
            #[inline]
            fn zero() -> $name {
                $name::new(<$native_ty>::zero(), 1, 0)
            }

            #[inline]
            fn is_zero(&self) -> bool {
                self.digits.is_zero()
            }
        }

        impl Add<$name> for $name {
            type Output = $name;

            #[inline]
            fn add(self, rhs: $name) -> $name {
                match self.scale.cmp(&rhs.scale) {
                    Ordering::Equal => {
                        $name::new(self.digits + rhs.digits, self.precision, self.scale)
                    }
                    Ordering::Less => self.rescale_to_new(rhs.scale) + rhs,
                    Ordering::Greater => rhs.rescale_to_new(self.scale) + self,
                }
            }
        }

        impl AddAssign<$name> for $name {
            #[inline]
            fn add_assign(&mut self, rhs: $name) {
                match self.scale.cmp(&rhs.scale) {
                    Ordering::Equal => {
                        self.digits += rhs.digits;
                    }
                    Ordering::Less => {
                        self.rescale(rhs.scale);
                        self.digits += rhs.digits;
                    }
                    Ordering::Greater => {
                        self.rescale(self.scale);
                        self.digits += rhs.digits;
                    }
                }
            }
        }

        impl Sub<$name> for $name {
            type Output = $name;

            #[inline]
            fn sub(self, rhs: $name) -> $name {
                match self.scale.cmp(&rhs.scale) {
                    Ordering::Equal => {
                        $name::new(self.digits - rhs.digits, self.precision, self.scale)
                    }
                    Ordering::Less => self.rescale_to_new(rhs.scale) - rhs,
                    Ordering::Greater => rhs.rescale_to_new(self.scale) - self,
                }
            }
        }

        impl SubAssign<$name> for $name {
            #[inline]
            fn sub_assign(&mut self, rhs: $name) {
                match self.scale.cmp(&rhs.scale) {
                    Ordering::Equal => {
                        self.digits -= rhs.digits;
                    }
                    Ordering::Less => {
                        self.rescale(rhs.scale);
                        self.digits -= rhs.digits;
                    }
                    Ordering::Greater => {
                        self.rescale(self.scale);
                        self.digits -= rhs.digits;
                    }
                }
            }
        }

        impl Ord for $name {
            fn cmp(&self, rhs: &Self) -> Ordering {
                match self.scale.cmp(&rhs.scale) {
                    Ordering::Equal => self.digits.cmp(&rhs.digits),
                    Ordering::Less => {
                        self.rescale_to_new(rhs.scale).digits.cmp(&rhs.digits)
                    }
                    Ordering::Greater => {
                        rhs.rescale_to_new(self.scale).digits.cmp(&self.digits)
                    }
                }
            }
        }

        impl PartialOrd for $name {
            fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
                Some(self.cmp(rhs))
            }

            fn lt(&self, rhs: &Self) -> bool {
                match self.scale.cmp(&rhs.scale) {
                    Ordering::Equal => self.digits < rhs.digits,
                    Ordering::Less => self.rescale_to_new(rhs.scale).digits < rhs.digits,
                    Ordering::Greater => {
                        rhs.rescale_to_new(self.scale).digits < self.digits
                    }
                }
            }

            fn le(&self, rhs: &Self) -> bool {
                match self.scale.cmp(&rhs.scale) {
                    Ordering::Equal => self.digits <= rhs.digits,
                    Ordering::Less => self.rescale_to_new(rhs.scale).digits <= rhs.digits,
                    Ordering::Greater => {
                        rhs.rescale_to_new(self.scale).digits <= self.digits
                    }
                }
            }

            fn gt(&self, rhs: &Self) -> bool {
                match self.scale.cmp(&rhs.scale) {
                    Ordering::Equal => self.digits > rhs.digits,
                    Ordering::Less => self.rescale_to_new(rhs.scale).digits > rhs.digits,
                    Ordering::Greater => {
                        rhs.rescale_to_new(self.scale).digits > self.digits
                    }
                }
            }

            fn ge(&self, rhs: &Self) -> bool {
                match self.scale.cmp(&rhs.scale) {
                    Ordering::Equal => self.digits >= rhs.digits,
                    Ordering::Less => self.rescale_to_new(rhs.scale).digits >= rhs.digits,
                    Ordering::Greater => {
                        rhs.rescale_to_new(self.scale).digits >= self.digits
                    }
                }
            }
        }

        impl JsonSerializable for $name {
            fn into_json_value(self) -> Option<Value> {
                unimplemented!("Unimplemented JsonSerializable::into_json_value for {}", stringify!($name))
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    f,
                    "Decimal<{}, {}>(\"{}\")",
                    self.precision,
                    self.scale,
                    self.to_string()
                )
            }
        }

        // To fix clippy you are deriving `Hash` but have implemented `PartialEq` explicitly
        impl Hash for $name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.digits.hash(state);
                self.precision.hash(state);
                self.scale.hash(state);
            }

            fn hash_slice<H: Hasher>(_data: &[Self], _state: &mut H) where
                Self: Sized, {
                unimplemented!("Unimplemented hash_slice for {}", stringify!($name))
            }
        }

        impl PartialEq<$name> for $name {
            #[inline]
            fn eq(&self, rhs: &$name) -> bool {
                // @todo What is a correct behaviour for it? Rescaling?
                self.digits == rhs.digits
            }
        }

        impl FromStr for $name {
            type Err = ParseDecimalError;

            fn from_str(s: &str) -> Result<Self, ParseDecimalError> {
                let (digits, precision, scale) = match s.find('.') {
                    // Decimal with empty scale
                    None => {
                        let digits = s.parse::<$native_ty>()?;

                        if digits.le(&<$native_ty>::zero()) {
                            (s.parse::<$native_ty>()?, s.len() - 1, 0)
                        } else {
                            (s.parse::<$native_ty>()?, s.len(), 0)
                        }
                    }
                    Some(loc) => {
                        let (lead, trail) = (&s[..loc], &s[loc + 1..]);

                        // Concat both parts to make bigint from int
                        let mut parts = String::from(lead);
                        parts.push_str(trail);

                        let digits = parts.parse::<$native_ty>()?;

                        if digits.lt(&<$native_ty>::zero()) {
                            (digits, lead.len() - 1, trail.len())
                        } else {
                            (digits, lead.len(), trail.len())
                        }
                    }
                };

                Ok($name::new(digits, precision, scale))
            }
        }
    };
}

impl JsonSerializable for i128 {
    fn into_json_value(self) -> Option<Value> {
        unimplemented!("into_json_value")
    }
}

impl ArrowNativeType for i128 {
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

unsafe impl NativeType for i128 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn is_valid(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Decimal128(_, _))
    }

    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }
}

pub trait ArrowDecimalNativeType: ArrowNativeType {
    fn pow_to(mul: u32) -> Self;

    fn as_string(self, scale: usize) -> String;
}

impl ArrowDecimalNativeType for i128 {
    fn pow_to(mul: u32) -> i128 {
        10_u64.pow(mul) as i128
    }

    #[inline]
    fn as_string(self, scale: usize) -> String {
        // Skip sign, because we split string to lead, trail
        let as_str = self.abs().to_string();
        let len = as_str.len();

        let (lead, trail) = (&as_str[..(len - scale)], &as_str[(len - scale)..]);

        let mut result = String::new();

        if self.lt(&i128::zero()) {
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

make_type!(Decimal128Type, i128, 38, 16);

make_type!(Decimal256Type, i256, 80, 32);

impl DecimalCast for Decimal128Type {
    fn from<T: ToPrimitive>(n: T, p: usize, s: usize) -> Option<Self> {
        Some(Decimal128Type::from_f64(n.to_f64().unwrap(), p, s).unwrap())
    }
}

impl DecimalCast for Decimal256Type {
    fn from<T: ToPrimitive>(n: T, p: usize, s: usize) -> Option<Self> {
        Some(Decimal256Type::from_f64(n.to_f64().unwrap(), p, s).unwrap())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn decimal_test_from_and_to_str() {
        let values = vec![
            // None
            ("0", Decimal128Type::new(0_i128, 1, 0)),
            // Positive
            ("1.0", Decimal128Type::new(10_i128, 1, 1)),
            ("1.001", Decimal128Type::new(1001_i128, 1, 3)),
            ("1", Decimal128Type::new(1_i128, 1, 0)),
            ("0.98", Decimal128Type::new(98_i128, 1, 2)),
            // Negative
            ("-1.0", Decimal128Type::new(-10_i128, 1, 1)),
            ("-1.001", Decimal128Type::new(-1001_i128, 1, 3)),
            ("-1", Decimal128Type::new(-1_i128, 1, 0)),
            //
            (
                "1.000000000001",
                Decimal128Type::new(1_000_000_000_001_i128, 1, 12),
            ),
            (
                "5.000000000005",
                Decimal128Type::new(5_000_000_000_005_i128, 1, 12),
            ),
        ];

        for (source, expected) in values {
            let actual = Decimal128Type::from_str(source).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(actual.to_string(), source);
        }
    }

    #[test]
    fn decimal_test_partial_eq() {
        let values = vec![
            // Eq scale
            (
                Decimal128Type::new(0_i128, 1, 0),
                Decimal128Type::new(0_i128, 1, 0),
                true,
            ),
            (
                Decimal128Type::new(0_i128, 1, 0),
                Decimal128Type::new(1_i128, 1, 0),
                false,
            ),
            // Scaling is disabled in PartialEq, probably we will use it, but I dont know for now
            // What is a correct behaviour
            // > scale
            // (
            //     Decimal128Type::new(10_i128, 1, 1),
            //     Decimal128Type::new(1_i128, 1, 0),
            //     true,
            // ),
            // (
            //     Decimal128Type::new(20_i128, 1, 1),
            //     Decimal128Type::new(1_i128, 1, 0),
            //     false,
            // ),
            // // < scale
            // (
            //     Decimal128Type::new(1_i128, 1, 0),
            //     Decimal128Type::new(10_i128, 1, 1),
            //     true,
            // ),
            // (
            //     Decimal128Type::new(1_i128, 1, 0),
            //     Decimal128Type::new(20_i128, 1, 1),
            //     false,
            // ),
        ];

        for (left, right, expected) in values {
            assert_eq!(
                left == right,
                expected,
                "{} == {}, expected {}",
                left.to_string(),
                right.to_string(),
                expected
            );
        }
    }

    #[test]
    fn decimal_test_add() {
        let values = vec![
            // without rescaling
            (
                Decimal128Type::new(0, 2, 0),
                Decimal128Type::new(5, 2, 0),
                Decimal128Type::new(5, 2, 0),
            ),
            (
                Decimal128Type::new(5, 2, 0),
                Decimal128Type::new(5, 2, 0),
                Decimal128Type::new(10, 2, 0),
            ),
            // with rescaling left
            (
                Decimal128Type::new(1, 1, 0),
                Decimal128Type::new(101, 1, 2),
                Decimal128Type::new(201, 1, 2),
            ),
            // with rescaling right
            (
                Decimal128Type::new(101, 1, 2),
                Decimal128Type::new(1, 1, 0),
                Decimal128Type::new(201, 1, 2),
            ),
        ];

        for (left, right, result) in values {
            assert_eq!(
                left + right,
                result,
                "{} + {} = {}",
                left.to_string(),
                right.to_string(),
                result.to_string()
            );
        }
    }

    #[test]
    fn decimal_test_sub() {
        let values = vec![
            // without rescaling
            (
                Decimal128Type::new(10, 2, 0),
                Decimal128Type::new(5, 2, 0),
                Decimal128Type::new(5, 2, 0),
            ),
            (
                Decimal128Type::new(5, 2, 0),
                Decimal128Type::new(5, 2, 0),
                Decimal128Type::new(0, 2, 0),
            ),
            // with rescaling left
            (
                Decimal128Type::new(2, 1, 0),
                Decimal128Type::new(101, 1, 2),
                Decimal128Type::new(99, 1, 2),
            ),
        ];

        for (left, right, result) in values {
            assert_eq!(
                left - right,
                result,
                "{} - {} = {}",
                left.to_string(),
                right.to_string(),
                result.to_string()
            );
        }
    }

    #[test]
    fn decimal_test_cmp() {
        let values = vec![
            (
                Decimal128Type::new(0_i128, 1, 0),
                Decimal128Type::new(0_i128, 1, 0),
                Ordering::Equal,
            ),
            (
                Decimal128Type::new(1_i128, 1, 0),
                Decimal128Type::new(0_i128, 1, 0),
                Ordering::Greater,
            ),
            (
                Decimal128Type::new(0_i128, 1, 0),
                Decimal128Type::new(1_i128, 1, 0),
                Ordering::Less,
            ),
        ];

        for (left, right, expected) in values {
            assert_eq!(left.cmp(&right), expected);
        }
    }

    #[test]
    fn decimal_test_cmp_lt() {
        let values = vec![
            (
                Decimal128Type::new(0_i128, 1, 0),
                Decimal128Type::new(1_i128, 1, 0),
                true,
            ),
            (
                Decimal128Type::new(1_i128, 1, 0),
                Decimal128Type::new(1_i128, 1, 0),
                false,
            ),
        ];

        for (left, right, expected) in values {
            assert_eq!(left < right, expected);
        }
    }

    #[test]
    fn decimal_test_cmp_le() {
        let values = vec![
            (
                Decimal128Type::new(0_i128, 1, 0),
                Decimal128Type::new(1_i128, 1, 0),
                true,
            ),
            (
                Decimal128Type::new(1_i128, 1, 0),
                Decimal128Type::new(1_i128, 1, 0),
                true,
            ),
            (
                Decimal128Type::new(2_i128, 1, 0),
                Decimal128Type::new(1_i128, 1, 0),
                false,
            ),
        ];

        for (left, right, expected) in values {
            assert_eq!(left <= right, expected);
        }
    }

    #[test]
    fn decimal_test_cmp_gt() {
        let values = vec![
            (
                Decimal128Type::new(1_i128, 1, 0),
                Decimal128Type::new(0_i128, 1, 0),
                true,
            ),
            (
                Decimal128Type::new(0_i128, 1, 0),
                Decimal128Type::new(1_i128, 1, 0),
                false,
            ),
        ];

        for (left, right, expected) in values {
            assert_eq!(left > right, expected);
        }
    }

    #[test]
    fn decimal_test_cmp_ge() {
        let values = vec![
            (
                Decimal128Type::new(1_i128, 1, 0),
                Decimal128Type::new(0_i128, 1, 0),
                true,
            ),
            (
                Decimal128Type::new(1_i128, 1, 0),
                Decimal128Type::new(1_i128, 1, 0),
                true,
            ),
            (
                Decimal128Type::new(0_i128, 1, 0),
                Decimal128Type::new(1_i128, 1, 0),
                false,
            ),
        ];

        for (left, right, expected) in values {
            assert_eq!(left >= right, expected);
        }
    }

    #[test]
    fn decimal_test_from_bytes_and_to_le_bytes() {
        let actual = Decimal128Type::from_bytes_with_precision_scale(
            &[192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            6,
            0,
        );
        assert_eq!(actual, Decimal128Type::new(8_887_000_000_i128, 6, 0));
        assert_eq!(
            actual.to_byte_slice(),
            [192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }
}
