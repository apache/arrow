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

#[cfg(feature = "simd")]
use packed_simd::*;
#[cfg(feature = "simd")]
use std::ops::{Add, BitAnd, BitAndAssign, BitOr, BitOrAssign, Div, Mul, Neg, Not, Sub};

use super::*;

/// A subtype of primitive type that represents numeric values.
///
/// SIMD operations are defined in this trait if available on the target system.
#[cfg(simd)]
pub trait ArrowNumericType: ArrowPrimitiveType
where
    Self::Simd: Add<Output = Self::Simd>
        + Sub<Output = Self::Simd>
        + Mul<Output = Self::Simd>
        + Div<Output = Self::Simd>
        + Copy,
    Self::SimdMask: BitAnd<Output = Self::SimdMask>
        + BitOr<Output = Self::SimdMask>
        + BitAndAssign
        + BitOrAssign
        + Not<Output = Self::SimdMask>
        + Copy,
{
    /// Defines the SIMD type that should be used for this numeric type
    type Simd;

    /// Defines the SIMD Mask type that should be used for this numeric type
    type SimdMask;

    /// The number of SIMD lanes available
    fn lanes() -> usize;

    /// Initializes a SIMD register to a constant value
    fn init(value: Self::Native) -> Self::Simd;

    /// Loads a slice into a SIMD register
    fn load(slice: &[Self::Native]) -> Self::Simd;

    /// Creates a new SIMD mask for this SIMD type filling it with `value`
    fn mask_init(value: bool) -> Self::SimdMask;

    /// Creates a new SIMD mask for this SIMD type from the lower-most bits of the given `mask`.
    /// The number of bits used corresponds to the number of lanes of this type
    fn mask_from_u64(mask: u64) -> Self::SimdMask;

    /// Creates a bitmask from the given SIMD mask.
    /// Each bit corresponds to one vector lane, starting with the least-significant bit.
    fn mask_to_u64(mask: &Self::SimdMask) -> u64;

    /// Gets the value of a single lane in a SIMD mask
    fn mask_get(mask: &Self::SimdMask, idx: usize) -> bool;

    /// Sets the value of a single lane of a SIMD mask
    fn mask_set(mask: Self::SimdMask, idx: usize, value: bool) -> Self::SimdMask;

    /// Selects elements of `a` and `b` using `mask`
    fn mask_select(mask: Self::SimdMask, a: Self::Simd, b: Self::Simd) -> Self::Simd;

    /// Returns `true` if any of the lanes in the mask are `true`
    fn mask_any(mask: Self::SimdMask) -> bool;

    /// Performs a SIMD binary operation
    fn bin_op<F: Fn(Self::Simd, Self::Simd) -> Self::Simd>(
        left: Self::Simd,
        right: Self::Simd,
        op: F,
    ) -> Self::Simd;

    /// SIMD version of equal
    fn eq(left: Self::Simd, right: Self::Simd) -> Self::SimdMask;

    /// SIMD version of not equal
    fn ne(left: Self::Simd, right: Self::Simd) -> Self::SimdMask;

    /// SIMD version of less than
    fn lt(left: Self::Simd, right: Self::Simd) -> Self::SimdMask;

    /// SIMD version of less than or equal to
    fn le(left: Self::Simd, right: Self::Simd) -> Self::SimdMask;

    /// SIMD version of greater than
    fn gt(left: Self::Simd, right: Self::Simd) -> Self::SimdMask;

    /// SIMD version of greater than or equal to
    fn ge(left: Self::Simd, right: Self::Simd) -> Self::SimdMask;

    /// Writes a SIMD result back to a slice
    fn write(simd_result: Self::Simd, slice: &mut [Self::Native]);

    fn unary_op<F: Fn(Self::Simd) -> Self::Simd>(a: Self::Simd, op: F) -> Self::Simd;
}

#[cfg(not(simd))]
pub trait ArrowNumericType: ArrowPrimitiveType {}

macro_rules! make_numeric_type {
    ($impl_ty:ty, $native_ty:ty, $simd_ty:ident, $simd_mask_ty:ident) => {
        #[cfg(simd)]
        impl ArrowNumericType for $impl_ty {
            type Simd = $simd_ty;

            type SimdMask = $simd_mask_ty;

            #[inline]
            fn lanes() -> usize {
                Self::Simd::lanes()
            }

            #[inline]
            fn init(value: Self::Native) -> Self::Simd {
                Self::Simd::splat(value)
            }

            #[inline]
            fn load(slice: &[Self::Native]) -> Self::Simd {
                unsafe { Self::Simd::from_slice_unaligned_unchecked(slice) }
            }

            #[inline]
            fn mask_init(value: bool) -> Self::SimdMask {
                Self::SimdMask::splat(value)
            }

            #[inline]
            fn mask_from_u64(mask: u64) -> Self::SimdMask {
                // this match will get removed by the compiler since the number of lanes is known at
                // compile-time for each concrete numeric type
                match Self::lanes() {
                    8 => {
                        // the bit position in each lane indicates the index of that lane
                        let vecidx = i64x8::new(1, 2, 4, 8, 16, 32, 64, 128);

                        // broadcast the lowermost 8 bits of mask to each lane
                        let vecmask = i64x8::splat((mask & 0xFF) as i64);
                        // compute whether the bit corresponding to each lanes index is set
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        // transmute is necessary because the different match arms return different
                        // mask types, at runtime only one of those expressions will exist per type,
                        // with the type being equal to `SimdMask`.
                        unsafe { std::mem::transmute(vecmask) }
                    }
                    16 => {
                        // same general logic as for 8 lanes, extended to 16 bits
                        let vecidx = i32x16::new(
                            1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096,
                            8192, 16384, 32768,
                        );

                        let vecmask = i32x16::splat((mask & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        unsafe { std::mem::transmute(vecmask) }
                    }
                    32 => {
                        // compute two separate m32x16 vector masks from  from the lower-most 32 bits of `mask`
                        // and then combine them into one m16x32 vector mask by writing and reading a temporary
                        let tmp = &mut [0_i16; 32];

                        let vecidx = i32x16::new(
                            1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096,
                            8192, 16384, 32768,
                        );

                        let vecmask = i32x16::splat((mask & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        i16x16::from_cast(vecmask)
                            .write_to_slice_unaligned(&mut tmp[0..16]);

                        let vecmask = i32x16::splat(((mask >> 16) & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        i16x16::from_cast(vecmask)
                            .write_to_slice_unaligned(&mut tmp[16..32]);

                        unsafe { std::mem::transmute(i16x32::from_slice_unaligned(tmp)) }
                    }
                    64 => {
                        // compute four m32x16 vector masks from  from all 64 bits of `mask`
                        // and convert them into one m8x64 vector mask by writing and reading a temporary
                        let tmp = &mut [0_i8; 64];

                        let vecidx = i32x16::new(
                            1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096,
                            8192, 16384, 32768,
                        );

                        let vecmask = i32x16::splat((mask & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        i8x16::from_cast(vecmask)
                            .write_to_slice_unaligned(&mut tmp[0..16]);

                        let vecmask = i32x16::splat(((mask >> 16) & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        i8x16::from_cast(vecmask)
                            .write_to_slice_unaligned(&mut tmp[16..32]);

                        let vecmask = i32x16::splat(((mask >> 32) & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        i8x16::from_cast(vecmask)
                            .write_to_slice_unaligned(&mut tmp[32..48]);

                        let vecmask = i32x16::splat(((mask >> 48) & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        i8x16::from_cast(vecmask)
                            .write_to_slice_unaligned(&mut tmp[48..64]);

                        unsafe { std::mem::transmute(i8x64::from_slice_unaligned(tmp)) }
                    }
                    _ => panic!("Invalid number of vector lanes"),
                }
            }

            #[inline]
            fn mask_to_u64(mask: &Self::SimdMask) -> u64 {
                mask.bitmask() as u64
            }

            #[inline]
            fn mask_get(mask: &Self::SimdMask, idx: usize) -> bool {
                unsafe { mask.extract_unchecked(idx) }
            }

            #[inline]
            fn mask_set(mask: Self::SimdMask, idx: usize, value: bool) -> Self::SimdMask {
                unsafe { mask.replace_unchecked(idx, value) }
            }

            /// Selects elements of `a` and `b` using `mask`
            #[inline]
            fn mask_select(
                mask: Self::SimdMask,
                a: Self::Simd,
                b: Self::Simd,
            ) -> Self::Simd {
                mask.select(a, b)
            }

            #[inline]
            fn mask_any(mask: Self::SimdMask) -> bool {
                mask.any()
            }

            #[inline]
            fn bin_op<F: Fn(Self::Simd, Self::Simd) -> Self::Simd>(
                left: Self::Simd,
                right: Self::Simd,
                op: F,
            ) -> Self::Simd {
                op(left, right)
            }

            #[inline]
            fn eq(left: Self::Simd, right: Self::Simd) -> Self::SimdMask {
                left.eq(right)
            }

            #[inline]
            fn ne(left: Self::Simd, right: Self::Simd) -> Self::SimdMask {
                left.ne(right)
            }

            #[inline]
            fn lt(left: Self::Simd, right: Self::Simd) -> Self::SimdMask {
                left.lt(right)
            }

            #[inline]
            fn le(left: Self::Simd, right: Self::Simd) -> Self::SimdMask {
                left.le(right)
            }

            #[inline]
            fn gt(left: Self::Simd, right: Self::Simd) -> Self::SimdMask {
                left.gt(right)
            }

            #[inline]
            fn ge(left: Self::Simd, right: Self::Simd) -> Self::SimdMask {
                left.ge(right)
            }

            #[inline]
            fn write(simd_result: Self::Simd, slice: &mut [Self::Native]) {
                unsafe { simd_result.write_to_slice_unaligned_unchecked(slice) };
            }

            #[inline]
            fn unary_op<F: Fn(Self::Simd) -> Self::Simd>(
                a: Self::Simd,
                op: F,
            ) -> Self::Simd {
                op(a)
            }
        }

        #[cfg(not(simd))]
        impl ArrowNumericType for $impl_ty {}
    };
}

make_numeric_type!(Int8Type, i8, i8x64, m8x64);
make_numeric_type!(Int16Type, i16, i16x32, m16x32);
make_numeric_type!(Int32Type, i32, i32x16, m32x16);
make_numeric_type!(Int64Type, i64, i64x8, m64x8);
make_numeric_type!(UInt8Type, u8, u8x64, m8x64);
make_numeric_type!(UInt16Type, u16, u16x32, m16x32);
make_numeric_type!(UInt32Type, u32, u32x16, m32x16);
make_numeric_type!(UInt64Type, u64, u64x8, m64x8);
make_numeric_type!(Float32Type, f32, f32x16, m32x16);
make_numeric_type!(Float64Type, f64, f64x8, m64x8);

make_numeric_type!(TimestampSecondType, i64, i64x8, m64x8);
make_numeric_type!(TimestampMillisecondType, i64, i64x8, m64x8);
make_numeric_type!(TimestampMicrosecondType, i64, i64x8, m64x8);
make_numeric_type!(TimestampNanosecondType, i64, i64x8, m64x8);
make_numeric_type!(Date32Type, i32, i32x16, m32x16);
make_numeric_type!(Date64Type, i64, i64x8, m64x8);
make_numeric_type!(Time32SecondType, i32, i32x16, m32x16);
make_numeric_type!(Time32MillisecondType, i32, i32x16, m32x16);
make_numeric_type!(Time64MicrosecondType, i64, i64x8, m64x8);
make_numeric_type!(Time64NanosecondType, i64, i64x8, m64x8);
make_numeric_type!(IntervalYearMonthType, i32, i32x16, m32x16);
make_numeric_type!(IntervalDayTimeType, i64, i64x8, m64x8);
make_numeric_type!(DurationSecondType, i64, i64x8, m64x8);
make_numeric_type!(DurationMillisecondType, i64, i64x8, m64x8);
make_numeric_type!(DurationMicrosecondType, i64, i64x8, m64x8);
make_numeric_type!(DurationNanosecondType, i64, i64x8, m64x8);

/// A subtype of primitive type that represents signed numeric values.
///
/// SIMD operations are defined in this trait if available on the target system.
#[cfg(simd)]
pub trait ArrowSignedNumericType: ArrowNumericType
where
    Self::SignedSimd: Neg<Output = Self::SignedSimd>,
{
    /// Defines the SIMD type that should be used for this numeric type
    type SignedSimd;

    /// Loads a slice of signed numeric type into a SIMD register
    fn load_signed(slice: &[Self::Native]) -> Self::SignedSimd;

    /// Performs a SIMD unary operation on signed numeric type
    fn signed_unary_op<F: Fn(Self::SignedSimd) -> Self::SignedSimd>(
        a: Self::SignedSimd,
        op: F,
    ) -> Self::SignedSimd;

    /// Writes a signed SIMD result back to a slice
    fn write_signed(simd_result: Self::SignedSimd, slice: &mut [Self::Native]);
}

#[cfg(not(simd))]
pub trait ArrowSignedNumericType: ArrowNumericType
where
    Self::Native: std::ops::Neg<Output = Self::Native>,
{
}

macro_rules! make_signed_numeric_type {
    ($impl_ty:ty, $simd_ty:ident) => {
        #[cfg(simd)]
        impl ArrowSignedNumericType for $impl_ty {
            type SignedSimd = $simd_ty;

            #[inline]
            fn load_signed(slice: &[Self::Native]) -> Self::SignedSimd {
                unsafe { Self::SignedSimd::from_slice_unaligned_unchecked(slice) }
            }

            #[inline]
            fn signed_unary_op<F: Fn(Self::SignedSimd) -> Self::SignedSimd>(
                a: Self::SignedSimd,
                op: F,
            ) -> Self::SignedSimd {
                op(a)
            }

            #[inline]
            fn write_signed(simd_result: Self::SignedSimd, slice: &mut [Self::Native]) {
                unsafe { simd_result.write_to_slice_unaligned_unchecked(slice) };
            }
        }

        #[cfg(not(simd))]
        impl ArrowSignedNumericType for $impl_ty {}
    };
}

make_signed_numeric_type!(Int8Type, i8x64);
make_signed_numeric_type!(Int16Type, i16x32);
make_signed_numeric_type!(Int32Type, i32x16);
make_signed_numeric_type!(Int64Type, i64x8);
make_signed_numeric_type!(Float32Type, f32x16);
make_signed_numeric_type!(Float64Type, f64x8);

#[cfg(simd)]
pub trait ArrowFloatNumericType: ArrowNumericType {
    fn pow(base: Self::Simd, raise: Self::Simd) -> Self::Simd;
}

#[cfg(not(simd))]
pub trait ArrowFloatNumericType: ArrowNumericType {}

macro_rules! make_float_numeric_type {
    ($impl_ty:ty, $simd_ty:ident) => {
        #[cfg(simd)]
        impl ArrowFloatNumericType for $impl_ty {
            #[inline]
            fn pow(base: Self::Simd, raise: Self::Simd) -> Self::Simd {
                base.powf(raise)
            }
        }

        #[cfg(not(simd))]
        impl ArrowFloatNumericType for $impl_ty {}
    };
}

make_float_numeric_type!(Float32Type, f32x16);
make_float_numeric_type!(Float64Type, f64x8);

#[cfg(all(test, simd_x86))]
mod tests {
    use crate::datatypes::{
        ArrowNumericType, Float32Type, Float64Type, Int32Type, Int64Type, Int8Type,
        UInt16Type,
    };
    use packed_simd::*;
    use FromCast;

    /// calculate the expected mask by iterating over all bits
    macro_rules! expected_mask {
        ($T:ty, $MASK:expr) => {{
            let mask = $MASK;
            // simd width of all types is currently 64 bytes -> 512 bits
            let lanes = 64 / std::mem::size_of::<$T>();
            // translate each set bit into a value of all ones (-1) of the correct type
            (0..lanes)
                .map(|i| (if (mask & (1 << i)) != 0 { -1 } else { 0 }))
                .collect::<Vec<$T>>()
        }};
    }

    #[test]
    fn test_mask_f64() {
        let mask = 0b10101010;
        let actual = Float64Type::mask_from_u64(mask);
        let expected = expected_mask!(i64, mask);
        let expected = m64x8::from_cast(i64x8::from_slice_unaligned(expected.as_slice()));

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_mask_u64() {
        let mask = 0b01010101;
        let actual = Int64Type::mask_from_u64(mask);
        let expected = expected_mask!(i64, mask);
        let expected = m64x8::from_cast(i64x8::from_slice_unaligned(expected.as_slice()));

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_mask_f32() {
        let mask = 0b10101010_10101010;
        let actual = Float32Type::mask_from_u64(mask);
        let expected = expected_mask!(i32, mask);
        let expected =
            m32x16::from_cast(i32x16::from_slice_unaligned(expected.as_slice()));

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_mask_i32() {
        let mask = 0b01010101_01010101;
        let actual = Int32Type::mask_from_u64(mask);
        let expected = expected_mask!(i32, mask);
        let expected =
            m32x16::from_cast(i32x16::from_slice_unaligned(expected.as_slice()));

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_mask_u16() {
        let mask = 0b01010101_01010101_10101010_10101010;
        let actual = UInt16Type::mask_from_u64(mask);
        let expected = expected_mask!(i16, mask);
        dbg!(&expected);
        let expected =
            m16x32::from_cast(i16x32::from_slice_unaligned(expected.as_slice()));

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_mask_i8() {
        let mask =
            0b01010101_01010101_10101010_10101010_01010101_01010101_10101010_10101010;
        let actual = Int8Type::mask_from_u64(mask);
        let expected = expected_mask!(i8, mask);
        let expected = m8x64::from_cast(i8x64::from_slice_unaligned(expected.as_slice()));

        assert_eq!(expected, actual);
    }
}
