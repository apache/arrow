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

//! Utils for working with bits

#[cfg(feature = "simd")]
use packed_simd::u8x64;

const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
const UNSET_BIT_MASK: [u8; 8] = [
    255 - 1,
    255 - 2,
    255 - 4,
    255 - 8,
    255 - 16,
    255 - 32,
    255 - 64,
    255 - 128,
];

/// Returns the nearest number that is `>=` than `num` and is a multiple of 64
#[inline]
pub fn round_upto_multiple_of_64(num: usize) -> usize {
    round_upto_power_of_2(num, 64)
}

/// Returns the nearest multiple of `factor` that is `>=` than `num`. Here `factor` must
/// be a power of 2.
pub fn round_upto_power_of_2(num: usize, factor: usize) -> usize {
    debug_assert!(factor > 0 && (factor & (factor - 1)) == 0);
    (num + (factor - 1)) & !(factor - 1)
}

/// Returns whether bit at position `i` in `data` is set or not
#[inline]
pub fn get_bit(data: &[u8], i: usize) -> bool {
    (data[i >> 3] & BIT_MASK[i & 7]) != 0
}

/// Returns whether bit at position `i` in `data` is set or not.
///
/// # Safety
///
/// Note this doesn't do any bound checking, for performance reason. The caller is
/// responsible to guarantee that `i` is within bounds.
#[inline]
pub unsafe fn get_bit_raw(data: *const u8, i: usize) -> bool {
    (*data.add(i >> 3) & BIT_MASK[i & 7]) != 0
}

/// Sets bit at position `i` for `data`
#[inline]
pub fn set_bit(data: &mut [u8], i: usize) {
    data[i >> 3] |= BIT_MASK[i & 7];
}

/// Sets bit at position `i` for `data`
///
/// # Safety
///
/// Note this doesn't do any bound checking, for performance reason. The caller is
/// responsible to guarantee that `i` is within bounds.
#[inline]
pub unsafe fn set_bit_raw(data: *mut u8, i: usize) {
    *data.add(i >> 3) |= BIT_MASK[i & 7];
}

/// Sets bit at position `i` for `data` to 0
#[inline]
pub fn unset_bit(data: &mut [u8], i: usize) {
    data[i >> 3] &= UNSET_BIT_MASK[i & 7];
}

/// Sets bit at position `i` for `data` to 0
///
/// # Safety
///
/// Note this doesn't do any bound checking, for performance reason. The caller is
/// responsible to guarantee that `i` is within bounds.
#[inline]
pub unsafe fn unset_bit_raw(data: *mut u8, i: usize) {
    *data.add(i >> 3) &= UNSET_BIT_MASK[i & 7];
}

/// Returns the ceil of `value`/`divisor`
#[inline]
pub fn ceil(value: usize, divisor: usize) -> usize {
    let (quot, rem) = (value / divisor, value % divisor);
    if rem > 0 && divisor > 0 {
        quot + 1
    } else {
        quot
    }
}

/// Performs SIMD bitwise binary operations.
///
/// # Safety
///
/// Note that each slice should be 64 bytes and it is the callers responsibility to ensure
/// that this is the case.  If passed slices larger than 64 bytes the operation will only
/// be performed on the first 64 bytes.  Slices less than 64 bytes will panic.
#[cfg(simd)]
pub unsafe fn bitwise_bin_op_simd<F>(left: &[u8], right: &[u8], result: &mut [u8], op: F)
where
    F: Fn(u8x64, u8x64) -> u8x64,
{
    let left_simd = u8x64::from_slice_unaligned_unchecked(left);
    let right_simd = u8x64::from_slice_unaligned_unchecked(right);
    let simd_result = op(left_simd, right_simd);
    simd_result.write_to_slice_unaligned_unchecked(result);
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::util::test_util::seedable_rng;
    use rand::Rng;

    #[test]
    fn test_round_upto_multiple_of_64() {
        assert_eq!(0, round_upto_multiple_of_64(0));
        assert_eq!(64, round_upto_multiple_of_64(1));
        assert_eq!(64, round_upto_multiple_of_64(63));
        assert_eq!(64, round_upto_multiple_of_64(64));
        assert_eq!(128, round_upto_multiple_of_64(65));
        assert_eq!(192, round_upto_multiple_of_64(129));
    }

    #[test]
    fn test_get_bit() {
        // 00001101
        assert_eq!(true, get_bit(&[0b00001101], 0));
        assert_eq!(false, get_bit(&[0b00001101], 1));
        assert_eq!(true, get_bit(&[0b00001101], 2));
        assert_eq!(true, get_bit(&[0b00001101], 3));

        // 01001001 01010010
        assert_eq!(true, get_bit(&[0b01001001, 0b01010010], 0));
        assert_eq!(false, get_bit(&[0b01001001, 0b01010010], 1));
        assert_eq!(false, get_bit(&[0b01001001, 0b01010010], 2));
        assert_eq!(true, get_bit(&[0b01001001, 0b01010010], 3));
        assert_eq!(false, get_bit(&[0b01001001, 0b01010010], 4));
        assert_eq!(false, get_bit(&[0b01001001, 0b01010010], 5));
        assert_eq!(true, get_bit(&[0b01001001, 0b01010010], 6));
        assert_eq!(false, get_bit(&[0b01001001, 0b01010010], 7));
        assert_eq!(false, get_bit(&[0b01001001, 0b01010010], 8));
        assert_eq!(true, get_bit(&[0b01001001, 0b01010010], 9));
        assert_eq!(false, get_bit(&[0b01001001, 0b01010010], 10));
        assert_eq!(false, get_bit(&[0b01001001, 0b01010010], 11));
        assert_eq!(true, get_bit(&[0b01001001, 0b01010010], 12));
        assert_eq!(false, get_bit(&[0b01001001, 0b01010010], 13));
        assert_eq!(true, get_bit(&[0b01001001, 0b01010010], 14));
        assert_eq!(false, get_bit(&[0b01001001, 0b01010010], 15));
    }

    #[test]
    fn test_get_bit_raw() {
        const NUM_BYTE: usize = 10;
        let mut buf = vec![0; NUM_BYTE];
        let mut expected = vec![];
        let mut rng = seedable_rng();
        for i in 0..8 * NUM_BYTE {
            let b = rng.gen_bool(0.5);
            expected.push(b);
            if b {
                set_bit(&mut buf[..], i)
            }
        }

        let raw_ptr = buf.as_ptr();
        for (i, b) in expected.iter().enumerate() {
            unsafe {
                assert_eq!(*b, get_bit_raw(raw_ptr, i));
            }
        }
    }

    #[test]
    fn test_set_bit() {
        let mut b = [0b00000010];
        set_bit(&mut b, 0);
        assert_eq!([0b00000011], b);
        set_bit(&mut b, 1);
        assert_eq!([0b00000011], b);
        set_bit(&mut b, 7);
        assert_eq!([0b10000011], b);
    }

    #[test]
    fn test_unset_bit() {
        let mut b = [0b11111101];
        unset_bit(&mut b, 0);
        assert_eq!([0b11111100], b);
        unset_bit(&mut b, 1);
        assert_eq!([0b11111100], b);
        unset_bit(&mut b, 7);
        assert_eq!([0b01111100], b);
    }

    #[test]
    fn test_set_bit_raw() {
        const NUM_BYTE: usize = 10;
        let mut buf = vec![0; NUM_BYTE];
        let mut expected = vec![];
        let mut rng = seedable_rng();
        for i in 0..8 * NUM_BYTE {
            let b = rng.gen_bool(0.5);
            expected.push(b);
            if b {
                unsafe {
                    set_bit_raw(buf.as_mut_ptr(), i);
                }
            }
        }

        let raw_ptr = buf.as_ptr();
        for (i, b) in expected.iter().enumerate() {
            unsafe {
                assert_eq!(*b, get_bit_raw(raw_ptr, i));
            }
        }
    }

    #[test]
    fn test_unset_bit_raw() {
        const NUM_BYTE: usize = 10;
        let mut buf = vec![255; NUM_BYTE];
        let mut expected = vec![];
        let mut rng = seedable_rng();
        for i in 0..8 * NUM_BYTE {
            let b = rng.gen_bool(0.5);
            expected.push(b);
            if !b {
                unsafe {
                    unset_bit_raw(buf.as_mut_ptr(), i);
                }
            }
        }

        let raw_ptr = buf.as_ptr();
        for (i, b) in expected.iter().enumerate() {
            unsafe {
                assert_eq!(*b, get_bit_raw(raw_ptr, i));
            }
        }
    }

    #[test]
    fn test_get_set_bit_roundtrip() {
        const NUM_BYTES: usize = 10;
        const NUM_SETS: usize = 10;

        let mut buffer: [u8; NUM_BYTES * 8] = [0; NUM_BYTES * 8];
        let mut v = HashSet::new();
        let mut rng = seedable_rng();
        for _ in 0..NUM_SETS {
            let offset = rng.gen_range(0, 8 * NUM_BYTES);
            v.insert(offset);
            set_bit(&mut buffer[..], offset);
        }
        for i in 0..NUM_BYTES * 8 {
            assert_eq!(v.contains(&i), get_bit(&buffer[..], i));
        }
    }

    #[test]
    #[cfg(all(any(target_arch = "x86", target_arch = "x86_64")))]
    fn test_ceil() {
        assert_eq!(ceil(0, 1), 0);
        assert_eq!(ceil(1, 1), 1);
        assert_eq!(ceil(1, 2), 1);
        assert_eq!(ceil(1, 8), 1);
        assert_eq!(ceil(7, 8), 1);
        assert_eq!(ceil(8, 8), 1);
        assert_eq!(ceil(9, 8), 2);
        assert_eq!(ceil(9, 9), 1);
        assert_eq!(ceil(10000000000, 10), 1000000000);
        assert_eq!(ceil(10, 10000000000), 1);
        assert_eq!(ceil(10000000000, 1000000000), 10);
    }

    #[test]
    #[cfg(simd)]
    fn test_bitwise_and_simd() {
        let buf1 = [0b00110011u8; 64];
        let buf2 = [0b11110000u8; 64];
        let mut buf3 = [0b00000000; 64];
        unsafe { bitwise_bin_op_simd(&buf1, &buf2, &mut buf3, |a, b| a & b) };
        for i in buf3.iter() {
            assert_eq!(&0b00110000u8, i);
        }
    }

    #[test]
    #[cfg(simd)]
    fn test_bitwise_or_simd() {
        let buf1 = [0b00110011u8; 64];
        let buf2 = [0b11110000u8; 64];
        let mut buf3 = [0b00000000; 64];
        unsafe { bitwise_bin_op_simd(&buf1, &buf2, &mut buf3, |a, b| a | b) };
        for i in buf3.iter() {
            assert_eq!(&0b11110011u8, i);
        }
    }
}
