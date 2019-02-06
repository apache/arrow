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

use packed_simd::u8x64;
use std::slice::{from_raw_parts, from_raw_parts_mut};

use crate::buffer::{Buffer, MutableBuffer};
use crate::builder::{BufferBuilderTrait, UInt8BufferBuilder};

/// SIMD accelerated version of bitwise binary operation for two `Buffer`'s.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn bitwise_bin_op_simd<F>(left: &Buffer, right: &Buffer, op: F) -> Buffer
where
    F: Fn(u8x64, u8x64) -> u8x64,
{
    let mut result = MutableBuffer::new(left.len()).with_bitset(left.len(), false);
    let lanes = u8x64::lanes();
    for i in (0..left.len()).step_by(lanes) {
        let left_data =
            unsafe { from_raw_parts(left.raw_data().offset(i as isize), lanes) };
        let left_simd = unsafe { u8x64::from_slice_unaligned_unchecked(left_data) };
        let right_data =
            unsafe { from_raw_parts(right.raw_data().offset(i as isize), lanes) };
        let right_simd = unsafe { u8x64::from_slice_unaligned_unchecked(right_data) };
        let simd_result = op(left_simd, right_simd);
        let result_slice: &mut [u8] = unsafe {
            from_raw_parts_mut(
                (result.data_mut().as_mut_ptr() as *mut u8).offset(i as isize),
                lanes,
            )
        };
        unsafe { simd_result.write_to_slice_unaligned_unchecked(result_slice) };
    }
    result.freeze()
}

/// Default version of bitwise binary operation for two `Buffer`'s where SIMD is not
/// available.
fn bitwise_bin_op_default<F>(left: &Buffer, right: &Buffer, op: F) -> Buffer
where
    F: Fn(&u8, &u8) -> u8,
{
    let mut builder = UInt8BufferBuilder::new(left.len());
    for i in 0..left.len() {
        unsafe {
            builder
                .append(op(
                    left.data().get_unchecked(i),
                    right.data().get_unchecked(i),
                ))
                .unwrap();
        }
    }
    builder.finish()
}

/// Bitwise binary AND for two `Buffer`'s.
pub fn bitwise_and(left: &Buffer, right: &Buffer) -> Buffer {
    assert_eq!(left.len(), right.len(), "Buffers must be the same size.");

    // SIMD implementation if available
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    return bitwise_bin_op_simd(&left, &right, |a, b| a & b);

    // Default implementation
    #[allow(unreachable_code)]
    bitwise_bin_op_default(&left, &right, |a, b| a & b)
}

/// Bitwise binary OR for two `Buffer`'s.
pub fn bitwise_or(left: &Buffer, right: &Buffer) -> Buffer {
    assert_eq!(left.len(), right.len(), "Buffers must be the same size.");

    // SIMD implementation if available
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    return bitwise_bin_op_simd(&left, &right, |a, b| a | b);

    // Default implementation
    #[allow(unreachable_code)]
    bitwise_bin_op_default(&left, &right, |a, b| a | b)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_bitwise_and() {
        let buf1 = Buffer::from([0b01101010]);
        let buf2 = Buffer::from([0b01001110]);
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            assert_eq!(
                Buffer::from([0b01001010]),
                bitwise_bin_op_simd(&buf1, &buf2, |a, b| a & b)
            );
        }
        assert_eq!(
            Buffer::from([0b01001010]),
            bitwise_bin_op_default(&buf1, &buf2, |a, b| a & b)
        );
        assert_eq!(Buffer::from([0b01001010]), bitwise_and(&buf1, &buf2));
    }

    #[test]
    fn test_bitwise_or() {
        let buf1 = Buffer::from([0b01101010]);
        let buf2 = Buffer::from([0b01001110]);
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            assert_eq!(
                Buffer::from([0b01101110]),
                bitwise_bin_op_simd(&buf1, &buf2, |a, b| a | b)
            );
        }
        assert_eq!(
            Buffer::from([0b01101110]),
            bitwise_bin_op_default(&buf1, &buf2, |a, b| a | b)
        );
        assert_eq!(Buffer::from([0b01101110]), bitwise_or(&buf1, &buf2));
    }

    #[test]
    #[should_panic(expected = "Buffers must be the same size.")]
    fn test_buffer_bitand_different_sizes() {
        let buf1 = Buffer::from([1_u8, 1_u8]);
        let buf2 = Buffer::from([0b01001110]);
        let _buf3 = bitwise_and(&buf1, &buf2);
    }
}
