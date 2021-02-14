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
use crate::util::bit_util;
#[cfg(feature = "simd")]
use packed_simd::u8x64;

#[cfg(feature = "avx512")]
use crate::arch::avx512::*;
use crate::util::bit_util::ceil;
#[cfg(any(feature = "simd", feature = "avx512"))]
use std::borrow::BorrowMut;

use super::{Buffer, MutableBuffer};

/// Apply a bitwise operation `simd_op` / `scalar_op` to two inputs using simd instructions and return the result as a Buffer.
/// The `simd_op` functions gets applied on chunks of 64 bytes (512 bits) at a time
/// and the `scalar_op` gets applied to remaining bytes.
/// Contrary to the non-simd version `bitwise_bin_op_helper`, the offset and length is specified in bytes
/// and this version does not support operations starting at arbitrary bit offsets.
#[cfg(simd)]
pub fn bitwise_bin_op_simd_helper<F_SIMD, F_SCALAR>(
    left: &Buffer,
    left_offset: usize,
    right: &Buffer,
    right_offset: usize,
    len: usize,
    simd_op: F_SIMD,
    scalar_op: F_SCALAR,
) -> Buffer
where
    F_SIMD: Fn(u8x64, u8x64) -> u8x64,
    F_SCALAR: Fn(u8, u8) -> u8,
{
    let mut result = MutableBuffer::new(len).with_bitset(len, false);
    let lanes = u8x64::lanes();

    let mut left_chunks = left.as_slice()[left_offset..].chunks_exact(lanes);
    let mut right_chunks = right.as_slice()[right_offset..].chunks_exact(lanes);
    let mut result_chunks = result.as_slice_mut().chunks_exact_mut(lanes);

    result_chunks
        .borrow_mut()
        .zip(left_chunks.borrow_mut().zip(right_chunks.borrow_mut()))
        .for_each(|(res, (left, right))| {
            unsafe { bit_util::bitwise_bin_op_simd(&left, &right, res, &simd_op) };
        });

    result_chunks
        .into_remainder()
        .iter_mut()
        .zip(
            left_chunks
                .remainder()
                .iter()
                .zip(right_chunks.remainder().iter()),
        )
        .for_each(|(res, (left, right))| {
            *res = scalar_op(*left, *right);
        });

    result.into()
}

/// Apply a bitwise operation `simd_op` / `scalar_op` to one input using simd instructions and return the result as a Buffer.
/// The `simd_op` functions gets applied on chunks of 64 bytes (512 bits) at a time
/// and the `scalar_op` gets applied to remaining bytes.
/// Contrary to the non-simd version `bitwise_unary_op_helper`, the offset and length is specified in bytes
/// and this version does not support operations starting at arbitrary bit offsets.
#[cfg(simd)]
pub fn bitwise_unary_op_simd_helper<F_SIMD, F_SCALAR>(
    left: &Buffer,
    left_offset: usize,
    len: usize,
    simd_op: F_SIMD,
    scalar_op: F_SCALAR,
) -> Buffer
where
    F_SIMD: Fn(u8x64) -> u8x64,
    F_SCALAR: Fn(u8) -> u8,
{
    let mut result = MutableBuffer::new(len).with_bitset(len, false);
    let lanes = u8x64::lanes();

    let mut left_chunks = left.as_slice()[left_offset..].chunks_exact(lanes);
    let mut result_chunks = result.as_slice_mut().chunks_exact_mut(lanes);

    result_chunks
        .borrow_mut()
        .zip(left_chunks.borrow_mut())
        .for_each(|(res, left)| unsafe {
            let data_simd = u8x64::from_slice_unaligned_unchecked(left);
            let simd_result = simd_op(data_simd);
            simd_result.write_to_slice_unaligned_unchecked(res);
        });

    result_chunks
        .into_remainder()
        .iter_mut()
        .zip(left_chunks.remainder().iter())
        .for_each(|(res, left)| {
            *res = scalar_op(*left);
        });

    result.into()
}

/// Apply a bitwise operation `op` to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
pub fn bitwise_bin_op_helper<F>(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
    op: F,
) -> Buffer
where
    F: Fn(u64, u64) -> u64,
{
    let left_chunks = left.bit_chunks(left_offset_in_bits, len_in_bits);
    let right_chunks = right.bit_chunks(right_offset_in_bits, len_in_bits);

    let chunks = left_chunks
        .iter()
        .zip(right_chunks.iter())
        .map(|(left, right)| op(left, right));
    // Soundness: `BitChunks` is a trusted len iterator
    let mut buffer = unsafe { MutableBuffer::from_trusted_len_iter(chunks) };

    let remainder_bytes = ceil(left_chunks.remainder_len(), 8);
    let rem = op(left_chunks.remainder_bits(), right_chunks.remainder_bits());
    // we are counting its starting from the least significant bit, to to_le_bytes should be correct
    let rem = &rem.to_le_bytes()[0..remainder_bytes];
    buffer.extend_from_slice(rem);

    buffer.into()
}

/// Apply a bitwise operation `op` to one input and return the result as a Buffer.
/// The input is treated as a bitmap, meaning that offset and length are specified in number of bits.
pub fn bitwise_unary_op_helper<F>(
    left: &Buffer,
    offset_in_bits: usize,
    len_in_bits: usize,
    op: F,
) -> Buffer
where
    F: Fn(u64) -> u64,
{
    // reserve capacity and set length so we can get a typed view of u64 chunks
    let mut result =
        MutableBuffer::new(ceil(len_in_bits, 8)).with_bitset(len_in_bits / 64 * 8, false);

    let left_chunks = left.bit_chunks(offset_in_bits, len_in_bits);
    let result_chunks = result.typed_data_mut::<u64>().iter_mut();

    result_chunks
        .zip(left_chunks.iter())
        .for_each(|(res, left)| {
            *res = op(left);
        });

    let remainder_bytes = ceil(left_chunks.remainder_len(), 8);
    let rem = op(left_chunks.remainder_bits());
    // we are counting its starting from the least significant bit, to to_le_bytes should be correct
    let rem = &rem.to_le_bytes()[0..remainder_bytes];
    result.extend_from_slice(rem);

    result.into()
}

#[cfg(all(target_arch = "x86_64", feature = "avx512"))]
pub fn buffer_bin_and(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    if left_offset_in_bits % 8 == 0
        && right_offset_in_bits % 8 == 0
        && len_in_bits % 8 == 0
    {
        let len = len_in_bits / 8;
        let left_offset = left_offset_in_bits / 8;
        let right_offset = right_offset_in_bits / 8;

        let mut result = MutableBuffer::new(len).with_bitset(len, false);

        let mut left_chunks =
            left.as_slice()[left_offset..].chunks_exact(AVX512_U8X64_LANES);
        let mut right_chunks =
            right.as_slice()[right_offset..].chunks_exact(AVX512_U8X64_LANES);
        let mut result_chunks =
            result.as_slice_mut().chunks_exact_mut(AVX512_U8X64_LANES);

        result_chunks
            .borrow_mut()
            .zip(left_chunks.borrow_mut().zip(right_chunks.borrow_mut()))
            .for_each(|(res, (left, right))| unsafe {
                avx512_bin_and(left, right, res);
            });

        result_chunks
            .into_remainder()
            .iter_mut()
            .zip(
                left_chunks
                    .remainder()
                    .iter()
                    .zip(right_chunks.remainder().iter()),
            )
            .for_each(|(res, (left, right))| {
                *res = *left & *right;
            });

        result.into()
    } else {
        bitwise_bin_op_helper(
            &left,
            left_offset_in_bits,
            right,
            right_offset_in_bits,
            len_in_bits,
            |a, b| a & b,
        )
    }
}

#[cfg(all(feature = "simd", not(feature = "avx512")))]
pub fn buffer_bin_and(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    if left_offset_in_bits % 8 == 0
        && right_offset_in_bits % 8 == 0
        && len_in_bits % 8 == 0
    {
        bitwise_bin_op_simd_helper(
            &left,
            left_offset_in_bits / 8,
            &right,
            right_offset_in_bits / 8,
            len_in_bits / 8,
            |a, b| a & b,
            |a, b| a & b,
        )
    } else {
        bitwise_bin_op_helper(
            &left,
            left_offset_in_bits,
            right,
            right_offset_in_bits,
            len_in_bits,
            |a, b| a & b,
        )
    }
}

// Note: do not target specific features like x86 without considering
// other targets like wasm32, as those would fail to build
#[cfg(all(not(any(feature = "simd", feature = "avx512"))))]
pub fn buffer_bin_and(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    bitwise_bin_op_helper(
        &left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a & b,
    )
}

#[cfg(all(target_arch = "x86_64", feature = "avx512"))]
pub fn buffer_bin_or(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    if left_offset_in_bits % 8 == 0
        && right_offset_in_bits % 8 == 0
        && len_in_bits % 8 == 0
    {
        let len = len_in_bits / 8;
        let left_offset = left_offset_in_bits / 8;
        let right_offset = right_offset_in_bits / 8;

        let mut result = MutableBuffer::new(len).with_bitset(len, false);

        let mut left_chunks =
            left.as_slice()[left_offset..].chunks_exact(AVX512_U8X64_LANES);
        let mut right_chunks =
            right.as_slice()[right_offset..].chunks_exact(AVX512_U8X64_LANES);
        let mut result_chunks =
            result.as_slice_mut().chunks_exact_mut(AVX512_U8X64_LANES);

        result_chunks
            .borrow_mut()
            .zip(left_chunks.borrow_mut().zip(right_chunks.borrow_mut()))
            .for_each(|(res, (left, right))| unsafe {
                avx512_bin_or(left, right, res);
            });

        result_chunks
            .into_remainder()
            .iter_mut()
            .zip(
                left_chunks
                    .remainder()
                    .iter()
                    .zip(right_chunks.remainder().iter()),
            )
            .for_each(|(res, (left, right))| {
                *res = *left | *right;
            });

        result.into()
    } else {
        bitwise_bin_op_helper(
            &left,
            left_offset_in_bits,
            right,
            right_offset_in_bits,
            len_in_bits,
            |a, b| a | b,
        )
    }
}

#[cfg(all(feature = "simd", not(feature = "avx512")))]
pub fn buffer_bin_or(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    if left_offset_in_bits % 8 == 0
        && right_offset_in_bits % 8 == 0
        && len_in_bits % 8 == 0
    {
        bitwise_bin_op_simd_helper(
            &left,
            left_offset_in_bits / 8,
            &right,
            right_offset_in_bits / 8,
            len_in_bits / 8,
            |a, b| a | b,
            |a, b| a | b,
        )
    } else {
        bitwise_bin_op_helper(
            &left,
            left_offset_in_bits,
            right,
            right_offset_in_bits,
            len_in_bits,
            |a, b| a | b,
        )
    }
}

#[cfg(all(not(any(feature = "simd", feature = "avx512"))))]
pub fn buffer_bin_or(
    left: &Buffer,
    left_offset_in_bits: usize,
    right: &Buffer,
    right_offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    bitwise_bin_op_helper(
        &left,
        left_offset_in_bits,
        right,
        right_offset_in_bits,
        len_in_bits,
        |a, b| a | b,
    )
}

pub fn buffer_unary_not(
    left: &Buffer,
    offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    // SIMD implementation if available and byte-aligned
    #[cfg(simd)]
    if offset_in_bits % 8 == 0 && len_in_bits % 8 == 0 {
        return bitwise_unary_op_simd_helper(
            &left,
            offset_in_bits / 8,
            len_in_bits / 8,
            |a| !a,
            |a| !a,
        );
    }
    // Default implementation
    #[allow(unreachable_code)]
    {
        bitwise_unary_op_helper(&left, offset_in_bits, len_in_bits, |a| !a)
    }
}
