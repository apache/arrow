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

//! The main type in the module is `Buffer`, a contiguous immutable memory region of
//! fixed size aligned at a 64-byte boundary. `MutableBuffer` is like `Buffer`, but it can
//! be mutated and grown.
#[cfg(feature = "simd")]
use packed_simd::u8x64;

use crate::{
    bytes::{Bytes, Deallocation},
    ffi,
};

use std::cmp;
use std::convert::AsRef;
use std::fmt::Debug;
use std::mem;
use std::ops::{BitAnd, BitOr, Not};
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::sync::Arc;

#[cfg(feature = "avx512")]
use crate::arch::avx512::*;
use crate::datatypes::ArrowNativeType;
use crate::error::{ArrowError, Result};
use crate::memory;
use crate::util::bit_chunk_iterator::BitChunks;
use crate::util::bit_util;
use crate::util::bit_util::ceil;
#[cfg(any(feature = "simd", feature = "avx512"))]
use std::borrow::BorrowMut;

/// Buffer is a contiguous memory region of fixed size and is aligned at a 64-byte
/// boundary. Buffer is immutable.
#[derive(Clone, PartialEq, Debug)]
pub struct Buffer {
    /// Reference-counted pointer to the internal byte buffer.
    data: Arc<Bytes>,

    /// The offset into the buffer.
    offset: usize,
}

impl Buffer {
    /// Creates a buffer from an existing memory region (must already be byte-aligned), this
    /// `Buffer` will free this piece of memory when dropped.
    ///
    /// # Arguments
    ///
    /// * `ptr` - Pointer to raw parts
    /// * `len` - Length of raw parts in **bytes**
    /// * `capacity` - Total allocated memory for the pointer `ptr`, in **bytes**
    ///
    /// # Safety
    ///
    /// This function is unsafe as there is no guarantee that the given pointer is valid for `len`
    /// bytes. If the `ptr` and `capacity` come from a `Buffer`, then this is guaranteed.
    pub unsafe fn from_raw_parts(ptr: *const u8, len: usize, capacity: usize) -> Self {
        Buffer::build_with_arguments(ptr, len, Deallocation::Native(capacity))
    }

    /// Creates a buffer from an existing memory region (must already be byte-aligned), this
    /// `Buffer` **does not** free this piece of memory when dropped.
    ///
    /// # Arguments
    ///
    /// * `ptr` - Pointer to raw parts
    /// * `len` - Length of raw parts in **bytes**
    /// * `data` - An [ffi::FFI_ArrowArray] with the data
    ///
    /// # Safety
    ///
    /// This function is unsafe as there is no guarantee that the given pointer is valid for `len`
    /// bytes and that the foreign deallocator frees the region.
    pub unsafe fn from_unowned(
        ptr: *const u8,
        len: usize,
        data: Arc<ffi::FFI_ArrowArray>,
    ) -> Self {
        Buffer::build_with_arguments(ptr, len, Deallocation::Foreign(data))
    }

    /// Auxiliary method to create a new Buffer
    unsafe fn build_with_arguments(
        ptr: *const u8,
        len: usize,
        deallocation: Deallocation,
    ) -> Self {
        let bytes = Bytes::new(ptr, len, deallocation);
        Buffer {
            data: Arc::new(bytes),
            offset: 0,
        }
    }

    /// Returns the number of bytes in the buffer
    pub fn len(&self) -> usize {
        self.data.len() - self.offset
    }

    /// Returns the capacity of this buffer.
    /// For exernally owned buffers, this returns zero
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Returns whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.data.len() - self.offset == 0
    }

    /// Returns the byte slice stored in this buffer
    pub fn data(&self) -> &[u8] {
        &self.data.as_slice()[self.offset..]
    }

    /// Returns a slice of this buffer, starting from `offset`.
    pub fn slice(&self, offset: usize) -> Self {
        assert!(
            offset <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        Self {
            data: self.data.clone(),
            offset: self.offset + offset,
        }
    }

    /// Returns a raw pointer for this buffer.
    ///
    /// Note that this should be used cautiously, and the returned pointer should not be
    /// stored anywhere, to avoid dangling pointers.
    pub fn raw_data(&self) -> *const u8 {
        unsafe { self.data.raw_data().add(self.offset) }
    }

    /// View buffer as typed slice.
    ///
    /// # Safety
    ///
    /// `ArrowNativeType` is public so that it can be used as a trait bound for other public
    /// components, such as the `ToByteSlice` trait.  However, this means that it can be
    /// implemented by user defined types, which it is not intended for.
    ///
    /// Also `typed_data::<bool>` is unsafe as `0x00` and `0x01` are the only valid values for
    /// `bool` in Rust.  However, `bool` arrays in Arrow are bit-packed which breaks this condition.
    pub unsafe fn typed_data<T: ArrowNativeType + num::Num>(&self) -> &[T] {
        assert_eq!(self.len() % mem::size_of::<T>(), 0);
        assert!(memory::is_ptr_aligned::<T>(self.raw_data() as *const T));
        from_raw_parts(
            self.raw_data() as *const T,
            self.len() / mem::size_of::<T>(),
        )
    }

    /// Returns a slice of this buffer starting at a certain bit offset.
    /// If the offset is byte-aligned the returned buffer is a shallow clone,
    /// otherwise a new buffer is allocated and filled with a copy of the bits in the range.
    pub fn bit_slice(&self, offset: usize, len: usize) -> Self {
        if offset % 8 == 0 && len % 8 == 0 {
            return self.slice(offset / 8);
        }

        bitwise_unary_op_helper(&self, offset, len, |a| a)
    }

    /// Returns a `BitChunks` instance which can be used to iterate over this buffers bits
    /// in larger chunks and starting at arbitrary bit offsets.
    /// Note that both `offset` and `length` are measured in bits.
    pub fn bit_chunks(&self, offset: usize, len: usize) -> BitChunks {
        BitChunks::new(&self.data.as_slice()[self.offset..], offset, len)
    }

    /// Returns the number of 1-bits in this buffer.
    pub fn count_set_bits(&self) -> usize {
        let len_in_bits = self.len() * 8;
        // self.offset is already taken into consideration by the bit_chunks implementation
        self.count_set_bits_offset(0, len_in_bits)
    }

    /// Returns the number of 1-bits in this buffer, starting from `offset` with `length` bits
    /// inspected. Note that both `offset` and `length` are measured in bits.
    pub fn count_set_bits_offset(&self, offset: usize, len: usize) -> usize {
        let chunks = self.bit_chunks(offset, len);
        let mut count = chunks.iter().map(|c| c.count_ones() as usize).sum();
        count += chunks.remainder_bits().count_ones() as usize;

        count
    }
}

/// Creating a `Buffer` instance by copying the memory from a `AsRef<[u8]>` into a newly
/// allocated memory region.
impl<T: AsRef<[u8]>> From<T> for Buffer {
    fn from(p: T) -> Self {
        // allocate aligned memory buffer
        let slice = p.as_ref();
        let len = slice.len() * mem::size_of::<u8>();
        let capacity = bit_util::round_upto_multiple_of_64(len);
        let buffer = memory::allocate_aligned(capacity);
        unsafe {
            memory::memcpy(buffer, slice.as_ptr(), len);
            Buffer::build_with_arguments(buffer, len, Deallocation::Native(capacity))
        }
    }
}

/// Apply a bitwise operation `simd_op` / `scalar_op` to two inputs using simd instructions and return the result as a Buffer.
/// The `simd_op` functions gets applied on chunks of 64 bytes (512 bits) at a time
/// and the `scalar_op` gets applied to remaining bytes.
/// Contrary to the non-simd version `bitwise_bin_op_helper`, the offset and length is specified in bytes
/// and this version does not support operations starting at arbitrary bit offsets.
#[cfg(simd_x86)]
fn bitwise_bin_op_simd_helper<F_SIMD, F_SCALAR>(
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

    let mut left_chunks = left.data()[left_offset..].chunks_exact(lanes);
    let mut right_chunks = right.data()[right_offset..].chunks_exact(lanes);
    let mut result_chunks = result.data_mut().chunks_exact_mut(lanes);

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

    result.freeze()
}

/// Apply a bitwise operation `simd_op` / `scalar_op` to one input using simd instructions and return the result as a Buffer.
/// The `simd_op` functions gets applied on chunks of 64 bytes (512 bits) at a time
/// and the `scalar_op` gets applied to remaining bytes.
/// Contrary to the non-simd version `bitwise_unary_op_helper`, the offset and length is specified in bytes
/// and this version does not support operations starting at arbitrary bit offsets.
#[cfg(simd_x86)]
fn bitwise_unary_op_simd_helper<F_SIMD, F_SCALAR>(
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

    let mut left_chunks = left.data()[left_offset..].chunks_exact(lanes);
    let mut result_chunks = result.data_mut().chunks_exact_mut(lanes);

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

    result.freeze()
}

/// Apply a bitwise operation `op` to two inputs and return the result as a Buffer.
/// The inputs are treated as bitmaps, meaning that offsets and length are specified in number of bits.
fn bitwise_bin_op_helper<F>(
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
    // reserve capacity and set length so we can get a typed view of u64 chunks
    let mut result =
        MutableBuffer::new(ceil(len_in_bits, 8)).with_bitset(len_in_bits / 64 * 8, false);

    let left_chunks = left.bit_chunks(left_offset_in_bits, len_in_bits);
    let right_chunks = right.bit_chunks(right_offset_in_bits, len_in_bits);
    let result_chunks = result.typed_data_mut::<u64>().iter_mut();

    result_chunks
        .zip(left_chunks.iter().zip(right_chunks.iter()))
        .for_each(|(res, (left, right))| {
            *res = op(left, right);
        });

    let remainder_bytes = ceil(left_chunks.remainder_len(), 8);
    let rem = op(left_chunks.remainder_bits(), right_chunks.remainder_bits());
    // we are counting its starting from the least significant bit, to to_le_bytes should be correct
    let rem = &rem.to_le_bytes()[0..remainder_bytes];
    result.extend_from_slice(rem);

    result.freeze()
}

/// Apply a bitwise operation `op` to one input and return the result as a Buffer.
/// The input is treated as a bitmap, meaning that offset and length are specified in number of bits.
fn bitwise_unary_op_helper<F>(
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

    result.freeze()
}

#[cfg(all(target_arch = "x86_64", feature = "avx512"))]
pub(super) fn buffer_bin_and(
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

        let mut left_chunks = left.data()[left_offset..].chunks_exact(AVX512_U8X64_LANES);
        let mut right_chunks =
            right.data()[right_offset..].chunks_exact(AVX512_U8X64_LANES);
        let mut result_chunks = result.data_mut().chunks_exact_mut(AVX512_U8X64_LANES);

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

        result.freeze()
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

#[cfg(simd_x86)]
pub(super) fn buffer_bin_and(
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
pub(super) fn buffer_bin_and(
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
pub(super) fn buffer_bin_or(
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

        let mut left_chunks = left.data()[left_offset..].chunks_exact(AVX512_U8X64_LANES);
        let mut right_chunks =
            right.data()[right_offset..].chunks_exact(AVX512_U8X64_LANES);
        let mut result_chunks = result.data_mut().chunks_exact_mut(AVX512_U8X64_LANES);

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

        result.freeze()
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

#[cfg(simd_x86)]
pub(super) fn buffer_bin_or(
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
pub(super) fn buffer_bin_or(
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

pub(super) fn buffer_unary_not(
    left: &Buffer,
    offset_in_bits: usize,
    len_in_bits: usize,
) -> Buffer {
    // SIMD implementation if available and byte-aligned
    #[cfg(simd_x86)]
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

impl<'a, 'b> BitAnd<&'b Buffer> for &'a Buffer {
    type Output = Result<Buffer>;

    fn bitand(self, rhs: &'b Buffer) -> Result<Buffer> {
        if self.len() != rhs.len() {
            return Err(ArrowError::ComputeError(
                "Buffers must be the same size to apply Bitwise AND.".to_string(),
            ));
        }

        let len_in_bits = self.len() * 8;
        Ok(buffer_bin_and(&self, 0, &rhs, 0, len_in_bits))
    }
}

impl<'a, 'b> BitOr<&'b Buffer> for &'a Buffer {
    type Output = Result<Buffer>;

    fn bitor(self, rhs: &'b Buffer) -> Result<Buffer> {
        if self.len() != rhs.len() {
            return Err(ArrowError::ComputeError(
                "Buffers must be the same size to apply Bitwise OR.".to_string(),
            ));
        }

        let len_in_bits = self.len() * 8;

        Ok(buffer_bin_or(&self, 0, &rhs, 0, len_in_bits))
    }
}

impl Not for &Buffer {
    type Output = Buffer;

    fn not(self) -> Buffer {
        let len_in_bits = self.len() * 8;
        buffer_unary_not(&self, 0, len_in_bits)
    }
}

unsafe impl Sync for Buffer {}
unsafe impl Send for Buffer {}

/// Similar to `Buffer`, but is growable and can be mutated. A mutable buffer can be
/// converted into a immutable buffer via the `freeze` method.
#[derive(Debug)]
pub struct MutableBuffer {
    data: *mut u8,
    len: usize,
    capacity: usize,
}

impl MutableBuffer {
    /// Allocate a new mutable buffer with initial capacity to be `capacity`.
    pub fn new(capacity: usize) -> Self {
        let new_capacity = bit_util::round_upto_multiple_of_64(capacity);
        let ptr = memory::allocate_aligned(new_capacity);
        Self {
            data: ptr,
            len: 0,
            capacity: new_capacity,
        }
    }

    /// creates a new [MutableBuffer] where every bit is initialized to `0`
    pub fn new_null(len: usize) -> Self {
        let num_bytes = bit_util::ceil(len, 8);
        MutableBuffer::new(num_bytes).with_bitset(num_bytes, false)
    }

    /// Set the bits in the range of `[0, end)` to 0 (if `val` is false), or 1 (if `val`
    /// is true). Also extend the length of this buffer to be `end`.
    ///
    /// This is useful when one wants to clear (or set) the bits and then manipulate
    /// the buffer directly (e.g., modifying the buffer by holding a mutable reference
    /// from `data_mut()`).
    pub fn with_bitset(mut self, end: usize, val: bool) -> Self {
        assert!(end <= self.capacity);
        let v = if val { 255 } else { 0 };
        unsafe {
            std::ptr::write_bytes(self.data, v, end);
            self.len = end;
        }
        self
    }

    /// Ensure that `count` bytes from `start` contain zero bits
    ///
    /// This is used to initialize the bits in a buffer, however, it has no impact on the
    /// `len` of the buffer and so can be used to initialize the memory region from
    /// `len` to `capacity`.
    pub fn set_null_bits(&mut self, start: usize, count: usize) {
        assert!(start + count <= self.capacity);
        unsafe {
            std::ptr::write_bytes(self.data.add(start), 0, count);
        }
    }

    /// Ensures that this buffer has at least `capacity` slots in this buffer. This will
    /// also ensure the new capacity will be a multiple of 64 bytes.
    ///
    /// Returns the new capacity for this buffer.
    pub fn reserve(&mut self, capacity: usize) -> usize {
        if capacity > self.capacity {
            let new_capacity = bit_util::round_upto_multiple_of_64(capacity);
            let new_capacity = cmp::max(new_capacity, self.capacity * 2);
            let new_data =
                unsafe { memory::reallocate(self.data, self.capacity, new_capacity) };
            self.data = new_data as *mut u8;
            self.capacity = new_capacity;
        }
        self.capacity
    }

    /// Resizes the buffer so that the `len` will equal to the `new_len`.
    ///
    /// If `new_len` is greater than `len`, the buffer's length is simply adjusted to be
    /// the former, optionally extending the capacity. The data between `len` and
    /// `new_len` will be zeroed out.
    ///
    /// If `new_len` is less than `len`, the buffer will be truncated.
    pub fn resize(&mut self, new_len: usize) {
        if new_len > self.len {
            self.reserve(new_len);
        } else {
            let new_capacity = bit_util::round_upto_multiple_of_64(new_len);
            if new_capacity < self.capacity {
                let new_data =
                    unsafe { memory::reallocate(self.data, self.capacity, new_capacity) };
                self.data = new_data as *mut u8;
                self.capacity = new_capacity;
            }
        }
        self.len = new_len;
    }

    /// Returns whether this buffer is empty or not.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the length (the number of bytes written) in this buffer.
    #[inline]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns the total capacity in this buffer.
    #[inline]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Clear all existing data from this buffer.
    pub fn clear(&mut self) {
        self.len = 0
    }

    /// Returns the data stored in this buffer as a slice.
    pub fn data(&self) -> &[u8] {
        if self.data.is_null() {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.raw_data(), self.len()) }
        }
    }

    /// Returns the data stored in this buffer as a mutable slice.
    pub fn data_mut(&mut self) -> &mut [u8] {
        if self.data.is_null() {
            &mut []
        } else {
            unsafe { std::slice::from_raw_parts_mut(self.raw_data_mut(), self.len()) }
        }
    }

    /// Returns a raw pointer for this buffer.
    ///
    /// Note that this should be used cautiously, and the returned pointer should not be
    /// stored anywhere, to avoid dangling pointers.
    #[inline]
    pub const fn raw_data(&self) -> *const u8 {
        self.data
    }

    #[inline]
    pub fn raw_data_mut(&mut self) -> *mut u8 {
        self.data
    }

    /// Freezes this buffer and return an immutable version of it.
    pub fn freeze(self) -> Buffer {
        let buffer_data = unsafe {
            Bytes::new(self.data, self.len, Deallocation::Native(self.capacity))
        };
        std::mem::forget(self);
        Buffer {
            data: Arc::new(buffer_data),
            offset: 0,
        }
    }

    /// View buffer as typed slice.
    pub fn typed_data_mut<T: ArrowNativeType>(&mut self) -> &mut [T] {
        assert_eq!(self.len() % mem::size_of::<T>(), 0);
        assert!(memory::is_ptr_aligned::<T>(self.raw_data() as *const T));
        unsafe {
            from_raw_parts_mut(
                self.raw_data() as *mut T,
                self.len() / mem::size_of::<T>(),
            )
        }
    }

    /// Extends the buffer from a byte slice, incrementing its capacity if needed.
    #[inline]
    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        let new_len = self.len + bytes.len();
        if new_len > self.capacity {
            self.reserve(new_len);
        }
        unsafe {
            memory::memcpy(self.data.add(self.len), bytes.as_ptr(), bytes.len());
        }
        self.len = new_len;
    }

    /// Extends the buffer by `len` with all bytes equal to `0u8`, incrementing its capacity if needed.
    pub fn extend(&mut self, len: usize) {
        let remaining_capacity = self.capacity - self.len;
        if len > remaining_capacity {
            self.reserve(self.len + len);
        }
        self.len += len;
    }
}

impl Drop for MutableBuffer {
    fn drop(&mut self) {
        if !self.data.is_null() {
            unsafe { memory::free_aligned(self.data, self.capacity) };
        }
    }
}

impl PartialEq for MutableBuffer {
    fn eq(&self, other: &MutableBuffer) -> bool {
        if self.len != other.len {
            return false;
        }
        if self.capacity != other.capacity {
            return false;
        }
        unsafe { memory::memcmp(self.data, other.data, self.len) == 0 }
    }
}

unsafe impl Sync for MutableBuffer {}
unsafe impl Send for MutableBuffer {}

#[cfg(test)]
mod tests {
    use std::ptr::null_mut;
    use std::thread;

    use super::*;
    use crate::datatypes::ToByteSlice;

    #[test]
    fn test_buffer_data_equality() {
        let buf1 = Buffer::from(&[0, 1, 2, 3, 4]);
        let buf2 = Buffer::from(&[0, 1, 2, 3, 4]);
        assert_eq!(buf1, buf2);

        // slice with same offset should still preserve equality
        let buf3 = buf1.slice(2);
        assert_ne!(buf1, buf3);
        let buf4 = buf2.slice(2);
        assert_eq!(buf3, buf4);

        // Different capacities should still preserve equality
        let mut buf2 = MutableBuffer::new(65);
        buf2.extend_from_slice(&[0, 1, 2, 3, 4]);

        let buf2 = buf2.freeze();
        assert_eq!(buf1, buf2);

        // unequal because of different elements
        let buf2 = Buffer::from(&[0, 0, 2, 3, 4]);
        assert_ne!(buf1, buf2);

        // unequal because of different length
        let buf2 = Buffer::from(&[0, 1, 2, 3]);
        assert_ne!(buf1, buf2);
    }

    #[test]
    fn test_from_raw_parts() {
        let buf = unsafe { Buffer::from_raw_parts(null_mut(), 0, 0) };
        assert_eq!(0, buf.len());
        assert_eq!(0, buf.data().len());
        assert_eq!(0, buf.capacity());
        assert!(buf.raw_data().is_null());

        let buf = Buffer::from(&[0, 1, 2, 3, 4]);
        assert_eq!(5, buf.len());
        assert!(!buf.raw_data().is_null());
        assert_eq!([0, 1, 2, 3, 4], buf.data());
    }

    #[test]
    fn test_from_vec() {
        let buf = Buffer::from(&[0, 1, 2, 3, 4]);
        assert_eq!(5, buf.len());
        assert!(!buf.raw_data().is_null());
        assert_eq!([0, 1, 2, 3, 4], buf.data());
    }

    #[test]
    fn test_copy() {
        let buf = Buffer::from(&[0, 1, 2, 3, 4]);
        let buf2 = buf;
        assert_eq!(5, buf2.len());
        assert_eq!(64, buf2.capacity());
        assert!(!buf2.raw_data().is_null());
        assert_eq!([0, 1, 2, 3, 4], buf2.data());
    }

    #[test]
    fn test_slice() {
        let buf = Buffer::from(&[2, 4, 6, 8, 10]);
        let buf2 = buf.slice(2);

        assert_eq!([6, 8, 10], buf2.data());
        assert_eq!(3, buf2.len());
        assert_eq!(unsafe { buf.raw_data().offset(2) }, buf2.raw_data());

        let buf3 = buf2.slice(1);
        assert_eq!([8, 10], buf3.data());
        assert_eq!(2, buf3.len());
        assert_eq!(unsafe { buf.raw_data().offset(3) }, buf3.raw_data());

        let buf4 = buf.slice(5);
        let empty_slice: [u8; 0] = [];
        assert_eq!(empty_slice, buf4.data());
        assert_eq!(0, buf4.len());
        assert!(buf4.is_empty());
        assert_eq!(buf2.slice(2).data(), &[10]);
    }

    #[test]
    #[should_panic(
        expected = "the offset of the new Buffer cannot exceed the existing length"
    )]
    fn test_slice_offset_out_of_bound() {
        let buf = Buffer::from(&[2, 4, 6, 8, 10]);
        buf.slice(6);
    }

    #[test]
    fn test_with_bitset() {
        let mut_buf = MutableBuffer::new(64).with_bitset(64, false);
        let buf = mut_buf.freeze();
        assert_eq!(0, buf.count_set_bits());

        let mut_buf = MutableBuffer::new(64).with_bitset(64, true);
        let buf = mut_buf.freeze();
        assert_eq!(512, buf.count_set_bits());
    }

    #[test]
    fn test_set_null_bits() {
        let mut mut_buf = MutableBuffer::new(64).with_bitset(64, true);
        mut_buf.set_null_bits(0, 64);
        let buf = mut_buf.freeze();
        assert_eq!(0, buf.count_set_bits());

        let mut mut_buf = MutableBuffer::new(64).with_bitset(64, true);
        mut_buf.set_null_bits(32, 32);
        let buf = mut_buf.freeze();
        assert_eq!(256, buf.count_set_bits());
    }

    #[test]
    fn test_bitwise_and() {
        let buf1 = Buffer::from([0b01101010]);
        let buf2 = Buffer::from([0b01001110]);
        assert_eq!(Buffer::from([0b01001010]), (&buf1 & &buf2).unwrap());
    }

    #[test]
    fn test_bitwise_or() {
        let buf1 = Buffer::from([0b01101010]);
        let buf2 = Buffer::from([0b01001110]);
        assert_eq!(Buffer::from([0b01101110]), (&buf1 | &buf2).unwrap());
    }

    #[test]
    fn test_bitwise_not() {
        let buf = Buffer::from([0b01101010]);
        assert_eq!(Buffer::from([0b10010101]), !&buf);
    }

    #[test]
    #[should_panic(expected = "Buffers must be the same size to apply Bitwise OR.")]
    fn test_buffer_bitand_different_sizes() {
        let buf1 = Buffer::from([1_u8, 1_u8]);
        let buf2 = Buffer::from([0b01001110]);
        let _buf3 = (&buf1 | &buf2).unwrap();
    }

    #[test]
    fn test_mutable_new() {
        let buf = MutableBuffer::new(63);
        assert_eq!(64, buf.capacity());
        assert_eq!(0, buf.len());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_mutable_extend_from_slice() {
        let mut buf = MutableBuffer::new(100);
        buf.extend_from_slice(b"hello");
        assert_eq!(5, buf.len());
        assert_eq!(b"hello", buf.data());

        buf.extend_from_slice(b" world");
        assert_eq!(11, buf.len());
        assert_eq!(b"hello world", buf.data());

        buf.clear();
        assert_eq!(0, buf.len());
        buf.extend_from_slice(b"hello arrow");
        assert_eq!(11, buf.len());
        assert_eq!(b"hello arrow", buf.data());
    }

    #[test]
    fn test_mutable_reserve() {
        let mut buf = MutableBuffer::new(1);
        assert_eq!(64, buf.capacity());

        // Reserving a smaller capacity should have no effect.
        let mut new_cap = buf.reserve(10);
        assert_eq!(64, new_cap);
        assert_eq!(64, buf.capacity());

        new_cap = buf.reserve(100);
        assert_eq!(128, new_cap);
        assert_eq!(128, buf.capacity());
    }

    #[test]
    fn test_mutable_resize() {
        let mut buf = MutableBuffer::new(1);
        assert_eq!(64, buf.capacity());
        assert_eq!(0, buf.len());

        buf.resize(20);
        assert_eq!(64, buf.capacity());
        assert_eq!(20, buf.len());

        buf.resize(10);
        assert_eq!(64, buf.capacity());
        assert_eq!(10, buf.len());

        buf.resize(100);
        assert_eq!(128, buf.capacity());
        assert_eq!(100, buf.len());

        buf.resize(30);
        assert_eq!(64, buf.capacity());
        assert_eq!(30, buf.len());

        buf.resize(0);
        assert_eq!(0, buf.capacity());
        assert_eq!(0, buf.len());
    }

    #[test]
    fn test_mutable_freeze() {
        let mut buf = MutableBuffer::new(1);
        buf.extend_from_slice(b"aaaa bbbb cccc dddd");
        assert_eq!(19, buf.len());
        assert_eq!(64, buf.capacity());
        assert_eq!(b"aaaa bbbb cccc dddd", buf.data());

        let immutable_buf = buf.freeze();
        assert_eq!(19, immutable_buf.len());
        assert_eq!(64, immutable_buf.capacity());
        assert_eq!(b"aaaa bbbb cccc dddd", immutable_buf.data());
    }

    #[test]
    fn test_mutable_equal() -> Result<()> {
        let mut buf = MutableBuffer::new(1);
        let mut buf2 = MutableBuffer::new(1);

        buf.extend_from_slice(&[0xaa]);
        buf2.extend_from_slice(&[0xaa, 0xbb]);
        assert!(buf != buf2);

        buf.extend_from_slice(&[0xbb]);
        assert_eq!(buf, buf2);

        buf2.reserve(65);
        assert!(buf != buf2);

        Ok(())
    }

    #[test]
    fn test_access_concurrently() {
        let buffer = Buffer::from(vec![1, 2, 3, 4, 5]);
        let buffer2 = buffer.clone();
        assert_eq!([1, 2, 3, 4, 5], buffer.data());

        let buffer_copy = thread::spawn(move || {
            // access buffer in another thread.
            buffer
        })
        .join();

        assert!(buffer_copy.is_ok());
        assert_eq!(buffer2, buffer_copy.ok().unwrap());
    }

    macro_rules! check_as_typed_data {
        ($input: expr, $native_t: ty) => {{
            let buffer = Buffer::from($input.to_byte_slice());
            let slice: &[$native_t] = unsafe { buffer.typed_data::<$native_t>() };
            assert_eq!($input, slice);
        }};
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_as_typed_data() {
        check_as_typed_data!(&[1i8, 3i8, 6i8], i8);
        check_as_typed_data!(&[1u8, 3u8, 6u8], u8);
        check_as_typed_data!(&[1i16, 3i16, 6i16], i16);
        check_as_typed_data!(&[1i32, 3i32, 6i32], i32);
        check_as_typed_data!(&[1i64, 3i64, 6i64], i64);
        check_as_typed_data!(&[1u16, 3u16, 6u16], u16);
        check_as_typed_data!(&[1u32, 3u32, 6u32], u32);
        check_as_typed_data!(&[1u64, 3u64, 6u64], u64);
        check_as_typed_data!(&[1f32, 3f32, 6f32], f32);
        check_as_typed_data!(&[1f64, 3f64, 6f64], f64);
    }

    #[test]
    fn test_count_bits() {
        assert_eq!(0, Buffer::from(&[0b00000000]).count_set_bits());
        assert_eq!(8, Buffer::from(&[0b11111111]).count_set_bits());
        assert_eq!(3, Buffer::from(&[0b00001101]).count_set_bits());
        assert_eq!(6, Buffer::from(&[0b01001001, 0b01010010]).count_set_bits());
        assert_eq!(16, Buffer::from(&[0b11111111, 0b11111111]).count_set_bits());
    }

    #[test]
    fn test_count_bits_slice() {
        assert_eq!(
            0,
            Buffer::from(&[0b11111111, 0b00000000])
                .slice(1)
                .count_set_bits()
        );
        assert_eq!(
            8,
            Buffer::from(&[0b11111111, 0b11111111])
                .slice(1)
                .count_set_bits()
        );
        assert_eq!(
            3,
            Buffer::from(&[0b11111111, 0b11111111, 0b00001101])
                .slice(2)
                .count_set_bits()
        );
        assert_eq!(
            6,
            Buffer::from(&[0b11111111, 0b01001001, 0b01010010])
                .slice(1)
                .count_set_bits()
        );
        assert_eq!(
            16,
            Buffer::from(&[0b11111111, 0b11111111, 0b11111111, 0b11111111])
                .slice(2)
                .count_set_bits()
        );
    }

    #[test]
    fn test_count_bits_offset_slice() {
        assert_eq!(8, Buffer::from(&[0b11111111]).count_set_bits_offset(0, 8));
        assert_eq!(3, Buffer::from(&[0b11111111]).count_set_bits_offset(0, 3));
        assert_eq!(5, Buffer::from(&[0b11111111]).count_set_bits_offset(3, 5));
        assert_eq!(1, Buffer::from(&[0b11111111]).count_set_bits_offset(3, 1));
        assert_eq!(0, Buffer::from(&[0b11111111]).count_set_bits_offset(8, 0));
        assert_eq!(2, Buffer::from(&[0b01010101]).count_set_bits_offset(0, 3));
        assert_eq!(
            16,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(0, 16)
        );
        assert_eq!(
            10,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(0, 10)
        );
        assert_eq!(
            10,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(3, 10)
        );
        assert_eq!(
            8,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(8, 8)
        );
        assert_eq!(
            5,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(11, 5)
        );
        assert_eq!(
            0,
            Buffer::from(&[0b11111111, 0b11111111]).count_set_bits_offset(16, 0)
        );
        assert_eq!(
            2,
            Buffer::from(&[0b01101101, 0b10101010]).count_set_bits_offset(7, 5)
        );
        assert_eq!(
            4,
            Buffer::from(&[0b01101101, 0b10101010]).count_set_bits_offset(7, 9)
        );
    }
}
