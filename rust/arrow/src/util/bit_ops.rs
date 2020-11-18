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

use crate::buffer::Buffer;

use bitvec::prelude::*;
use bitvec::slice::ChunksExact;

use std::fmt::Debug;

///
/// Immutable bit slice view of `Buffer` data.
///
/// `BufferBitSlice` does not own any underlying data, but rather wraps references
/// to the underlying data in a `Buffer` and has methods for addressing and interacting with
/// individual bits
#[derive(Debug)]
pub struct BufferBitSlice<'a> {
    bit_slice: &'a BitSlice<LocalBits, u8>,
}

impl<'a> BufferBitSlice<'a> {
    ///
    /// Creates a immutable bit slice over the given data
    #[inline]
    pub fn new(buffer_data: &'a [u8]) -> Self {
        let bit_slice = BitSlice::<LocalBits, _>::from_slice(buffer_data).unwrap();

        BufferBitSlice {
            bit_slice: &bit_slice,
        }
    }

    ///
    /// Returns immutable view with the given offset in bits and length in bits.
    /// This view have zero-copy representation over the actual data.
    #[inline]
    pub fn slicing(&self, offset_in_bits: usize, len_in_bits: usize) -> Self {
        Self {
            bit_slice: &self.bit_slice[offset_in_bits..offset_in_bits + len_in_bits],
        }
    }

    ///
    /// Returns bit chunks in given byte width.
    /// This can be u64(native Arrow byte representation size) or any other unsigned primitive like:
    /// u8, u16, u32, u128 and usize.
    ///
    /// This method is generic over the given primitives to enable user to filter out
    /// any upper/lower nibble/s which is not used like:
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow::buffer::Buffer;
    /// let input: &[u8] = &[
    ///     0b11111111, 0b00000000, 0b11111111, 0b00000000,
    ///     0b11111111, 0b00000000, 0b11111111, 0b00000000,
    /// ];
    ///
    /// let buffer: Buffer = Buffer::from(input);
    /// let bit_slice = buffer.bit_slice();
    /// // Interpret bit slice as u8
    /// let chunks = bit_slice.chunks::<u8>();
    ///
    /// // Filter out null bytes for compression
    /// let bytes = chunks.into_native_iter().filter(|e| *e != 0x00_u8).collect::<Vec<u8>>();
    /// assert_eq!(bytes.len(), 4);
    /// ```
    /// Native representations in Arrow follows 64-bit convention.
    /// Chunks can still be reinterpreted in any primitive type lower than u64.
    #[inline]
    pub fn chunks<T>(&self) -> BufferBitChunksExact<T>
    where
        T: BitMemory,
    {
        let offset_size_in_bits = 8 * std::mem::size_of::<T>();
        let chunks_exact = self.bit_slice.chunks_exact(offset_size_in_bits);
        let remainder_bits = chunks_exact.remainder();
        let remainder: T = if remainder_bits.is_empty() {
            T::default()
        } else {
            remainder_bits.load::<T>()
        };
        BufferBitChunksExact {
            chunks_exact,
            remainder,
            remainder_len_in_bits: remainder_bits.len(),
        }
    }

    ///
    /// Converts the bit view into a Buffer.
    /// Buffer is always byte-aligned and well-aligned.
    #[inline]
    pub fn as_buffer(&self) -> Buffer {
        Buffer::from(self.bit_slice.as_slice())
    }

    ///
    /// Count ones in the given bit view
    #[inline]
    pub fn count_ones(&self) -> usize {
        self.bit_slice.count_ones()
    }

    ///
    /// Count zeros in the given bit view
    #[inline]
    pub fn count_zeros(&self) -> usize {
        self.bit_slice.count_zeros()
    }

    ///
    /// Get bit value at the given index in this bit view
    #[inline]
    pub fn get_bit(&self, index: usize) -> bool {
        *unsafe { self.bit_slice.get_unchecked(index) }
    }

    ///
    /// Get bits in this view as vector of booleans
    #[inline]
    pub fn typed_bits(&self) -> Vec<bool> {
        self.bit_slice.iter().copied().collect()
    }

    ///
    /// Get manipulated data as byte slice
    #[inline]
    pub fn to_slice(&self) -> &[u8] {
        self.bit_slice.as_slice()
    }
}

impl<'a> PartialEq for BufferBitSlice<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.bit_slice == other.bit_slice
    }
}

///
/// Conversion from mutable slice to immutable bit slice
impl<'a> From<&'a [u8]> for BufferBitSlice<'a> {
    fn from(data: &'a [u8]) -> Self {
        BufferBitSlice::new(data)
    }
}

///
/// Mutable bit slice view of buffer data
///
/// `BufferBitSliceMut` does not own any underlying data, but rather
/// has methods for addressing and interacting with individual bits.
#[derive(Debug)]
pub struct BufferBitSliceMut<'a> {
    bit_slice: &'a mut BitSlice<LocalBits, u8>,
}

impl<'a> BufferBitSliceMut<'a> {
    ///
    /// Creates a mutable bit slice over the given data
    #[inline]
    pub fn new(buffer_data: &'a mut [u8]) -> Self {
        let bit_slice = BitSlice::<LocalBits, _>::from_slice_mut(buffer_data).unwrap();

        BufferBitSliceMut { bit_slice }
    }

    ///
    /// Returns mutable view with the given offset in bits and length in bits.
    /// This view have zero-copy representation over the actual data.
    #[inline]
    pub fn slicing(&'a mut self, offset_in_bits: usize, len_in_bits: usize) -> Self {
        Self {
            bit_slice: &mut self.bit_slice[offset_in_bits..offset_in_bits + len_in_bits],
        }
    }

    ///
    /// Sets all bits in this slice to the given value
    #[inline]
    pub fn set_bit_all(&mut self, value: bool) {
        self.bit_slice.set_all(value)
    }

    ///
    /// Set given bit at the position to a given value
    #[inline]
    pub fn set_bit(&mut self, index: usize, value: bool) {
        unsafe { self.bit_slice.set_unchecked(index, value) }
    }

    ///
    /// Converts the bit view into a Buffer.
    /// Buffer is always byte-aligned and well-aligned.
    #[inline]
    pub fn as_buffer(&self) -> Buffer {
        Buffer::from(self.bit_slice.as_slice())
    }

    ///
    /// Count ones in the given bit view
    #[inline]
    pub fn count_ones(&self) -> usize {
        self.bit_slice.count_ones()
    }

    ///
    /// Count zeros in the given bit view
    #[inline]
    pub fn count_zeros(&self) -> usize {
        self.bit_slice.count_zeros()
    }

    ///
    /// Get bit value at the given index in this bit view
    #[inline]
    pub fn get_bit(&self, index: usize) -> bool {
        *unsafe { self.bit_slice.get_unchecked(index) }
    }

    ///
    /// Get bits in this view as vector of booleans
    #[inline]
    pub fn typed_bits(&self) -> Vec<bool> {
        self.bit_slice.iter().copied().collect()
    }

    ///
    /// Get manipulated data as byte slice
    #[inline]
    pub fn to_slice(&self) -> &[u8] {
        self.bit_slice.as_slice()
    }
}

impl<'a> PartialEq for BufferBitSliceMut<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.bit_slice == other.bit_slice
    }
}

///
/// Conversion from mutable slice to mutable bit slice
impl<'a> From<&'a mut [u8]> for BufferBitSliceMut<'a> {
    fn from(data: &'a mut [u8]) -> Self {
        BufferBitSliceMut::new(data)
    }
}

///
/// Exact chunk view over the bit slice
///
/// The view is represented as some number of aligned T-sized chunks,
/// followed by some number of remainder bits
#[derive(Clone, Debug)]
pub struct BufferBitChunksExact<'a, T>
where
    T: BitMemory,
{
    chunks_exact: ChunksExact<'a, LocalBits, u8>,
    remainder: T,
    remainder_len_in_bits: usize,
}

impl<'a, T> BufferBitChunksExact<'a, T>
where
    T: BitMemory,
{
    ///
    /// Returns remainder bit length from the exact chunk iterator
    #[inline(always)]
    pub fn remainder_bit_len(&self) -> usize {
        self.remainder_len_in_bits
    }

    ///
    /// Returns the remainder bits interpreted as given type.
    #[inline(always)]
    pub fn remainder_bits(&self) -> T {
        self.remainder
    }

    ///
    /// Returns an iterator which interprets underlying chunk's view's bits as given type.
    #[inline(always)]
    pub fn into_native_iter(self) -> impl Iterator<Item = T> + 'a
    where
        T: BitMemory,
    {
        self.chunks_exact.map(|e| e.load::<T>())
    }

    ///
    /// Returns underlying iterator as it is
    #[inline(always)]
    pub fn iter(&self) -> &ChunksExact<'a, LocalBits, u8> {
        &self.chunks_exact
    }
}

///
/// Implements consuming iterator for exact chunk iterator
impl<'a, T> IntoIterator for BufferBitChunksExact<'a, T>
where
    T: BitMemory,
{
    type Item = &'a BitSlice<LocalBits, u8>;
    type IntoIter = ChunksExact<'a, LocalBits, u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.chunks_exact
    }
}

#[cfg(all(test, target_endian = "little"))]
mod tests_bit_slices_little_endian {
    use super::*;
    use crate::datatypes::ToByteSlice;

    #[test]
    fn test_bit_slice_iter_aligned() {
        let input: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7];
        let buffer: Buffer = Buffer::from(input);

        let bit_slice = buffer.bit_slice();
        let result = bit_slice.chunks().into_native_iter().collect::<Vec<u64>>();

        assert_eq!(vec![0x0706050403020100], result);
    }

    #[test]
    fn test_bit_slice_iter_unaligned() {
        let input: &[u8] = &[
            0b11110000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000,
            0b00100000, 0b01000000, 0b11111010,
        ];
        let buffer: Buffer = Buffer::from(input);

        // After the 4th bit get a bit slice of 64 bits.
        let bit_slice = buffer.bit_slice().slicing(4, 64);
        let chunks = bit_slice.chunks::<u64>();

        // 64 bits perfectly fits.
        assert_eq!(0, chunks.remainder_bit_len());
        assert_eq!(0, chunks.remainder_bits());

        let result = chunks.into_native_iter().collect::<Vec<u64>>();

        assert_eq!(
            vec![0b1010_0100_0000_0010_0000_0001_0000_0000_1000_0000_0100_0000_0010_0000_0001_1111],
            result
        );
    }

    #[test]
    fn test_bit_slice_iter_unaligned_remainder_1_byte() {
        let input: &[u8] = &[
            0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000,
            0b00100000, 0b01000000, 0b11111111,
        ];
        let buffer: Buffer = Buffer::from(input);

        // After the 4th bit get a bit slice of 66 bits.
        let bit_slice = buffer.bit_slice().slicing(4, 66);
        let chunks = bit_slice.chunks::<u64>();

        // 66 bits doesn't fit into 64 bits primitive type, so there are 2 remainder bits.
        assert_eq!(2, chunks.remainder_bit_len());
        assert_eq!(0b00000011, chunks.remainder_bits());

        let result = chunks.into_native_iter().collect::<Vec<u64>>();

        assert_eq!(
            vec![0b1111_0100_0000_0010_0000_0001_0000_0000_1000_0000_0100_0000_0010_0000_0001_0000],
            result
        );
    }

    #[test]
    fn test_bit_slice_iter_unaligned_remainder_bits_across_bytes() {
        let input: &[u8] = &[0b00111111, 0b11111100];
        let buffer: Buffer = Buffer::from(input);

        // remainder contains bits from both bytes
        // result should be the highest 2 bits from first byte followed by lowest 5 bits of second bytes
        let bit_slice = buffer.bit_slice().slicing(6, 7);
        let chunks = bit_slice.chunks::<u64>();

        assert_eq!(7, chunks.remainder_bit_len());
        assert_eq!(0b1110000, chunks.remainder_bits());
    }

    #[test]
    fn test_bit_slice_iter_unaligned_remainder_bits_large() {
        let input: &[u8] = &[
            0b11111111, 0b00000000, 0b11111111, 0b00000000, 0b11111111, 0b00000000,
            0b11111111, 0b00000000, 0b11111111,
        ];
        let buffer: Buffer = Buffer::from(input);

        let bit_slice = buffer.bit_slice().slicing(2, 63);
        let chunks = bit_slice.chunks::<u64>();

        assert_eq!(63, chunks.remainder_bit_len());
        assert_eq!(
            0b100_0000_0011_1111_1100_0000_0011_1111_1100_0000_0011_1111_1100_0000_0011_1111,
            chunks.remainder_bits()
        );
    }

    #[test]
    fn test_bit_slice_iter_reinterpret() {
        assert_eq!(LocalBits::default(), Lsb0::default());
        let buffer_slice = &[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice();
        // Name of the bit slice comes from byte slice, since it is still on the stack and behaves similarly to Rust's byte slice.
        let buffer = Buffer::from(buffer_slice);

        // Let's get the whole buffer.
        let bit_slice = buffer.bit_slice().slicing(0, buffer_slice.len() * 8);
        // Let's also get a chunked bits as u8, not u64 this time...
        let chunks = bit_slice.chunks::<u8>();

        let result = chunks.into_native_iter().collect::<Vec<_>>();
        assert_eq!(buffer_slice.to_vec(), result);
    }
}
