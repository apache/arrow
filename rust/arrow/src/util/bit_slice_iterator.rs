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
/// Bit slice representation of buffer data
#[derive(Debug)]
pub struct BufferBitSlice<'a> {
    buffer_data: &'a [u8],
    bit_slice: &'a BitSlice<LocalBits, u8>,
}

impl<'a> BufferBitSlice<'a> {
    ///
    /// Creates a bit slice over the given data
    #[inline]
    pub fn new(buffer_data: &'a [u8]) -> Self {
        let bit_slice = BitSlice::<LocalBits, _>::from_slice(buffer_data).unwrap();

        BufferBitSlice {
            buffer_data,
            bit_slice,
        }
    }

    ///
    /// Returns immutable view with the given offset in bits and length in bits.
    /// This view have zero-copy representation over the actual data.
    #[inline(always)]
    pub fn view(&self, offset_in_bits: usize, len_in_bits: usize) -> Self {
        Self {
            buffer_data: self.buffer_data,
            bit_slice: &self.bit_slice[offset_in_bits..offset_in_bits + len_in_bits],
        }
    }

    ///
    /// Returns bit chunks in native 64-bit allocation size.
    /// Native representations in Arrow follows 64-bit convention.
    /// Chunks can still be reinterpreted in any primitive type lower than u64.
    #[inline(always)]
    pub fn chunks<T>(&self) -> BufferBitChunksExact<T>
    where
        T: BitMemory,
    {
        let offset_size_in_bits = 8 * std::mem::size_of::<T>();
        let chunks_exact = self.bit_slice.chunks_exact(offset_size_in_bits);
        let remainder_bits = chunks_exact.remainder();
        let remainder: T = if remainder_bits.len() == 0 {
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
    /// Converts the bit view into the Buffer.
    /// Buffer is always byte-aligned and well-aligned.
    #[inline]
    pub fn as_buffer(&self) -> Buffer {
        Buffer::from(self.bit_slice.as_slice())
    }
}

///
/// Exact chunk view over the bit slice
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
    /// Interprets underlying chunk's view's bits as a given type.
    #[inline(always)]
    pub fn interpret(self) -> impl Iterator<Item = T> + 'a
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
        let result = bit_slice.chunks().interpret().collect::<Vec<u64>>();

        assert_eq!(vec![0x0706050403020100], result);
    }

    #[test]
    fn test_bit_slice_iter_unaligned() {
        let input: &[u8] = &[
            0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000,
            0b00100000, 0b01000000, 0b11111111,
        ];
        let buffer: Buffer = Buffer::from(input);

        let bit_slice = buffer.bit_slice().view(4, 64);
        let chunks = bit_slice.chunks::<u64>();

        assert_eq!(0, chunks.remainder_bit_len());
        assert_eq!(0, chunks.remainder_bits());

        let result = chunks.interpret().collect::<Vec<u64>>();

        assert_eq!(
            vec![0b1111_01000000_00100000_00010000_00001000_00000100_00000010_00000001_0000],
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

        let bit_slice = buffer.bit_slice().view(4, 66);
        let chunks = bit_slice.chunks::<u64>();

        assert_eq!(2, chunks.remainder_bit_len());
        assert_eq!(0b00000011, chunks.remainder_bits());

        let result = chunks.interpret().collect::<Vec<u64>>();

        assert_eq!(
            vec![0b1111_01000000_00100000_00010000_00001000_00000100_00000010_00000001_0000],
            result
        );
    }

    #[test]
    fn test_bit_slice_iter_unaligned_remainder_bits_across_bytes() {
        let input: &[u8] = &[0b00111111, 0b11111100];
        let buffer: Buffer = Buffer::from(input);

        // remainder contains bits from both bytes
        // result should be the highest 2 bits from first byte followed by lowest 5 bits of second bytes
        let bit_slice = buffer.bit_slice().view(6, 7);
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

        let bit_slice = buffer.bit_slice().view(2, 63);
        let chunks = bit_slice.chunks::<u64>();

        assert_eq!(63, chunks.remainder_bit_len());
        assert_eq!(
            0b1000000_00111111_11000000_00111111_11000000_00111111_11000000_00111111,
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
        let bit_slice = buffer.bit_slice().view(0, buffer_slice.len() * 8);
        // Let's also get a chunked bits as u8, not u64 this time...
        let chunks = bit_slice.chunks::<u8>();

        let result = chunks.interpret().collect::<Vec<_>>();
        assert_eq!(buffer_slice.to_vec(), result);
    }
}
