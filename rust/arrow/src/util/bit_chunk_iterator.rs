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
use crate::util::bit_util::ceil;
use std::fmt::Debug;

#[derive(Debug)]
pub struct BitChunks<'a> {
    buffer: &'a Buffer,
    raw_data: *const u8,
    offset: usize,
    chunk_len: usize,
    remainder_len: usize,
}

impl<'a> BitChunks<'a> {
    pub fn new(buffer: &'a Buffer, offset: usize, len: usize) -> Self {
        assert!(ceil(offset + len, 8) <= buffer.len() * 8);

        let byte_offset = offset / 8;
        let offset = offset % 8;

        let raw_data = unsafe { buffer.raw_data().add(byte_offset) };

        let chunk_bits = 64;

        let chunk_len = len / chunk_bits;
        let remainder_len = len & (chunk_bits - 1);

        BitChunks::<'a> {
            buffer: &buffer,
            raw_data,
            offset,
            chunk_len,
            remainder_len,
        }
    }
}

#[derive(Debug)]
pub struct BitChunkIterator<'a> {
    buffer: &'a Buffer,
    raw_data: *const u8,
    offset: usize,
    chunk_len: usize,
    index: usize,
}

impl<'a> BitChunks<'a> {
    #[inline]
    pub const fn remainder_len(&self) -> usize {
        self.remainder_len
    }

    #[inline]
    pub fn remainder_bits(&self) -> u64 {
        let bit_len = self.remainder_len;
        if bit_len == 0 {
            0
        } else {
            let byte_len = ceil(bit_len, 8);

            let mut bits = 0;
            for i in 0..byte_len {
                let byte = unsafe {
                    std::ptr::read(
                        self.raw_data
                            .add(self.chunk_len * std::mem::size_of::<u64>() + i),
                    )
                };
                bits |= (byte as u64) << (i * 8);
            }

            let offset = self.offset as u64;

            (bits >> offset) & ((1 << bit_len) - 1)
        }
    }

    #[inline]
    pub const fn iter(&self) -> BitChunkIterator<'a> {
        BitChunkIterator::<'a> {
            buffer: self.buffer,
            raw_data: self.raw_data,
            offset: self.offset,
            chunk_len: self.chunk_len,
            index: 0,
        }
    }
}

impl<'a> IntoIterator for BitChunks<'a> {
    type Item = u64;
    type IntoIter = BitChunkIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Iterator for BitChunkIterator<'_> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<u64> {
        if self.index >= self.chunk_len {
            return None;
        }

        // cast to *const u64 should be fine since we are using read_unaligned
        #[allow(clippy::cast_ptr_alignment)]
        let current = unsafe {
            std::ptr::read_unaligned((self.raw_data as *const u64).add(self.index))
        };

        let combined = if self.offset == 0 {
            current
        } else {
            // cast to *const u64 should be fine since we are using read_unaligned
            #[allow(clippy::cast_ptr_alignment)]
            let next = unsafe {
                std::ptr::read_unaligned(
                    (self.raw_data as *const u64).add(self.index + 1),
                )
            };
            current >> self.offset
                | (next & ((1 << self.offset) - 1)) << (64 - self.offset)
        };

        self.index += 1;

        Some(combined)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.chunk_len - self.index,
            Some(self.chunk_len - self.index),
        )
    }
}

impl ExactSizeIterator for BitChunkIterator<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.chunk_len - self.index
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::Buffer;

    #[test]
    fn test_iter_aligned() {
        let input: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7];
        let buffer: Buffer = Buffer::from(input);

        let bitchunks = buffer.bit_chunks(0, 64);
        let result = bitchunks.into_iter().collect::<Vec<_>>();

        assert_eq!(vec![0x0706050403020100], result);
    }

    #[test]
    fn test_iter_unaligned() {
        let input: &[u8] = &[
            0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000,
            0b00100000, 0b01000000, 0b11111111,
        ];
        let buffer: Buffer = Buffer::from(input);

        let bitchunks = buffer.bit_chunks(4, 64);

        assert_eq!(0, bitchunks.remainder_len());
        assert_eq!(0, bitchunks.remainder_bits());

        let result = bitchunks.into_iter().collect::<Vec<_>>();

        //assert_eq!(vec![0b00010000, 0b00100000, 0b01000000, 0b10000000, 0b00000000, 0b00000001, 0b00000010, 0b11110100], result);
        assert_eq!(
            vec![0b1111010000000010000000010000000010000000010000000010000000010000],
            result
        );
    }

    #[test]
    fn test_iter_unaligned_remainder_1_byte() {
        let input: &[u8] = &[
            0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000,
            0b00100000, 0b01000000, 0b11111111,
        ];
        let buffer: Buffer = Buffer::from(input);

        let bitchunks = buffer.bit_chunks(4, 66);

        assert_eq!(2, bitchunks.remainder_len());
        assert_eq!(0b00000011, bitchunks.remainder_bits());

        let result = bitchunks.into_iter().collect::<Vec<_>>();

        //assert_eq!(vec![0b00010000, 0b00100000, 0b01000000, 0b10000000, 0b00000000, 0b00000001, 0b00000010, 0b11110100], result);
        assert_eq!(
            vec![0b1111010000000010000000010000000010000000010000000010000000010000],
            result
        );
    }
}
