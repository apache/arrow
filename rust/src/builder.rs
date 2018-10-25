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

//! Defines a `BufferBuilder` capable of creating a `Buffer` which can be used as an internal
//! buffer in an `ArrayData` object.

use std::io::Write;
use std::marker::PhantomData;
use std::mem;

use buffer::{Buffer, MutableBuffer};
use datatypes::{ArrowPrimitiveType, ToByteSlice};
use error::{ArrowError, Result};
use util::bit_util;

/// Buffer builder with zero-copy build method
pub struct BufferBuilder<T>
where
    T: ArrowPrimitiveType,
{
    buffer: MutableBuffer,
    len: i64,
    _marker: PhantomData<T>,
}

macro_rules! impl_buffer_builder {
    ($native_ty:ident) => {
        impl BufferBuilder<$native_ty> {
            /// Creates a builder with a fixed initial capacity
            pub fn new(capacity: i64) -> Self {
                let buffer = MutableBuffer::new(capacity as usize * mem::size_of::<$native_ty>());
                Self {
                    buffer,
                    len: 0,
                    _marker: PhantomData,
                }
            }

            /// Returns the number of array elements (slots) in the builder
            pub fn len(&self) -> i64 {
                self.len
            }

            /// Returns the current capacity of the builder (number of elements)
            pub fn capacity(&self) -> i64 {
                let byte_capacity = self.buffer.capacity();
                (byte_capacity / mem::size_of::<$native_ty>()) as i64
            }

            /// Push a value into the builder, growing the internal buffer as needed
            pub fn push(&mut self, v: $native_ty) -> Result<()> {
                self.reserve(1)?;
                self.write_bytes(v.to_byte_slice(), 1)
            }

            /// Push a slice of type T, growing the internal buffer as needed
            pub fn push_slice(&mut self, slice: &[$native_ty]) -> Result<()> {
                let array_slots = slice.len() as i64;
                self.reserve(array_slots)?;
                self.write_bytes(slice.to_byte_slice(), array_slots)
            }

            /// Reserve memory for n elements of type T
            pub fn reserve(&mut self, n: i64) -> Result<()> {
                let new_capacity = self.len + n;
                if new_capacity > self.capacity() {
                    return self.grow(new_capacity);
                }
                Ok(())
            }

            /// Grow the internal buffer to `new_capacity`, where `new_capacity` is the capacity in
            /// elements of type T
            fn grow(&mut self, new_capacity: i64) -> Result<()> {
                let byte_capacity = mem::size_of::<$native_ty>() * new_capacity as usize;
                self.buffer.resize(byte_capacity)?;
                Ok(())
            }

            /// Build an immutable `Buffer` from the existing internal `MutableBuffer`'s memory
            pub fn finish(self) -> Buffer {
                self.buffer.freeze()
            }

            /// Writes a byte slice to the underlying buffer and updates the `len`, i.e. the number array
            /// elements in the builder.  Also, converts the `io::Result` required by the `Write` trait
            /// to the Arrow `Result` type.
            fn write_bytes(&mut self, bytes: &[u8], len_added: i64) -> Result<()> {
                let write_result = self.buffer.write(bytes);
                // `io::Result` has many options one of which we use, so pattern matching is overkill here
                if write_result.is_err() {
                    Err(ArrowError::MemoryError(
                        "Could not write to Buffer, not big enough".to_string(),
                    ))
                } else {
                    self.len += len_added;
                    Ok(())
                }
            }
        }
    };
}

impl_buffer_builder!(u8);
impl_buffer_builder!(u16);
impl_buffer_builder!(u32);
impl_buffer_builder!(u64);
impl_buffer_builder!(i8);
impl_buffer_builder!(i16);
impl_buffer_builder!(i32);
impl_buffer_builder!(i64);
impl_buffer_builder!(f32);
impl_buffer_builder!(f64);

impl BufferBuilder<bool> {
    /// Creates a builder with a fixed initial capacity
    pub fn new(capacity: i64) -> Self {
        let byte_capacity = bit_util::ceil(capacity, 8);
        let actual_capacity = bit_util::round_upto_multiple_of_64(byte_capacity) as usize;
        let mut buffer = MutableBuffer::new(actual_capacity);
        buffer.set_null_bits(0, actual_capacity);
        Self {
            buffer,
            len: 0,
            _marker: PhantomData,
        }
    }

    /// Returns the number of array elements (slots) in the builder
    pub fn len(&self) -> i64 {
        self.len
    }

    /// Returns the current capacity of the builder (number of elements)
    pub fn capacity(&self) -> i64 {
        let byte_capacity = self.buffer.capacity() as i64;
        byte_capacity * 8
    }

    /// Push a value into the builder, growing the internal buffer as needed
    pub fn push(&mut self, v: bool) -> Result<()> {
        self.reserve(1)?;
        if v {
            // For performance the `len` of the buffer is not updated on each push but
            // is updated in the `freeze` method instead
            unsafe {
                bit_util::set_bit_raw(self.buffer.raw_data() as *mut u8, (self.len) as usize);
            }
        }
        self.len += 1;
        Ok(())
    }

    /// Push a slice of type T, growing the internal buffer as needed
    pub fn push_slice(&mut self, slice: &[bool]) -> Result<()> {
        let array_slots = slice.len();
        for i in 0..array_slots {
            self.push(slice[i])?;
        }
        Ok(())
    }

    /// Reserve memory for n elements of type T
    pub fn reserve(&mut self, n: i64) -> Result<()> {
        let new_capacity = self.len + n;
        if new_capacity > self.capacity() {
            return self.grow(new_capacity);
        }
        Ok(())
    }

    /// Grow the internal buffer to `new_capacity`, where `new_capacity` is the capacity in
    /// elements of type T
    fn grow(&mut self, new_capacity: i64) -> Result<()> {
        let new_byte_capacity = bit_util::ceil(new_capacity, 8) as usize;
        let existing_capacity = self.buffer.capacity();
        let capacity_added = self.buffer.resize(new_byte_capacity)?;
        self.buffer.set_null_bits(existing_capacity, capacity_added);
        Ok(())
    }

    /// Build an immutable `Buffer` from the existing internal `MutableBuffer`'s memory
    pub fn finish(mut self) -> Buffer {
        // `push` does not update the buffer's `len` to it is done before `freeze` is called
        let new_buffer_len = bit_util::ceil(self.len, 8);
        self.buffer.set_len(new_buffer_len as usize);
        self.buffer.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_i32_empty() {
        let b = BufferBuilder::<i32>::new(5);
        assert_eq!(0, b.len());
        assert_eq!(16, b.capacity());
        let a = b.finish();
        assert_eq!(0, a.len());
    }

    #[test]
    fn test_builder_i32_alloc_zero_bytes() {
        let mut b = BufferBuilder::<i32>::new(0);
        b.push(123).unwrap();
        let a = b.finish();
        assert_eq!(4, a.len());
    }

    #[test]
    fn test_builder_i32() {
        let mut b = BufferBuilder::<i32>::new(5);
        for i in 0..5 {
            b.push(i).unwrap();
        }
        assert_eq!(16, b.capacity());
        let a = b.finish();
        assert_eq!(20, a.len());
    }

    #[test]
    fn test_builder_i32_grow_buffer() {
        let mut b = BufferBuilder::<i32>::new(2);
        assert_eq!(16, b.capacity());
        for i in 0..20 {
            b.push(i).unwrap();
        }
        assert_eq!(32, b.capacity());
        let a = b.finish();
        assert_eq!(80, a.len());
    }

    #[test]
    fn test_reserve() {
        let mut b = BufferBuilder::<u8>::new(2);
        assert_eq!(64, b.capacity());
        b.reserve(64).unwrap();
        assert_eq!(64, b.capacity());
        b.reserve(65).unwrap();
        assert_eq!(128, b.capacity());

        let mut b = BufferBuilder::<i32>::new(2);
        assert_eq!(16, b.capacity());
        b.reserve(16).unwrap();
        assert_eq!(16, b.capacity());
        b.reserve(17).unwrap();
        assert_eq!(32, b.capacity());
    }

    #[test]
    fn test_push_slice() {
        let mut b = BufferBuilder::<u8>::new(0);
        b.push_slice("Hello, ".as_bytes()).unwrap();
        b.push_slice("World!".as_bytes()).unwrap();
        let buffer = b.finish();
        assert_eq!(13, buffer.len());

        let mut b = BufferBuilder::<i32>::new(0);
        b.push_slice(&[32, 54]).unwrap();
        let buffer = b.finish();
        assert_eq!(8, buffer.len());
    }

    #[test]
    fn test_write_bytes() {
        let mut b = BufferBuilder::<bool>::new(4);
        b.push(false).unwrap();
        b.push(true).unwrap();
        b.push(false).unwrap();
        b.push(true).unwrap();
        assert_eq!(4, b.len());
        assert_eq!(512, b.capacity());
        let buffer = b.finish();
        assert_eq!(1, buffer.len());

        let mut b = BufferBuilder::<bool>::new(4);
        b.push_slice(&[false, true, false, true]).unwrap();
        assert_eq!(4, b.len());
        assert_eq!(512, b.capacity());
        let buffer = b.finish();
        assert_eq!(1, buffer.len());
    }

    #[test]
    fn test_write_bytes_i32() {
        let mut b = BufferBuilder::<i32>::new(4);
        let bytes = [8, 16, 32, 64].to_byte_slice();
        b.write_bytes(bytes, 4).unwrap();
        assert_eq!(4, b.len());
        assert_eq!(16, b.capacity());
        let buffer = b.finish();
        assert_eq!(16, buffer.len());
    }

    #[test]
    #[should_panic(expected = "Could not write to Buffer, not big enough")]
    fn test_write_too_many_bytes() {
        let mut b = BufferBuilder::<i32>::new(0);
        let bytes = [8, 16, 32, 64].to_byte_slice();
        b.write_bytes(bytes, 4).unwrap();
    }

    #[test]
    fn test_boolean_builder_increases_buffer_len() {
        // 00000010 01001000
        let buf = Buffer::from([72_u8, 2_u8]);
        let mut builder = BufferBuilder::<bool>::new(8);

        for i in 0..10 {
            if i == 3 || i == 6 || i == 9 {
                builder.push(true).unwrap();
            } else {
                builder.push(false).unwrap();
            }
        }
        let buf2 = builder.finish();

        assert_eq!(buf.len(), buf2.len());
        assert_eq!(buf.data(), buf2.data());
    }
}
