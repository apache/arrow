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

use libc;
use std::cmp;
use std::mem;
use std::ptr;
use std::slice;

use super::buffer::*;
use super::datatypes::*;
use super::memory::*;

/// Buffer builder with zero-copy build method
pub struct Builder<T>
where
    T: ArrowPrimitiveType,
{
    data: *mut T,
    len: usize,
    capacity: usize,
}

impl<T> Builder<T>
where
    T: ArrowPrimitiveType,
{
    /// Creates a builder with a default capacity
    pub fn new() -> Self {
        Builder::with_capacity(64)
    }

    /// Creates a builder with a fixed capacity
    pub fn with_capacity(capacity: usize) -> Self {
        let sz = mem::size_of::<T>();
        let buffer = allocate_aligned((capacity * sz) as i64).unwrap();
        Builder {
            len: 0,
            capacity,
            data: buffer as *mut T,
        }
    }

    /// Get the number of elements in the builder
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get the capacity of the builder (number of elements)
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the internal byte-aligned memory buffer as a mutable slice
    pub fn slice_mut(&mut self, start: usize, end: usize) -> &mut [T] {
        assert!(
            end <= self.capacity as usize,
            "the end of the slice must be within the capacity"
        );
        assert!(
            start <= end,
            "the start of the slice cannot exceed the end of the slice"
        );
        unsafe {
            slice::from_raw_parts_mut(self.data.offset(start as isize), (end - start) as usize)
        }
    }

    /// Override the length
    pub fn set_len(&mut self, len: usize) {
        self.len = len;
    }

    /// Push a value into the builder, growing the internal buffer as needed
    pub fn push(&mut self, v: T) {
        assert!(!self.data.is_null(), "cannot push onto uninitialized data");
        if self.len == self.capacity {
            // grow capacity by 64 bytes or double the current capacity, whichever is greater
            let new_capacity = cmp::max(64, self.capacity * 2);
            self.grow(new_capacity);
        }
        assert!(self.len < self.capacity, "new length exceeds capacity");
        unsafe {
            *self.data.offset(self.len as isize) = v;
        }
        self.len += 1;
    }

    /// Set a value at a slot in the allocated memory without adjusting the length
    pub fn set(&mut self, i: usize, v: T) {
        assert!(
            !self.data.is_null(),
            "cannot set value if data is uninitialized"
        );
        assert!(i < self.capacity, "index exceeds capacity");
        unsafe {
            *self.data.offset(i as isize) = v;
        }
    }

    /// push a slice of type T, growing the internal buffer as needed
    pub fn push_slice(&mut self, slice: &[T]) {
        self.reserve(slice.len());
        let sz = mem::size_of::<T>();
        unsafe {
            libc::memcpy(
                self.data.offset(self.len() as isize) as *mut libc::c_void,
                slice.as_ptr() as *const libc::c_void,
                slice.len() * sz,
            );
        }
        self.len += slice.len();
    }

    /// Reserve memory for n elements of type T
    pub fn reserve(&mut self, n: usize) {
        if self.len + n > self.capacity {
            let new_capacity = cmp::max(self.capacity * 2, n);
            self.grow(new_capacity);
        }
    }

    /// Grow the buffer to the new size n (number of elements of type T)
    fn grow(&mut self, new_capacity: usize) {
        let sz = mem::size_of::<T>();
        let old_buffer = self.data;
        let new_buffer = allocate_aligned((new_capacity * sz) as i64).unwrap();
        unsafe {
            libc::memcpy(
                new_buffer as *mut libc::c_void,
                old_buffer as *const libc::c_void,
                self.len * sz,
            );
        }
        self.capacity = new_capacity;
        self.data = new_buffer as *mut T;
        free_aligned(old_buffer as *const u8);
    }

    /// Build a Buffer from the existing memory
    pub fn finish(&mut self) -> Buffer {
        assert!(!self.data.is_null(), "data has not been initialized");
        let p = self.data;
        self.data = ptr::null_mut(); // ensure builder cannot be re-used
        Buffer::from_raw_parts(p as *mut u8, self.len)
    }
}

impl<T> Drop for Builder<T>
where
    T: ArrowPrimitiveType,
{
    fn drop(&mut self) {
        if !self.data.is_null() {
            free_aligned(self.data as *const u8);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_i32_empty() {
        let mut b: Builder<i32> = Builder::with_capacity(5);
        let a = b.finish();
        assert_eq!(0, a.len());
    }

    #[test]
    fn test_builder_i32_alloc_zero_bytes() {
        let mut b: Builder<i32> = Builder::with_capacity(0);
        b.push(123);
        let a = b.finish();
        assert_eq!(1, a.len());
    }

    #[test]
    fn test_builder_i32() {
        let mut b: Builder<i32> = Builder::with_capacity(5);
        for i in 0..5 {
            b.push(i);
        }
        let a = b.finish();
        assert_eq!(5, a.len());
    }

    #[test]
    fn test_builder_i32_grow_buffer() {
        let mut b: Builder<i32> = Builder::with_capacity(2);
        for i in 0..5 {
            b.push(i);
        }
        let a = b.finish();
        assert_eq!(5, a.len());
    }

    #[test]
    fn test_reserve() {
        let mut b: Builder<u8> = Builder::with_capacity(2);
        assert_eq!(2, b.capacity());
        b.reserve(2);
        assert_eq!(2, b.capacity());
        b.reserve(3);
        assert_eq!(4, b.capacity());
    }

    #[test]
    fn test_push_slice() {
        let mut b: Builder<u8> = Builder::new();
        b.push_slice("Hello, ".as_bytes());
        b.push_slice("World!".as_bytes());
        let buffer = b.finish();
        assert_eq!(13, buffer.len());
    }

    #[test]
    fn test_slice_empty_at_end() {
        let mut b: Builder<u8> = Builder::with_capacity(2);
        let s = b.slice_mut(2, 2);
        assert_eq!(0, s.len());
    }

    #[test]
    #[should_panic(expected = "the end of the slice must be within the capacity")]
    fn test_slice_start_out_of_bounds() {
        let mut b: Builder<u8> = Builder::with_capacity(2);
        b.slice_mut(3, 3); // should panic
    }

    #[test]
    #[should_panic(expected = "the end of the slice must be within the capacity")]
    fn test_slice_end_out_of_bounds() {
        let mut b: Builder<u8> = Builder::with_capacity(2);
        b.slice_mut(0, 3); // should panic
    }

    #[test]
    #[should_panic(expected = "the start of the slice cannot exceed the end of the slice")]
    fn test_slice_end_before_start() {
        let mut b: Builder<u8> = Builder::with_capacity(2);
        b.slice_mut(1, 0); // should panic
    }
}
