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
use super::memory::*;

#[cfg(windows)]
#[link(name = "msvcrt")]
extern "C" {
    fn _aligned_free(prt: *const u8);
}

/// Buffer builder with zero-copy build method
pub struct Builder<T> {
    data: *mut T,
    len: usize,
    capacity: usize,
}

impl<T> Builder<T> {
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
            data: unsafe { mem::transmute::<*const u8, *mut T>(buffer) },
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the internal byte-aligned memory buffer as a mutable slice
    pub fn slice_mut(&self, start: usize, end: usize) -> &mut [T] {
        assert!(start <= end);
        assert!(start < self.len as usize);
        assert!(end <= self.len as usize);
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
        assert!(!self.data.is_null());
        if self.len == self.capacity {
            let new_capacity = self.capacity;
            self.grow(new_capacity * 2);
        }
        assert!(self.len < self.capacity);
        unsafe {
            *self.data.offset(self.len as isize) = v;
        }
        self.len += 1;
    }

    /// push a slice of type T, growing the internal buffer as needed
    pub fn push_slice(&mut self, slice: &[T]) {
        self.reserve(slice.len());
        let sz = mem::size_of::<T>();
        unsafe {
            libc::memcpy(
                mem::transmute::<*mut T, *mut libc::c_void>(self.data.offset(self.len() as isize)),
                mem::transmute::<*const T, *const libc::c_void>(slice.as_ptr()),
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
        unsafe {
            let old_buffer = self.data;
            let new_buffer = allocate_aligned((new_capacity * sz) as i64).unwrap();
            libc::memcpy(
                mem::transmute::<*const u8, *mut libc::c_void>(new_buffer),
                mem::transmute::<*const T, *const libc::c_void>(old_buffer),
                self.len * sz,
            );
            self.capacity = new_capacity;
            self.data = mem::transmute::<*const u8, *mut T>(new_buffer);
            libc::free(mem::transmute::<*mut T, *mut libc::c_void>(old_buffer));
        }
    }

    /// Build a Buffer from the existing memory
    pub fn finish(&mut self) -> Buffer<T> {
        assert!(!self.data.is_null());
        let p = unsafe { mem::transmute::<*mut T, *const T>(self.data) };
        self.data = ptr::null_mut(); // ensure builder cannot be re-used
        Buffer::from_raw_parts(p, self.len as i32)
    }
}

impl<T> Drop for Builder<T> {
    #[cfg(windows)]
    fn drop(&mut self) {
        if !self.data.is_null() {
            unsafe {
                let p = mem::transmute::<*const T, *const u8>(self.data);
                _aligned_free(p);
            }
        }
    }

    #[cfg(not(windows))]
    fn drop(&mut self) {
        if !self.data.is_null() {
            unsafe {
                let p = mem::transmute::<*const T, *mut libc::c_void>(self.data);
                libc::free(p);
            }
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
    fn test_builder_i32() {
        let mut b: Builder<i32> = Builder::with_capacity(5);
        for i in 0..5 {
            b.push(i);
        }
        let a = b.finish();
        assert_eq!(5, a.len());
        for i in 0..5 {
            assert_eq!(&i, a.get(i as usize));
        }
    }

    #[test]
    fn test_builder_i32_grow_buffer() {
        let mut b: Builder<i32> = Builder::with_capacity(2);
        for i in 0..5 {
            b.push(i);
        }
        let a = b.finish();
        assert_eq!(5, a.len());
        for i in 0..5 {
            assert_eq!(&i, a.get(i as usize));
        }
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

        let s = String::from_utf8(buffer.iter().collect::<Vec<u8>>()).unwrap();
        assert_eq!("Hello, World!", s);
    }

}
