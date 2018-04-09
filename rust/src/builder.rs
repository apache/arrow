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
use std::mem;
use std::ptr;

use super::buffer::*;
use super::memory::*;

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

    /// Push a value into the builder, growing the internal buffer as needed
    pub fn push(&mut self, v: T) {
        assert!(!self.data.is_null());
        if self.len == self.capacity {
            let sz = mem::size_of::<T>();
            let new_capacity = self.capacity * 2;
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
                mem::drop(old_buffer);
            }
        }
        assert!(self.len < self.capacity);
        unsafe {
            *self.data.offset(self.len as isize) = v;
        }
        self.len += 1;
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
    fn drop(&mut self) {
        if !self.data.is_null() {
            mem::drop(self.data)
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

}
