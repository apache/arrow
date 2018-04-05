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

use bytes::Bytes;
use libc;
use std::mem;
use std::ptr;
use std::slice;

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
            data: unsafe { mem::transmute::<*const u8, *mut T>(buffer) }
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
        unsafe { *self.data.offset(self.len as isize) = v; }
        self.len += 1;
    }

    /// Build a Buffer from the existing memory
    pub fn build(&mut self) -> Buffer<T> {
        assert!(!self.data.is_null());
        let p = unsafe { mem::transmute::<*mut T, *const T>(self.data) };
        self.data = ptr::null_mut(); // ensure builder cannot be re-used
        Buffer {
            len: self.len as i32,
            data: p
        }
    }
}

/// Buffer<T> is essentially just a Vec<T> aligned at a 64-byte boundary
pub struct Buffer<T> {
    data: *const T,
    len: i32,
}

impl<T> Buffer<T> {
    pub fn len(&self) -> i32 {
        self.len
    }

    pub fn data(&self) -> *const T {
        self.data
    }

    pub fn slice(&self, start: usize, end: usize) -> &[T] {
        assert!(start <= end);
        assert!(start < self.len as usize);
        assert!(end <= self.len as usize);
        unsafe { slice::from_raw_parts(self.data.offset(start as isize), (end - start) as usize) }
    }

    /// Get a reference to the value at the specified offset
    pub fn get(&self, i: usize) -> &T {
        unsafe { &(*self.data.offset(i as isize)) }
    }

    /// Deprecated method (used by Bitmap)
    pub fn set(&mut self, i: usize, v: T) {
        unsafe {
            let p = mem::transmute::<*const T, *mut T>(self.data);
            *p.offset(i as isize) = v;
        }
    }

    /// Return an iterator over the values in the buffer
    pub fn iter(&self) -> BufferIterator<T> {
        BufferIterator {
            data: self.data,
            len: self.len,
            index: 0,
        }
    }
}

impl<T> Drop for Buffer<T> {
    fn drop(&mut self) {
        mem::drop(self.data)
    }
}

pub struct BufferIterator<T> {
    data: *const T,
    len: i32,
    index: isize,
}

impl<T> Iterator for BufferIterator<T>
where
    T: Copy,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.len as isize {
            self.index += 1;
            Some(unsafe { *self.data.offset(self.index - 1) })
        } else {
            None
        }
    }
}

macro_rules! array_from_primitive {
    ($DT: ty) => {
        impl From<Vec<$DT>> for Buffer<$DT> {
            fn from(v: Vec<$DT>) -> Self {
                // allocate aligned memory buffer
                let len = v.len();
                let sz = mem::size_of::<$DT>();
                let buffer = allocate_aligned((len * sz) as i64).unwrap();
                Buffer {
                    len: len as i32,
                    data: unsafe {
                        let dst = mem::transmute::<*const u8, *mut libc::c_void>(buffer);
                        libc::memcpy(
                            dst,
                            mem::transmute::<*const $DT, *const libc::c_void>(v.as_ptr()),
                            len * sz,
                        );
                        mem::transmute::<*mut libc::c_void, *const $DT>(dst)
                    },
                }
            }
        }
    };
}

array_from_primitive!(bool);
array_from_primitive!(f32);
array_from_primitive!(f64);
array_from_primitive!(u8);
array_from_primitive!(u16);
array_from_primitive!(u32);
array_from_primitive!(u64);
array_from_primitive!(i8);
array_from_primitive!(i16);
array_from_primitive!(i32);
array_from_primitive!(i64);

impl From<Bytes> for Buffer<u8> {
    fn from(bytes: Bytes) -> Self {
        // allocate aligned
        let len = bytes.len();
        let sz = mem::size_of::<u8>();
        let buf_mem = allocate_aligned((len * sz) as i64).unwrap();
        Buffer {
            len: len as i32,
            data: unsafe {
                let dst = mem::transmute::<*const u8, *mut libc::c_void>(buf_mem);
                libc::memcpy(
                    dst,
                    mem::transmute::<*const u8, *const libc::c_void>(bytes.as_ptr()),
                    len * sz,
                );
                mem::transmute::<*mut libc::c_void, *const u8>(dst)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_i32_empty() {
        let mut b: Builder<i32> = Builder::with_capacity(5);
        let a = b.build();
        assert_eq!(0, a.len());
    }

    #[test]
    fn test_builder_i32() {
        let mut b: Builder<i32> = Builder::with_capacity(5);
        for i in 0..5 {
            b.push(i);
        }
        let a = b.build();
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
        let a = b.build();
        assert_eq!(5, a.len());
        for i in 0..5 {
            assert_eq!(&i, a.get(i as usize));
        }
    }

    #[test]
    fn test_buffer_i32() {
        let b: Buffer<i32> = Buffer::from(vec![1, 2, 3, 4, 5]);
        assert_eq!(5, b.len);
    }

    #[test]
    fn test_iterator_i32() {
        let b: Buffer<i32> = Buffer::from(vec![1, 2, 3, 4, 5]);
        let it = b.iter();
        let v: Vec<i32> = it.map(|n| n + 1).collect();
        assert_eq!(vec![2, 3, 4, 5, 6], v);
    }

    #[test]
    fn test_buffer_eq() {
        let a = Buffer::from(vec![1, 2, 3, 4, 5]);
        let b = Buffer::from(vec![5, 4, 3, 2, 1]);
        let c = a.iter()
            .zip(b.iter())
            .map(|(a, b)| a == b)
            .collect::<Vec<bool>>();
        assert_eq!(c, vec![false, false, true, false, false]);
    }

    #[test]
    fn test_buffer_lt() {
        let a = Buffer::from(vec![1, 2, 3, 4, 5]);
        let b = Buffer::from(vec![5, 4, 3, 2, 1]);
        let c = a.iter()
            .zip(b.iter())
            .map(|(a, b)| a < b)
            .collect::<Vec<bool>>();
        assert_eq!(c, vec![true, true, false, false, false]);
    }

    #[test]
    fn test_buffer_gt() {
        let a = Buffer::from(vec![1, 2, 3, 4, 5]);
        let b = Buffer::from(vec![5, 4, 3, 2, 1]);
        let c = a.iter()
            .zip(b.iter())
            .map(|(a, b)| a > b)
            .collect::<Vec<bool>>();
        assert_eq!(c, vec![false, false, false, true, true]);
    }

    #[test]
    fn test_buffer_add() {
        let a = Buffer::from(vec![1, 2, 3, 4, 5]);
        let b = Buffer::from(vec![5, 4, 3, 2, 1]);
        let c = a.iter()
            .zip(b.iter())
            .map(|(a, b)| a + b)
            .collect::<Vec<i32>>();
        assert_eq!(c, vec![6, 6, 6, 6, 6]);
    }

    #[test]
    fn test_buffer_multiply() {
        let a = Buffer::from(vec![1, 2, 3, 4, 5]);
        let b = Buffer::from(vec![5, 4, 3, 2, 1]);
        let c = a.iter()
            .zip(b.iter())
            .map(|(a, b)| a * b)
            .collect::<Vec<i32>>();
        assert_eq!(c, vec![5, 8, 9, 8, 5]);
    }
}
