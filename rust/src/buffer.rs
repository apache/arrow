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
use std::slice;

use super::datatypes::*;
use super::memory::*;

/// Buffer<T> is essentially just a Vec<T> for fixed-width primitive types and the start of the
/// memory region is aligned at a 64-byte boundary
pub struct Buffer<T>
where
    T: ArrowPrimitiveType,
{
    /// Contiguous memory region holding instances of primitive T
    data: *const T,
    /// Number of elements in the buffer
    len: usize,
}

impl<T> Buffer<T>
where
    T: ArrowPrimitiveType,
{
    /// create a buffer from an existing region of memory (must already be byte-aligned)
    pub unsafe fn from_raw_parts(data: *const T, len: usize) -> Self {
        Buffer { data, len }
    }

    /// Get the number of elements in the buffer
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get a pointer to the data contained by the buffer
    pub fn data(&self) -> *const T {
        self.data
    }

    pub fn slice(&self, start: usize, end: usize) -> &[T] {
        assert!(end <= self.len);
        assert!(start <= end);
        unsafe { slice::from_raw_parts(self.data.offset(start as isize), end - start) }
    }

    /// Get a reference to the value at the specified offset
    pub fn get(&self, i: usize) -> &T {
        assert!(i < self.len);
        unsafe { &(*self.data.offset(i as isize)) }
    }

    /// Write to a slot in the buffer
    pub fn set(&mut self, i: usize, v: T) {
        assert!(i < self.len);
        let p = self.data as *mut T;
        unsafe {
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

/// Release the underlying memory when the Buffer goes out of scope
impl<T> Drop for Buffer<T>
where
    T: ArrowPrimitiveType,
{
    fn drop(&mut self) {
        free_aligned(self.data as *const u8);
    }
}

/// Iterator over the elements of a buffer
pub struct BufferIterator<T>
where
    T: ArrowPrimitiveType,
{
    data: *const T,
    len: usize,
    index: isize,
}

impl<T> Iterator for BufferIterator<T>
where
    T: ArrowPrimitiveType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.len as isize {
            let value = unsafe { *self.data.offset(self.index) };
            self.index += 1;
            Some(value)
        } else {
            None
        }
    }
}

/// Copy the memory from a Vec<T> into a newly allocated Buffer<T>
impl<T> From<Vec<T>> for Buffer<T>
where
    T: ArrowPrimitiveType,
{
    fn from(v: Vec<T>) -> Self {
        // allocate aligned memory buffer
        let len = v.len();
        let sz = mem::size_of::<T>();
        let buffer = allocate_aligned((len * sz) as i64).unwrap();
        Buffer {
            len,
            data: unsafe {
                let dst = mem::transmute::<*const u8, *mut libc::c_void>(buffer);
                libc::memcpy(
                    dst,
                    mem::transmute::<*const T, *const libc::c_void>(v.as_ptr()),
                    len * sz,
                );
                mem::transmute::<*mut libc::c_void, *const T>(dst)
            },
        }
    }
}

impl From<Bytes> for Buffer<u8> {
    fn from(bytes: Bytes) -> Self {
        // allocate aligned
        let len = bytes.len();
        let sz = mem::size_of::<u8>();
        let buf_mem = allocate_aligned((len * sz) as i64).unwrap();
        let dst = buf_mem as *mut libc::c_void;
        Buffer {
            len,
            data: unsafe {
                libc::memcpy(dst, bytes.as_ptr() as *const libc::c_void, len * sz);
                dst as *mut u8
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    #[should_panic]
    fn test_get_out_of_bounds() {
        let a = Buffer::from(vec![1, 2, 3, 4, 5]);
        a.get(123); // should panic
    }

    #[test]
    fn slice_empty_at_end() {
        let a = Buffer::from(vec![1, 2, 3, 4, 5]);
        let s = a.slice(5, 5);
        assert_eq!(0, s.len());
    }

    #[test]
    #[should_panic]
    fn slice_start_out_of_bounds() {
        let a = Buffer::from(vec![1, 2, 3, 4, 5]);
        a.slice(6, 6); // should panic
    }

    #[test]
    #[should_panic]
    fn slice_end_out_of_bounds() {
        let a = Buffer::from(vec![1, 2, 3, 4, 5]);
        a.slice(0, 6); // should panic
    }

    #[test]
    #[should_panic]
    fn slice_end_before_start() {
        let a = Buffer::from(vec![1, 2, 3, 4, 5]);
        a.slice(3, 2); // should panic
    }
}
