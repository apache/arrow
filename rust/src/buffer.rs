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

use std::mem;
use std::sync::Arc;

use memory;

/// Buffer is a contiguous memory region of fixed size and is aligned at a 64-byte
/// boundary. Buffer is immutable.
#[derive(PartialEq, Debug)]
pub struct Buffer {
    /// Reference-counted pointer to the internal byte buffer.
    data: Arc<BufferData>,

    /// The offset into the buffer.
    offset: usize,
}

#[derive(Debug)]
struct BufferData {
    /// The raw pointer into the buffer bytes
    ptr: *const u8,

    /// The length of the buffer
    len: usize,
}

impl PartialEq for BufferData {
    fn eq(&self, other: &BufferData) -> bool {
        if self.len != other.len {
            return false;
        }
        unsafe { memory::memcmp(self.ptr, other.ptr, self.len as usize) == 0 }
    }
}

/// Release the underlying memory when the current buffer goes out of scope
impl Drop for BufferData {
    fn drop(&mut self) {
        memory::free_aligned(self.ptr);
    }
}

impl Buffer {
    /// Creates a buffer from an existing memory region (must already be byte-aligned)
    pub fn from_raw_parts(ptr: *const u8, len: usize) -> Self {
        let buf_data = BufferData { ptr: ptr, len: len };
        Buffer {
            data: Arc::new(buf_data),
            offset: 0,
        }
    }

    /// Returns the number of bytes in the buffer
    pub fn len(&self) -> usize {
        self.data.len as usize
    }

    /// Returns whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.data.len == 0
    }

    /// Returns the byte slice stored in this buffer
    pub fn data(&self) -> &[u8] {
        unsafe { ::std::slice::from_raw_parts(self.data.ptr, self.data.len) }
    }

    /// Returns a raw pointer for this buffer.
    ///
    /// Note that this should be used cautiously, and the returned pointer should not be
    /// stored anywhere, to avoid dangling pointers.
    pub fn raw_data(&self) -> *const u8 {
        self.data.ptr
    }

    /// Returns an empty buffer.
    pub fn empty() -> Buffer {
        Buffer::from_raw_parts(::std::ptr::null(), 0)
    }
}

impl Clone for Buffer {
    fn clone(&self) -> Buffer {
        Buffer {
            data: self.data.clone(),
            offset: self.offset,
        }
    }
}

/// Creating a `Buffer` instance by copying the memory from a `Vec<u8>` into a newly
/// allocated memory region.
impl<T: AsRef<[u8]>> From<T> for Buffer {
    fn from(p: T) -> Self {
        // allocate aligned memory buffer
        let slice = p.as_ref();
        let len = slice.len() * mem::size_of::<u8>();
        let buffer = memory::allocate_aligned((len) as i64).unwrap();
        unsafe {
            memory::memcpy(buffer, slice.as_ptr(), len);
        }
        Buffer::from_raw_parts(buffer, len)
    }
}

unsafe impl Sync for Buffer {}
unsafe impl Send for Buffer {}

#[cfg(test)]
mod tests {
    use std::ptr::null_mut;
    use std::thread;

    use super::Buffer;

    #[test]
    fn test_buffer_data_equality() {
        let buf1 = Buffer::from(&[0, 1, 2, 3, 4]);
        let mut buf2 = Buffer::from(&[0, 1, 2, 3, 4]);
        assert_eq!(buf1, buf2);

        buf2 = Buffer::from(&[0, 0, 2, 3, 4]);
        assert!(buf1 != buf2);

        buf2 = Buffer::from(&[0, 1, 2, 3]);
        assert!(buf1 != buf2);
    }

    #[test]
    fn test_buffer_from_raw_parts() {
        let buf = Buffer::from_raw_parts(null_mut(), 0);
        assert_eq!(0, buf.len());
        assert_eq!(0, buf.data().len());
        assert!(buf.raw_data().is_null());

        let buf = Buffer::from(&[0, 1, 2, 3, 4]);
        assert_eq!(5, buf.len());
        assert!(!buf.raw_data().is_null());
        assert_eq!(&[0, 1, 2, 3, 4], buf.data());
    }

    #[test]
    fn test_buffer_from_vec() {
        let buf = Buffer::from(&[0, 1, 2, 3, 4]);
        assert_eq!(5, buf.len());
        assert!(!buf.raw_data().is_null());
        assert_eq!(&[0, 1, 2, 3, 4], buf.data());
    }

    #[test]
    fn test_buffer_copy() {
        let buf = Buffer::from(&[0, 1, 2, 3, 4]);
        let buf2 = buf.clone();
        assert_eq!(5, buf2.len());
        assert!(!buf2.raw_data().is_null());
        assert_eq!(&[0, 1, 2, 3, 4], buf2.data());
    }

    #[test]
    fn test_access_buffer_concurrently() {
        let buffer = Buffer::from(vec![1, 2, 3, 4, 5]);
        let buffer2 = buffer.clone();
        assert_eq!(&[1, 2, 3, 4, 5], buffer.data());

        let buffer_copy = thread::spawn(move || {
            // access buffer in another thread.
            buffer.clone()
        }).join();

        assert!(buffer_copy.is_ok());
        assert_eq!(buffer2, buffer_copy.ok().unwrap());
    }
}
