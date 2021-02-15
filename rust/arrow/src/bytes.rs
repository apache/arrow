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

//! This module contains an implementation of a contiguous immutable memory region that knows
//! how to de-allocate itself, [`Bytes`].

use core::slice;
use std::{fmt::Debug, fmt::Formatter};
use std::{ptr::NonNull, sync::Arc};

use crate::ffi;

use super::{alloc, alloc::NativeType};

/// Mode of deallocating memory regions
pub enum Deallocation {
    /// Native deallocation, using Rust deallocator with Arrow-specific memory aligment
    Native(usize),
    // Foreign interface, via a callback
    Foreign(Arc<ffi::FFI_ArrowArray>),
}

impl Debug for Deallocation {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Deallocation::Native(capacity) => {
                write!(f, "Deallocation::Native {{ capacity: {} }}", capacity)
            }
            Deallocation::Foreign(_) => {
                write!(f, "Deallocation::Foreign {{ capacity: unknown }}")
            }
        }
    }
}

/// A continuous, fixed-size, immutable memory region that knows how to de-allocate itself.
/// This structs' API is inspired by the `bytes::Bytes`, but it is not limited to using rust's
/// global allocator nor u8 aligmnent.
///
/// In the most common case, this buffer is allocated using [`allocate_aligned`](memory::allocate_aligned)
/// and deallocated accordingly [`free_aligned`](memory::free_aligned).
/// When the region is allocated by an foreign allocator, [Deallocation::Foreign], this calls the
/// foreign deallocator to deallocate the region when it is no longer needed.
pub struct Bytes<T: NativeType> {
    /// The raw pointer to be begining of the region
    ptr: NonNull<T>,

    /// The number of bytes visible to this region. This is always smaller than its capacity (when avaliable).
    len: usize,

    /// how to deallocate this region
    deallocation: Deallocation,
}

impl<T: NativeType> Bytes<T> {
    /// Takes ownership of an allocated memory region,
    ///
    /// # Arguments
    ///
    /// * `ptr` - Pointer to raw parts
    /// * `len` - Length of raw parts in **bytes**
    /// * `capacity` - Total allocated memory for the pointer `ptr`, in **bytes**
    ///
    /// # Safety
    ///
    /// This function is unsafe as there is no guarantee that the given pointer is valid for `len`
    /// bytes. If the `ptr` and `capacity` come from a `Buffer`, then this is guaranteed.
    #[inline]
    pub unsafe fn new(
        ptr: std::ptr::NonNull<T>,
        len: usize,
        deallocation: Deallocation,
    ) -> Self {
        Self {
            ptr,
            len,
            deallocation,
        }
    }

    #[inline]
    fn as_slice(&self) -> &[T] {
        self
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn ptr(&self) -> NonNull<T> {
        self.ptr
    }

    pub fn capacity(&self) -> usize {
        match self.deallocation {
            Deallocation::Native(capacity) => capacity,
            // we cannot determine this in general,
            // and thus we state that this is externally-owned memory
            Deallocation::Foreign(_) => 0,
        }
    }
}

impl<T: NativeType> Drop for Bytes<T> {
    #[inline]
    fn drop(&mut self) {
        match &self.deallocation {
            Deallocation::Native(capacity) => {
                unsafe { alloc::free_aligned(self.ptr, *capacity) };
            }
            // foreign interface knows how to deallocate itself.
            Deallocation::Foreign(_) => (),
        }
    }
}

impl<T: NativeType> std::ops::Deref for Bytes<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl<T: NativeType> PartialEq for Bytes<T> {
    fn eq(&self, other: &Bytes<T>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: NativeType> Debug for Bytes<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Bytes {{ ptr: {:?}, len: {}, data: ", self.ptr, self.len,)?;

        f.debug_list().entries(self.iter()).finish()?;

        write!(f, " }}")
    }
}

// This is sound because `Bytes` is an immutable container
unsafe impl<T: NativeType> Send for Bytes<T> {}
unsafe impl<T: NativeType> Sync for Bytes<T> {}
