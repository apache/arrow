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
//! Note that this is a low-level functionality of this crate.

use std::{fmt::Debug, fmt::Formatter};
use std::{mem::ManuallyDrop, sync::Arc};

use crate::ffi;

/// Mode of deallocating memory regions
pub enum Deallocation {
    /// Native deallocation, using the native deallocator
    Native,
    /// Foreign interface, via a callback
    Foreign(Arc<ffi::FFI_ArrowArray>),
}

impl Debug for Deallocation {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Deallocation::Native => {
                write!(f, "Deallocation::Native")
            }
            Deallocation::Foreign(_) => {
                write!(f, "Deallocation::Foreign {{ capacity: unknown }}")
            }
        }
    }
}

/// A continuous, fixed-size, immutable memory region that knows how to de-allocate itself.
/// This structs' API is inspired by the `bytes::Bytes`, but it is not limited to using rust's
/// global allocator, which is useful for FFI.
///
/// In the most common case, this buffer's memory is managed by the Global allocator.
/// When the region is allocated by an foreign allocator, [Deallocation::Foreign], this calls the
/// foreign deallocator to deallocate the region when it is no longer needed.
pub struct Bytes {
    /// The bytes
    data: ManuallyDrop<Vec<u8>>,

    /// how to deallocate this region
    deallocation: Deallocation,
}

unsafe impl Send for Bytes {}
unsafe impl Sync for Bytes {}

impl Bytes {
    pub unsafe fn new(data: Vec<u8>, deallocation: Deallocation) -> Self {
        Self {
            data: ManuallyDrop::new(data),
            deallocation,
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data[..]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        match self.deallocation {
            Deallocation::Native => self.data.capacity(),
            // we cannot determine this in general,
            // and thus we state that this is externally-owned memory
            Deallocation::Foreign(_) => 0,
        }
    }
}

impl Drop for Bytes {
    #[inline]
    fn drop(&mut self) {
        match &self.deallocation {
            Deallocation::Native => {
                unsafe { ManuallyDrop::drop(&mut self.data) };
            }
            // foreign interface deallocates itself. As such, we forget the vector
            Deallocation::Foreign(_) => {}
        }
    }
}

impl PartialEq for Bytes {
    fn eq(&self, other: &Bytes) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Debug for Bytes {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Bytes {{ data: ")?;

        f.debug_list().entries(self.as_slice().iter()).finish()?;

        write!(f, " }}")
    }
}
