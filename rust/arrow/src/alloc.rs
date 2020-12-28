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

/// This module is largely copied (with minor simplifications) from the implementation of Rust's std alloc.
/// Rust's current allocator's API is `unstable`, but it solves our main use-case: a container with a custom alignment.
/// The problem that this module solves is to offer an API to allocate, reallocate and deallocate
/// Memory blocks. Main responsibilities:
/// * safeguards against null pointers
/// * panic on Out of memory (OOM)
/// * global thread-safe tracking of allocations
/// * only allocate initialized regions (zero).
/// This module makes no assumptions about type alignments or buffer sizes. Consumers use [std::alloc::Layout] to
/// share this information with this module.
use std::alloc::handle_alloc_error;
use std::ptr::NonNull;
use std::{alloc::Layout, sync::atomic::AtomicIsize};

// If this number is not zero after all objects have been `drop`, there is a memory leak
pub static mut ALLOCATIONS: AtomicIsize = AtomicIsize::new(0);

/// A memory region
#[derive(Debug, Copy, Clone)]
pub struct MemoryBlock {
    pub ptr: NonNull<u8>,
    pub size: usize,
}

/// Returns a dangling pointer aligned with `layout.align()`.
#[inline]
pub fn dangling(layout: Layout) -> NonNull<u8> {
    // SAFETY: align is guaranteed to be non-zero
    unsafe { NonNull::new_unchecked(layout.align() as *mut u8) }
}

/// Allocates a new memory region. Returns a dangling pointer iff `layout.size() == 0`.
/// # Panic
/// This function panics whenever it is impossible to allocate a new region (e.g. OOM)
#[inline]
pub fn alloc(layout: Layout) -> MemoryBlock {
    debug_assert!(layout.align() > 0);
    unsafe {
        let size = layout.size();
        if size == 0 {
            MemoryBlock {
                ptr: dangling(layout),
                size: 0,
            }
        } else {
            let raw_ptr = std::alloc::alloc_zeroed(layout);
            let ptr = NonNull::new(raw_ptr).unwrap_or_else(|| handle_alloc_error(layout));
            ALLOCATIONS
                .fetch_add(layout.size() as isize, std::sync::atomic::Ordering::SeqCst);
            MemoryBlock { ptr, size }
        }
    }
}

/// Deallocates a previously allocated region. This can be safely called with a dangling pointer iff `layout.size() == 0`.
/// # Safety
/// This function requires the region to be allocated according to the `layout`.
#[inline]
pub unsafe fn dealloc(ptr: NonNull<u8>, layout: Layout) {
    if layout.size() != 0 {
        std::alloc::dealloc(ptr.as_ptr(), layout);
        ALLOCATIONS
            .fetch_sub(layout.size() as isize, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Initializes a [MemoryBlock] with zeros starting at `offset`.
#[inline]
unsafe fn init_zero(memory: &mut MemoryBlock, offset: usize) {
    memory
        .ptr
        .as_ptr()
        .add(offset)
        .write_bytes(0, memory.size - offset);
}

/// Grows a memory region, potentially reallocating it.
// This is similar to AllocRef::grow, but without placement, since it is a no-op, and init, since we always
// allocate initialized to 0.
#[inline]
pub unsafe fn grow(ptr: NonNull<u8>, layout: Layout, new_size: usize) -> MemoryBlock {
    let size = layout.size();
    debug_assert!(
        new_size >= size,
        "`new_size` must be greater than or equal to `memory.size()`"
    );

    if size == new_size {
        return MemoryBlock { ptr, size };
    }

    if layout.size() == 0 {
        let new_layout = Layout::from_size_align_unchecked(new_size, layout.align());
        alloc(new_layout)
    } else {
        let ptr = std::alloc::realloc(ptr.as_ptr(), layout, new_size);
        let mut memory = MemoryBlock {
            ptr: NonNull::new(ptr).unwrap_or_else(|| handle_alloc_error(layout)),
            size: new_size,
        };
        ALLOCATIONS.fetch_add(
            (new_size - size) as isize,
            std::sync::atomic::Ordering::SeqCst,
        );
        init_zero(&mut memory, size);
        memory
    }
}

// similar to AllocRef::shrink, but without placement, since it is a no-op
#[inline]
pub unsafe fn shrink(ptr: NonNull<u8>, layout: Layout, new_size: usize) -> MemoryBlock {
    let size = layout.size();
    debug_assert!(
        new_size <= size,
        "`new_size` must be smaller than or equal to `memory.size()`"
    );

    if size == new_size {
        return MemoryBlock { ptr, size };
    }

    if new_size == 0 {
        dealloc(ptr, layout);
        MemoryBlock {
            ptr: dangling(layout),
            size: 0,
        }
    } else {
        // `realloc` probably checks for `new_size < size` or something similar.
        let ptr = std::alloc::realloc(ptr.as_ptr(), layout, new_size);
        let ptr = NonNull::new(ptr).unwrap_or_else(|| handle_alloc_error(layout));
        ALLOCATIONS.fetch_sub(
            (size - new_size) as isize,
            std::sync::atomic::Ordering::SeqCst,
        );
        MemoryBlock {
            ptr,
            size: new_size,
        }
    }
}
