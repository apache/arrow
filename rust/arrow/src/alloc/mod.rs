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

//! Defines memory-related functions, such as allocate/deallocate/reallocate memory
//! regions, cache and allocation alignments.

use std::mem::align_of;
use std::ptr::NonNull;
use std::{
    alloc::{handle_alloc_error, Layout},
    sync::atomic::AtomicIsize,
};

mod alignment;
pub use alignment::ALIGNMENT;

///
/// As you can see this is global and lives as long as the program lives.
/// Be careful to not write anything to this pointer in any scenario.
/// If you use allocation methods shown here you won't have any problems.
const BYPASS_PTR: NonNull<u8> = unsafe { NonNull::new_unchecked(ALIGNMENT as *mut u8) };

// If this number is not zero after all objects have been `drop`, there is a memory leak
pub static mut ALLOCATIONS: AtomicIsize = AtomicIsize::new(0);

/// Allocates a cache-aligned memory region of `size` bytes with uninitialized values.
/// This is more performant than using [allocate_aligned_zeroed] when all bytes will have
/// an unknown or non-zero value and is semantically similar to `malloc`.
pub fn allocate_aligned(size: usize) -> NonNull<u8> {
    unsafe {
        if size == 0 {
            // In a perfect world, there is no need to request zero size allocation.
            // Currently, passing zero sized layout to alloc is UB.
            // This will dodge allocator api for any type.
            BYPASS_PTR
        } else {
            ALLOCATIONS.fetch_add(size as isize, std::sync::atomic::Ordering::SeqCst);

            let layout = Layout::from_size_align_unchecked(size, ALIGNMENT);
            let raw_ptr = std::alloc::alloc(layout);
            NonNull::new(raw_ptr).unwrap_or_else(|| handle_alloc_error(layout))
        }
    }
}

/// Allocates a cache-aligned memory region of `size` bytes with `0u8` on all of them.
/// This is more performant than using [allocate_aligned] and setting all bytes to zero
/// and is semantically similar to `calloc`.
pub fn allocate_aligned_zeroed(size: usize) -> NonNull<u8> {
    unsafe {
        if size == 0 {
            // In a perfect world, there is no need to request zero size allocation.
            // Currently, passing zero sized layout to alloc is UB.
            // This will dodge allocator api for any type.
            BYPASS_PTR
        } else {
            ALLOCATIONS.fetch_add(size as isize, std::sync::atomic::Ordering::SeqCst);

            let layout = Layout::from_size_align_unchecked(size, ALIGNMENT);
            let raw_ptr = std::alloc::alloc_zeroed(layout);
            NonNull::new(raw_ptr).unwrap_or_else(|| handle_alloc_error(layout))
        }
    }
}

/// # Safety
///
/// This function is unsafe because undefined behavior can result if the caller does not ensure all
/// of the following:
///
/// * ptr must denote a block of memory currently allocated via this allocator,
///
/// * size must be the same size that was used to allocate that block of memory,
pub unsafe fn free_aligned(ptr: NonNull<u8>, size: usize) {
    if ptr != BYPASS_PTR {
        ALLOCATIONS.fetch_sub(size as isize, std::sync::atomic::Ordering::SeqCst);
        std::alloc::dealloc(
            ptr.as_ptr(),
            Layout::from_size_align_unchecked(size, ALIGNMENT),
        );
    }
}

/// # Safety
///
/// This function is unsafe because undefined behavior can result if the caller does not ensure all
/// of the following:
///
/// * ptr must be currently allocated via this allocator,
///
/// * new_size must be greater than zero.
///
/// * new_size, when rounded up to the nearest multiple of [ALIGNMENT], must not overflow (i.e.,
/// the rounded value must be less than usize::MAX).
pub unsafe fn reallocate(
    ptr: NonNull<u8>,
    old_size: usize,
    new_size: usize,
) -> NonNull<u8> {
    if ptr == BYPASS_PTR {
        return allocate_aligned(new_size);
    }

    if new_size == 0 {
        free_aligned(ptr, old_size);
        return BYPASS_PTR;
    }

    ALLOCATIONS.fetch_add(
        new_size as isize - old_size as isize,
        std::sync::atomic::Ordering::SeqCst,
    );
    let raw_ptr = std::alloc::realloc(
        ptr.as_ptr(),
        Layout::from_size_align_unchecked(old_size, ALIGNMENT),
        new_size,
    );
    NonNull::new(raw_ptr).unwrap_or_else(|| {
        handle_alloc_error(Layout::from_size_align_unchecked(new_size, ALIGNMENT))
    })
}

/// # Safety
///
/// Behavior is undefined if any of the following conditions are violated:
///
/// * `src` must be valid for reads of `len * size_of::<u8>()` bytes.
///
/// * `dst` must be valid for writes of `len * size_of::<u8>()` bytes.
///
/// * Both `src` and `dst` must be properly aligned.
///
/// `memcpy` creates a bitwise copy of `T`, regardless of whether `T` is [`Copy`]. If `T` is not
/// [`Copy`], using both the values in the region beginning at `*src` and the region beginning at
/// `*dst` can [violate memory safety][read-ownership].
pub unsafe fn memcpy(dst: NonNull<u8>, src: NonNull<u8>, count: usize) {
    if src != BYPASS_PTR {
        std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_ptr(), count)
    }
}

pub fn is_ptr_aligned<T>(p: NonNull<u8>) -> bool {
    p.as_ptr().align_offset(align_of::<T>()) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate() {
        for _ in 0..10 {
            let p = allocate_aligned(1024);
            // make sure this is 64-byte aligned
            assert_eq!(0, (p.as_ptr() as usize) % 64);
            unsafe { free_aligned(p, 1024) };
        }
    }
}
