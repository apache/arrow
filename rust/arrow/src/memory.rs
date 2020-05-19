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
//! regions.

use std::alloc::Layout;
use std::mem::align_of;
use std::ptr::NonNull;

pub const ALIGNMENT: usize = 64;

///
/// As you can see this is global and lives as long as the program lives.
/// Be careful to not write anything to this pointer in any scenario.
/// If you use allocation methods shown here you won't have any problems.
const BYPASS_PTR: NonNull<u8> = unsafe { NonNull::new_unchecked(ALIGNMENT as *mut u8) };

pub fn allocate_aligned(size: usize) -> *mut u8 {
    unsafe {
        if size == 0 {
            // In a perfect world, there is no need to request zero size allocation.
            // Currently, passing zero sized layout to alloc is UB.
            // This will dodge allocator api for any type.
            BYPASS_PTR.as_ptr()
        } else {
            let layout = Layout::from_size_align_unchecked(size, ALIGNMENT);
            std::alloc::alloc_zeroed(layout)
        }
    }
}

pub unsafe fn free_aligned(ptr: *mut u8, size: usize) {
    if size != 0x00 && ptr != BYPASS_PTR.as_ptr() {
        std::alloc::dealloc(ptr, Layout::from_size_align_unchecked(size, ALIGNMENT));
    }
}

pub unsafe fn reallocate(ptr: *mut u8, old_size: usize, new_size: usize) -> *mut u8 {
    if ptr == BYPASS_PTR.as_ptr() {
        allocate_aligned(new_size)
    } else {
        let new_ptr = std::alloc::realloc(
            ptr,
            Layout::from_size_align_unchecked(old_size, ALIGNMENT),
            new_size,
        );
        if !new_ptr.is_null() && new_size > old_size {
            new_ptr.add(old_size).write_bytes(0, new_size - old_size);
        }
        new_ptr
    }
}

pub unsafe fn memcpy(dst: *mut u8, src: *const u8, len: usize) {
    if len != 0x00 && src != BYPASS_PTR.as_ptr() {
        std::ptr::copy_nonoverlapping(src, dst, len)
    }
}

extern "C" {
    pub fn memcmp(p1: *const u8, p2: *const u8, len: usize) -> i32;
}

/// Check if the pointer `p` is aligned to offset `a`.
pub fn is_aligned<T>(p: *const T, a: usize) -> bool {
    let a_minus_one = a.wrapping_sub(1);
    let pmoda = p as usize & a_minus_one;
    pmoda == 0
}

pub fn is_ptr_aligned<T>(p: *const T) -> bool {
    p.align_offset(align_of::<T>()) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate() {
        for _ in 0..10 {
            let p = allocate_aligned(1024);
            // make sure this is 64-byte aligned
            assert_eq!(0, (p as usize) % 64);
        }
    }

    #[test]
    fn test_is_aligned() {
        // allocate memory aligned to 64-byte
        let mut ptr = allocate_aligned(10);
        assert_eq!(true, is_aligned::<u8>(ptr, 1));
        assert_eq!(true, is_aligned::<u8>(ptr, 2));
        assert_eq!(true, is_aligned::<u8>(ptr, 4));

        // now make the memory aligned to 63-byte
        ptr = unsafe { ptr.offset(1) };
        assert_eq!(true, is_aligned::<u8>(ptr, 1));
        assert_eq!(false, is_aligned::<u8>(ptr, 2));
        assert_eq!(false, is_aligned::<u8>(ptr, 4));
    }
}
