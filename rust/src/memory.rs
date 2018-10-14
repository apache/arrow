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

use super::error::{ArrowError, Result};

const ALIGNMENT: usize = 64;

#[cfg(windows)]
#[link(name = "msvcrt")]
extern "C" {
    fn _aligned_malloc(size: libc::size_t, alignment: libc::size_t) -> libc::size_t;
    fn _aligned_free(prt: *const u8);
}

#[cfg(windows)]
pub fn allocate_aligned(size: i64) -> Result<*mut u8> {
    let page = unsafe { _aligned_malloc(size as libc::size_t, ALIGNMENT as libc::size_t) };
    match page {
        0 => Err(ArrowError::MemoryError(
            "Failed to allocate memory".to_string(),
        )),
        _ => Ok(unsafe { mem::transmute::<libc::size_t, *mut u8>(page) }),
    }
}

#[cfg(not(windows))]
pub fn allocate_aligned(size: i64) -> Result<*mut u8> {
    unsafe {
        let mut page: *mut libc::c_void = mem::uninitialized();
        let result = libc::posix_memalign(&mut page, ALIGNMENT, size as usize);
        match result {
            0 => Ok(mem::transmute::<*mut libc::c_void, *mut u8>(page)),
            _ => Err(ArrowError::MemoryError(
                "Failed to allocate memory".to_string(),
            )),
        }
    }
}

#[cfg(windows)]
pub fn free_aligned(p: *const u8) {
    unsafe {
        _aligned_free(p);
    }
}

#[cfg(not(windows))]
pub fn free_aligned(p: *const u8) {
    unsafe {
        libc::free(mem::transmute::<*const u8, *mut libc::c_void>(p));
    }
}

pub fn reallocate(old_size: usize, new_size: usize, pointer: *const u8) -> Result<*const u8> {
    unsafe {
        let old_src = mem::transmute::<*const u8, *mut libc::c_void>(pointer);
        let result = allocate_aligned(new_size as i64)?;
        let dst = mem::transmute::<*const u8, *mut libc::c_void>(result);
        libc::memcpy(dst, old_src, cmp::min(old_size, new_size));
        free_aligned(pointer);
        Ok(result)
    }
}

pub unsafe fn memcpy(dst: *mut u8, src: *const u8, len: usize) {
    let src = mem::transmute::<*const u8, *const libc::c_void>(src);
    let dst = mem::transmute::<*mut u8, *mut libc::c_void>(dst);
    libc::memcpy(dst, src, len);
}

extern "C" {
    #[inline]
    pub fn memcmp(p1: *const u8, p2: *const u8, len: usize) -> i32;
}

/// Check if the pointer `p` is aligned to offset `a`.
pub fn is_aligned<T>(p: *const T, a: usize) -> bool {
    let a_minus_one = a.wrapping_sub(1);
    let pmoda = p as usize & a_minus_one;
    pmoda == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate() {
        for _ in 0..10 {
            let p = allocate_aligned(1024).unwrap();
            // make sure this is 64-byte aligned
            assert_eq!(0, (p as usize) % 64);
        }
    }

    #[test]
    fn test_is_aligned() {
        // allocate memory aligned to 64-byte
        let mut ptr = allocate_aligned(10).unwrap();
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
