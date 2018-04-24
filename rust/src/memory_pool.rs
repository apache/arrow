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

use super::error::ArrowError;
use super::error::Result;

const ALIGNMENT: usize = 64;

/// Memory pool for allocating memory. It's also responsible for tracking memory usage.
pub trait MemoryPool {
    /// Allocate memory.
    /// The implementation should ensures that allocated memory is aligned.
    fn allocate(&self, size: usize) -> Result<*mut u8>;

    /// Reallocate memory.
    /// If the implementation doesn't support reallocating aligned memory, it allocates new memory
    /// and copied old memory to it.
    fn reallocate(&self, old_size: usize, new_size: usize, pointer: *mut u8) -> Result<*mut u8>;

    /// Free memory.
    fn free(&self, ptr: *mut u8);
}

/// Implementation of memory pool using libc api.
#[allow(dead_code)]
struct LibcMemoryPool;

impl MemoryPool for LibcMemoryPool {
    fn allocate(&self, size: usize) -> Result<*mut u8> {
        unsafe {
            let mut page: *mut libc::c_void = mem::uninitialized();
            let result = libc::posix_memalign(&mut page, ALIGNMENT, size);
            match result {
                0 => Ok(mem::transmute::<*mut libc::c_void, *mut u8>(page)),
                _ => Err(ArrowError::MemoryError(
                    "Failed to allocate memory".to_string(),
                )),
            }
        }
    }

    fn reallocate(&self, old_size: usize, new_size: usize, pointer: *mut u8) -> Result<*mut u8> {
        unsafe {
            let old_src = mem::transmute::<*mut u8, *mut libc::c_void>(pointer);
            let result = self.allocate(new_size)?;
            let dst = mem::transmute::<*mut u8, *mut libc::c_void>(result);
            libc::memcpy(dst, old_src, cmp::min(old_size, new_size));
            libc::free(old_src);
            Ok(result)
        }
    }

    fn free(&self, ptr: *mut u8) {
        unsafe { libc::free(mem::transmute::<*mut u8, *mut libc::c_void>(ptr)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate() {
        let memory_pool = LibcMemoryPool {};

        for _ in 0..10 {
            let p = memory_pool.allocate(1024).unwrap();
            // make sure this is 64-byte aligned
            assert_eq!(0, (p as usize) % ALIGNMENT);
            memory_pool.free(p);
        }
    }

    #[test]
    fn test_reallocate() {
        let memory_pool = LibcMemoryPool {};

        for _ in 0..10 {
            let p1 = memory_pool.allocate(1024).unwrap();
            let p2 = memory_pool.reallocate(1024, 2048, p1).unwrap();
            // make sure this is 64-byte aligned
            assert_eq!(0, (p1 as usize) % ALIGNMENT);
            assert_eq!(0, (p2 as usize) % ALIGNMENT);
            memory_pool.free(p2);
        }
    }
}
