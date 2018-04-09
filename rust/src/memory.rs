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
use std::mem;

use super::error::ArrowError;

const ALIGNMENT: usize = 64;

pub fn allocate_aligned(size: i64) -> Result<*const u8, ArrowError> {
    unsafe {
        let mut page: *mut libc::c_void = mem::uninitialized();
        let result = libc::posix_memalign(&mut page, ALIGNMENT, size as usize);
        match result {
            0 => Ok(mem::transmute::<*mut libc::c_void, *const u8>(page)),
            _ => Err(ArrowError::MemoryError(
                "Failed to allocate memory".to_string(),
            )),
        }
    }
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

}
