use libc;
use std::sync::Arc;
use std::mem;

use super::error::ArrowError;
use super::error::Result;

const ALIGNMENT: usize = 64;

pub trait MemoryPool {
  fn allocate_aligned(&self, size: usize) -> Result<*mut u8>;
  fn free(&self, ptr: *mut u8);
}

struct LibcMemoryPool;

impl MemoryPool for LibcMemoryPool {
  fn allocate_aligned(&self, size: usize) -> Result<*mut u8> {
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

  fn free(&self, ptr: *mut u8) {
    unsafe {
      libc::free(mem::transmute::<*mut u8, *mut libc::c_void>(ptr))
    }
  }
}


#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_allocate() {
    let memory_pool = LibcMemoryPool {};

    for _ in 0..10 {
      let p = memory_pool.allocate_aligned(1024).unwrap();
      // make sure this is 64-byte aligned
      assert_eq!(0, (p as usize) % 64);
    }
  }

}
