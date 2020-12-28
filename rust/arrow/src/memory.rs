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

//! This module is built on top of [arrow::alloc] and its main responsibility is to offer an API
//! to allocate/reallocate and deallocate memory aligned with cache-lines.

use std::alloc::Layout;
use std::ptr::NonNull;

use crate::{alloc, util::bit_util};

// NOTE: Below code (`ALIGNMENT`) is written for spatial/temporal prefetcher optimizations. Memory allocation
// should align well with usage pattern of cache access and block sizes on layers of storage levels from
// registers to non-volatile memory. These alignments are all cache aware alignments incorporated
// from [cuneiform](https://crates.io/crates/cuneiform) crate. This approach mimicks Intel TBB's
// cache_aligned_allocator which exploits cache locality and minimizes prefetch signals
// resulting in less round trip time between the layers of storage.
// For further info: https://software.intel.com/en-us/node/506094

// 32-bit architecture and things other than netburst microarchitecture are using 64 bytes.
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "x86")]
pub const ALIGNMENT: usize = 1 << 6;

// Intel x86_64:
// L2D streamer from L1:
// Loads data or instructions from memory to the second-level cache. To use the streamer,
// organize the data or instructions in blocks of 128 bytes, aligned on 128 bytes.
// - https://www.intel.com/content/dam/www/public/us/en/documents/manuals/64-ia-32-architectures-optimization-manual.pdf
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "x86_64")]
pub const ALIGNMENT: usize = 1 << 7;

// 24Kc:
// Data Line Size
// - https://s3-eu-west-1.amazonaws.com/downloads-mips/documents/MD00346-2B-24K-DTS-04.00.pdf
// - https://gitlab.e.foundation/e/devices/samsung/n7100/stable_android_kernel_samsung_smdk4412/commit/2dbac10263b2f3c561de68b4c369bc679352ccee
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "mips")]
pub const ALIGNMENT: usize = 1 << 5;
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "mips64")]
pub const ALIGNMENT: usize = 1 << 5;

// Defaults for powerpc
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "powerpc")]
pub const ALIGNMENT: usize = 1 << 5;

// Defaults for the ppc 64
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "powerpc64")]
pub const ALIGNMENT: usize = 1 << 6;

// e.g.: sifive
// - https://github.com/torvalds/linux/blob/master/Documentation/devicetree/bindings/riscv/sifive-l2-cache.txt#L41
// in general all of them are the same.
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "riscv")]
pub const ALIGNMENT: usize = 1 << 6;

// This size is same across all hardware for this architecture.
// - https://docs.huihoo.com/doxygen/linux/kernel/3.7/arch_2s390_2include_2asm_2cache_8h.html
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "s390x")]
pub const ALIGNMENT: usize = 1 << 8;

// This size is same across all hardware for this architecture.
// - https://docs.huihoo.com/doxygen/linux/kernel/3.7/arch_2sparc_2include_2asm_2cache_8h.html#a9400cc2ba37e33279bdbc510a6311fb4
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "sparc")]
pub const ALIGNMENT: usize = 1 << 5;
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "sparc64")]
pub const ALIGNMENT: usize = 1 << 6;

// On ARM cache line sizes are fixed. both v6 and v7.
// Need to add board specific or platform specific things later.
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "thumbv6")]
pub const ALIGNMENT: usize = 1 << 5;
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "thumbv7")]
pub const ALIGNMENT: usize = 1 << 5;

// Operating Systems cache size determines this.
// Currently no way to determine this without runtime inference.
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "wasm32")]
pub const ALIGNMENT: usize = FALLBACK_ALIGNMENT;

// Same as v6 and v7.
// List goes like that:
// Cortex A, M, R, ARM v7, v7-M, Krait and NeoverseN uses this size.
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "arm")]
pub const ALIGNMENT: usize = 1 << 5;

// Combined from 4 sectors. Volta says 128.
// Prevent chunk optimizations better to go to the default size.
// If you have smaller data with less padded functionality then use 32 with force option.
// - https://devtalk.nvidia.com/default/topic/803600/variable-cache-line-width-/
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "nvptx")]
pub const ALIGNMENT: usize = 1 << 7;
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "nvptx64")]
pub const ALIGNMENT: usize = 1 << 7;

// This size is same across all hardware for this architecture.
/// Cache and allocation multiple alignment size
#[cfg(target_arch = "aarch64")]
pub const ALIGNMENT: usize = 1 << 6;

#[doc(hidden)]
/// Fallback cache and allocation multiple alignment size
const FALLBACK_ALIGNMENT: usize = 1 << 6;

/// A struct to keep track of cache-aligned contiguous memory regions.
/// Similar to `std::alloc::RawVec<u8>`, with the following differences:
/// * (re)allocates along (arch-specific) cache lines
/// * (re)allocates in multiples of 64 bytes.
/// * (re)allocates initialized with zeros.
#[derive(Debug)]
pub struct RawBytes {
    // pointer is dangling iff cap == 0
    ptr: NonNull<u8>,
    cap: usize,
}

impl RawBytes {
    /// Creates a [RawBytes] without allocations.
    pub fn new() -> Self {
        Self {
            // safe: ALIGNMENT > 0
            ptr: alloc::dangling(unsafe {
                Layout::from_size_align_unchecked(0, ALIGNMENT)
            }),
            cap: 0,
        }
    }

    /// Creates a [RawBytes] with at least `capacity` bytes initialized to zero.
    ///
    /// # Panics
    ///
    /// Panics if the requested capacity exceeds `isize::MAX` bytes.
    ///
    /// # Aborts
    ///
    /// Aborts on OOM.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::allocate(capacity)
    }

    /// Gets the capacity of the allocation.
    #[inline(always)]
    pub const fn capacity(&self) -> usize {
        self.cap
    }

    #[inline]
    fn allocate(capacity: usize) -> Self {
        let capacity = bit_util::round_upto_multiple_of_64(capacity);
        let layout = Layout::from_size_align(capacity, ALIGNMENT)
            .unwrap_or_else(|_| capacity_overflow());
        alloc_guard(layout.size());

        let memory = alloc::alloc(layout);
        Self {
            ptr: memory.ptr,
            cap: memory.size,
        }
    }

    /// Returns if the buffer needs to grow to fulfill the needed extra capacity.
    #[inline]
    fn needs_to_grow(&self, len: usize, additional: usize) -> bool {
        additional > self.capacity().wrapping_sub(len)
    }

    /// reserves `additional` bytes assuming that the underlying container has size `len`,
    /// guaranteeing that [RawBytes] has at least `len + additional` bytes.
    #[inline]
    pub fn reserve(&mut self, len: usize, additional: usize) {
        if self.needs_to_grow(len, additional) {
            self.grow(len, additional)
        }
    }

    #[inline]
    fn set_memory(&mut self, memory: alloc::MemoryBlock) {
        self.ptr = memory.ptr;
        self.cap = memory.size;
    }

    // equivalent to `RawVec::grow_amortized(); RawVec::finish()` with the following differences:
    // * panics instead of `Result` (i.e. overflows crash)
    // * capacity changes are always multiples of 64
    // * alignment is constant and equal to `ALIGNMENT`
    #[inline]
    fn grow(&mut self, len: usize, additional: usize) {
        let capacity = len
            .checked_add(additional)
            .unwrap_or_else(|| capacity_overflow());
        let capacity = bit_util::round_upto_multiple_of_64(capacity);
        let capacity = std::cmp::max(self.cap * 2, capacity);

        let new_layout = Layout::from_size_align(capacity, ALIGNMENT)
            .unwrap_or_else(|_| capacity_overflow());

        alloc_guard(new_layout.size());

        let memory = if let Some((ptr, old_capacity)) = self.current_memory() {
            let old_layout =
                unsafe { Layout::from_size_align_unchecked(old_capacity, ALIGNMENT) };
            debug_assert_eq!(old_layout.align(), new_layout.align());
            unsafe { alloc::grow(ptr, old_layout, new_layout.size()) }
        } else {
            alloc::alloc(new_layout)
        };

        self.set_memory(memory);
    }

    #[inline]
    fn current_memory(&self) -> Option<(NonNull<u8>, usize)> {
        if self.cap == 0 {
            // the pointer is not even valid, so we do not even expose it.
            None
        } else {
            Some((self.ptr, self.cap))
        }
    }

    /// Returns the pointer to the start of the allocation. Note that this is
    /// a dangling pointer iff `capacity == 0`.
    #[inline(always)]
    pub fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }
}

impl Drop for RawBytes {
    fn drop(&mut self) {
        if let Some((ptr, capacity)) = self.current_memory() {
            // this is safe because we own `ptr` and it is allocated (the invariant that ptr is valid iff cap > 0)
            unsafe { dealloc(ptr, capacity) }
        }
    }
}

/// Deallocates a memory region previously allocated by [RawBytes].
/// # Safety
/// Do not use if the pointer was not allocated via `RawBytes`.
pub(super) unsafe fn dealloc(ptr: NonNull<u8>, capacity: usize) {
    alloc::dealloc(ptr, Layout::from_size_align_unchecked(capacity, ALIGNMENT))
}

// We need to guarantee the following:
// * We don't ever allocate `> isize::MAX` byte-size objects.
// * We don't overflow `usize::MAX` and actually allocate too little.
//
// On 64-bit we just need to check for overflow since trying to allocate
// `> isize::MAX` bytes will surely fail. On 32-bit and 16-bit we need to add
// an extra guard for this in case we're running on a platform which can use
// all 4GB in user-space, e.g., PAE or x32.
#[inline]
fn alloc_guard(alloc_size: usize) {
    if std::mem::size_of::<usize>() < 8 && alloc_size > isize::MAX as usize {
        panic!("capacity overflow");
    }
}

// One central function responsible for reporting capacity overflows. This'll
// ensure that the code generation related to these panics is minimal as there's
// only one location which panics rather than a bunch throughout the module.
fn capacity_overflow() -> ! {
    panic!("capacity overflow");
}

/// Check if the pointer `p` is aligned with `T`
pub fn is_ptr_aligned<T>(p: NonNull<u8>) -> bool {
    p.as_ptr().align_offset(std::mem::align_of::<T>()) == 0
}
