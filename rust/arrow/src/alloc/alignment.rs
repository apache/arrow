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

// NOTE: Below code is written for spatial/temporal prefetcher optimizations. Memory allocation
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
pub const ALIGNMENT: usize = 1 << 6;

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
