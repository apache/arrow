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

#include <xsimd/xsimd.hpp>

#include "parquet/bloom_filter_avx2_internal.h"

namespace parquet::internal {

// Parquet SBBF probe (256-bit block, 8 SALT-derived bits per probe). Unrelated
// to cpp/src/arrow/acero/bloom_filter_avx2.cc -- Acero's blocked bloom filter
// is a different algorithm (64-bit block, ~4 bits per probe, in-memory only)
// and the two kernels are not interchangeable. See the Parquet spec for the
// SBBF on-disk layout this kernel must match.
//
// Spelled in xsimd rather than reusing the autovectorized body in
// bloom_filter_block_impl_internal.h: only clang lowers that body to a single vptest;
// gcc and MSVC emit a longer horizontal vpor reduction.
bool FindHashBlockAvx2(
    std::span<const uint32_t, BlockSplitBloomFilter::kBitsSetPerBlock> block,
    std::span<const uint32_t, BlockSplitBloomFilter::kBitsSetPerBlock> salt,
    uint32_t key) {
  static_assert(BlockSplitBloomFilter::kBitsSetPerBlock == 8,
                "AVX2 SBBF probe kernel assumes 8 bits set per 256-bit block");
  // Pin to avx2: the default-arch batch widens to 16 lanes under an AVX-512
  // baseline and would over-read the 32-byte block.
  using batch = xsimd::batch<uint32_t, xsimd::avx2>;
  const batch mask = batch(uint32_t{1}) << xsimd::bitwise_rshift<27>(
                         batch(key) * batch::load_unaligned(salt.data()));
  const batch miss = xsimd::bitwise_andnot(mask, batch::load_unaligned(block.data()));
  // `miss != 0` (one extra vpcmpeqd) is deliberate: reinterpreting `miss`
  // directly as a batch_bool would skip the compare but feed non-canonical
  // lane values into batch_bool, which relies on xsimd's AVX2 backend
  // lowering none() to a whole-register vptest. That lowering is not part
  // of xsimd's documented contract.
  return xsimd::none(miss != batch(uint32_t{0}));
}

}  // namespace parquet::internal
