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
// bloom_filter_block_inc.h: only clang lowers that body to a single vptest;
// gcc and MSVC emit a longer horizontal vpor reduction.
bool FindHashBlockAvx2(const uint32_t* block, const uint32_t* salt, uint32_t key) {
  using batch = xsimd::batch<uint32_t, xsimd::avx2>;
  const batch mask = batch(uint32_t{1})
                     << ((batch(key) * batch::load_unaligned(salt)) >> 27);
  const batch miss = xsimd::bitwise_andnot(mask, batch::load_unaligned(block));
  // `miss != 0` (one extra vpcmpeqd) is deliberate: reinterpreting `miss`
  // directly as a batch_bool would skip the compare but feed non-canonical
  // lane values into batch_bool, which relies on xsimd's AVX2 backend
  // lowering none() to a whole-register vptest. That lowering is not part
  // of xsimd's documented contract.
  return xsimd::none(miss != batch(uint32_t{0}));
}

}  // namespace parquet::internal
