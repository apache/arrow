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
#pragma once

#include <cstdint>

// Used to make sure ODR rule isn't violated.
#ifndef PARQUET_IMPL_NAMESPACE
#  error "PARQUET_IMPL_NAMESPACE must be defined"
#endif

namespace parquet::internal::PARQUET_IMPL_NAMESPACE {

// Branchless OR-accumulator reduction: the short-circuit `return false` shape
// blocks the compiler from collapsing the 8-lane probe into a single
// horizontal block test. This shape autovectorizes to SSE/NEON at the
// baseline; AVX2 has its own xsimd kernel (see bloom_filter_avx2.cc) because
// gcc/MSVC don't lower this reduction to a single vptest.
inline bool FindHashBlockImpl(const uint32_t* block, const uint32_t* salt, uint32_t key) {
  constexpr int kBitsSetPerBlock = 8;
  uint32_t miss = 0;
  for (int i = 0; i < kBitsSetPerBlock; ++i) {
    const uint32_t mask = static_cast<uint32_t>(1) << ((key * salt[i]) >> 27);
    miss |= (~block[i] & mask);
  }
  return miss == 0;
}

}  // namespace parquet::internal::PARQUET_IMPL_NAMESPACE
