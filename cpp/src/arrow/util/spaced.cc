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

#include <array>

#include "arrow/util/dispatch_internal.h"
#include "arrow/util/spaced_internal.h"
#include "arrow/util/spaced_simd_internal.h"
#include "arrow/util/spaced_simd_kernel_internal.h"
#include "arrow/util/ubsan.h"

namespace arrow::util::internal {

namespace {

// For an 8 bit chunk mask, row[mask][j] is the number of valid lanes below
// lane j if lane j is valid, 0 otherwise.
struct SourceIndexTable {
  uint8_t row[256][8] = {};
  constexpr SourceIndexTable() {
    for (unsigned m = 0; m < 256; ++m) {
      for (int j = 0, prefix = 0; j < 8; ++j) {
        if ((m >> j) & 1) row[m][j] = static_cast<uint8_t>(prefix++);
      }
    }
  }
};
constexpr SourceIndexTable kSourceIndex{};

// Chunk kernel (see spaced::ChunkedWordKernel) on 8 values without SIMD.
// Every lane finds its source through the table and is cleared with a mask,
// so that the lanes are independent and there is no branch.
template <typename Uint>
struct ExpandChunkScalar {
  static constexpr int kLanes = 8;

  static void Expand(Uint* dst, const Uint* src, unsigned mask) {
    const uint8_t* index = kSourceIndex.row[mask];
    Uint values[kLanes];
    for (int j = 0; j < kLanes; ++j) {
      values[j] = SafeLoad(src + index[j]);
    }
    for (int j = 0; j < kLanes; ++j) {
      dst[j] = values[j] & (Uint{0} - static_cast<Uint>((mask >> j) & 1));
    }
  }
};

// Words with at most that many valid values are scattered one by one.
constexpr int kSparseValues = 8;

template <typename Uint>
using Kernel = spaced::ChunkedWordKernel<Uint, ExpandChunkScalar<Uint>>;

}  // namespace

template <typename Uint>
void SpacedExpandRightwardNone(Uint* buffer, int num_values, int null_count,
                               const uint8_t* valid_bits, int64_t valid_bits_offset) {
  constexpr int kDenseNulls = 4;
  spaced::InPlaceExpander<Uint, Kernel<Uint>, kDenseNulls, kSparseValues>::Expand(
      buffer, num_values, null_count, valid_bits, valid_bits_offset);
}

template <typename Uint>
void SpacedExpandNone(const Uint* src, int num_values, int null_count,
                      const uint8_t* valid_bits, int64_t valid_bits_offset,
                      Uint* output) {
  spaced::OutOfPlaceExpander<Uint, Kernel<Uint>, kSparseValues>::Expand(
      src, num_values, null_count, valid_bits, valid_bits_offset, output);
}

template void SpacedExpandRightwardNone<uint32_t>(uint32_t*, int, int, const uint8_t*,
                                                  int64_t);
template void SpacedExpandRightwardNone<uint64_t>(uint64_t*, int, int, const uint8_t*,
                                                  int64_t);
template void SpacedExpandNone<uint32_t>(const uint32_t*, int, int, const uint8_t*,
                                         int64_t, uint32_t*);
template void SpacedExpandNone<uint64_t>(const uint64_t*, int, int, const uint8_t*,
                                         int64_t, uint64_t*);

namespace {

template <typename Uint>
struct SpacedExpandRightwardDynamicFunction {
  using FunctionType = decltype(&SpacedExpandRightwardNone<Uint>);

  static constexpr auto targets() {
    return std::array{
        ARROW_DISPATCH_TARGET_NONE(&SpacedExpandRightwardNone<Uint>)      //
        ARROW_DISPATCH_TARGET_NEON(&SpacedExpandRightwardNeon<Uint>)      //
        ARROW_DISPATCH_TARGET_SSE4_2(&SpacedExpandRightwardSse4_2<Uint>)  //
        ARROW_DISPATCH_TARGET_AVX2(&SpacedExpandRightwardAvx2<Uint>)      //
        ARROW_DISPATCH_TARGET_AVX512(&SpacedExpandRightwardAvx512<Uint>)  //
    };
  }
};

template <typename Uint>
struct SpacedExpandDynamicFunction {
  using FunctionType = decltype(&SpacedExpandNone<Uint>);

  static constexpr auto targets() {
    return std::array{
        ARROW_DISPATCH_TARGET_NONE(&SpacedExpandNone<Uint>)      //
        ARROW_DISPATCH_TARGET_NEON(&SpacedExpandNeon<Uint>)      //
        ARROW_DISPATCH_TARGET_SSE4_2(&SpacedExpandSse4_2<Uint>)  //
        ARROW_DISPATCH_TARGET_AVX2(&SpacedExpandAvx2<Uint>)      //
        ARROW_DISPATCH_TARGET_AVX512(&SpacedExpandAvx512<Uint>)  //
    };
  }
};

template <typename Uint>
void SpacedExpandRightwardDispatch(void* buffer, int num_values, int null_count,
                                   const uint8_t* valid_bits, int64_t valid_bits_offset) {
  static const ::arrow::internal::DynamicDispatch<
      SpacedExpandRightwardDynamicFunction<Uint>>
      dispatch;
  dispatch(static_cast<Uint*>(buffer), num_values, null_count, valid_bits,
           valid_bits_offset);
}

template <typename Uint>
void SpacedExpandDispatch(const void* src, int num_values, int null_count,
                          const uint8_t* valid_bits, int64_t valid_bits_offset,
                          void* output) {
  static const ::arrow::internal::DynamicDispatch<SpacedExpandDynamicFunction<Uint>>
      dispatch;
  dispatch(static_cast<const Uint*>(src), num_values, null_count, valid_bits,
           valid_bits_offset, static_cast<Uint*>(output));
}

}  // namespace

void SpacedExpandRightward32(void* buffer, int num_values, int null_count,
                             const uint8_t* valid_bits, int64_t valid_bits_offset) {
  SpacedExpandRightwardDispatch<uint32_t>(buffer, num_values, null_count, valid_bits,
                                          valid_bits_offset);
}

void SpacedExpandRightward64(void* buffer, int num_values, int null_count,
                             const uint8_t* valid_bits, int64_t valid_bits_offset) {
  SpacedExpandRightwardDispatch<uint64_t>(buffer, num_values, null_count, valid_bits,
                                          valid_bits_offset);
}

void SpacedExpand32(const void* src, int num_values, int null_count,
                    const uint8_t* valid_bits, int64_t valid_bits_offset, void* output) {
  SpacedExpandDispatch<uint32_t>(src, num_values, null_count, valid_bits,
                                 valid_bits_offset, output);
}

void SpacedExpand64(const void* src, int num_values, int null_count,
                    const uint8_t* valid_bits, int64_t valid_bits_offset, void* output) {
  SpacedExpandDispatch<uint64_t>(src, num_values, null_count, valid_bits,
                                 valid_bits_offset, output);
}

}  // namespace arrow::util::internal
