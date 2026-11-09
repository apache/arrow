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

namespace arrow::util::internal {

namespace {

template <typename Uint>
struct SpacedExpandRightwardDynamicFunction {
  using FunctionType = decltype(&SpacedExpandRightwardScalar<Uint>);

  static constexpr auto targets() {
    return std::array{
        ARROW_DISPATCH_TARGET_NONE(&SpacedExpandRightwardScalar<Uint>)    //
        ARROW_DISPATCH_TARGET_NEON(&SpacedExpandRightwardNeon<Uint>)      //
        ARROW_DISPATCH_TARGET_SSE4_2(&SpacedExpandRightwardSse4_2<Uint>)  //
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

}  // namespace arrow::util::internal
