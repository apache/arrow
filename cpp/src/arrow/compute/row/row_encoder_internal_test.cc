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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/compute/row/row_encoder_internal.h"

#include "arrow/array/validate.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

namespace arrow::compute::internal {

// GH-43733: Test that the key encoder can handle boolean scalar values well.
TEST(TestKeyEncoder, BooleanScalar) {
  for (auto scalar : {BooleanScalar{}, BooleanScalar{true}, BooleanScalar{false}}) {
    BooleanKeyEncoder key_encoder;
    SCOPED_TRACE("scalar " + scalar.ToString());
    constexpr int64_t kBatchLength = 10;
    std::array<int32_t, kBatchLength> lengths{};
    key_encoder.AddLength(ExecValue{&scalar}, kBatchLength, lengths.data());
    // Check that the lengths are all 2.
    constexpr int32_t kPayloadWidth =
        BooleanKeyEncoder::kByteWidth + BooleanKeyEncoder::kExtraByteForNull;
    for (int i = 0; i < kBatchLength; ++i) {
      ASSERT_EQ(kPayloadWidth, lengths[i]);
    }
    std::array<std::array<uint8_t, kPayloadWidth>, kBatchLength> payloads{};
    std::array<uint8_t*, kBatchLength> payload_ptrs{};
    // Reset the payload pointers to point to the beginning of each payload.
    // This is necessary because the key encoder may have modified the pointers.
    auto reset_payload_ptrs = [&payload_ptrs, &payloads]() {
      std::transform(payloads.begin(), payloads.end(), payload_ptrs.begin(),
                     [](auto& payload) -> uint8_t* { return payload.data(); });
    };
    reset_payload_ptrs();
    ASSERT_OK(key_encoder.Encode(ExecValue{&scalar}, kBatchLength, payload_ptrs.data()));
    reset_payload_ptrs();
    ASSERT_OK_AND_ASSIGN(auto array_data,
                         key_encoder.Decode(payload_ptrs.data(), kBatchLength,
                                            ::arrow::default_memory_pool()));
    ASSERT_EQ(kBatchLength, array_data->length);
    auto boolean_array = std::make_shared<BooleanArray>(array_data);
    ASSERT_OK(arrow::internal::ValidateArrayFull(*array_data));
    ASSERT_OK_AND_ASSIGN(
        auto expected_array,
        MakeArrayFromScalar(scalar, kBatchLength, ::arrow::default_memory_pool()));
    AssertArraysEqual(*expected_array, *boolean_array);
  }
}

}  // namespace arrow::compute::internal
