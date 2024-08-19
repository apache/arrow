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
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

namespace arrow::compute::internal {

// GH-43733: Test that the key encoder can handle boolean scalar values well.
TEST(TestKeyEncoder, BooleanScalar) {
  for (auto scalar : {BooleanScalar{}, BooleanScalar{true}, BooleanScalar{false}}) {
    BooleanKeyEncoder key_encoder;
    SCOPED_TRACE("scalar " + scalar.ToString());
    constexpr int64_t batch_length = 10;
    std::array<int32_t, batch_length> lengths{};
    std::array<std::array<uint8_t, 2>, batch_length> payloads{};
    key_encoder.AddLength(ExecValue{&scalar}, /*batch_length=*/10, lengths.data());
    std::array<uint8_t*, batch_length> payload_ptrs{};
    auto reset_and_get_payload_ptrs = [&]() -> uint8_t** {
      for (int i = 0; i < batch_length; ++i) {
        payload_ptrs[i] = payloads[i].data();
      }
      return payload_ptrs.data();
    };
    auto data_payload = reset_and_get_payload_ptrs();
    ASSERT_OK(key_encoder.Encode(ExecValue{&scalar}, batch_length, data_payload));
    data_payload = reset_and_get_payload_ptrs();
    ASSERT_OK_AND_ASSIGN(
        auto array_data,
        key_encoder.Decode(data_payload, batch_length, ::arrow::default_memory_pool()));
    ASSERT_EQ(batch_length, array_data->length);
    if (!scalar.is_valid) {
      ASSERT_EQ(batch_length, array_data->null_count);
    } else {
      ASSERT_EQ(0, array_data->null_count);
      auto boolean_array = std::make_shared<BooleanArray>(array_data);
      if (scalar.value) {
        ASSERT_EQ(batch_length, boolean_array->true_count());
      } else {
        ASSERT_EQ(batch_length, boolean_array->false_count());
      }
    }
  }
}

}  // namespace arrow::compute::internal
