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

#include "arrow/array.h"
#include "arrow/array/validate.h"
#include "arrow/scalar.h"
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

// Encodes `value` (an array, or a scalar repeated `length` times) and decodes it
// back, laying out per-row buffers the way RowEncoder does.
Result<std::shared_ptr<ArrayData>> RoundTripThroughKeyEncoder(KeyEncoder* encoder,
                                                              const ExecValue& value,
                                                              int32_t length) {
  std::vector<int32_t> lengths(length, 0);
  encoder->AddLength(value, length, lengths.data());

  std::vector<int32_t> offsets(length + 1, 0);
  for (int32_t i = 0; i < length; ++i) {
    offsets[i + 1] = offsets[i] + lengths[i];
  }
  std::vector<uint8_t> bytes(offsets[length]);
  std::vector<uint8_t*> payload_ptrs(length);
  auto reset_payload_ptrs = [&] {
    for (int32_t i = 0; i < length; ++i) {
      payload_ptrs[i] = bytes.data() + offsets[i];
    }
  };

  reset_payload_ptrs();
  ARROW_RETURN_NOT_OK(encoder->Encode(value, length, payload_ptrs.data()));
  reset_payload_ptrs();
  return encoder->Decode(payload_ptrs.data(), length, ::arrow::default_memory_pool());
}

// Round-trip view keys as an array: inline, out-of-line, empty, and null values.
TEST(TestKeyEncoder, BinaryViewArray) {
  for (const auto& ty : {utf8_view(), binary_view()}) {
    SCOPED_TRACE("type " + ty->ToString());
    auto array =
        ArrayFromJSON(ty, R"(["short", null, "a long out-of-line value", "", "x"])");

    BinaryViewKeyEncoder key_encoder(ty);
    ASSERT_OK_AND_ASSIGN(
        auto decoded, RoundTripThroughKeyEncoder(&key_encoder, ExecValue{*array->data()},
                                                 static_cast<int32_t>(array->length())));

    ASSERT_OK(arrow::internal::ValidateArrayFull(*decoded));
    ASSERT_EQ(decoded->type->id(), ty->id());
    AssertArraysEqual(*array, *MakeArray(decoded), /*verbose=*/true);
  }
}

TEST(TestKeyEncoder, BinaryViewScalar) {
  constexpr int32_t kBatchLength = 8;
  for (const auto& ty : {utf8_view(), binary_view()}) {
    SCOPED_TRACE("type " + ty->ToString());
    // Scalar input path: inline, out-of-line, and null.
    for (const auto& scalar :
         {ScalarFromJSON(ty, R"("short")"),
          ScalarFromJSON(ty, R"("a long out-of-line value")"), MakeNullScalar(ty)}) {
      SCOPED_TRACE("scalar " + scalar->ToString());
      BinaryViewKeyEncoder key_encoder(ty);
      ASSERT_OK_AND_ASSIGN(
          auto decoded, RoundTripThroughKeyEncoder(&key_encoder, ExecValue{scalar.get()},
                                                   kBatchLength));

      ASSERT_OK(arrow::internal::ValidateArrayFull(*decoded));
      ASSERT_EQ(decoded->type->id(), ty->id());
      ASSERT_OK_AND_ASSIGN(
          auto expected,
          MakeArrayFromScalar(*scalar, kBatchLength, ::arrow::default_memory_pool()));
      AssertArraysEqual(*expected, *MakeArray(decoded), /*verbose=*/true);
    }
  }
}

}  // namespace arrow::compute::internal
