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

#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
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

TEST(TestKeyEncoder, ListScalar) {
  // Test with a list of int32_t.
  {
    // Handle non null values.
    auto element_encoder = std::make_shared<FixedWidthKeyEncoder>(::arrow::int32());
    ListKeyEncoder<ListType> key_encoder{::arrow::list(::arrow::int32()),
                                         ::arrow::int32(), element_encoder};
    auto element_array = ::arrow::ArrayFromJSON(::arrow::int32(), "[1, 2, null, 4, 5]");
    ListScalar scalar{element_array};
    constexpr int64_t kBatchLength = 10;
    std::vector<int32_t> lengths(kBatchLength);
    key_encoder.AddLength(ExecValue{&scalar}, kBatchLength, lengths.data());
    // Check that the lengths are all 5 + 5 * 5 = 30.
    constexpr int64_t kPayloadWidth = 30;
    for (int i = 0; i < kBatchLength; ++i) {
      ASSERT_EQ(kPayloadWidth, lengths[i]) << i;
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
    auto list_array = std::make_shared<ListArray>(array_data);
    ASSERT_OK(arrow::internal::ValidateArrayFull(*array_data));
    auto element_builder = std::make_shared<Int32Builder>();
    ::arrow::ListBuilder builder(default_memory_pool(), element_builder);
    for (int i = 0; i < 10; ++i) {
      ASSERT_OK(builder.Append());
      ASSERT_OK(element_builder->Append(1));
      ASSERT_OK(element_builder->Append(2));
      ASSERT_OK(element_builder->AppendNull());
      ASSERT_OK(element_builder->Append(4));
      ASSERT_OK(element_builder->Append(5));
    }
    std::shared_ptr<Array> expected_array;
    ASSERT_OK(builder.Finish(&expected_array));

    // Expect the list array to be equal to the expected array.
    AssertArraysEqual(*expected_array, *list_array);
  }
  {
    // Handle non null values.
    auto element_encoder = std::make_shared<FixedWidthKeyEncoder>(::arrow::int32());
    ListKeyEncoder<ListType> key_encoder{::arrow::list(::arrow::int32()),
                                         ::arrow::int32(), element_encoder};
    auto element_array = ::arrow::ArrayFromJSON(::arrow::int32(), "[1, 2, null, 4, 5]");
    ListScalar scalar{element_array, /*is_valid=*/false};
    constexpr int64_t kBatchLength = 10;
    std::vector<int32_t> lengths(kBatchLength);
    key_encoder.AddLength(ExecValue{&scalar}, kBatchLength, lengths.data());
    // Check that the lengths are all 5.
    constexpr int64_t kPayloadWidth = 5;
    for (int i = 0; i < kBatchLength; ++i) {
      ASSERT_EQ(kPayloadWidth, lengths[i]) << i;
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
    auto list_array = std::make_shared<ListArray>(array_data);
    ASSERT_OK(arrow::internal::ValidateArrayFull(*array_data));
    auto element_builder = std::make_shared<Int32Builder>();
    ::arrow::ListBuilder builder(default_memory_pool(), element_builder);
    for (int i = 0; i < 10; ++i) {
      ASSERT_OK(builder.AppendNull());
    }
    std::shared_ptr<Array> expected_array;
    ASSERT_OK(builder.Finish(&expected_array));

    // Expect the list array to be equal to the expected array.
    AssertArraysEqual(*expected_array, *list_array);
  }
  {
    // Handle non null values.
    auto element_encoder =
        std::make_shared<VarLengthKeyEncoder<StringType>>(::arrow::utf8());
    ListKeyEncoder<ListType> key_encoder{::arrow::list(::arrow::utf8()), ::arrow::utf8(),
                                         element_encoder};
    auto element_array = ::arrow::ArrayFromJSON(::arrow::utf8(), R"(["a", "bcd", null])");
    ListScalar scalar{element_array};
    constexpr int64_t kBatchLength = 10;
    std::vector<int32_t> lengths(kBatchLength);
    key_encoder.AddLength(ExecValue{&scalar}, kBatchLength, lengths.data());
    // Check that the lengths are all 5 + 5 * 3 + 4 = 24.
    constexpr int64_t kPayloadWidth = 24;
    for (int i = 0; i < kBatchLength; ++i) {
      ASSERT_EQ(kPayloadWidth, lengths[i]) << i;
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
    auto list_array = std::make_shared<ListArray>(array_data);
    ASSERT_OK(arrow::internal::ValidateArrayFull(*array_data));
    auto element_builder = std::make_shared<StringBuilder>();
    ::arrow::ListBuilder builder(default_memory_pool(), element_builder);
    for (int i = 0; i < 10; ++i) {
      ASSERT_OK(builder.Append());
      ASSERT_OK(element_builder->Append("a"));
      ASSERT_OK(element_builder->Append("bcd"));
      ASSERT_OK(element_builder->AppendNull());
    }
    std::shared_ptr<Array> expected_array;
    ASSERT_OK(builder.Finish(&expected_array));

    // Expect the list array to be equal to the expected array.
    AssertArraysEqual(*expected_array, *list_array);
  }
}

TEST(TestKeyEncoder, ListArray) {
  auto element_type = ::arrow::int32();
  auto list_type = ::arrow::list(element_type);
  auto list_array =
      ::arrow::ArrayFromJSON(list_type, "[[], [1, 2], [3], null, [4, 5, 6, 7]]");
  auto element_encoder = std::make_shared<FixedWidthKeyEncoder>(element_type);
  ListKeyEncoder<ListType> key_encoder{list_type, element_type, element_encoder};
  std::vector<int32_t> lengths(list_array->length(), 0);
  // Add the lengths of the list array.
  key_encoder.AddLength(ExecValue{*list_array->data()}, list_array->length(),
                        lengths.data());
  std::vector<std::vector<uint8_t>> payloads(list_array->length());
  for (int i = 0; i < list_array->length(); ++i) {
    payloads[i].resize(lengths[i]);
  }
  std::vector<uint8_t*> payload_ptrs(list_array->length());
  auto reset_payload_ptrs = [&payload_ptrs, &payloads]() {
    std::transform(payloads.begin(), payloads.end(), payload_ptrs.begin(),
                   [](auto& payload) -> uint8_t* { return payload.data(); });
  };
  reset_payload_ptrs();
  ASSERT_OK(key_encoder.Encode(ExecValue{*list_array->data()}, list_array->length(),
                               payload_ptrs.data()));
  reset_payload_ptrs();
  ASSERT_OK_AND_ASSIGN(
      auto array_data,
      key_encoder.Decode(payload_ptrs.data(), static_cast<int32_t>(list_array->length()),
                         ::arrow::default_memory_pool()));
  auto list_array_decoded = std::make_shared<ListArray>(array_data);
  ASSERT_OK(arrow::internal::ValidateArrayFull(*array_data));
  // check that the decoded list array is equal to the original list array.
  AssertArraysEqual(*list_array, *list_array_decoded);
}

TEST(TestRowEncoder, SupportedTypes) {
  ExecContext context;
  for (const auto& fixed_sized_types :
       {::arrow::int8(), ::arrow::int16(), ::arrow::int32(), ::arrow::int64(),
        ::arrow::uint8(), ::arrow::uint16(), ::arrow::uint32(), ::arrow::uint64(),
        ::arrow::float32(), ::arrow::float64(), ::arrow::fixed_size_binary(10)}) {
    RowEncoder encoder;
    ASSERT_OK(encoder.Init({fixed_sized_types}, &context));
  }
  for (const auto& var_len_binary_type : {::arrow::binary(), ::arrow::large_binary(),
                                          ::arrow::utf8(), ::arrow::large_utf8()}) {
    RowEncoder encoder;
    ASSERT_OK(encoder.Init({var_len_binary_type}, &context));
  }

  for (const auto& dictionary_type :
       {::arrow::dictionary(::arrow::int8(), ::arrow::utf8()),
        ::arrow::dictionary(::arrow::int8(), ::arrow::int8())}) {
    RowEncoder encoder;
    ASSERT_OK(encoder.Init({dictionary_type}, &context));
  }
}

TEST(TestRowEncoder, UnsupportedTypes) {
  ExecContext context;
  for (const auto& binary_view_type : {utf8_view(), binary_view()}) {
    RowEncoder encoder;
    ASSERT_NOT_OK(encoder.Init({binary_view_type}, &context));
  }
  for (const auto& list_type :
       {list_view(::arrow::int8()), large_list_view(::arrow::int8()),
        fixed_size_list(::arrow::int8(), 10)}) {
    RowEncoder encoder;
    ASSERT_NOT_OK(encoder.Init({list_type}, &context));
  }
  {
    RowEncoder encoder;
    auto struct_type = struct_({field("a", ::arrow::int8())});
    ASSERT_NOT_OK(encoder.Init({struct_type}, &context));
  }
  {
    RowEncoder encoder;
    auto map_type = map(::arrow::int8(), ::arrow::int8());
    ASSERT_NOT_OK(encoder.Init({map_type}, &context));
  }
  // Nested list type is unsupported currently
  {
    RowEncoder encoder;
    auto nested_list_type = list(list(::arrow::int8()));
    ASSERT_NOT_OK(encoder.Init({nested_list_type}, &context));
  }
}

}  // namespace arrow::compute::internal
