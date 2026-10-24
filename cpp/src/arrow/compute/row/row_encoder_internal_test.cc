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
#include "arrow/buffer.h"
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
    std::array<int64_t, kBatchLength> lengths{};
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
  std::vector<int64_t> lengths(length, 0);
  encoder->AddLength(value, length, lengths.data());

  std::vector<int64_t> offsets(length + 1, 0);
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

TEST(TestKeyEncoder, LARGE_MEMORY_TEST(ValueLengthOver2GB)) {
  constexpr int64_t kValueLength = (int64_t{1} << 31) + 13;
  constexpr int64_t kEncodedValueOverhead = sizeof(uint8_t) + sizeof(int64_t);
  // A single value exceeds the signed 32-bit length limit. Only its length is
  // inspected, so the buffer need not be initialized or copied by this test.
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> bytes, AllocateBuffer(kValueLength));
  LargeBinaryScalar scalar(bytes);
  VarLengthKeyEncoder<LargeBinaryType> encoder(large_binary());
  int64_t length = 0;
  encoder.AddLength(ExecValue(&scalar), /*batch_length=*/1, &length);
  // The encoded value includes a null byte and an int64_t length prefix.
  ASSERT_EQ(length, kValueLength + kEncodedValueOverhead);

  // Length calculation must not narrow either scalar or array values, nor the
  // combined length of multiple columns in a single encoded row.
  // Reuse the same buffer through the array path and add to the existing length
  // rather than resetting it, as when encoding a second column in the same row.
  auto offsets = Buffer::FromVector<int64_t>({0, kValueLength});
  auto array = ArrayData::Make(large_binary(), /*length=*/1, {nullptr, offsets, bytes},
                               /*null_count=*/0);
  encoder.AddLength(ExecValue(*array), /*batch_length=*/1, &length);
  ASSERT_EQ(length, 2 * (kValueLength + kEncodedValueOverhead));
}

TEST(TestRowEncoder, LARGE_MEMORY_TEST(AppendOver2GB)) {
  constexpr int64_t kValueLength = 64 * 1024;
  constexpr int32_t kBatchLength = 16385;
  constexpr int32_t kNumBatches = 2;
  constexpr int32_t kNumRows = kBatchLength * kNumBatches;
  constexpr int32_t kNumDecodedValues = 4;
  ExecContext ctx;
  RowEncoder encoder;
  encoder.Init({large_utf8()}, &ctx);
  // Each batch encodes 16,385 * (64 KiB + 9) bytes, below the signed 32-bit
  // offset limit. The second append takes the persistent row buffer over 2 GiB.
  // Scalar input avoids allocating another expanded copy of those strings.
  const auto scalar = std::make_shared<LargeStringScalar>(std::string(kValueLength, 'x'));
  const ExecBatch batch({scalar}, kBatchLength);
  ASSERT_OK(encoder.EncodeAndAppend(ExecSpan(batch)));
  ASSERT_OK(encoder.EncodeAndAppend(ExecSpan(batch)));
  ASSERT_EQ(encoder.num_rows(), kNumRows);

  // Read rows on both sides of the signed 32-bit offset boundary, including
  // the special all-null row, without decoding the entire large buffer.
  const int32_t row_ids[] = {0, kBatchLength - 1, kNumRows - 2, kNumRows - 1,
                             RowEncoder::kRowIdForNulls()};
  ASSERT_OK_AND_ASSIGN(auto decoded, encoder.Decode(kNumDecodedValues + 1, row_ids));
  ASSERT_OK_AND_ASSIGN(auto expected, MakeArrayFromScalar(*scalar, kNumDecodedValues));
  ASSERT_OK_AND_ASSIGN(auto nulls, MakeArrayOfNull(large_utf8(), /*length=*/1));
  AssertArraysEqual(*expected,
                    *decoded[0].make_array()->Slice(/*offset=*/0, kNumDecodedValues));
  AssertArraysEqual(*nulls, *decoded[0].make_array()->Slice(kNumDecodedValues));
  ASSERT_EQ(encoder.encoded_row(0), encoder.encoded_row(kNumRows - 1));

  // Clear must also reset the large offsets so that the encoder can be reused.
  encoder.Clear();
  ASSERT_EQ(encoder.num_rows(), 0);
  ASSERT_OK(encoder.EncodeAndAppend(ExecSpan(ExecBatch({scalar}, /*length=*/1))));
  ASSERT_EQ(encoder.num_rows(), 1);
}

TEST(TestRowEncoder, LARGE_MEMORY_TEST(FixedWidthDecodeAt2GB)) {
  constexpr int32_t kValueLength = 64 * 1024;
  constexpr int32_t kNumDecodedRows = 32 * 1024;
  constexpr int64_t kDecodedBytes = int64_t{kNumDecodedRows} * kValueLength;
  const std::string value(kValueLength, 'x');
  const auto scalar = std::make_shared<FixedSizeBinaryScalar>(value);
  ExecContext ctx;
  RowEncoder encoder;
  encoder.Init({fixed_size_binary(kValueLength)}, &ctx);
  ASSERT_OK(encoder.EncodeAndAppend(ExecSpan(ExecBatch({scalar}, /*length=*/1))));
  ASSERT_EQ(encoder.num_rows(), 1);

  // Repeating a single row keeps the encoded input small. Only the decoded
  // output reaches 32,768 * 64 KiB = 2 GiB, exceeding the signed 32-bit size
  // limit independently of the offsets used to store encoded rows.
  const std::vector<int32_t> row_ids(kNumDecodedRows, 0);
  ASSERT_OK_AND_ASSIGN(auto decoded, encoder.Decode(kNumDecodedRows, row_ids.data()));
  const auto array =
      std::static_pointer_cast<FixedSizeBinaryArray>(decoded[0].make_array());
  ASSERT_OK(array->ValidateFull());
  ASSERT_EQ(array->length(), kNumDecodedRows);
  ASSERT_EQ(array->null_count(), 0);
  ASSERT_EQ(array->data()->buffers[1]->size(), kDecodedBytes);
  // Check values throughout the output without allocating a second 2 GiB array.
  for (const int32_t row_id : {0, kNumDecodedRows / 2, kNumDecodedRows - 1}) {
    ASSERT_EQ(array->GetView(row_id), value);
  }
}

}  // namespace arrow::compute::internal
