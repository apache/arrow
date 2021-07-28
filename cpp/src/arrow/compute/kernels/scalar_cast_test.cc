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

#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/buffer.h"
#include "arrow/chunked_array.h"
#include "arrow/extension_type.h"
#include "arrow/status.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"

#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/test_util.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {

static std::shared_ptr<Array> InvalidUtf8(std::shared_ptr<DataType> type) {
  return ArrayFromJSON(type,
                       "["
                       R"(
                       "Hi",
                       "olá mundo",
                       "你好世界",
                       "",
                       )"
                       "\"\xa0\xa1\""
                       "]");
}

static std::vector<std::shared_ptr<DataType>> kNumericTypes = {
    uint8(), int8(),   uint16(), int16(),   uint32(),
    int32(), uint64(), int64(),  float32(), float64()};

static std::vector<std::shared_ptr<DataType>> kDictionaryIndexTypes = {
    int8(), uint8(), int16(), uint16(), int32(), uint32(), int64(), uint64()};

static std::vector<std::shared_ptr<DataType>> kBaseBinaryTypes = {
    binary(), utf8(), large_binary(), large_utf8()};

static void AssertBufferSame(const Array& left, const Array& right, int buffer_index) {
  ASSERT_EQ(left.data()->buffers[buffer_index].get(),
            right.data()->buffers[buffer_index].get());
}

static void CheckCast(std::shared_ptr<Array> input, std::shared_ptr<Array> expected,
                      CastOptions options = CastOptions{}) {
  options.to_type = expected->type();
  CheckScalarUnary("cast", input, expected, &options);
}

static void CheckCastFails(std::shared_ptr<Array> input, CastOptions options) {
  ASSERT_RAISES(Invalid, Cast(input, options))
      << "\n  to_type: " << options.to_type->ToString()
      << "\n  input:   " << input->ToString();

  if (input->type_id() == Type::EXTENSION) {
    // ExtensionScalar not implemented
    return;
  }

  // For the scalars, check that at least one of the input fails (since many
  // of the tests contains a mix of passing and failing values). In some
  // cases we will want to check more precisely
  int64_t num_failing = 0;
  for (int64_t i = 0; i < input->length(); ++i) {
    ASSERT_OK_AND_ASSIGN(auto scalar, input->GetScalar(i));
    num_failing += static_cast<int>(Cast(scalar, options).status().IsInvalid());
  }
  ASSERT_GT(num_failing, 0);
}

static void CheckCastZeroCopy(std::shared_ptr<Array> input,
                              std::shared_ptr<DataType> to_type,
                              CastOptions options = CastOptions::Safe()) {
  ASSERT_OK_AND_ASSIGN(auto converted, Cast(*input, to_type, options));
  ValidateOutput(*converted);

  ASSERT_EQ(input->data()->buffers.size(), converted->data()->buffers.size());
  for (size_t i = 0; i < input->data()->buffers.size(); ++i) {
    AssertBufferSame(*input, *converted, static_cast<int>(i));
  }
}

static std::shared_ptr<Array> MaskArrayWithNullsAt(std::shared_ptr<Array> input,
                                                   std::vector<int> indices_to_mask) {
  auto masked = input->data()->Copy();
  masked->buffers[0] = *AllocateEmptyBitmap(input->length());
  masked->null_count = kUnknownNullCount;

  using arrow::internal::Bitmap;
  Bitmap is_valid(masked->buffers[0], 0, input->length());
  if (auto original = input->null_bitmap()) {
    is_valid.CopyFrom(Bitmap(original, input->offset(), input->length()));
  } else {
    is_valid.SetBitsTo(true);
  }

  for (int i : indices_to_mask) {
    is_valid.SetBitTo(i, false);
  }
  return MakeArray(masked);
}

TEST(Cast, CanCast) {
  auto ExpectCanCast = [](std::shared_ptr<DataType> from,
                          std::vector<std::shared_ptr<DataType>> to_set,
                          bool expected = true) {
    for (auto to : to_set) {
      EXPECT_EQ(CanCast(*from, *to), expected) << "  from: " << from->ToString() << "\n"
                                               << "    to: " << to->ToString();
    }
  };

  auto ExpectCannotCast = [ExpectCanCast](std::shared_ptr<DataType> from,
                                          std::vector<std::shared_ptr<DataType>> to_set) {
    ExpectCanCast(from, to_set, /*expected=*/false);
  };

  ExpectCanCast(null(), {boolean()});
  ExpectCanCast(null(), kNumericTypes);
  ExpectCanCast(null(), kBaseBinaryTypes);
  ExpectCanCast(
      null(), {date32(), date64(), time32(TimeUnit::MILLI), timestamp(TimeUnit::SECOND)});
  ExpectCanCast(dictionary(uint16(), null()), {null()});

  ExpectCanCast(boolean(), {boolean()});
  ExpectCanCast(boolean(), kNumericTypes);
  ExpectCanCast(boolean(), {utf8(), large_utf8()});
  ExpectCanCast(dictionary(int32(), boolean()), {boolean()});

  ExpectCannotCast(boolean(), {null()});
  ExpectCannotCast(boolean(), {binary(), large_binary()});
  ExpectCannotCast(boolean(), {date32(), date64(), time32(TimeUnit::MILLI),
                               timestamp(TimeUnit::SECOND)});

  for (auto from_numeric : kNumericTypes) {
    ExpectCanCast(from_numeric, {boolean()});
    ExpectCanCast(from_numeric, kNumericTypes);
    ExpectCanCast(from_numeric, {utf8(), large_utf8()});
    ExpectCanCast(dictionary(int32(), from_numeric), {from_numeric});

    ExpectCannotCast(from_numeric, {null()});
  }

  for (auto from_base_binary : kBaseBinaryTypes) {
    ExpectCanCast(from_base_binary, {boolean()});
    ExpectCanCast(from_base_binary, kNumericTypes);
    ExpectCanCast(from_base_binary, kBaseBinaryTypes);
    ExpectCanCast(dictionary(int64(), from_base_binary), {from_base_binary});

    // any cast which is valid for the dictionary is valid for the DictionaryArray
    ExpectCanCast(dictionary(uint32(), from_base_binary), kBaseBinaryTypes);
    ExpectCanCast(dictionary(int16(), from_base_binary), kNumericTypes);

    ExpectCannotCast(from_base_binary, {null()});
  }

  ExpectCanCast(utf8(), {timestamp(TimeUnit::MILLI)});
  ExpectCanCast(large_utf8(), {timestamp(TimeUnit::NANO)});
  ExpectCannotCast(timestamp(TimeUnit::MICRO),
                   kBaseBinaryTypes);  // no formatting supported

  ExpectCannotCast(fixed_size_binary(3),
                   {fixed_size_binary(3)});  // FIXME missing identity cast

  ExtensionTypeGuard smallint_guard(smallint());
  ExpectCanCast(smallint(), {int16()});  // cast storage
  ExpectCanCast(smallint(),
                kNumericTypes);  // any cast which is valid for storage is supported
  ExpectCannotCast(null(), {smallint()});  // FIXME missing common cast from null
}

TEST(Cast, SameTypeZeroCopy) {
  std::shared_ptr<Array> arr = ArrayFromJSON(int32(), "[0, null, 2, 3, 4]");
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> result, Cast(*arr, int32()));

  AssertBufferSame(*arr, *result, 0);
  AssertBufferSame(*arr, *result, 1);
}

TEST(Cast, ZeroChunks) {
  auto chunked_i32 = std::make_shared<ChunkedArray>(ArrayVector{}, int32());
  ASSERT_OK_AND_ASSIGN(Datum result, Cast(chunked_i32, utf8()));

  ASSERT_EQ(result.kind(), Datum::CHUNKED_ARRAY);
  AssertChunkedEqual(*result.chunked_array(), ChunkedArray({}, utf8()));
}

TEST(Cast, CastDoesNotProvideDefaultOptions) {
  std::shared_ptr<Array> arr = ArrayFromJSON(int32(), "[0, null, 2, 3, 4]");
  ASSERT_RAISES(Invalid, CallFunction("cast", {arr}));
}

TEST(Cast, FromBoolean) {
  std::string vals = "[1, 0, null, 1, 0, 1, 1, null, 0, 0, 1]";
  CheckCast(ArrayFromJSON(boolean(), vals), ArrayFromJSON(int32(), vals));
}

TEST(Cast, ToBoolean) {
  for (auto type : kNumericTypes) {
    CheckCast(ArrayFromJSON(type, "[0, null, 127, 1, 0]"),
              ArrayFromJSON(boolean(), "[false, null, true, true, false]"));
  }

  // Check negative numbers
  for (auto type : {int8(), float64()}) {
    CheckCast(ArrayFromJSON(type, "[0, null, 127, -1, 0]"),
              ArrayFromJSON(boolean(), "[false, null, true, true, false]"));
  }
}

TEST(Cast, ToIntUpcast) {
  std::vector<bool> is_valid = {true, false, true, true, true};

  // int8 to int32
  CheckCast(ArrayFromJSON(int8(), "[0, null, 127, -1, 0]"),
            ArrayFromJSON(int32(), "[0, null, 127, -1, 0]"));

  // uint8 to int16, no overflow/underrun
  CheckCast(ArrayFromJSON(uint8(), "[0, 100, 200, 255, 0]"),
            ArrayFromJSON(int16(), "[0, 100, 200, 255, 0]"));
}

TEST(Cast, OverflowInNullSlot) {
  CheckCast(
      MaskArrayWithNullsAt(ArrayFromJSON(int32(), "[0, 87654321, 2000, 1000, 0]"), {1}),
      ArrayFromJSON(int16(), "[0, null, 2000, 1000, 0]"));
}

TEST(Cast, ToIntDowncastSafe) {
  // int16 to uint8, no overflow/underflow
  CheckCast(ArrayFromJSON(int16(), "[0, null, 200, 1, 2]"),
            ArrayFromJSON(uint8(), "[0, null, 200, 1, 2]"));

  // int16 to uint8, overflow
  CheckCastFails(ArrayFromJSON(int16(), "[0, null, 256, 0, 0]"),
                 CastOptions::Safe(uint8()));
  // ... and underflow
  CheckCastFails(ArrayFromJSON(int16(), "[0, null, -1, 0, 0]"),
                 CastOptions::Safe(uint8()));

  // int32 to int16, no overflow/underflow
  CheckCast(ArrayFromJSON(int32(), "[0, null, 2000, 1, 2]"),
            ArrayFromJSON(int16(), "[0, null, 2000, 1, 2]"));

  // int32 to int16, overflow
  CheckCastFails(ArrayFromJSON(int32(), "[0, null, 2000, 70000, 2]"),
                 CastOptions::Safe(int16()));

  // ... and underflow
  CheckCastFails(ArrayFromJSON(int32(), "[0, null, 2000, -70000, 2]"),
                 CastOptions::Safe(int16()));

  CheckCastFails(ArrayFromJSON(int32(), "[0, null, 2000, -70000, 2]"),
                 CastOptions::Safe(uint8()));
}

TEST(Cast, IntegerSignedToUnsigned) {
  auto i32s = ArrayFromJSON(int32(), "[-2147483648, null, -1, 65535, 2147483647]");
  // Same width
  CheckCastFails(i32s, CastOptions::Safe(uint32()));
  // Wider
  CheckCastFails(i32s, CastOptions::Safe(uint64()));
  // Narrower
  CheckCastFails(i32s, CastOptions::Safe(uint16()));

  CastOptions options;
  options.allow_int_overflow = true;

  CheckCast(i32s,
            ArrayFromJSON(uint32(), "[2147483648, null, 4294967295, 65535, 2147483647]"),
            options);
  CheckCast(i32s,
            ArrayFromJSON(
                uint64(),
                "[18446744071562067968, null, 18446744073709551615, 65535, 2147483647]"),
            options);
  CheckCast(i32s, ArrayFromJSON(uint16(), "[0, null, 65535, 65535, 65535]"), options);

  // Fail because of overflow (instead of underflow).
  i32s = ArrayFromJSON(int32(), "[0, null, 0, 65536, 2147483647]");
  CheckCastFails(i32s, CastOptions::Safe(uint16()));

  CheckCast(i32s, ArrayFromJSON(uint16(), "[0, null, 0, 0, 65535]"), options);
}

TEST(Cast, IntegerUnsignedToSigned) {
  auto u32s = ArrayFromJSON(uint32(), "[4294967295, null, 0, 32768]");
  // Same width
  CheckCastFails(u32s, CastOptions::Safe(int32()));

  // Narrower
  CheckCastFails(u32s, CastOptions::Safe(int16()));
  CheckCastFails(u32s->Slice(1), CastOptions::Safe(int16()));

  CastOptions options;
  options.allow_int_overflow = true;

  CheckCast(u32s, ArrayFromJSON(int32(), "[-1, null, 0, 32768]"), options);
  CheckCast(u32s, ArrayFromJSON(int64(), "[4294967295, null, 0, 32768]"), options);
  CheckCast(u32s, ArrayFromJSON(int16(), "[-1, null, 0, -32768]"), options);
}

TEST(Cast, ToIntDowncastUnsafe) {
  CastOptions options;
  options.allow_int_overflow = true;

  // int16 to uint8, no overflow/underflow
  CheckCast(ArrayFromJSON(int16(), "[0, null, 200, 1, 2]"),
            ArrayFromJSON(uint8(), "[0, null, 200, 1, 2]"), options);

  // int16 to uint8, with overflow/underflow
  CheckCast(ArrayFromJSON(int16(), "[0, null, 256, 1, 2, -1]"),
            ArrayFromJSON(uint8(), "[0, null, 0, 1, 2, 255]"), options);

  // int32 to int16, no overflow/underflow
  CheckCast(ArrayFromJSON(int32(), "[0, null, 2000, 1, 2, -1]"),
            ArrayFromJSON(int16(), "[0, null, 2000, 1, 2, -1]"), options);

  // int32 to int16, with overflow/underflow
  CheckCast(ArrayFromJSON(int32(), "[0, null, 2000, 70000, -70000]"),
            ArrayFromJSON(int16(), "[0, null, 2000, 4464, -4464]"), options);
}

TEST(Cast, FloatingToInt) {
  for (auto from : {float32(), float64()}) {
    for (auto to : {int32(), int64()}) {
      // float to int no truncation
      CheckCast(ArrayFromJSON(from, "[1.0, null, 0.0, -1.0, 5.0]"),
                ArrayFromJSON(to, "[1, null, 0, -1, 5]"));

      // float to int truncate error
      auto opts = CastOptions::Safe(to);
      CheckCastFails(ArrayFromJSON(from, "[1.5, 0.0, null, 0.5, -1.5, 5.5]"), opts);

      // float to int truncate allowed
      opts.allow_float_truncate = true;
      CheckCast(ArrayFromJSON(from, "[1.5, 0.0, null, 0.5, -1.5, 5.5]"),
                ArrayFromJSON(to, "[1, 0, null, 0, -1, 5]"), opts);
    }
  }
}

TEST(Cast, IntToFloating) {
  for (auto from : {uint32(), int32()}) {
    std::string two_24 = "[16777216, 16777217]";

    CheckCastFails(ArrayFromJSON(from, two_24), CastOptions::Safe(float32()));

    CheckCast(ArrayFromJSON(from, two_24)->Slice(0, 1),
              ArrayFromJSON(float32(), two_24)->Slice(0, 1));
  }

  auto i64s = ArrayFromJSON(int64(),
                            "[-9223372036854775808, -9223372036854775807, 0,"
                            "  9223372036854775806,  9223372036854775807]");
  CheckCastFails(i64s, CastOptions::Safe(float64()));

  // Masking those values with nulls makes this safe
  CheckCast(MaskArrayWithNullsAt(i64s, {0, 1, 3, 4}),
            ArrayFromJSON(float64(), "[null, null, 0, null, null]"));

  CheckCastFails(ArrayFromJSON(uint64(), "[9007199254740992, 9007199254740993]"),
                 CastOptions::Safe(float64()));
}

TEST(Cast, Decimal128ToInt) {
  auto options = CastOptions::Safe(int64());

  for (bool allow_int_overflow : {false, true}) {
    for (bool allow_decimal_truncate : {false, true}) {
      options.allow_int_overflow = allow_int_overflow;
      options.allow_decimal_truncate = allow_decimal_truncate;

      auto no_overflow_no_truncation = ArrayFromJSON(decimal(38, 10), R"([
          "02.0000000000",
         "-11.0000000000",
          "22.0000000000",
        "-121.0000000000",
        null])");
      CheckCast(no_overflow_no_truncation,
                ArrayFromJSON(int64(), "[2, -11, 22, -121, null]"), options);
    }
  }

  for (bool allow_int_overflow : {false, true}) {
    options.allow_int_overflow = allow_int_overflow;
    auto truncation_but_no_overflow = ArrayFromJSON(decimal(38, 10), R"([
          "02.1000000000",
         "-11.0000004500",
          "22.0000004500",
        "-121.1210000000",
        null])");

    options.allow_decimal_truncate = true;
    CheckCast(truncation_but_no_overflow,
              ArrayFromJSON(int64(), "[2, -11, 22, -121, null]"), options);

    options.allow_decimal_truncate = false;
    CheckCastFails(truncation_but_no_overflow, options);
  }

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;

    auto overflow_no_truncation = ArrayFromJSON(decimal(38, 10), R"([
        "12345678901234567890000.0000000000",
        "99999999999999999999999.0000000000",
        null])");

    options.allow_int_overflow = true;
    CheckCast(
        overflow_no_truncation,
        ArrayFromJSON(int64(),
                      // 12345678901234567890000 % 2**64, 99999999999999999999999 % 2**64
                      "[4807115922877858896, 200376420520689663, null]"),
        options);

    options.allow_int_overflow = false;
    CheckCastFails(overflow_no_truncation, options);
  }

  for (bool allow_int_overflow : {false, true}) {
    for (bool allow_decimal_truncate : {false, true}) {
      options.allow_int_overflow = allow_int_overflow;
      options.allow_decimal_truncate = allow_decimal_truncate;

      auto overflow_and_truncation = ArrayFromJSON(decimal(38, 10), R"([
        "12345678901234567890000.0045345000",
        "99999999999999999999999.0000344300",
        null])");

      if (options.allow_int_overflow && options.allow_decimal_truncate) {
        CheckCast(overflow_and_truncation,
                  ArrayFromJSON(
                      int64(),
                      // 12345678901234567890000 % 2**64, 99999999999999999999999 % 2**64
                      "[4807115922877858896, 200376420520689663, null]"),
                  options);
      } else {
        CheckCastFails(overflow_and_truncation, options);
      }
    }
  }

  Decimal128Builder builder(decimal(38, -4));
  for (auto d : {Decimal128("1234567890000."), Decimal128("-120000.")}) {
    ASSERT_OK_AND_ASSIGN(d, d.Rescale(0, -4));
    ASSERT_OK(builder.Append(d));
  }
  ASSERT_OK_AND_ASSIGN(auto negative_scale, builder.Finish());
  options.allow_int_overflow = true;
  options.allow_decimal_truncate = true;
  CheckCast(negative_scale, ArrayFromJSON(int64(), "[1234567890000, -120000]"), options);
}

TEST(Cast, Decimal256ToInt) {
  auto options = CastOptions::Safe(int64());

  for (bool allow_int_overflow : {false, true}) {
    for (bool allow_decimal_truncate : {false, true}) {
      options.allow_int_overflow = allow_int_overflow;
      options.allow_decimal_truncate = allow_decimal_truncate;

      auto no_overflow_no_truncation = ArrayFromJSON(decimal256(40, 10), R"([
          "02.0000000000",
         "-11.0000000000",
          "22.0000000000",
        "-121.0000000000",
        null])");
      CheckCast(no_overflow_no_truncation,
                ArrayFromJSON(int64(), "[2, -11, 22, -121, null]"), options);
    }
  }

  for (bool allow_int_overflow : {false, true}) {
    options.allow_int_overflow = allow_int_overflow;
    auto truncation_but_no_overflow = ArrayFromJSON(decimal256(40, 10), R"([
          "02.1000000000",
         "-11.0000004500",
          "22.0000004500",
        "-121.1210000000",
        null])");

    options.allow_decimal_truncate = true;
    CheckCast(truncation_but_no_overflow,
              ArrayFromJSON(int64(), "[2, -11, 22, -121, null]"), options);

    options.allow_decimal_truncate = false;
    CheckCastFails(truncation_but_no_overflow, options);
  }

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;

    auto overflow_no_truncation = ArrayFromJSON(decimal256(40, 10), R"([
        "1234567890123456789000000.0000000000",
        "9999999999999999999999999.0000000000",
        null])");

    options.allow_int_overflow = true;
    CheckCast(overflow_no_truncation,
              ArrayFromJSON(
                  int64(),
                  // 1234567890123456789000000 % 2**64, 9999999999999999999999999 % 2**64
                  "[1096246371337547584, 1590897978359414783, null]"),
              options);

    options.allow_int_overflow = false;
    CheckCastFails(overflow_no_truncation, options);
  }

  for (bool allow_int_overflow : {false, true}) {
    for (bool allow_decimal_truncate : {false, true}) {
      options.allow_int_overflow = allow_int_overflow;
      options.allow_decimal_truncate = allow_decimal_truncate;

      auto overflow_and_truncation = ArrayFromJSON(decimal256(40, 10), R"([
        "1234567890123456789000000.0045345000",
        "9999999999999999999999999.0000344300",
        null])");

      if (options.allow_int_overflow && options.allow_decimal_truncate) {
        CheckCast(
            overflow_and_truncation,
            ArrayFromJSON(
                int64(),
                // 1234567890123456789000000 % 2**64, 9999999999999999999999999 % 2**64
                "[1096246371337547584, 1590897978359414783, null]"),
            options);
      } else {
        CheckCastFails(overflow_and_truncation, options);
      }
    }
  }

  Decimal256Builder builder(decimal256(40, -4));
  for (auto d : {Decimal256("1234567890000."), Decimal256("-120000.")}) {
    ASSERT_OK_AND_ASSIGN(d, d.Rescale(0, -4));
    ASSERT_OK(builder.Append(d));
  }
  ASSERT_OK_AND_ASSIGN(auto negative_scale, builder.Finish());
  options.allow_int_overflow = true;
  options.allow_decimal_truncate = true;
  CheckCast(negative_scale, ArrayFromJSON(int64(), "[1234567890000, -120000]"), options);
}

TEST(Cast, Decimal128ToDecimal128) {
  CastOptions options;

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;

    auto no_truncation = ArrayFromJSON(decimal(38, 10), R"([
          "02.0000000000",
          "30.0000000000",
          "22.0000000000",
        "-121.0000000000",
        null])");
    auto expected = ArrayFromJSON(decimal(28, 0), R"([
          "02.",
          "30.",
          "22.",
        "-121.",
        null])");

    CheckCast(no_truncation, expected, options);
    CheckCast(expected, no_truncation, options);
  }

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;

    // Same scale, different precision
    auto d_5_2 = ArrayFromJSON(decimal(5, 2), R"([
          "12.34",
           "0.56"])");
    auto d_4_2 = ArrayFromJSON(decimal(4, 2), R"([
          "12.34",
           "0.56"])");

    CheckCast(d_5_2, d_4_2, options);
    CheckCast(d_4_2, d_5_2, options);
  }

  auto d_38_10 = ArrayFromJSON(decimal(38, 10), R"([
      "-02.1234567890",
       "30.1234567890",
      null])");

  auto d_28_0 = ArrayFromJSON(decimal(28, 0), R"([
      "-02.",
       "30.",
      null])");

  auto d_38_10_roundtripped = ArrayFromJSON(decimal(38, 10), R"([
      "-02.0000000000",
       "30.0000000000",
      null])");

  // Rescale which leads to truncation
  options.allow_decimal_truncate = true;
  CheckCast(d_38_10, d_28_0, options);
  CheckCast(d_28_0, d_38_10_roundtripped, options);

  options.allow_decimal_truncate = false;
  options.to_type = d_28_0->type();
  CheckCastFails(d_38_10, options);
  CheckCast(d_28_0, d_38_10_roundtripped, options);

  // Precision loss without rescale leads to truncation
  auto d_4_2 = ArrayFromJSON(decimal(4, 2), R"(["12.34"])");
  for (auto expected : {
           ArrayFromJSON(decimal(3, 2), R"(["12.34"])"),
           ArrayFromJSON(decimal(4, 3), R"(["12.340"])"),
           ArrayFromJSON(decimal(2, 1), R"(["12.3"])"),
       }) {
    options.allow_decimal_truncate = true;
    CheckCast(d_4_2, expected, options);

    options.allow_decimal_truncate = false;
    options.to_type = expected->type();
    CheckCastFails(d_4_2, options);
  }
}

TEST(Cast, Decimal256ToDecimal256) {
  CastOptions options;

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;

    auto no_truncation = ArrayFromJSON(decimal256(38, 10), R"([
          "02.0000000000",
          "30.0000000000",
          "22.0000000000",
        "-121.0000000000",
        null])");
    auto expected = ArrayFromJSON(decimal256(28, 0), R"([
          "02.",
          "30.",
          "22.",
        "-121.",
        null])");

    CheckCast(no_truncation, expected, options);
    CheckCast(expected, no_truncation, options);
  }

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;

    // Same scale, different precision
    auto d_5_2 = ArrayFromJSON(decimal256(5, 2), R"([
          "12.34",
           "0.56"])");
    auto d_4_2 = ArrayFromJSON(decimal256(4, 2), R"([
          "12.34",
           "0.56"])");

    CheckCast(d_5_2, d_4_2, options);
    CheckCast(d_4_2, d_5_2, options);
  }

  auto d_38_10 = ArrayFromJSON(decimal256(38, 10), R"([
      "-02.1234567890",
       "30.1234567890",
      null])");

  auto d_28_0 = ArrayFromJSON(decimal256(28, 0), R"([
      "-02.",
       "30.",
      null])");

  auto d_38_10_roundtripped = ArrayFromJSON(decimal256(38, 10), R"([
      "-02.0000000000",
       "30.0000000000",
      null])");

  // Rescale which leads to truncation
  options.allow_decimal_truncate = true;
  CheckCast(d_38_10, d_28_0, options);
  CheckCast(d_28_0, d_38_10_roundtripped, options);

  options.allow_decimal_truncate = false;
  options.to_type = d_28_0->type();
  CheckCastFails(d_38_10, options);
  CheckCast(d_28_0, d_38_10_roundtripped, options);

  // Precision loss without rescale leads to truncation
  auto d_4_2 = ArrayFromJSON(decimal256(4, 2), R"(["12.34"])");
  for (auto expected : {
           ArrayFromJSON(decimal256(3, 2), R"(["12.34"])"),
           ArrayFromJSON(decimal256(4, 3), R"(["12.340"])"),
           ArrayFromJSON(decimal256(2, 1), R"(["12.3"])"),
       }) {
    options.allow_decimal_truncate = true;
    CheckCast(d_4_2, expected, options);

    options.allow_decimal_truncate = false;
    options.to_type = expected->type();
    CheckCastFails(d_4_2, options);
  }
}

TEST(Cast, Decimal128ToDecimal256) {
  CastOptions options;

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;

    auto no_truncation = ArrayFromJSON(decimal(38, 10), R"([
          "02.0000000000",
          "30.0000000000",
          "22.0000000000",
        "-121.0000000000",
        null])");
    auto expected = ArrayFromJSON(decimal256(48, 0), R"([
          "02.",
          "30.",
          "22.",
        "-121.",
        null])");

    CheckCast(no_truncation, expected, options);
  }

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;

    // Same scale, different precision
    auto d_5_2 = ArrayFromJSON(decimal(5, 2), R"([
          "12.34",
           "0.56"])");
    auto d_4_2 = ArrayFromJSON(decimal256(4, 2), R"([
          "12.34",
           "0.56"])");
    auto d_40_2 = ArrayFromJSON(decimal256(40, 2), R"([
          "12.34",
           "0.56"])");

    CheckCast(d_5_2, d_4_2, options);
    CheckCast(d_5_2, d_40_2, options);
  }

  auto d128_38_10 = ArrayFromJSON(decimal(38, 10), R"([
      "-02.1234567890",
       "30.1234567890",
      null])");

  auto d128_28_0 = ArrayFromJSON(decimal(28, 0), R"([
      "-02.",
       "30.",
      null])");

  auto d256_28_0 = ArrayFromJSON(decimal256(28, 0), R"([
      "-02.",
       "30.",
      null])");

  auto d256_38_10_roundtripped = ArrayFromJSON(decimal256(38, 10), R"([
      "-02.0000000000",
       "30.0000000000",
      null])");

  // Rescale which leads to truncation
  options.allow_decimal_truncate = true;
  CheckCast(d128_38_10, d256_28_0, options);
  CheckCast(d128_28_0, d256_38_10_roundtripped, options);

  options.allow_decimal_truncate = false;
  options.to_type = d256_28_0->type();
  CheckCastFails(d128_38_10, options);
  CheckCast(d128_28_0, d256_38_10_roundtripped, options);

  // Precision loss without rescale leads to truncation
  auto d128_4_2 = ArrayFromJSON(decimal(4, 2), R"(["12.34"])");
  for (auto expected : {
           ArrayFromJSON(decimal256(3, 2), R"(["12.34"])"),
           ArrayFromJSON(decimal256(4, 3), R"(["12.340"])"),
           ArrayFromJSON(decimal256(2, 1), R"(["12.3"])"),
       }) {
    options.allow_decimal_truncate = true;
    CheckCast(d128_4_2, expected, options);

    options.allow_decimal_truncate = false;
    options.to_type = expected->type();
    CheckCastFails(d128_4_2, options);
  }
}

TEST(Cast, Decimal256ToDecimal128) {
  CastOptions options;

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;

    auto no_truncation = ArrayFromJSON(decimal256(42, 10), R"([
          "02.0000000000",
          "30.0000000000",
          "22.0000000000",
        "-121.0000000000",
        null])");
    auto expected = ArrayFromJSON(decimal(28, 0), R"([
          "02.",
          "30.",
          "22.",
        "-121.",
        null])");

    CheckCast(no_truncation, expected, options);
  }

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;

    // Same scale, different precision
    auto d_5_2 = ArrayFromJSON(decimal256(42, 2), R"([
          "12.34",
           "0.56"])");
    auto d_4_2 = ArrayFromJSON(decimal(4, 2), R"([
          "12.34",
           "0.56"])");

    CheckCast(d_5_2, d_4_2, options);
  }

  auto d256_52_10 = ArrayFromJSON(decimal256(52, 10), R"([
      "-02.1234567890",
       "30.1234567890",
      null])");

  auto d256_42_0 = ArrayFromJSON(decimal256(42, 0), R"([
      "-02.",
       "30.",
      null])");

  auto d128_28_0 = ArrayFromJSON(decimal(28, 0), R"([
      "-02.",
       "30.",
      null])");

  auto d128_38_10_roundtripped = ArrayFromJSON(decimal(38, 10), R"([
      "-02.0000000000",
       "30.0000000000",
      null])");

  // Rescale which leads to truncation
  options.allow_decimal_truncate = true;
  CheckCast(d256_52_10, d128_28_0, options);
  CheckCast(d256_42_0, d128_38_10_roundtripped, options);

  options.allow_decimal_truncate = false;
  options.to_type = d128_28_0->type();
  CheckCastFails(d256_52_10, options);
  CheckCast(d256_42_0, d128_38_10_roundtripped, options);

  // Precision loss without rescale leads to truncation
  auto d256_4_2 = ArrayFromJSON(decimal256(4, 2), R"(["12.34"])");
  for (auto expected : {
           ArrayFromJSON(decimal(3, 2), R"(["12.34"])"),
           ArrayFromJSON(decimal(4, 3), R"(["12.340"])"),
           ArrayFromJSON(decimal(2, 1), R"(["12.3"])"),
       }) {
    options.allow_decimal_truncate = true;
    CheckCast(d256_4_2, expected, options);

    options.allow_decimal_truncate = false;
    options.to_type = expected->type();
    CheckCastFails(d256_4_2, options);
  }
}

TEST(Cast, FloatingToDecimal) {
  for (auto float_type : {float32(), float64()}) {
    for (auto decimal_type : {decimal(5, 2), decimal256(5, 2)}) {
      CheckCast(
          ArrayFromJSON(float_type, "[0.0, null, 123.45, 123.456, 999.994]"),
          ArrayFromJSON(decimal_type, R"(["0.00", null, "123.45", "123.46", "999.99"])"));

      // Overflow
      CastOptions options;
      options.to_type = decimal_type;
      CheckCastFails(ArrayFromJSON(float_type, "[999.996]"), options);

      options.allow_decimal_truncate = true;
      CheckCast(
          ArrayFromJSON(float_type, "[0.0, null, 999.996, 123.45, 999.994]"),
          ArrayFromJSON(decimal_type, R"(["0.00", null, "0.00", "123.45", "999.99"])"),
          options);
    }
  }

  for (auto decimal_type : {decimal128, decimal256}) {
    // 2**64 + 2**41 (exactly representable as a float)
    CheckCast(ArrayFromJSON(float32(), "[1.8446746e+19, -1.8446746e+19]"),
              ArrayFromJSON(decimal_type(20, 0),
                            R"(["18446746272732807168", "-18446746272732807168"])"));

    CheckCast(
        ArrayFromJSON(float64(), "[1.8446744073709556e+19, -1.8446744073709556e+19]"),
        ArrayFromJSON(decimal_type(20, 0),
                      R"(["18446744073709555712", "-18446744073709555712"])"));

    CheckCast(ArrayFromJSON(float32(), "[1.8446746e+15, -1.8446746e+15]"),
              ArrayFromJSON(decimal_type(20, 4),
                            R"(["1844674627273280.7168", "-1844674627273280.7168"])"));

    CheckCast(
        ArrayFromJSON(float64(), "[1.8446744073709556e+15, -1.8446744073709556e+15]"),
        ArrayFromJSON(decimal_type(20, 4),
                      R"(["1844674407370955.5712", "-1844674407370955.5712"])"));

    // Edge cases are tested for Decimal128::FromReal() and Decimal256::FromReal
  }
}

TEST(Cast, DecimalToFloating) {
  for (auto float_type : {float32(), float64()}) {
    for (auto decimal_type : {decimal(5, 2), decimal256(5, 2)}) {
      CheckCast(ArrayFromJSON(decimal_type, R"(["0.00", null, "123.45", "999.99"])"),
                ArrayFromJSON(float_type, "[0.0, null, 123.45, 999.99]"));
    }
  }

  // Edge cases are tested for Decimal128::ToReal() and Decimal256::ToReal()
}

TEST(Cast, TimestampToTimestamp) {
  struct TimestampTypePair {
    std::shared_ptr<DataType> coarse, fine;
  };

  CastOptions options;

  for (auto types : {
           TimestampTypePair{timestamp(TimeUnit::SECOND), timestamp(TimeUnit::MILLI)},
           TimestampTypePair{timestamp(TimeUnit::MILLI), timestamp(TimeUnit::MICRO)},
           TimestampTypePair{timestamp(TimeUnit::MICRO), timestamp(TimeUnit::NANO)},
       }) {
    auto coarse = ArrayFromJSON(types.coarse, "[0, null, 200, 1, 2]");
    auto promoted = ArrayFromJSON(types.fine, "[0, null, 200000, 1000, 2000]");

    // multiply/promote
    CheckCast(coarse, promoted);

    auto will_be_truncated = ArrayFromJSON(types.fine, "[0, null, 200456, 1123, 2456]");

    // with truncation disallowed, fails
    options.allow_time_truncate = false;
    options.to_type = types.coarse;
    CheckCastFails(will_be_truncated, options);

    // with truncation allowed, divide/truncate
    options.allow_time_truncate = true;
    CheckCast(will_be_truncated, coarse, options);
  }

  for (auto types : {
           TimestampTypePair{timestamp(TimeUnit::SECOND), timestamp(TimeUnit::MICRO)},
           TimestampTypePair{timestamp(TimeUnit::MILLI), timestamp(TimeUnit::NANO)},
       }) {
    auto coarse = ArrayFromJSON(types.coarse, "[0, null, 200, 1, 2]");
    auto promoted = ArrayFromJSON(types.fine, "[0, null, 200000000, 1000000, 2000000]");

    // multiply/promote
    CheckCast(coarse, promoted);

    auto will_be_truncated =
        ArrayFromJSON(types.fine, "[0, null, 200456000, 1123000, 2456000]");

    // with truncation disallowed, fails
    options.allow_time_truncate = false;
    options.to_type = types.coarse;
    CheckCastFails(will_be_truncated, options);

    // with truncation allowed, divide/truncate
    options.allow_time_truncate = true;
    CheckCast(will_be_truncated, coarse, options);
  }

  for (auto types : {
           TimestampTypePair{timestamp(TimeUnit::SECOND), timestamp(TimeUnit::NANO)},
       }) {
    auto coarse = ArrayFromJSON(types.coarse, "[0, null, 200, 1, 2]");
    auto promoted =
        ArrayFromJSON(types.fine, "[0, null, 200000000000, 1000000000, 2000000000]");

    // multiply/promote
    CheckCast(coarse, promoted);

    auto will_be_truncated =
        ArrayFromJSON(types.fine, "[0, null, 200456000000, 1123000000, 2456000000]");

    // with truncation disallowed, fails
    options.allow_time_truncate = false;
    options.to_type = types.coarse;
    CheckCastFails(will_be_truncated, options);

    // with truncation allowed, divide/truncate
    options.allow_time_truncate = true;
    CheckCast(will_be_truncated, coarse, options);
  }
}

TEST(Cast, TimestampZeroCopy) {
  for (auto zero_copy_to_type : {
           timestamp(TimeUnit::SECOND),
           int64(),  // ARROW-1773, cast to integer
       }) {
    CheckCastZeroCopy(
        ArrayFromJSON(timestamp(TimeUnit::SECOND), "[0, null, 2000, 1000, 0]"),
        zero_copy_to_type);
  }
  CheckCastZeroCopy(ArrayFromJSON(int64(), "[0, null, 2000, 1000, 0]"),
                    timestamp(TimeUnit::SECOND));
}

TEST(Cast, TimestampToTimestampMultiplyOverflow) {
  CastOptions options;
  options.to_type = timestamp(TimeUnit::NANO);
  // 1000-01-01, 1800-01-01 , 2000-01-01, 2300-01-01, 3000-01-01
  CheckCastFails(
      ArrayFromJSON(timestamp(TimeUnit::SECOND),
                    "[-30610224000, -5364662400, 946684800, 10413792000, 32503680000]"),
      options);
}

TEST(Cast, TimestampToDate) {
  for (auto date : {
           // 2000-01-01, 2000-01-02, null
           ArrayFromJSON(date32(), "[10957, 10958, null]"),
           ArrayFromJSON(date64(), "[946684800000, 946771200000, null]"),
       }) {
    for (auto ts : {
             ArrayFromJSON(timestamp(TimeUnit::SECOND), "[946684800, 946771200, null]"),
             ArrayFromJSON(timestamp(TimeUnit::MILLI),
                           "[946684800000, 946771200000, null]"),
             ArrayFromJSON(timestamp(TimeUnit::MICRO),
                           "[946684800000000, 946771200000000, null]"),
             ArrayFromJSON(timestamp(TimeUnit::NANO),
                           "[946684800000000000, 946771200000000000, null]"),
         }) {
      CheckCast(ts, date);
    }

    for (auto ts : {
             ArrayFromJSON(timestamp(TimeUnit::SECOND), "[946684801, 946771201, null]"),
             ArrayFromJSON(timestamp(TimeUnit::MILLI),
                           "[946684800001, 946771200001, null]"),
             ArrayFromJSON(timestamp(TimeUnit::MICRO),
                           "[946684800000001, 946771200000001, null]"),
             ArrayFromJSON(timestamp(TimeUnit::NANO),
                           "[946684800000000001, 946771200000000001, null]"),
         }) {
      auto options = CastOptions::Safe(date->type());
      CheckCastFails(ts, options);

      options.allow_time_truncate = true;
      CheckCast(ts, date, options);
    }

    auto options = CastOptions::Safe(date->type());
    auto ts = ArrayFromJSON(timestamp(TimeUnit::SECOND), "[946684800, 946771200, 1]");
    CheckCastFails(ts, options);

    // Make sure that nulls are excluded from the truncation checks
    CheckCast(MaskArrayWithNullsAt(ts, {2}), date);
  }
}

TEST(Cast, TimeToTime) {
  struct TimeTypePair {
    std::shared_ptr<DataType> coarse, fine;
  };

  CastOptions options;

  for (auto types : {
           TimeTypePair{time32(TimeUnit::SECOND), time32(TimeUnit::MILLI)},
           TimeTypePair{time32(TimeUnit::MILLI), time64(TimeUnit::MICRO)},
           TimeTypePair{time64(TimeUnit::MICRO), time64(TimeUnit::NANO)},
       }) {
    auto coarse = ArrayFromJSON(types.coarse, "[0, null, 200, 1, 2]");
    auto promoted = ArrayFromJSON(types.fine, "[0, null, 200000, 1000, 2000]");

    // multiply/promote
    CheckCast(coarse, promoted);

    auto will_be_truncated = ArrayFromJSON(types.fine, "[0, null, 200456, 1123, 2456]");

    // with truncation disallowed, fails
    options.allow_time_truncate = false;
    options.to_type = types.coarse;
    CheckCastFails(will_be_truncated, options);

    // with truncation allowed, divide/truncate
    options.allow_time_truncate = true;
    CheckCast(will_be_truncated, coarse, options);
  }

  for (auto types : {
           TimeTypePair{time32(TimeUnit::SECOND), time64(TimeUnit::MICRO)},
           TimeTypePair{time32(TimeUnit::MILLI), time64(TimeUnit::NANO)},
       }) {
    auto coarse = ArrayFromJSON(types.coarse, "[0, null, 200, 1, 2]");
    auto promoted = ArrayFromJSON(types.fine, "[0, null, 200000000, 1000000, 2000000]");

    // multiply/promote
    CheckCast(coarse, promoted);

    auto will_be_truncated =
        ArrayFromJSON(types.fine, "[0, null, 200456000, 1123000, 2456000]");

    // with truncation disallowed, fails
    options.allow_time_truncate = false;
    options.to_type = types.coarse;
    CheckCastFails(will_be_truncated, options);

    // with truncation allowed, divide/truncate
    options.allow_time_truncate = true;
    CheckCast(will_be_truncated, coarse, options);
  }

  for (auto types : {
           TimeTypePair{time32(TimeUnit::SECOND), time64(TimeUnit::NANO)},
       }) {
    auto coarse = ArrayFromJSON(types.coarse, "[0, null, 200, 1, 2]");
    auto promoted =
        ArrayFromJSON(types.fine, "[0, null, 200000000000, 1000000000, 2000000000]");

    // multiply/promote
    CheckCast(coarse, promoted);

    auto will_be_truncated =
        ArrayFromJSON(types.fine, "[0, null, 200456000000, 1123000000, 2456000000]");

    // with truncation disallowed, fails
    options.allow_time_truncate = false;
    options.to_type = types.coarse;
    CheckCastFails(will_be_truncated, options);

    // with truncation allowed, divide/truncate
    options.allow_time_truncate = true;
    CheckCast(will_be_truncated, coarse, options);
  }
}

TEST(Cast, TimeZeroCopy) {
  for (auto zero_copy_to_type : {
           time32(TimeUnit::SECOND),
           int32(),  // ARROW-1773: cast to int32
       }) {
    CheckCastZeroCopy(ArrayFromJSON(time32(TimeUnit::SECOND), "[0, null, 2000, 1000, 0]"),
                      zero_copy_to_type);
  }
  CheckCastZeroCopy(ArrayFromJSON(int32(), "[0, null, 2000, 1000, 0]"),
                    time32(TimeUnit::SECOND));

  for (auto zero_copy_to_type : {
           time64(TimeUnit::MICRO),
           int64(),  // ARROW-1773: cast to int64
       }) {
    CheckCastZeroCopy(ArrayFromJSON(time64(TimeUnit::MICRO), "[0, null, 2000, 1000, 0]"),
                      zero_copy_to_type);
  }
  CheckCastZeroCopy(ArrayFromJSON(int64(), "[0, null, 2000, 1000, 0]"),
                    time64(TimeUnit::MICRO));
}

TEST(Cast, DateToDate) {
  auto day_32 = ArrayFromJSON(date32(), "[0, null, 100, 1, 10]");
  auto day_64 = ArrayFromJSON(date64(), R"([
               0,
            null,
      8640000000,
        86400000,
       864000000])");

  // Multiply promotion
  CheckCast(day_32, day_64);

  // No truncation
  CheckCast(day_64, day_32);

  auto day_64_will_be_truncated = ArrayFromJSON(date64(), R"([
               0,
            null,
      8640000123,
        86400456,
       864000789])");

  // Disallow truncate
  CastOptions options;
  options.to_type = date32();
  CheckCastFails(day_64_will_be_truncated, options);

  // Divide, truncate
  options.allow_time_truncate = true;
  CheckCast(day_64_will_be_truncated, day_32, options);
}

TEST(Cast, DateZeroCopy) {
  for (auto zero_copy_to_type : {
           date32(),
           int32(),  // ARROW-1773: cast to int32
       }) {
    CheckCastZeroCopy(ArrayFromJSON(date32(), "[0, null, 2000, 1000, 0]"),
                      zero_copy_to_type);
  }
  CheckCastZeroCopy(ArrayFromJSON(int32(), "[0, null, 2000, 1000, 0]"), date32());

  for (auto zero_copy_to_type : {
           date64(),
           int64(),  // ARROW-1773: cast to int64
       }) {
    CheckCastZeroCopy(ArrayFromJSON(date64(), "[0, null, 2000, 1000, 0]"),
                      zero_copy_to_type);
  }
  CheckCastZeroCopy(ArrayFromJSON(int64(), "[0, null, 2000, 1000, 0]"), date64());
}

TEST(Cast, DurationToDuration) {
  struct DurationTypePair {
    std::shared_ptr<DataType> coarse, fine;
  };

  CastOptions options;

  for (auto types : {
           DurationTypePair{duration(TimeUnit::SECOND), duration(TimeUnit::MILLI)},
           DurationTypePair{duration(TimeUnit::MILLI), duration(TimeUnit::MICRO)},
           DurationTypePair{duration(TimeUnit::MICRO), duration(TimeUnit::NANO)},
       }) {
    auto coarse = ArrayFromJSON(types.coarse, "[0, null, 200, 1, 2]");
    auto promoted = ArrayFromJSON(types.fine, "[0, null, 200000, 1000, 2000]");

    // multiply/promote
    CheckCast(coarse, promoted);

    auto will_be_truncated = ArrayFromJSON(types.fine, "[0, null, 200456, 1123, 2456]");

    // with truncation disallowed, fails
    options.allow_time_truncate = false;
    options.to_type = types.coarse;
    CheckCastFails(will_be_truncated, options);

    // with truncation allowed, divide/truncate
    options.allow_time_truncate = true;
    CheckCast(will_be_truncated, coarse, options);
  }

  for (auto types : {
           DurationTypePair{duration(TimeUnit::SECOND), duration(TimeUnit::MICRO)},
           DurationTypePair{duration(TimeUnit::MILLI), duration(TimeUnit::NANO)},
       }) {
    auto coarse = ArrayFromJSON(types.coarse, "[0, null, 200, 1, 2]");
    auto promoted = ArrayFromJSON(types.fine, "[0, null, 200000000, 1000000, 2000000]");

    // multiply/promote
    CheckCast(coarse, promoted);

    auto will_be_truncated =
        ArrayFromJSON(types.fine, "[0, null, 200000456, 1000123, 2000456]");

    // with truncation disallowed, fails
    options.allow_time_truncate = false;
    options.to_type = types.coarse;
    CheckCastFails(will_be_truncated, options);

    // with truncation allowed, divide/truncate
    options.allow_time_truncate = true;
    CheckCast(will_be_truncated, coarse, options);
  }

  for (auto types : {
           DurationTypePair{duration(TimeUnit::SECOND), duration(TimeUnit::NANO)},
       }) {
    auto coarse = ArrayFromJSON(types.coarse, "[0, null, 200, 1, 2]");
    auto promoted =
        ArrayFromJSON(types.fine, "[0, null, 200000000000, 1000000000, 2000000000]");

    // multiply/promote
    CheckCast(coarse, promoted);

    auto will_be_truncated =
        ArrayFromJSON(types.fine, "[0, null, 200000000456, 1000000123, 2000000456]");

    // with truncation disallowed, fails
    options.allow_time_truncate = false;
    options.to_type = types.coarse;
    CheckCastFails(will_be_truncated, options);

    // with truncation allowed, divide/truncate
    options.allow_time_truncate = true;
    CheckCast(will_be_truncated, coarse, options);
  }
}

TEST(Cast, DurationZeroCopy) {
  for (auto zero_copy_to_type : {
           duration(TimeUnit::SECOND),
           int64(),  // ARROW-1773: cast to int64
       }) {
    CheckCastZeroCopy(
        ArrayFromJSON(duration(TimeUnit::SECOND), "[0, null, 2000, 1000, 0]"),
        zero_copy_to_type);
  }
  CheckCastZeroCopy(ArrayFromJSON(int64(), "[0, null, 2000, 1000, 0]"),
                    duration(TimeUnit::SECOND));
}

TEST(Cast, DurationToDurationMultiplyOverflow) {
  CastOptions options;
  options.to_type = duration(TimeUnit::NANO);
  CheckCastFails(
      ArrayFromJSON(duration(TimeUnit::SECOND), "[10000000000, 1, 2, 3, 10000000000]"),
      options);
}

TEST(Cast, MiscToFloating) {
  for (auto to_type : {float32(), float64()}) {
    CheckCast(ArrayFromJSON(int16(), "[0, null, 200, 1, 2]"),
              ArrayFromJSON(to_type, "[0, null, 200, 1, 2]"));

    CheckCast(ArrayFromJSON(float32(), "[0, null, 200, 1, 2]"),
              ArrayFromJSON(to_type, "[0, null, 200, 1, 2]"));

    CheckCast(ArrayFromJSON(boolean(), "[true, null, false, false, true]"),
              ArrayFromJSON(to_type, "[1, null, 0, 0, 1]"));
  }
}

TEST(Cast, UnsupportedInputType) {
  // Casting to a supported target type, but with an unsupported input type
  // for the target type.
  const auto arr = ArrayFromJSON(int32(), "[1, 2, 3]");

  const auto to_type = list(utf8());
  const char* expected_message = "Unsupported cast from int32 to list";

  // Try through concrete API
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented, ::testing::HasSubstr(expected_message),
                                  Cast(*arr, to_type));

  // Try through general kernel API
  CastOptions options;
  options.to_type = to_type;
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented, ::testing::HasSubstr(expected_message),
                                  CallFunction("cast", {arr}, &options));
}

TEST(Cast, UnsupportedTargetType) {
  // Casting to an unsupported target type
  const auto arr = ArrayFromJSON(int32(), "[1, 2, 3]");
  const auto to_type = dense_union({field("a", int32())});

  // Try through concrete API
  const char* expected_message = "Unsupported cast from int32 to dense_union";
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented, ::testing::HasSubstr(expected_message),
                                  Cast(*arr, to_type));

  // Try through general kernel API
  CastOptions options;
  options.to_type = to_type;
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented, ::testing::HasSubstr(expected_message),
                                  CallFunction("cast", {arr}, &options));
}

TEST(Cast, StringToBoolean) {
  for (auto string_type : {utf8(), large_utf8()}) {
    CheckCast(ArrayFromJSON(string_type, R"(["False", null, "true", "True", "false"])"),
              ArrayFromJSON(boolean(), "[false, null, true, true, false]"));

    CheckCast(ArrayFromJSON(string_type, R"(["0", null, "1", "1", "0"])"),
              ArrayFromJSON(boolean(), "[false, null, true, true, false]"));

    auto options = CastOptions::Safe(boolean());
    CheckCastFails(ArrayFromJSON(string_type, R"(["false "])"), options);
    CheckCastFails(ArrayFromJSON(string_type, R"(["T"])"), options);
  }
}

TEST(Cast, StringToInt) {
  for (auto string_type : {utf8(), large_utf8()}) {
    for (auto signed_type : {int8(), int16(), int32(), int64()}) {
      CheckCast(ArrayFromJSON(string_type, R"(["0", null, "127", "-1", "0"])"),
                ArrayFromJSON(signed_type, "[0, null, 127, -1, 0]"));
    }

    CheckCast(
        ArrayFromJSON(string_type, R"(["2147483647", null, "-2147483648", "0", "0"])"),
        ArrayFromJSON(int32(), "[2147483647, null, -2147483648, 0, 0]"));

    CheckCast(ArrayFromJSON(
                  string_type,
                  R"(["9223372036854775807", null, "-9223372036854775808", "0", "0"])"),
              ArrayFromJSON(int64(),
                            "[9223372036854775807, null, -9223372036854775808, 0, 0]"));

    for (auto unsigned_type : {uint8(), uint16(), uint32(), uint64()}) {
      CheckCast(ArrayFromJSON(string_type, R"(["0", null, "127", "255", "0"])"),
                ArrayFromJSON(unsigned_type, "[0, null, 127, 255, 0]"));
    }

    CheckCast(
        ArrayFromJSON(string_type, R"(["2147483647", null, "4294967295", "0", "0"])"),
        ArrayFromJSON(uint32(), "[2147483647, null, 4294967295, 0, 0]"));

    CheckCast(ArrayFromJSON(
                  string_type,
                  R"(["9223372036854775807", null, "18446744073709551615", "0", "0"])"),
              ArrayFromJSON(uint64(),
                            "[9223372036854775807, null, 18446744073709551615, 0, 0]"));

    for (std::string not_int8 : {
             "z",
             "12 z",
             "128",
             "-129",
             "0.5",
         }) {
      auto options = CastOptions::Safe(int8());
      CheckCastFails(ArrayFromJSON(string_type, "[\"" + not_int8 + "\"]"), options);
    }

    for (std::string not_uint8 : {
             "256",
             "-1",
             "0.5",
         }) {
      auto options = CastOptions::Safe(uint8());
      CheckCastFails(ArrayFromJSON(string_type, "[\"" + not_uint8 + "\"]"), options);
    }
  }
}

TEST(Cast, StringToFloating) {
  for (auto string_type : {utf8(), large_utf8()}) {
    for (auto float_type : {float32(), float64()}) {
      auto strings =
          ArrayFromJSON(string_type, R"(["0.1", null, "127.3", "1e3", "200.4", "0.5"])");
      auto floats = ArrayFromJSON(float_type, "[0.1, null, 127.3, 1000, 200.4, 0.5]");
      CheckCast(strings, floats);

      for (std::string not_float : {
               "z",
           }) {
        auto options = CastOptions::Safe(float32());
        CheckCastFails(ArrayFromJSON(string_type, "[\"" + not_float + "\"]"), options);
      }

#if !defined(_WIN32) || defined(NDEBUG)
      // Test that casting is locale-independent
      // French locale uses the comma as decimal point
      LocaleGuard locale_guard("fr_FR.UTF-8");
      CheckCast(strings, floats);
#endif
    }
  }
}

TEST(Cast, StringToTimestamp) {
  for (auto string_type : {utf8(), large_utf8()}) {
    auto strings = ArrayFromJSON(string_type, R"(["1970-01-01", null, "2000-02-29"])");

    CheckCast(strings,
              ArrayFromJSON(timestamp(TimeUnit::SECOND), "[0, null, 951782400]"));

    CheckCast(strings,
              ArrayFromJSON(timestamp(TimeUnit::MICRO), "[0, null, 951782400000000]"));

    for (auto unit :
         {TimeUnit::SECOND, TimeUnit::MILLI, TimeUnit::MICRO, TimeUnit::NANO}) {
      for (std::string not_ts : {
               "",
               "xxx",
           }) {
        auto options = CastOptions::Safe(timestamp(unit));
        CheckCastFails(ArrayFromJSON(string_type, "[\"" + not_ts + "\"]"), options);
      }
    }

    // NOTE: timestamp parsing is tested comprehensively in parsing-util-test.cc
  }
}

static void AssertBinaryZeroCopy(std::shared_ptr<Array> lhs, std::shared_ptr<Array> rhs) {
  // null bitmap and data buffers are always zero-copied
  AssertBufferSame(*lhs, *rhs, 0);
  AssertBufferSame(*lhs, *rhs, 2);

  if (offset_bit_width(lhs->type_id()) == offset_bit_width(rhs->type_id())) {
    // offset buffer is zero copied if possible
    AssertBufferSame(*lhs, *rhs, 1);
    return;
  }

  // offset buffers are equivalent
  ArrayVector offsets;
  for (auto array : {lhs, rhs}) {
    auto length = array->length();
    auto buffer = array->data()->buffers[1];
    offsets.push_back(offset_bit_width(array->type_id()) == 32
                          ? *Cast(Int32Array(length, buffer), int64())
                          : std::make_shared<Int64Array>(length, buffer));
  }
  AssertArraysEqual(*offsets[0], *offsets[1]);
}

TEST(Cast, BinaryToString) {
  for (auto bin_type : {binary(), large_binary()}) {
    for (auto string_type : {utf8(), large_utf8()}) {
      // empty -> empty always works
      CheckCast(ArrayFromJSON(bin_type, "[]"), ArrayFromJSON(string_type, "[]"));

      auto invalid_utf8 = InvalidUtf8(bin_type);

      // invalid utf-8 masked by a null bit is not an error
      CheckCast(MaskArrayWithNullsAt(InvalidUtf8(bin_type), {4}),
                MaskArrayWithNullsAt(InvalidUtf8(string_type), {4}));

      // error: invalid utf-8
      auto options = CastOptions::Safe(string_type);
      CheckCastFails(invalid_utf8, options);

      // override utf-8 check
      options.allow_invalid_utf8 = true;
      ASSERT_OK_AND_ASSIGN(auto strings, Cast(*invalid_utf8, string_type, options));
      ASSERT_RAISES(Invalid, strings->ValidateFull());
      AssertBinaryZeroCopy(invalid_utf8, strings);
    }
  }
}

TEST(Cast, BinaryOrStringToBinary) {
  for (auto from_type : {utf8(), large_utf8(), binary(), large_binary()}) {
    for (auto to_type : {binary(), large_binary()}) {
      // empty -> empty always works
      CheckCast(ArrayFromJSON(from_type, "[]"), ArrayFromJSON(to_type, "[]"));

      auto invalid_utf8 = InvalidUtf8(from_type);

      // invalid utf-8 is not an error for binary
      ASSERT_OK_AND_ASSIGN(auto strings, Cast(*invalid_utf8, to_type));
      ValidateOutput(*strings);
      AssertBinaryZeroCopy(invalid_utf8, strings);

      // invalid utf-8 masked by a null bit is not an error
      CheckCast(MaskArrayWithNullsAt(InvalidUtf8(from_type), {4}),
                MaskArrayWithNullsAt(InvalidUtf8(to_type), {4}));
    }
  }
}

TEST(Cast, StringToString) {
  for (auto from_type : {utf8(), large_utf8()}) {
    for (auto to_type : {utf8(), large_utf8()}) {
      // empty -> empty always works
      CheckCast(ArrayFromJSON(from_type, "[]"), ArrayFromJSON(to_type, "[]"));

      auto invalid_utf8 = InvalidUtf8(from_type);

      // invalid utf-8 masked by a null bit is not an error
      CheckCast(MaskArrayWithNullsAt(invalid_utf8, {4}),
                MaskArrayWithNullsAt(InvalidUtf8(to_type), {4}));

      // override utf-8 check
      auto options = CastOptions::Safe(to_type);
      options.allow_invalid_utf8 = true;
      // utf-8 is not checked by Cast when the origin guarantees utf-8
      ASSERT_OK_AND_ASSIGN(auto strings, Cast(*invalid_utf8, to_type, options));
      ASSERT_RAISES(Invalid, strings->ValidateFull());
      AssertBinaryZeroCopy(invalid_utf8, strings);
    }
  }
}

TEST(Cast, IntToString) {
  for (auto string_type : {utf8(), large_utf8()}) {
    CheckCast(ArrayFromJSON(int8(), "[0, 1, 127, -128, null]"),
              ArrayFromJSON(string_type, R"(["0", "1", "127", "-128", null])"));

    CheckCast(ArrayFromJSON(uint8(), "[0, 1, 255, null]"),
              ArrayFromJSON(string_type, R"(["0", "1", "255", null])"));

    CheckCast(ArrayFromJSON(int16(), "[0, 1, 32767, -32768, null]"),
              ArrayFromJSON(string_type, R"(["0", "1", "32767", "-32768", null])"));

    CheckCast(ArrayFromJSON(uint16(), "[0, 1, 65535, null]"),
              ArrayFromJSON(string_type, R"(["0", "1", "65535", null])"));

    CheckCast(
        ArrayFromJSON(int32(), "[0, 1, 2147483647, -2147483648, null]"),
        ArrayFromJSON(string_type, R"(["0", "1", "2147483647", "-2147483648", null])"));

    CheckCast(ArrayFromJSON(uint32(), "[0, 1, 4294967295, null]"),
              ArrayFromJSON(string_type, R"(["0", "1", "4294967295", null])"));

    CheckCast(
        ArrayFromJSON(int64(), "[0, 1, 9223372036854775807, -9223372036854775808, null]"),
        ArrayFromJSON(
            string_type,
            R"(["0", "1", "9223372036854775807", "-9223372036854775808", null])"));

    CheckCast(ArrayFromJSON(uint64(), "[0, 1, 18446744073709551615, null]"),
              ArrayFromJSON(string_type, R"(["0", "1", "18446744073709551615", null])"));
  }
}

TEST(Cast, FloatingToString) {
  for (auto string_type : {utf8(), large_utf8()}) {
    CheckCast(
        ArrayFromJSON(float32(), "[0.0, -0.0, 1.5, -Inf, Inf, NaN, null]"),
        ArrayFromJSON(string_type, R"(["0", "-0", "1.5", "-inf", "inf", "nan", null])"));

    CheckCast(
        ArrayFromJSON(float64(), "[0.0, -0.0, 1.5, -Inf, Inf, NaN, null]"),
        ArrayFromJSON(string_type, R"(["0", "-0", "1.5", "-inf", "inf", "nan", null])"));
  }
}

TEST(Cast, BooleanToString) {
  for (auto string_type : {utf8(), large_utf8()}) {
    CheckCast(ArrayFromJSON(boolean(), "[true, true, false, null]"),
              ArrayFromJSON(string_type, R"(["true", "true", "false", null])"));
  }
}

TEST(Cast, ListToPrimitive) {
  ASSERT_RAISES(NotImplemented,
                Cast(*ArrayFromJSON(list(int8()), "[[1, 2], [3, 4]]"), uint8()));

  ASSERT_RAISES(
      NotImplemented,
      Cast(*ArrayFromJSON(list(binary()), R"([["1", "2"], ["3", "4"]])"), utf8()));
}

using make_list_t = std::shared_ptr<DataType>(const std::shared_ptr<DataType>&);

static const auto list_factories = std::vector<make_list_t*>{&list, &large_list};

static void CheckListToList(const std::vector<std::shared_ptr<DataType>>& value_types,
                            const std::string& json_data) {
  for (auto make_src_list : list_factories) {
    for (auto make_dest_list : list_factories) {
      for (const auto& src_value_type : value_types) {
        for (const auto& dest_value_type : value_types) {
          const auto src_type = make_src_list(src_value_type);
          const auto dest_type = make_dest_list(dest_value_type);
          ARROW_SCOPED_TRACE("src_type = ", src_type->ToString(),
                             ", dest_type = ", dest_type->ToString());
          CheckCast(ArrayFromJSON(src_type, json_data),
                    ArrayFromJSON(dest_type, json_data));
        }
      }
    }
  }
}

TEST(Cast, ListToList) {
  CheckListToList({int32(), float32(), int64()},
                  "[[0], [1], null, [2, 3, 4], [5, 6], null, [], [7], [8, 9]]");
}

TEST(Cast, ListToListNoNulls) {
  // ARROW-12568
  CheckListToList({int32(), float32(), int64()},
                  "[[0], [1], [2, 3, 4], [5, 6], [], [7], [8, 9]]");
}

TEST(Cast, ListToListOptionsPassthru) {
  for (auto make_src_list : list_factories) {
    for (auto make_dest_list : list_factories) {
      auto list_int32 = ArrayFromJSON(make_src_list(int32()), "[[87654321]]");

      auto options = CastOptions::Safe(make_dest_list(int16()));
      CheckCastFails(list_int32, options);

      options.allow_int_overflow = true;
      CheckCast(list_int32, ArrayFromJSON(make_dest_list(int16()), "[[32689]]"), options);
    }
  }
}

TEST(Cast, IdentityCasts) {
  // ARROW-4102
  auto CheckIdentityCast = [](std::shared_ptr<DataType> type, const std::string& json) {
    CheckCastZeroCopy(ArrayFromJSON(type, json), type);
  };

  CheckIdentityCast(null(), "[null, null, null]");
  CheckIdentityCast(boolean(), "[false, true, null, false]");

  for (auto type : kNumericTypes) {
    CheckIdentityCast(type, "[1, 2, null, 4]");
  }
  CheckIdentityCast(binary(), R"(["foo", "bar"])");
  CheckIdentityCast(utf8(), R"(["foo", "bar"])");
  CheckIdentityCast(fixed_size_binary(3), R"(["foo", "bar"])");

  CheckIdentityCast(list(int8()), "[[1, 2], [null], [], [3]]");

  CheckIdentityCast(time32(TimeUnit::MILLI), "[1, 2, 3, 4]");
  CheckIdentityCast(time64(TimeUnit::MICRO), "[1, 2, 3, 4]");
  CheckIdentityCast(date32(), "[1, 2, 3, 4]");
  CheckIdentityCast(date64(), "[86400000, 0]");
  CheckIdentityCast(timestamp(TimeUnit::SECOND), "[1, 2, 3, 4]");

  CheckIdentityCast(dictionary(int8(), int8()), "[1, 2, 3, 1, null, 3]");
}

TEST(Cast, EmptyCasts) {
  // ARROW-4766: 0-length arrays should not segfault
  auto CheckCastEmpty = [](std::shared_ptr<DataType> from, std::shared_ptr<DataType> to) {
    // Python creates array with nullptr instead of 0-length (valid) buffers.
    auto data = ArrayData::Make(from, /* length */ 0, /* buffers */ {nullptr, nullptr});
    CheckCast(MakeArray(data), ArrayFromJSON(to, "[]"));
  };

  for (auto numeric : kNumericTypes) {
    CheckCastEmpty(boolean(), numeric);
    CheckCastEmpty(numeric, boolean());
  }
}

TEST(Cast, CastWithNoValidityBitmapButUnknownNullCount) {
  // ARROW-12672 segfault when casting slightly malformed array
  // (no validity bitmap but atomic null count non-zero)
  auto values = ArrayFromJSON(boolean(), "[true, true, false]");

  ASSERT_OK_AND_ASSIGN(auto expected, Cast(*values, int8()));

  ASSERT_EQ(values->data()->buffers[0], NULLPTR);
  values->data()->null_count = kUnknownNullCount;
  ASSERT_OK_AND_ASSIGN(auto result, Cast(*values, int8()));

  AssertArraysEqual(*expected, *result);
}

// ----------------------------------------------------------------------
// Test casting from NullType

TEST(Cast, FromNull) {
  for (auto to_type : {
           null(),
           uint8(),
           int8(),
           uint16(),
           int16(),
           uint32(),
           int32(),
           uint64(),
           int64(),
           float32(),
           float64(),
           date32(),
           date64(),
           fixed_size_binary(10),
           binary(),
           utf8(),
       }) {
    ASSERT_OK_AND_ASSIGN(auto expected, MakeArrayOfNull(to_type, 10));
    CheckCast(std::make_shared<NullArray>(10), expected);
  }
}

TEST(Cast, FromNullToDictionary) {
  auto from = std::make_shared<NullArray>(10);
  auto to_type = dictionary(int8(), boolean());

  ASSERT_OK_AND_ASSIGN(auto expected, MakeArrayOfNull(to_type, 10));
  CheckCast(from, expected);
}

// ----------------------------------------------------------------------
// Test casting from DictionaryType

TEST(Cast, FromDictionary) {
  ArrayVector dictionaries;
  dictionaries.push_back(std::make_shared<NullArray>(5));

  for (auto num_type : kNumericTypes) {
    dictionaries.push_back(ArrayFromJSON(num_type, "[23, 12, 45, 12, null]"));
  }

  for (auto string_type : kBaseBinaryTypes) {
    dictionaries.push_back(
        ArrayFromJSON(string_type, R"(["foo", "bar", "baz", "foo", null])"));
  }

  for (auto dict : dictionaries) {
    for (auto index_type : kDictionaryIndexTypes) {
      auto indices = ArrayFromJSON(index_type, "[4, 0, 1, 2, 0, 4, null, 2]");
      ASSERT_OK_AND_ASSIGN(auto expected, Take(*dict, *indices));

      ASSERT_OK_AND_ASSIGN(
          auto dict_arr, DictionaryArray::FromArrays(dictionary(index_type, dict->type()),
                                                     indices, dict));
      CheckCast(dict_arr, expected);
    }
  }

  for (auto dict : dictionaries) {
    if (dict->type_id() == Type::NA) continue;

    // Test with a nullptr bitmap buffer (ARROW-3208)
    auto indices = ArrayFromJSON(int8(), "[0, 0, 1, 2, 0, 3, 3, 2]");
    ASSERT_OK_AND_ASSIGN(auto no_nulls, Take(*dict, *indices));
    ASSERT_EQ(no_nulls->null_count(), 0);

    ASSERT_OK_AND_ASSIGN(Datum encoded, DictionaryEncode(no_nulls));

    // Make a new dict array with nullptr bitmap buffer
    auto data = encoded.array()->Copy();
    data->buffers[0] = nullptr;
    data->null_count = 0;
    std::shared_ptr<Array> dict_array = std::make_shared<DictionaryArray>(data);
    ValidateOutput(*dict_array);

    CheckCast(dict_array, no_nulls);
  }
}

std::shared_ptr<Array> SmallintArrayFromJSON(const std::string& json_data) {
  auto arr = ArrayFromJSON(int16(), json_data);
  auto ext_data = arr->data()->Copy();
  ext_data->type = smallint();
  return MakeArray(ext_data);
}

TEST(Cast, ExtensionTypeToIntDowncast) {
  auto smallint = std::make_shared<SmallintType>();
  ExtensionTypeGuard smallint_guard(smallint);

  std::shared_ptr<Array> result;
  std::vector<bool> is_valid = {true, false, true, true, true};

  // Smallint(int16) to int16
  CheckCastZeroCopy(SmallintArrayFromJSON("[0, 100, 200, 1, 2]"), int16());

  // Smallint(int16) to uint8, no overflow/underrun
  CheckCast(SmallintArrayFromJSON("[0, 100, 200, 1, 2]"),
            ArrayFromJSON(uint8(), "[0, 100, 200, 1, 2]"));

  // Smallint(int16) to uint8, with overflow
  {
    CastOptions options;
    options.to_type = uint8();
    CheckCastFails(SmallintArrayFromJSON("[0, null, 256, 1, 3]"), options);

    options.allow_int_overflow = true;
    CheckCast(SmallintArrayFromJSON("[0, null, 256, 1, 3]"),
              ArrayFromJSON(uint8(), "[0, null, 0, 1, 3]"), options);
  }

  // Smallint(int16) to uint8, with underflow
  {
    CastOptions options;
    options.to_type = uint8();
    CheckCastFails(SmallintArrayFromJSON("[0, null, -1, 1, 3]"), options);

    options.allow_int_overflow = true;
    CheckCast(SmallintArrayFromJSON("[0, null, -1, 1, 3]"),
              ArrayFromJSON(uint8(), "[0, null, 255, 1, 3]"), options);
  }
}

TEST(Cast, DictTypeToAnotherDict) {
  auto check_cast = [&](const std::shared_ptr<DataType>& in_type,
                        const std::shared_ptr<DataType>& out_type,
                        const std::string& json_str,
                        const CastOptions& options = CastOptions()) {
    auto arr = ArrayFromJSON(in_type, json_str);
    auto exp = in_type->Equals(out_type) ? arr : ArrayFromJSON(out_type, json_str);
    // this checks for scalars as well
    CheckCast(arr, exp, options);
  };

  //    check same type passed on to casting
  check_cast(dictionary(int8(), int16()), dictionary(int8(), int16()),
             "[1, 2, 3, 1, null, 3]");
  check_cast(dictionary(int8(), int16()), dictionary(int32(), int64()),
             "[1, 2, 3, 1, null, 3]");
  check_cast(dictionary(int8(), int16()), dictionary(int32(), float64()),
             "[1, 2, 3, 1, null, 3]");
  check_cast(dictionary(int32(), utf8()), dictionary(int8(), utf8()),
             R"(["a", "b", "a", null])");

  auto arr = ArrayFromJSON(dictionary(int32(), int32()), "[1, 1000]");
  // check casting unsafe values (checking for unsafe indices is unnecessary, because it
  // would create an invalid index array which results in a ValidateOutput failure)
  ASSERT_OK_AND_ASSIGN(auto casted,
                       Cast(arr, dictionary(int8(), int8()), CastOptions::Unsafe()));
  ValidateOutput(casted);

  // check safe casting values
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Integer value 1000 not in range"),
      Cast(arr, dictionary(int8(), int8()), CastOptions::Safe()));
}

}  // namespace compute
}  // namespace arrow
