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
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"

#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"
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

static std::shared_ptr<Array> FixedSizeInvalidUtf8(std::shared_ptr<DataType> type) {
  if (type->id() == Type::FIXED_SIZE_BINARY) {
    // Assume a particular width for testing
    EXPECT_EQ(3, checked_cast<const FixedSizeBinaryType&>(*type).byte_width());
  }
  return ArrayFromJSON(type,
                       "["
                       R"(
                       "Hi!",
                       "lá",
                       "你",
                       "   ",
                       )"
                       "\"\xa0\xa1\xa2\""
                       "]");
}

static std::vector<std::shared_ptr<DataType>> kNumericTypes = {
    uint8(), int8(),   uint16(), int16(),   uint32(),
    int32(), uint64(), int64(),  float32(), float64()};

static std::vector<std::shared_ptr<DataType>> kIntegerTypes = {
    int8(), uint8(), int16(), uint16(), int32(), uint32(), int64(), uint64()};

static std::vector<std::shared_ptr<DataType>> kDictionaryIndexTypes = kIntegerTypes;

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
      << "\n  to_type:   " << options.to_type.ToString()
      << "\n  from_type: " << input->type()->ToString()
      << "\n  input:     " << input->ToString();

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
                   {binary(), large_binary()});  // no formatting supported

  ExpectCanCast(fixed_size_binary(3),
                {binary(), utf8(), large_binary(), large_utf8(), fixed_size_binary(3)});
  // Doesn't fail since a kernel exists (but it will return an error when executed)
  // ExpectCannotCast(fixed_size_binary(3), {fixed_size_binary(5)});

  ExtensionTypeGuard smallint_guard(smallint());
  ExpectCanCast(smallint(), {int16()});  // cast storage
  ExpectCanCast(smallint(),
                kNumericTypes);  // any cast which is valid for storage is supported
  ExpectCanCast(null(), {smallint()});
  ExpectCanCast(tinyint(), {smallint()});  // cast between compatible storage types

  ExpectCanCast(date32(), {utf8(), large_utf8()});
  ExpectCanCast(date64(), {utf8(), large_utf8()});
  ExpectCanCast(timestamp(TimeUnit::NANO), {utf8(), large_utf8()});
  ExpectCanCast(timestamp(TimeUnit::MICRO), {utf8(), large_utf8()});
  ExpectCanCast(time32(TimeUnit::MILLI), {utf8(), large_utf8()});
  ExpectCanCast(time64(TimeUnit::NANO), {utf8(), large_utf8()});
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

      auto no_overflow_no_truncation = ArrayFromJSON(decimal128(38, 10), R"([
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
    auto truncation_but_no_overflow = ArrayFromJSON(decimal128(38, 10), R"([
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

    auto overflow_no_truncation = ArrayFromJSON(decimal128(38, 10), R"([
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

      auto overflow_and_truncation = ArrayFromJSON(decimal128(38, 10), R"([
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

  Decimal128Builder builder(decimal128(38, -4));
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

TEST(Cast, IntegerToDecimal) {
  for (auto decimal_type : {decimal128(22, 2), decimal256(22, 2)}) {
    for (auto integer_type : kIntegerTypes) {
      CheckCast(
          ArrayFromJSON(integer_type, "[0, 7, null, 100, 99]"),
          ArrayFromJSON(decimal_type, R"(["0.00", "7.00", null, "100.00", "99.00"])"));
    }
  }

  // extreme value
  for (auto decimal_type : {decimal128(19, 0), decimal256(19, 0)}) {
    CheckCast(ArrayFromJSON(int64(), "[-9223372036854775808, 9223372036854775807]"),
              ArrayFromJSON(decimal_type,
                            R"(["-9223372036854775808", "9223372036854775807"])"));
  }
  for (auto decimal_type : {decimal128(20, 0), decimal256(20, 0)}) {
    CheckCast(ArrayFromJSON(uint64(), "[0, 18446744073709551615]"),
              ArrayFromJSON(decimal_type, R"(["0", "18446744073709551615"])"));
  }

  // insufficient output precision
  {
    CastOptions options;

    options.to_type = decimal128(5, 3);
    CheckCastFails(ArrayFromJSON(int8(), "[0]"), options);

    options.to_type = decimal256(76, 67);
    CheckCastFails(ArrayFromJSON(int32(), "[0]"), options);
  }
}

TEST(Cast, Decimal128ToDecimal128) {
  CastOptions options;

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;

    auto no_truncation = ArrayFromJSON(decimal128(38, 10), R"([
          "02.0000000000",
          "30.0000000000",
          "22.0000000000",
        "-121.0000000000",
        null])");
    auto expected = ArrayFromJSON(decimal128(28, 0), R"([
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
    auto d_5_2 = ArrayFromJSON(decimal128(5, 2), R"([
          "12.34",
           "0.56"])");
    auto d_4_2 = ArrayFromJSON(decimal128(4, 2), R"([
          "12.34",
           "0.56"])");

    CheckCast(d_5_2, d_4_2, options);
    CheckCast(d_4_2, d_5_2, options);
  }

  auto d_38_10 = ArrayFromJSON(decimal128(38, 10), R"([
      "-02.1234567890",
       "30.1234567890",
      null])");

  auto d_28_0 = ArrayFromJSON(decimal128(28, 0), R"([
      "-02.",
       "30.",
      null])");

  auto d_38_10_roundtripped = ArrayFromJSON(decimal128(38, 10), R"([
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
  auto d_4_2 = ArrayFromJSON(decimal128(4, 2), R"(["12.34"])");
  for (auto expected : {
           ArrayFromJSON(decimal128(3, 2), R"(["12.34"])"),
           ArrayFromJSON(decimal128(4, 3), R"(["12.340"])"),
           ArrayFromJSON(decimal128(2, 1), R"(["12.3"])"),
       }) {
    options.allow_decimal_truncate = true;
    ASSERT_OK_AND_ASSIGN(auto invalid, Cast(d_4_2, expected->type(), options));
    ASSERT_RAISES(Invalid, invalid.make_array()->ValidateFull());

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
    ASSERT_OK_AND_ASSIGN(auto invalid, Cast(d_4_2, expected->type(), options));
    ASSERT_RAISES(Invalid, invalid.make_array()->ValidateFull());

    options.allow_decimal_truncate = false;
    options.to_type = expected->type();
    CheckCastFails(d_4_2, options);
  }
}

TEST(Cast, Decimal128ToDecimal256) {
  CastOptions options;

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;

    auto no_truncation = ArrayFromJSON(decimal128(38, 10), R"([
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
    auto d_5_2 = ArrayFromJSON(decimal128(5, 2), R"([
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

  auto d128_38_10 = ArrayFromJSON(decimal128(38, 10), R"([
      "-02.1234567890",
       "30.1234567890",
      null])");

  auto d128_28_0 = ArrayFromJSON(decimal128(28, 0), R"([
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
  auto d128_4_2 = ArrayFromJSON(decimal128(4, 2), R"(["12.34"])");
  for (auto expected : {
           ArrayFromJSON(decimal256(3, 2), R"(["12.34"])"),
           ArrayFromJSON(decimal256(4, 3), R"(["12.340"])"),
           ArrayFromJSON(decimal256(2, 1), R"(["12.3"])"),
       }) {
    options.allow_decimal_truncate = true;
    ASSERT_OK_AND_ASSIGN(auto invalid, Cast(d128_4_2, expected->type(), options));
    ASSERT_RAISES(Invalid, invalid.make_array()->ValidateFull());

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
    auto expected = ArrayFromJSON(decimal128(28, 0), R"([
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
    auto d_4_2 = ArrayFromJSON(decimal128(4, 2), R"([
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

  auto d128_28_0 = ArrayFromJSON(decimal128(28, 0), R"([
      "-02.",
       "30.",
      null])");

  auto d128_38_10_roundtripped = ArrayFromJSON(decimal128(38, 10), R"([
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
           ArrayFromJSON(decimal128(3, 2), R"(["12.34"])"),
           ArrayFromJSON(decimal128(4, 3), R"(["12.340"])"),
           ArrayFromJSON(decimal128(2, 1), R"(["12.3"])"),
       }) {
    options.allow_decimal_truncate = true;
    ASSERT_OK_AND_ASSIGN(auto invalid, Cast(d256_4_2, expected->type(), options));
    ASSERT_RAISES(Invalid, invalid.make_array()->ValidateFull());

    options.allow_decimal_truncate = false;
    options.to_type = expected->type();
    CheckCastFails(d256_4_2, options);
  }
}

TEST(Cast, FloatingToDecimal) {
  for (auto float_type : {float32(), float64()}) {
    for (auto decimal_type : {decimal128(5, 2), decimal256(5, 2)}) {
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
    for (auto decimal_type : {decimal128(5, 2), decimal256(5, 2)}) {
      CheckCast(ArrayFromJSON(decimal_type, R"(["0.00", null, "123.45", "999.99"])"),
                ArrayFromJSON(float_type, "[0.0, null, 123.45, 999.99]"));
    }
  }

  // Edge cases are tested for Decimal128::ToReal() and Decimal256::ToReal()
}

TEST(Cast, DecimalToString) {
  for (auto string_type : {utf8(), large_utf8()}) {
    for (auto decimal_type : {decimal128(5, 2), decimal256(5, 2)}) {
      CheckCast(ArrayFromJSON(decimal_type, R"(["0.00", null, "123.45", "999.99"])"),
                ArrayFromJSON(string_type, R"(["0.00", null, "123.45", "999.99"])"));
    }
  }
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

  options.to_type = timestamp(TimeUnit::SECOND);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), "[0, null, 200, 1, 2]"),
            ArrayFromJSON(timestamp(TimeUnit::SECOND), "[0, null, 200, 1, 2]"), options);

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

  CheckCastZeroCopy(
      ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), "[0, null, 2000, 1000, 0]"),
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

constexpr char kTimestampJson[] =
    R"(["1970-01-01T00:00:59.123456789","2000-02-29T23:23:23.999999999",
          "1899-01-01T00:59:20.001001001","2033-05-18T03:33:20.000000000",
          "2020-01-01T01:05:05.001", "2019-12-31T02:10:10.002",
          "2019-12-30T03:15:15.003", "2009-12-31T04:20:20.004132",
          "2010-01-01T05:25:25.005321", "2010-01-03T06:30:30.006163",
          "2010-01-04T07:35:35", "2006-01-01T08:40:40", "2005-12-31T09:45:45",
          "2008-12-28", "2008-12-29", "2012-01-01 01:02:03", null])";
constexpr char kTimestampSecondsJson[] =
    R"(["1970-01-01T00:00:59","2000-02-29T23:23:23",
          "1899-01-01T00:59:20","2033-05-18T03:33:20",
          "2020-01-01T01:05:05", "2019-12-31T02:10:10",
          "2019-12-30T03:15:15", "2009-12-31T04:20:20",
          "2010-01-01T05:25:25", "2010-01-03T06:30:30",
          "2010-01-04T07:35:35", "2006-01-01T08:40:40",
          "2005-12-31T09:45:45", "2008-12-28", "2008-12-29",
          "2012-01-01 01:02:03", null])";
constexpr char kTimestampExtremeJson[] =
    R"(["1677-09-20T00:00:59.123456", "2262-04-13T23:23:23.999999"])";

class CastTimezone : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef _WIN32
    // Initialize timezone database on Windows
    ASSERT_OK(InitTestTimezoneDatabase());
#endif
  }
};

TEST(Cast, TimestampToDate) {
  // See scalar_temporal_test.cc
  auto timestamps = ArrayFromJSON(timestamp(TimeUnit::NANO), kTimestampJson);
  auto date_32 = ArrayFromJSON(date32(),
                               R"([
          0, 11016, -25932, 23148,
          18262, 18261, 18260, 14609,
          14610, 14612, 14613, 13149,
          13148, 14241, 14242, 15340, null
      ])");
  auto date_64 = ArrayFromJSON(date64(),
                               R"([
          0, 951782400000, -2240524800000, 1999987200000,
          1577836800000, 1577750400000, 1577664000000, 1262217600000,
          1262304000000, 1262476800000, 1262563200000, 1136073600000,
          1135987200000, 1230422400000, 1230508800000, 1325376000000, null
      ])");
  // See TestOutsideNanosecondRange in scalar_temporal_test.cc
  auto timestamps_extreme =
      ArrayFromJSON(timestamp(TimeUnit::MICRO),
                    R"(["1677-09-20T00:00:59.123456", "2262-04-13T23:23:23.999999"])");
  auto date_32_extreme = ArrayFromJSON(date32(), "[-106753, 106753]");
  auto date_64_extreme = ArrayFromJSON(date64(), "[-9223459200000, 9223459200000]");

  CheckCast(timestamps, date_32);
  CheckCast(timestamps, date_64);
  CheckCast(timestamps_extreme, date_32_extreme);
  CheckCast(timestamps_extreme, date_64_extreme);
  for (auto u : TimeUnit::values()) {
    auto unit = timestamp(u);
    CheckCast(ArrayFromJSON(unit, kTimestampSecondsJson), date_32);
    CheckCast(ArrayFromJSON(unit, kTimestampSecondsJson), date_64);
  }
}

TEST_F(CastTimezone, ZonedTimestampToDate) {
  {
    // See TestZoned in scalar_temporal_test.cc
    auto timestamps =
        ArrayFromJSON(timestamp(TimeUnit::NANO, "Pacific/Marquesas"), kTimestampJson);
    auto date_32 = ArrayFromJSON(date32(),
                                 R"([
          -1, 11016, -25933, 23147,
          18261, 18260, 18259, 14608,
          14609, 14611, 14612, 13148,
          13148, 14240, 14241, 15339, null
      ])");
    auto date_64 = ArrayFromJSON(date64(), R"([
          -86400000, 951782400000, -2240611200000, 1999900800000,
          1577750400000, 1577664000000, 1577577600000, 1262131200000,
          1262217600000, 1262390400000, 1262476800000, 1135987200000,
          1135987200000, 1230336000000, 1230422400000, 1325289600000, null
      ])");
    CheckCast(timestamps, date_32);
    CheckCast(timestamps, date_64);
  }

  auto date_32 = ArrayFromJSON(date32(), R"([
          0, 11017, -25932, 23148,
          18262, 18261, 18260, 14609,
          14610, 14612, 14613, 13149,
          13148, 14241, 14242, 15340, null
      ])");
  auto date_64 = ArrayFromJSON(date64(), R"([
          0, 951868800000, -2240524800000, 1999987200000, 1577836800000,
          1577750400000, 1577664000000, 1262217600000, 1262304000000,
          1262476800000, 1262563200000, 1136073600000, 1135987200000,
          1230422400000, 1230508800000, 1325376000000, null
      ])");

  for (auto u : TimeUnit::values()) {
    auto timestamps =
        ArrayFromJSON(timestamp(u, "Australia/Broken_Hill"), kTimestampSecondsJson);
    CheckCast(timestamps, date_32);
    CheckCast(timestamps, date_64);
  }

  // Invalid timezone
  for (auto u : TimeUnit::values()) {
    auto timestamps =
        ArrayFromJSON(timestamp(u, "Mars/Mariner_Valley"), kTimestampSecondsJson);
    CheckCastFails(timestamps, CastOptions::Unsafe(date32()));
    CheckCastFails(timestamps, CastOptions::Unsafe(date64()));
  }
}

TEST(Cast, TimestampToTime) {
  // See scalar_temporal_test.cc
  auto timestamps = ArrayFromJSON(timestamp(TimeUnit::NANO), kTimestampJson);
  // See TestOutsideNanosecondRange in scalar_temporal_test.cc
  auto timestamps_extreme =
      ArrayFromJSON(timestamp(TimeUnit::MICRO), kTimestampExtremeJson);
  auto timestamps_us = ArrayFromJSON(timestamp(TimeUnit::MICRO), R"([
          "1970-01-01T00:00:59.123456","2000-02-29T23:23:23.999999",
          "1899-01-01T00:59:20.001001","2033-05-18T03:33:20.000000",
          "2020-01-01T01:05:05.001", "2019-12-31T02:10:10.002",
          "2019-12-30T03:15:15.003", "2009-12-31T04:20:20.004132",
          "2010-01-01T05:25:25.005321", "2010-01-03T06:30:30.006163",
          "2010-01-04T07:35:35", "2006-01-01T08:40:40", "2005-12-31T09:45:45",
          "2008-12-28", "2008-12-29", "2012-01-01 01:02:03", null])");
  auto timestamps_ms = ArrayFromJSON(timestamp(TimeUnit::MILLI), R"([
          "1970-01-01T00:00:59.123","2000-02-29T23:23:23.999",
          "1899-01-01T00:59:20.001","2033-05-18T03:33:20.000",
          "2020-01-01T01:05:05.001", "2019-12-31T02:10:10.002",
          "2019-12-30T03:15:15.003", "2009-12-31T04:20:20.004",
          "2010-01-01T05:25:25.005", "2010-01-03T06:30:30.006",
          "2010-01-04T07:35:35", "2006-01-01T08:40:40", "2005-12-31T09:45:45",
          "2008-12-28", "2008-12-29", "2012-01-01 01:02:03", null])");
  auto timestamps_s = ArrayFromJSON(timestamp(TimeUnit::SECOND), kTimestampSecondsJson);

  auto times = ArrayFromJSON(time64(TimeUnit::NANO), R"([
          59123456789, 84203999999999, 3560001001001, 12800000000000,
          3905001000000, 7810002000000, 11715003000000, 15620004132000,
          19525005321000, 23430006163000, 27335000000000, 31240000000000,
          35145000000000, 0, 0, 3723000000000, null
      ])");
  auto times_ns_us = ArrayFromJSON(time64(TimeUnit::MICRO), R"([
          59123456, 84203999999, 3560001001, 12800000000,
          3905001000, 7810002000, 11715003000, 15620004132,
          19525005321, 23430006163, 27335000000, 31240000000,
          35145000000, 0, 0, 3723000000, null
      ])");
  auto times_ns_ms = ArrayFromJSON(time32(TimeUnit::MILLI), R"([
          59123, 84203999, 3560001, 12800000,
          3905001, 7810002, 11715003, 15620004,
          19525005, 23430006, 27335000, 31240000,
          35145000, 0, 0, 3723000, null
      ])");
  auto times_us_ns = ArrayFromJSON(time64(TimeUnit::NANO), R"([
          59123456000, 84203999999000, 3560001001000, 12800000000000,
          3905001000000, 7810002000000, 11715003000000, 15620004132000,
          19525005321000, 23430006163000, 27335000000000, 31240000000000,
          35145000000000, 0, 0, 3723000000000, null
      ])");
  auto times_ms_ns = ArrayFromJSON(time64(TimeUnit::NANO), R"([
          59123000000, 84203999000000, 3560001000000, 12800000000000,
          3905001000000, 7810002000000, 11715003000000, 15620004000000,
          19525005000000, 23430006000000, 27335000000000, 31240000000000,
          35145000000000, 0, 0, 3723000000000, null
      ])");
  auto times_ms_us = ArrayFromJSON(time64(TimeUnit::MICRO), R"([
          59123000, 84203999000, 3560001000, 12800000000,
          3905001000, 7810002000, 11715003000, 15620004000,
          19525005000, 23430006000, 27335000000, 31240000000,
          35145000000, 0, 0, 3723000000, null
      ])");

  auto times_extreme = ArrayFromJSON(time64(TimeUnit::MICRO), "[59123456, 84203999999]");
  auto times_s = ArrayFromJSON(time32(TimeUnit::SECOND), R"([
          59, 84203, 3560, 12800,
          3905, 7810, 11715, 15620,
          19525, 23430, 27335, 31240,
          35145, 0, 0, 3723, null
      ])");
  auto times_ms = ArrayFromJSON(time32(TimeUnit::MILLI), R"([
          59000, 84203000, 3560000, 12800000,
          3905000, 7810000, 11715000, 15620000,
          19525000, 23430000, 27335000, 31240000,
          35145000, 0, 0, 3723000, null
      ])");
  auto times_us = ArrayFromJSON(time64(TimeUnit::MICRO), R"([
          59000000, 84203000000, 3560000000, 12800000000,
          3905000000, 7810000000, 11715000000, 15620000000,
          19525000000, 23430000000, 27335000000, 31240000000,
          35145000000, 0, 0, 3723000000, null
      ])");
  auto times_ns = ArrayFromJSON(time64(TimeUnit::NANO), R"([
          59000000000, 84203000000000, 3560000000000, 12800000000000,
          3905000000000, 7810000000000, 11715000000000, 15620000000000,
          19525000000000, 23430000000000, 27335000000000, 31240000000000,
          35145000000000, 0, 0, 3723000000000, null
      ])");

  CheckCast(timestamps, times);
  CheckCastFails(timestamps, CastOptions::Safe(time64(TimeUnit::MICRO)));
  CheckCast(timestamps_extreme, times_extreme);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::SECOND), kTimestampSecondsJson), times_s);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::SECOND), kTimestampSecondsJson), times_ms);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::MILLI), kTimestampSecondsJson), times_s);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::MILLI), kTimestampSecondsJson), times_ms);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::MICRO), kTimestampSecondsJson), times_us);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::MICRO), kTimestampSecondsJson), times_ns);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::MICRO), kTimestampSecondsJson), times_ms);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::MICRO), kTimestampSecondsJson), times_s);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::NANO), kTimestampSecondsJson), times_ns);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::NANO), kTimestampSecondsJson), times_us);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::NANO), kTimestampSecondsJson), times_ms);
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::NANO), kTimestampSecondsJson), times_s);

  CastOptions truncate = CastOptions::Safe();
  truncate.allow_time_truncate = true;

  // Truncation tests
  CheckCastFails(timestamps, CastOptions::Safe(time64(TimeUnit::MICRO)));
  CheckCastFails(timestamps, CastOptions::Safe(time32(TimeUnit::MILLI)));
  CheckCastFails(timestamps, CastOptions::Safe(time32(TimeUnit::SECOND)));
  CheckCastFails(timestamps_us, CastOptions::Safe(time32(TimeUnit::MILLI)));
  CheckCastFails(timestamps_us, CastOptions::Safe(time32(TimeUnit::SECOND)));
  CheckCastFails(timestamps_ms, CastOptions::Safe(time32(TimeUnit::SECOND)));
  CheckCast(timestamps, times_ns_us, truncate);
  CheckCast(timestamps, times_ns_ms, truncate);
  CheckCast(timestamps, times_s, truncate);
  CheckCast(timestamps_us, times_ns_ms, truncate);
  CheckCast(timestamps_us, times_s, truncate);
  CheckCast(timestamps_ms, times_s, truncate);

  // Upscaling tests
  CheckCast(timestamps_us, times_us_ns);
  CheckCast(timestamps_ms, times_ms_ns);
  CheckCast(timestamps_ms, times_ms_us);
  CheckCast(timestamps_s, times_ns);
  CheckCast(timestamps_s, times_us);
  CheckCast(timestamps_s, times_ms);

  // Invalid timezone
  for (auto u : TimeUnit::values()) {
    auto timestamps =
        ArrayFromJSON(timestamp(u, "Mars/Mariner_Valley"), kTimestampSecondsJson);
    if (u == TimeUnit::SECOND || u == TimeUnit::MILLI) {
      CheckCastFails(timestamps, CastOptions::Unsafe(time32(u)));
    } else {
      CheckCastFails(timestamps, CastOptions::Unsafe(time64(u)));
    }
  }
}

TEST_F(CastTimezone, ZonedTimestampToTime) {
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::NANO, "Pacific/Marquesas"), kTimestampJson),
            ArrayFromJSON(time64(TimeUnit::NANO), R"([
          52259123456789, 50003999999999, 56480001001001, 65000000000000,
          56105001000000, 60010002000000, 63915003000000, 67820004132000,
          71725005321000, 75630006163000, 79535000000000, 83440000000000,
          945000000000, 52200000000000, 52200000000000, 55923000000000, null
      ])"));

  auto time_s = R"([
          34259, 35603, 35960, 47000,
          41705, 45610, 49515, 53420,
          57325, 61230, 65135, 69040,
          72945, 37800, 37800, 41523, null
      ])";
  auto time_ms = R"([
          34259000, 35603000, 35960000, 47000000,
          41705000, 45610000, 49515000, 53420000,
          57325000, 61230000, 65135000, 69040000,
          72945000, 37800000, 37800000, 41523000, null
      ])";
  auto time_us = R"([
          34259000000, 35603000000, 35960000000, 47000000000,
          41705000000, 45610000000, 49515000000, 53420000000,
          57325000000, 61230000000, 65135000000, 69040000000,
          72945000000, 37800000000, 37800000000, 41523000000, null
      ])";
  auto time_ns = R"([
          34259000000000, 35603000000000, 35960000000000, 47000000000000,
          41705000000000, 45610000000000, 49515000000000, 53420000000000,
          57325000000000, 61230000000000, 65135000000000, 69040000000000,
          72945000000000, 37800000000000, 37800000000000, 41523000000000, null
      ])";
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::SECOND, "Australia/Broken_Hill"),
                          kTimestampSecondsJson),
            ArrayFromJSON(time32(TimeUnit::SECOND), time_s));
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::MILLI, "Australia/Broken_Hill"),
                          kTimestampSecondsJson),
            ArrayFromJSON(time32(TimeUnit::MILLI), time_ms));
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::MICRO, "Australia/Broken_Hill"),
                          kTimestampSecondsJson),
            ArrayFromJSON(time64(TimeUnit::MICRO), time_us));
  CheckCast(ArrayFromJSON(timestamp(TimeUnit::NANO, "Australia/Broken_Hill"),
                          kTimestampSecondsJson),
            ArrayFromJSON(time64(TimeUnit::NANO), time_ns));
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

TEST(Cast, DateToString) {
  for (auto string_type : {utf8(), large_utf8()}) {
    CheckCast(ArrayFromJSON(date32(), "[0, null]"),
              ArrayFromJSON(string_type, R"(["1970-01-01", null])"));
    CheckCast(ArrayFromJSON(date64(), "[86400000, null]"),
              ArrayFromJSON(string_type, R"(["1970-01-02", null])"));
  }
}

TEST(Cast, TimeToString) {
  for (auto string_type : {utf8(), large_utf8()}) {
    CheckCast(ArrayFromJSON(time32(TimeUnit::SECOND), "[1, 62]"),
              ArrayFromJSON(string_type, R"(["00:00:01", "00:01:02"])"));
    CheckCast(
        ArrayFromJSON(time64(TimeUnit::NANO), "[0, 1]"),
        ArrayFromJSON(string_type, R"(["00:00:00.000000000", "00:00:00.000000001"])"));
  }
}

TEST(Cast, TimestampToString) {
  for (auto string_type : {utf8(), large_utf8()}) {
    CheckCast(
        ArrayFromJSON(timestamp(TimeUnit::SECOND), "[-30610224000, -5364662400]"),
        ArrayFromJSON(string_type, R"(["1000-01-01 00:00:00", "1800-01-01 00:00:00"])"));

    CheckCast(
        ArrayFromJSON(timestamp(TimeUnit::MILLI), "[-30610224000000, -5364662400000]"),
        ArrayFromJSON(string_type,
                      R"(["1000-01-01 00:00:00.000", "1800-01-01 00:00:00.000"])"));

    CheckCast(
        ArrayFromJSON(timestamp(TimeUnit::MICRO),
                      "[-30610224000000000, -5364662400000000]"),
        ArrayFromJSON(string_type,
                      R"(["1000-01-01 00:00:00.000000", "1800-01-01 00:00:00.000000"])"));

    CheckCast(
        ArrayFromJSON(timestamp(TimeUnit::NANO),
                      "[-596933876543210988, 349837323456789012]"),
        ArrayFromJSON(
            string_type,
            R"(["1951-02-01 01:02:03.456789012", "1981-02-01 01:02:03.456789012"])"));
  }
}

TEST_F(CastTimezone, TimestampWithZoneToString) {
  for (auto string_type : {utf8(), large_utf8()}) {
    CheckCast(
        ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), "[-30610224000, -5364662400]"),
        ArrayFromJSON(string_type,
                      R"(["1000-01-01 00:00:00Z", "1800-01-01 00:00:00Z"])"));

    CheckCast(
        ArrayFromJSON(timestamp(TimeUnit::SECOND, "America/Phoenix"),
                      "[-34226955, 1456767743]"),
        ArrayFromJSON(string_type,
                      R"(["1968-11-30 13:30:45-0700", "2016-02-29 10:42:23-0700"])"));

    CheckCast(ArrayFromJSON(timestamp(TimeUnit::MILLI, "America/Phoenix"),
                            "[-34226955877, 1456767743456]"),
              ArrayFromJSON(
                  string_type,
                  R"(["1968-11-30 13:30:44.123-0700", "2016-02-29 10:42:23.456-0700"])"));

    CheckCast(
        ArrayFromJSON(timestamp(TimeUnit::MICRO, "America/Phoenix"),
                      "[-34226955877000, 1456767743456789]"),
        ArrayFromJSON(
            string_type,
            R"(["1968-11-30 13:30:44.123000-0700", "2016-02-29 10:42:23.456789-0700"])"));

    CheckCast(
        ArrayFromJSON(timestamp(TimeUnit::NANO, "America/Phoenix"),
                      "[-34226955876543211, 1456767743456789246]"),
        ArrayFromJSON(
            string_type,
            R"(["1968-11-30 13:30:44.123456789-0700", "2016-02-29 10:42:23.456789246-0700"])"));
  }
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
    CheckCastZeroCopy(ArrayFromJSON(date64(), "[0, null, 172800000, 86400000, 0]"),
                      zero_copy_to_type);
  }
  CheckCastZeroCopy(ArrayFromJSON(int64(), "[0, null, 172800000, 86400000, 0]"),
                    date64());
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

TEST(Cast, DurationToString) {
  for (auto string_type : {utf8(), large_utf8()}) {
    for (auto unit : TimeUnit::values()) {
      CheckCast(ArrayFromJSON(duration(unit), "[0, null, 1234567, 2000]"),
                ArrayFromJSON(string_type, R"(["0", null, "1234567", "2000"])"));
    }
  }
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
  const char* expected_message = "Unsupported cast to dense_union<a: int32=0> from int32";
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
      CheckCast(
          ArrayFromJSON(string_type, R"(["0", null, "127", "-1", "0", "0x0", "0x7F"])"),
          ArrayFromJSON(signed_type, "[0, null, 127, -1, 0, 0, 127]"));
    }

    CheckCast(ArrayFromJSON(string_type, R"(["2147483647", null, "-2147483648", "0",
          "0X0", "0x7FFFFFFF", "0XFFFFfFfF", "0Xf0000000"])"),
              ArrayFromJSON(
                  int32(),
                  "[2147483647, null, -2147483648, 0, 0, 2147483647, -1, -268435456]"));

    CheckCast(ArrayFromJSON(string_type,
                            R"(["9223372036854775807", null, "-9223372036854775808", "0",
                    "0x0", "0x7FFFFFFFFFFFFFFf", "0XF000000000000001"])"),
              ArrayFromJSON(int64(),
                            "[9223372036854775807, null, -9223372036854775808, 0, 0, "
                            "9223372036854775807, -1152921504606846975]"));

    for (auto unsigned_type : {uint8(), uint16(), uint32(), uint64()}) {
      CheckCast(ArrayFromJSON(string_type,
                              R"(["0", null, "127", "255", "0", "0X0", "0xff", "0x7f"])"),
                ArrayFromJSON(unsigned_type, "[0, null, 127, 255, 0, 0, 255, 127]"));
    }

    CheckCast(
        ArrayFromJSON(string_type, R"(["2147483647", null, "4294967295", "0",
                                    "0x0", "0x7FFFFFFf", "0xFFFFFFFF"])"),
        ArrayFromJSON(uint32(),
                      "[2147483647, null, 4294967295, 0, 0, 2147483647, 4294967295]"));

    CheckCast(ArrayFromJSON(string_type,
                            R"(["9223372036854775807", null, "18446744073709551615", "0",
                    "0x0", "0x7FFFFFFFFFFFFFFf", "0xfFFFFFFFFFFFFFFf"])"),
              ArrayFromJSON(uint64(),
                            "[9223372036854775807, null, 18446744073709551615, 0, 0, "
                            "9223372036854775807, 18446744073709551615]"));

    for (std::string not_int8 : {
             "z",
             "12 z",
             "128",
             "-129",
             "0.5",
             "0x",
             "0xfff",
             "-0xf0",
         }) {
      auto options = CastOptions::Safe(int8());
      CheckCastFails(ArrayFromJSON(string_type, "[\"" + not_int8 + "\"]"), options);
    }

    for (std::string not_uint8 : {"256", "-1", "0.5", "0x", "0x3wa", "0x123"}) {
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

TEST(Cast, StringToDecimal) {
  for (auto string_type : {utf8(), large_utf8()}) {
    for (auto decimal_type : {decimal128(5, 2), decimal256(5, 2)}) {
      auto strings =
          ArrayFromJSON(string_type, R"(["0.01", null, "127.32", "200.43", "0.54"])");
      auto decimals =
          ArrayFromJSON(decimal_type, R"(["0.01", null, "127.32", "200.43", "0.54"])");
      CheckCast(strings, decimals);

      for (const auto& not_decimal : std::vector<std::string>{"z"}) {
        auto options = CastOptions::Safe(decimal128(5, 2));
        CheckCastFails(ArrayFromJSON(string_type, "[\"" + not_decimal + "\"]"), options);
      }

#if !defined(_WIN32) || defined(NDEBUG)
      // Test that casting is locale-independent
      // French locale uses the comma as decimal point
      LocaleGuard locale_guard("fr_FR.UTF-8");
      CheckCast(strings, decimals);
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

    auto zoned = ArrayFromJSON(string_type,
                               R"(["2020-02-29T00:00:00Z", "2020-03-02T10:11:12+0102"])");
    auto mixed = ArrayFromJSON(string_type,
                               R"(["2020-03-02T10:11:12+0102", "2020-02-29T00:00:00"])");

    // Timestamp with zone offset should not parse as naive
    CheckCastFails(zoned, CastOptions::Safe(timestamp(TimeUnit::SECOND)));

    // Mixed zoned/unzoned should not parse as naive
    CheckCastFails(mixed, CastOptions::Safe(timestamp(TimeUnit::SECOND)));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("expected no zone offset"),
        Cast(mixed, CastOptions::Safe(timestamp(TimeUnit::SECOND))));

    // ...or as timestamp with timezone
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("expected a zone offset"),
        Cast(mixed, CastOptions::Safe(timestamp(TimeUnit::SECOND, "UTC"))));

    // Unzoned should not parse as timestamp with timezone
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("expected a zone offset"),
        Cast(strings, CastOptions::Safe(timestamp(TimeUnit::SECOND, "UTC"))));

    // Timestamp with zone offset can parse as any time zone (since they're unambiguous)
    CheckCast(zoned, ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"),
                                   "[1582934400, 1583140152]"));
    CheckCast(zoned, ArrayFromJSON(timestamp(TimeUnit::SECOND, "America/Phoenix"),
                                   "[1582934400, 1583140152]"));

    // NOTE: timestamp parsing is tested comprehensively in value_parsing_test.cc
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

  auto from_type = fixed_size_binary(3);
  auto invalid_utf8 = FixedSizeInvalidUtf8(from_type);
  for (auto string_type : {utf8(), large_utf8()}) {
    CheckCast(ArrayFromJSON(from_type, "[]"), ArrayFromJSON(string_type, "[]"));

    // invalid utf-8 masked by a null bit is not an error
    CheckCast(MaskArrayWithNullsAt(invalid_utf8, {4}),
              MaskArrayWithNullsAt(FixedSizeInvalidUtf8(string_type), {4}));

    // error: invalid utf-8
    auto options = CastOptions::Safe(string_type);
    CheckCastFails(invalid_utf8, options);

    // override utf-8 check
    options.allow_invalid_utf8 = true;
    ASSERT_OK_AND_ASSIGN(auto strings, Cast(*invalid_utf8, string_type, options));
    ASSERT_RAISES(Invalid, strings->ValidateFull());

    // N.B. null buffer is not always the same if input sliced
    AssertBufferSame(*invalid_utf8, *strings, 0);

    // ARROW-16757: we no longer zero copy, but the contents are equal
    ASSERT_NE(invalid_utf8->data()->buffers[1].get(), strings->data()->buffers[2].get());
    ASSERT_TRUE(invalid_utf8->data()->buffers[1]->Equals(*strings->data()->buffers[2]));
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

  auto from_type = fixed_size_binary(3);
  auto invalid_utf8 = FixedSizeInvalidUtf8(from_type);
  CheckCast(invalid_utf8, invalid_utf8);
  CheckCastFails(invalid_utf8, CastOptions::Safe(fixed_size_binary(5)));
  for (auto to_type : {binary(), large_binary()}) {
    CheckCast(ArrayFromJSON(from_type, "[]"), ArrayFromJSON(to_type, "[]"));
    ASSERT_OK_AND_ASSIGN(auto strings, Cast(*invalid_utf8, to_type));
    ValidateOutput(*strings);

    // N.B. null buffer is not always the same if input sliced
    AssertBufferSame(*invalid_utf8, *strings, 0);

    // ARROW-16757: we no longer zero copy, but the contents are equal
    ASSERT_NE(invalid_utf8->data()->buffers[1].get(), strings->data()->buffers[2].get());
    ASSERT_TRUE(invalid_utf8->data()->buffers[1]->Equals(*strings->data()->buffers[2]));

    // invalid utf-8 masked by a null bit is not an error
    CheckCast(MaskArrayWithNullsAt(invalid_utf8, {4}),
              MaskArrayWithNullsAt(FixedSizeInvalidUtf8(to_type), {4}));
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

static void CheckFSLToFSL(const std::vector<std::shared_ptr<DataType>>& value_types,
                          const std::string& json_data,
                          const std::string& tweaked_val_bit_string,
                          bool children_nulls = true) {
  for (const auto& src_value_type : value_types) {
    for (const auto& dest_value_type : value_types) {
      const auto src_type = fixed_size_list(src_value_type, 2);
      const auto dest_type = fixed_size_list(dest_value_type, 2);
      ARROW_SCOPED_TRACE("src_type = ", src_type->ToString(),
                         ", dest_type = ", dest_type->ToString());
      auto src_array = ArrayFromJSON(src_type, json_data);
      CheckCast(src_array, ArrayFromJSON(dest_type, json_data));
      {
        auto tweaked_array = TweakValidityBit(src_array, 1, false);
        CheckCast(tweaked_array, ArrayFromJSON(dest_type, tweaked_val_bit_string));
      }

      // Sliced Children
      const auto child_data =
          children_nulls ? "[1, 2, null, 4, 5, null]" : "[1, 2, 3, 4, 5, 6]";
      auto children_src = ArrayFromJSON(src_value_type, child_data);
      children_src = children_src->Slice(2);
      auto fsl = std::make_shared<FixedSizeListArray>(src_type, 2, children_src);
      {
        const auto expected_data = children_nulls ? "[null, 4, 5, null]" : "[3, 4, 5, 6]";
        auto children_dst = ArrayFromJSON(dest_value_type, expected_data);
        auto expected = std::make_shared<FixedSizeListArray>(dest_type, 2, children_dst);
        CheckCast(fsl, expected);
      }
      {
        const auto expected_data =
            children_nulls ? "[[null, 4], null]" : "[[3, 4], null]";
        auto tweaked_array = TweakValidityBit(fsl, 1, false);
        auto expected = ArrayFromJSON(dest_type, expected_data);
        CheckCast(tweaked_array, expected);
      }

      // Invalid fixed_size_list cast.
      const auto incorrect_dest_type = fixed_size_list(dest_value_type, 3);
      ASSERT_RAISES(TypeError, Cast(src_array, CastOptions::Safe(incorrect_dest_type)))
          << "Size of FixedList is not the same.";
    }
  }
}

TEST(Cast, FSLToFSL) {
  CheckFSLToFSL({int32(), float32(), int64()}, "[[0, 1], [2, 3], [null, 5], null]",
                /*tweaked_val_bit_string=*/"[[0, 1], null, [null, 5], null]");
}

TEST(Cast, FSLToFSLNoNulls) {
  CheckFSLToFSL({int32(), float32(), int64()}, "[[0, 1], [2, 3], [4, 5]]",
                /*tweaked_val_bit_string=*/"[[0, 1], null, [4, 5]]",
                /*children_null=*/false);
}

TEST(Cast, FSLToFSLOptionsPassThru) {
  auto fsl_int32 = ArrayFromJSON(fixed_size_list(int32(), 1), "[[87654321]]");

  auto options = CastOptions::Safe(fixed_size_list(int16(), 1));
  CheckCastFails(fsl_int32, options);

  options.allow_int_overflow = true;
  CheckCast(fsl_int32, ArrayFromJSON(fixed_size_list(int16(), 1), "[[32689]]"), options);
}

TEST(Cast, CastMap) {
  const std::string map_json =
      "[[[\"x\", 1], [\"y\", 8], [\"z\", 9]], [[\"x\", 6]], [[\"y\", 36]]]";
  const std::string map_json_nullable =
      "[[[\"x\", 1], [\"y\", null], [\"z\", 9]], null, [[\"y\", 36]]]";

  auto CheckMapCast = [map_json,
                       map_json_nullable](const std::shared_ptr<DataType>& dst_type) {
    std::shared_ptr<DataType> src_type =
        std::make_shared<MapType>(field("x", utf8(), false), field("y", int64()));
    std::shared_ptr<Array> src = ArrayFromJSON(src_type, map_json);
    std::shared_ptr<Array> dst = ArrayFromJSON(dst_type, map_json);
    CheckCast(src, dst);

    src = ArrayFromJSON(src_type, map_json_nullable);
    dst = ArrayFromJSON(dst_type, map_json_nullable);
    CheckCast(src, dst);
  };

  // Can rename fields
  CheckMapCast(std::make_shared<MapType>(field("a", utf8(), false), field("b", int64())));
  // Can map keys and values
  CheckMapCast(map(large_utf8(), field("y", int32())));
  // Can cast a map to a to a list<struct<keys=.., values=..>>
  CheckMapCast(list(struct_({field("a", utf8()), field("b", int64())})));
  // Can cast a map to a large_list<struct<keys=.., values=..>>
  CheckMapCast(large_list(struct_({field("a", utf8()), field("b", int64())})));

  // Can rename nested field names
  std::shared_ptr<DataType> src_type = map(utf8(), field("x", list(field("a", int64()))));
  std::shared_ptr<DataType> dst_type = map(utf8(), field("y", list(field("b", int64()))));

  std::shared_ptr<Array> src =
      ArrayFromJSON(src_type, "[[[\"1\", [1,2,3]]], [[\"2\", [4,5,6]]]]");
  std::shared_ptr<Array> dst =
      ArrayFromJSON(dst_type, "[[[\"1\", [1,2,3]]], [[\"2\", [4,5,6]]]]");

  CheckCast(src, dst);

  // Cannot cast to a list<struct<[fields]>> if there are not exactly 2 fields
  dst_type = list(
      struct_({field("key", int32()), field("value", int64()), field("extra", int64())}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("must be cast to a list<struct> with exactly two fields"),
      Cast(src, dst_type));
}

static void CheckStructToStruct(
    const std::vector<std::shared_ptr<DataType>>& value_types) {
  for (const auto& src_value_type : value_types) {
    for (const auto& dest_value_type : value_types) {
      std::vector<std::string> field_names = {"a", "b"};
      std::shared_ptr<Array> a1, b1, a2, b2;
      a1 = ArrayFromJSON(src_value_type, "[1, 2, 3, 4, null]");
      b1 = ArrayFromJSON(src_value_type, "[null, 7, 8, 9, 0]");
      a2 = ArrayFromJSON(dest_value_type, "[1, 2, 3, 4, null]");
      b2 = ArrayFromJSON(dest_value_type, "[null, 7, 8, 9, 0]");
      ASSERT_OK_AND_ASSIGN(auto src, StructArray::Make({a1, b1}, field_names));
      ASSERT_OK_AND_ASSIGN(auto dest, StructArray::Make({a2, b2}, field_names));

      CheckCast(src, dest);

      std::shared_ptr<Buffer> null_bitmap;
      BitmapFromVector<int>({0, 1, 0, 1, 0}, &null_bitmap);

      ASSERT_OK_AND_ASSIGN(auto src_nulls,
                           StructArray::Make({a1, b1}, field_names, null_bitmap));
      ASSERT_OK_AND_ASSIGN(auto dest_nulls,
                           StructArray::Make({a2, b2}, field_names, null_bitmap));
      CheckCast(src_nulls, dest_nulls);
    }
  }
}

static void CheckStructToStructSubset(
    const std::vector<std::shared_ptr<DataType>>& value_types) {
  for (const auto& src_value_type : value_types) {
    ARROW_SCOPED_TRACE("From type: ", src_value_type->ToString());
    for (const auto& dest_value_type : value_types) {
      ARROW_SCOPED_TRACE("To type: ", dest_value_type->ToString());

      std::vector<std::string> field_names = {"a", "b", "c", "d", "e"};

      std::shared_ptr<Array> a1, b1, c1, d1, e1;
      a1 = ArrayFromJSON(src_value_type, "[1, 2, 5]");
      b1 = ArrayFromJSON(src_value_type, "[3, 4, 7]");
      c1 = ArrayFromJSON(src_value_type, "[9, 11, 44]");
      d1 = ArrayFromJSON(src_value_type, "[6, 51, 49]");
      e1 = ArrayFromJSON(src_value_type, "[19, 17, 74]");

      std::shared_ptr<Array> a2, b2, c2, d2, e2;
      a2 = ArrayFromJSON(dest_value_type, "[1, 2, 5]");
      b2 = ArrayFromJSON(dest_value_type, "[3, 4, 7]");
      c2 = ArrayFromJSON(dest_value_type, "[9, 11, 44]");
      d2 = ArrayFromJSON(dest_value_type, "[6, 51, 49]");
      e2 = ArrayFromJSON(dest_value_type, "[19, 17, 74]");

      ASSERT_OK_AND_ASSIGN(auto src,
                           StructArray::Make({a1, b1, c1, d1, e1}, field_names));
      ASSERT_OK_AND_ASSIGN(auto dest1,
                           StructArray::Make({a2}, std::vector<std::string>{"a"}));
      CheckCast(src, dest1);

      ASSERT_OK_AND_ASSIGN(
          auto dest2, StructArray::Make({b2, c2}, std::vector<std::string>{"b", "c"}));
      CheckCast(src, dest2);

      ASSERT_OK_AND_ASSIGN(
          auto dest3,
          StructArray::Make({c2, d2, e2}, std::vector<std::string>{"c", "d", "e"}));
      CheckCast(src, dest3);

      ASSERT_OK_AND_ASSIGN(
          auto dest4, StructArray::Make({a2, b2, c2, e2},
                                        std::vector<std::string>{"a", "b", "c", "e"}));
      CheckCast(src, dest4);

      ASSERT_OK_AND_ASSIGN(
          auto dest5, StructArray::Make({a2, b2, c2, d2, e2}, {"a", "b", "c", "d", "e"}));
      CheckCast(src, dest5);

      // field does not exist
      const auto dest6 = arrow::struct_({std::make_shared<Field>("a", int8()),
                                         std::make_shared<Field>("d", int16()),
                                         std::make_shared<Field>("f", int64())});
      const auto options6 = CastOptions::Safe(dest6);
      EXPECT_RAISES_WITH_MESSAGE_THAT(
          TypeError,
          ::testing::HasSubstr("struct fields don't match or are in the wrong order"),
          Cast(src, options6));

      // fields in wrong order
      const auto dest7 = arrow::struct_({std::make_shared<Field>("a", int8()),
                                         std::make_shared<Field>("c", int16()),
                                         std::make_shared<Field>("b", int64())});
      const auto options7 = CastOptions::Safe(dest7);
      EXPECT_RAISES_WITH_MESSAGE_THAT(
          TypeError,
          ::testing::HasSubstr("struct fields don't match or are in the wrong order"),
          Cast(src, options7));

      // duplicate missing field names
      const auto dest8 = arrow::struct_(
          {std::make_shared<Field>("a", int8()), std::make_shared<Field>("c", int16()),
           std::make_shared<Field>("d", int32()), std::make_shared<Field>("a", int64())});
      const auto options8 = CastOptions::Safe(dest8);
      EXPECT_RAISES_WITH_MESSAGE_THAT(
          TypeError,
          ::testing::HasSubstr("struct fields don't match or are in the wrong order"),
          Cast(src, options8));

      // duplicate present field names
      ASSERT_OK_AND_ASSIGN(
          auto src_duplicate_field_names,
          StructArray::Make({a1, b1, c1}, std::vector<std::string>{"a", "a", "a"}));

      ASSERT_OK_AND_ASSIGN(auto dest1_duplicate_field_names,
                           StructArray::Make({a2}, std::vector<std::string>{"a"}));
      CheckCast(src_duplicate_field_names, dest1_duplicate_field_names);

      ASSERT_OK_AND_ASSIGN(
          auto dest2_duplicate_field_names,
          StructArray::Make({a2, b2}, std::vector<std::string>{"a", "a"}));
      CheckCast(src_duplicate_field_names, dest2_duplicate_field_names);

      ASSERT_OK_AND_ASSIGN(
          auto dest3_duplicate_field_names,
          StructArray::Make({a2, b2, c2}, std::vector<std::string>{"a", "a", "a"}));
      CheckCast(src_duplicate_field_names, dest3_duplicate_field_names);
    }
  }
}

static void CheckStructToStructSubsetWithNulls(
    const std::vector<std::shared_ptr<DataType>>& value_types) {
  for (const auto& src_value_type : value_types) {
    ARROW_SCOPED_TRACE("From type: ", src_value_type->ToString());
    for (const auto& dest_value_type : value_types) {
      ARROW_SCOPED_TRACE("To type: ", dest_value_type->ToString());

      std::vector<std::string> field_names = {"a", "b", "c", "d", "e"};

      std::shared_ptr<Array> a1, b1, c1, d1, e1;
      a1 = ArrayFromJSON(src_value_type, "[1, 2, 5]");
      b1 = ArrayFromJSON(src_value_type, "[3, null, 7]");
      c1 = ArrayFromJSON(src_value_type, "[9, 11, 44]");
      d1 = ArrayFromJSON(src_value_type, "[6, 51, null]");
      e1 = ArrayFromJSON(src_value_type, "[null, 17, 74]");

      std::shared_ptr<Array> a2, b2, c2, d2, e2;
      a2 = ArrayFromJSON(dest_value_type, "[1, 2, 5]");
      b2 = ArrayFromJSON(dest_value_type, "[3, null, 7]");
      c2 = ArrayFromJSON(dest_value_type, "[9, 11, 44]");
      d2 = ArrayFromJSON(dest_value_type, "[6, 51, null]");
      e2 = ArrayFromJSON(dest_value_type, "[null, 17, 74]");

      std::shared_ptr<Buffer> null_bitmap;
      BitmapFromVector<int>({0, 1, 0}, &null_bitmap);

      ASSERT_OK_AND_ASSIGN(auto src_null, StructArray::Make({a1, b1, c1, d1, e1},
                                                            field_names, null_bitmap));
      ASSERT_OK_AND_ASSIGN(
          auto dest1_null,
          StructArray::Make({a2}, std::vector<std::string>{"a"}, null_bitmap));
      CheckCast(src_null, dest1_null);

      ASSERT_OK_AND_ASSIGN(
          auto dest2_null,
          StructArray::Make({b2, c2}, std::vector<std::string>{"b", "c"}, null_bitmap));
      CheckCast(src_null, dest2_null);

      ASSERT_OK_AND_ASSIGN(
          auto dest3_null,
          StructArray::Make({a2, d2, e2}, std::vector<std::string>{"a", "d", "e"},
                            null_bitmap));
      CheckCast(src_null, dest3_null);

      ASSERT_OK_AND_ASSIGN(
          auto dest4_null,
          StructArray::Make({a2, b2, c2, e2},
                            std::vector<std::string>{"a", "b", "c", "e"}, null_bitmap));
      CheckCast(src_null, dest4_null);

      ASSERT_OK_AND_ASSIGN(
          auto dest5_null,
          StructArray::Make({a2, b2, c2, d2, e2},
                            std::vector<std::string>{"a", "b", "c", "d", "e"},
                            null_bitmap));
      CheckCast(src_null, dest5_null);

      // field does not exist
      const auto dest6_null = arrow::struct_({std::make_shared<Field>("a", int8()),
                                              std::make_shared<Field>("d", int16()),
                                              std::make_shared<Field>("f", int64())});
      const auto options6_null = CastOptions::Safe(dest6_null);
      EXPECT_RAISES_WITH_MESSAGE_THAT(
          TypeError,
          ::testing::HasSubstr("struct fields don't match or are in the wrong order"),
          Cast(src_null, options6_null));

      // fields in wrong order
      const auto dest7_null = arrow::struct_({std::make_shared<Field>("a", int8()),
                                              std::make_shared<Field>("c", int16()),
                                              std::make_shared<Field>("b", int64())});
      const auto options7_null = CastOptions::Safe(dest7_null);
      EXPECT_RAISES_WITH_MESSAGE_THAT(
          TypeError,
          ::testing::HasSubstr("struct fields don't match or are in the wrong order"),
          Cast(src_null, options7_null));

      // duplicate missing field names
      const auto dest8_null = arrow::struct_(
          {std::make_shared<Field>("a", int8()), std::make_shared<Field>("c", int16()),
           std::make_shared<Field>("d", int32()), std::make_shared<Field>("a", int64())});
      const auto options8_null = CastOptions::Safe(dest8_null);
      EXPECT_RAISES_WITH_MESSAGE_THAT(
          TypeError,
          ::testing::HasSubstr("struct fields don't match or are in the wrong order"),
          Cast(src_null, options8_null));

      // duplicate present field values
      ASSERT_OK_AND_ASSIGN(
          auto src_duplicate_field_names_null,
          StructArray::Make({a1, b1, c1}, std::vector<std::string>{"a", "a", "a"},
                            null_bitmap));

      ASSERT_OK_AND_ASSIGN(
          auto dest1_duplicate_field_names_null,
          StructArray::Make({a2}, std::vector<std::string>{"a"}, null_bitmap));
      CheckCast(src_duplicate_field_names_null, dest1_duplicate_field_names_null);

      ASSERT_OK_AND_ASSIGN(
          auto dest2_duplicate_field_names_null,
          StructArray::Make({a2, b2}, std::vector<std::string>{"a", "a"}, null_bitmap));
      CheckCast(src_duplicate_field_names_null, dest2_duplicate_field_names_null);

      ASSERT_OK_AND_ASSIGN(
          auto dest3_duplicate_field_names_null,
          StructArray::Make({a2, b2, c2}, std::vector<std::string>{"a", "a", "a"},
                            null_bitmap));
      CheckCast(src_duplicate_field_names_null, dest3_duplicate_field_names_null);
    }
  }
}

TEST(Cast, StructToSameSizedAndNamedStruct) { CheckStructToStruct(NumericTypes()); }

TEST(Cast, StructToStructSubset) { CheckStructToStructSubset(NumericTypes()); }

TEST(Cast, StructToStructSubsetWithNulls) {
  CheckStructToStructSubsetWithNulls(NumericTypes());
}

TEST(Cast, StructToSameSizedButDifferentNamedStruct) {
  std::vector<std::string> field_names = {"a", "b"};
  std::shared_ptr<Array> a, b;
  a = ArrayFromJSON(int8(), "[1, 2]");
  b = ArrayFromJSON(int8(), "[3, 4]");
  ASSERT_OK_AND_ASSIGN(auto src, StructArray::Make({a, b}, field_names));

  const auto dest = arrow::struct_(
      {std::make_shared<Field>("c", int8()), std::make_shared<Field>("d", int8())});
  const auto options = CastOptions::Safe(dest);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("struct fields don't match or are in the wrong order"),
      Cast(src, options));
}

TEST(Cast, StructToBiggerStruct) {
  std::vector<std::string> field_names = {"a", "b"};
  std::shared_ptr<Array> a, b;
  a = ArrayFromJSON(int8(), "[1, 2]");
  b = ArrayFromJSON(int8(), "[3, 4]");
  ASSERT_OK_AND_ASSIGN(auto src, StructArray::Make({a, b}, field_names));

  const auto dest = arrow::struct_({std::make_shared<Field>("a", int8()),
                                    std::make_shared<Field>("b", int8()),
                                    std::make_shared<Field>("c", int8())});
  const auto options = CastOptions::Safe(dest);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("struct fields don't match or are in the wrong order"),
      Cast(src, options));
}

TEST(Cast, StructToDifferentNullabilityStruct) {
  {
    // OK to go from non-nullable to nullable...
    std::vector<std::shared_ptr<Field>> fields_src_non_nullable = {
        std::make_shared<Field>("a", int8(), false),
        std::make_shared<Field>("b", int8(), false),
        std::make_shared<Field>("c", int8(), false)};
    std::shared_ptr<Array> a_src_non_nullable, b_src_non_nullable, c_src_non_nullable;
    a_src_non_nullable = ArrayFromJSON(int8(), "[11, 23, 56]");
    b_src_non_nullable = ArrayFromJSON(int8(), "[32, 46, 37]");
    c_src_non_nullable = ArrayFromJSON(int8(), "[95, 11, 44]");
    ASSERT_OK_AND_ASSIGN(
        auto src_non_nullable,
        StructArray::Make({a_src_non_nullable, b_src_non_nullable, c_src_non_nullable},
                          fields_src_non_nullable));

    std::shared_ptr<Array> a_dest_nullable, b_dest_nullable, c_dest_nullable;
    a_dest_nullable = ArrayFromJSON(int64(), "[11, 23, 56]");
    b_dest_nullable = ArrayFromJSON(int64(), "[32, 46, 37]");
    c_dest_nullable = ArrayFromJSON(int64(), "[95, 11, 44]");

    std::vector<std::shared_ptr<Field>> fields_dest1_nullable = {
        std::make_shared<Field>("a", int64(), true),
        std::make_shared<Field>("b", int64(), true),
        std::make_shared<Field>("c", int64(), true)};
    ASSERT_OK_AND_ASSIGN(
        auto dest1_nullable,
        StructArray::Make({a_dest_nullable, b_dest_nullable, c_dest_nullable},
                          fields_dest1_nullable));
    CheckCast(src_non_nullable, dest1_nullable);

    std::vector<std::shared_ptr<Field>> fields_dest2_nullable = {
        std::make_shared<Field>("a", int64(), true),
        std::make_shared<Field>("c", int64(), true)};
    ASSERT_OK_AND_ASSIGN(
        auto dest2_nullable,
        StructArray::Make({a_dest_nullable, c_dest_nullable}, fields_dest2_nullable));
    CheckCast(src_non_nullable, dest2_nullable);

    std::vector<std::shared_ptr<Field>> fields_dest3_nullable = {
        std::make_shared<Field>("b", int64(), true)};
    ASSERT_OK_AND_ASSIGN(auto dest3_nullable,
                         StructArray::Make({b_dest_nullable}, fields_dest3_nullable));
    CheckCast(src_non_nullable, dest3_nullable);
  }
  {
    // But NOT OK to go from nullable to non-nullable...
    std::vector<std::shared_ptr<Field>> fields_src_nullable = {
        std::make_shared<Field>("a", int8(), true),
        std::make_shared<Field>("b", int8(), true),
        std::make_shared<Field>("c", int8(), true)};
    std::shared_ptr<Array> a_src_nullable, b_src_nullable, c_src_nullable;
    a_src_nullable = ArrayFromJSON(int8(), "[1, null, 5]");
    b_src_nullable = ArrayFromJSON(int8(), "[3, 4, null]");
    c_src_nullable = ArrayFromJSON(int8(), "[9, 11, 44]");
    ASSERT_OK_AND_ASSIGN(
        auto src_nullable,
        StructArray::Make({a_src_nullable, b_src_nullable, c_src_nullable},
                          fields_src_nullable));

    std::vector<std::shared_ptr<Field>> fields_dest1_non_nullable = {
        std::make_shared<Field>("a", int64(), false),
        std::make_shared<Field>("b", int64(), false),
        std::make_shared<Field>("c", int64(), false)};
    const auto dest1_non_nullable = arrow::struct_(fields_dest1_non_nullable);
    const auto options1_non_nullable = CastOptions::Safe(dest1_non_nullable);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        TypeError,
        ::testing::HasSubstr("cannot cast nullable field to non-nullable field"),
        Cast(src_nullable, options1_non_nullable));

    std::vector<std::shared_ptr<Field>> fields_dest2_non_nullble = {
        std::make_shared<Field>("a", int64(), false),
        std::make_shared<Field>("c", int64(), false)};
    const auto dest2_non_nullable = arrow::struct_(fields_dest2_non_nullble);
    const auto options2_non_nullable = CastOptions::Safe(dest2_non_nullable);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        TypeError,
        ::testing::HasSubstr("cannot cast nullable field to non-nullable field"),
        Cast(src_nullable, options2_non_nullable));

    std::vector<std::shared_ptr<Field>> fields_dest3_non_nullble = {
        std::make_shared<Field>("c", int64(), false)};
    const auto dest3_non_nullable = arrow::struct_(fields_dest3_non_nullble);
    const auto options3_non_nullable = CastOptions::Safe(dest3_non_nullable);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        TypeError,
        ::testing::HasSubstr("cannot cast nullable field to non-nullable field"),
        Cast(src_nullable, options3_non_nullable));
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

std::shared_ptr<Array> TinyintArrayFromJSON(const std::string& json_data) {
  auto arr = ArrayFromJSON(int8(), json_data);
  auto ext_data = arr->data()->Copy();
  ext_data->type = tinyint();
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

TEST(Cast, PrimitiveToExtension) {
  {
    auto primitive_array = ArrayFromJSON(uint8(), "[0, 1, 3]");
    auto extension_array = SmallintArrayFromJSON("[0, 1, 3]");
    CastOptions options;
    options.to_type = smallint();
    CheckCast(primitive_array, extension_array, options);
  }
  {
    CastOptions options;
    options.to_type = smallint();
    CheckCastFails(ArrayFromJSON(utf8(), "[\"hello\"]"), options);
  }
}

TEST(Cast, ExtensionDictToExtension) {
  auto extension_array = SmallintArrayFromJSON("[1, 2, 1]");
  auto indices_array = ArrayFromJSON(int32(), "[0, 1, 0]");

  ASSERT_OK_AND_ASSIGN(auto dict_array,
                       DictionaryArray::FromArrays(indices_array, extension_array));

  CastOptions options;
  options.to_type = smallint();
  CheckCast(dict_array, extension_array, options);
}

TEST(Cast, IntToExtensionTypeDowncast) {
  CheckCast(ArrayFromJSON(uint8(), "[0, 100, 200, 1, 2]"),
            SmallintArrayFromJSON("[0, 100, 200, 1, 2]"));

  // int32 to Smallint(int16), with overflow
  {
    CastOptions options;
    options.to_type = smallint();
    CheckCastFails(ArrayFromJSON(int32(), "[0, null, 32768, 1, 3]"), options);

    options.allow_int_overflow = true;
    CheckCast(ArrayFromJSON(int32(), "[0, null, 32768, 1, 3]"),
              SmallintArrayFromJSON("[0, null, -32768, 1, 3]"), options);
  }

  // int32 to Smallint(int16), with underflow
  {
    CastOptions options;
    options.to_type = smallint();
    CheckCastFails(ArrayFromJSON(int32(), "[0, null, -32769, 1, 3]"), options);

    options.allow_int_overflow = true;
    CheckCast(ArrayFromJSON(int32(), "[0, null, -32769, 1, 3]"),
              SmallintArrayFromJSON("[0, null, 32767, 1, 3]"), options);
  }

  // Cannot cast between extension types when storage types differ
  {
    CastOptions options;
    options.to_type = smallint();
    auto tiny_array = TinyintArrayFromJSON("[0, 1, 3]");
    ASSERT_NOT_OK(Cast(tiny_array, smallint(), options));
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

TEST(Cast, NoOutBitmapIfInIsAllValid) {
  auto a = ArrayFromJSON(int8(), "[1]");
  CastOptions options;
  options.to_type = int32();
  ASSERT_OK_AND_ASSIGN(auto result, CallFunction("cast", {a}, &options));
  auto res = result.make_array();
  ASSERT_EQ(a->data()->buffers[0], nullptr);
  ASSERT_EQ(res->data()->buffers[0], nullptr);
}

}  // namespace compute
}  // namespace arrow
