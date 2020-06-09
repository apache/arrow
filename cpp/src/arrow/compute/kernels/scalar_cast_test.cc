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

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/extension_type.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"

#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/test_util.h"

namespace arrow {
namespace compute {

using internal::checked_cast;

static constexpr const char* kInvalidUtf8 = "\xa0\xa1";

static std::vector<std::shared_ptr<DataType>> kNumericTypes = {
    uint8(), int8(),   uint16(), int16(),   uint32(),
    int32(), uint64(), int64(),  float32(), float64()};

static void AssertBufferSame(const Array& left, const Array& right, int buffer_index) {
  ASSERT_EQ(left.data()->buffers[buffer_index].get(),
            right.data()->buffers[buffer_index].get());
}

class TestCast : public TestBase {
 public:
  void CheckPass(const Array& input, const Array& expected,
                 const std::shared_ptr<DataType>& out_type, const CastOptions& options) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> result, Cast(input, out_type, options));
    ASSERT_OK(result->ValidateFull());
    AssertArraysEqual(expected, *result, /*verbose=*/true);
  }

  template <typename InType, typename I_TYPE>
  void CheckFails(const std::shared_ptr<DataType>& in_type,
                  const std::vector<I_TYPE>& in_values, const std::vector<bool>& is_valid,
                  const std::shared_ptr<DataType>& out_type, const CastOptions& options) {
    std::shared_ptr<Array> input;
    if (is_valid.size() > 0) {
      ArrayFromVector<InType, I_TYPE>(in_type, is_valid, in_values, &input);
    } else {
      ArrayFromVector<InType, I_TYPE>(in_type, in_values, &input);
    }
    ASSERT_RAISES(Invalid, Cast(*input, out_type, options));
  }

  void CheckZeroCopy(const Array& input, const std::shared_ptr<DataType>& out_type) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> result, Cast(input, out_type));
    ASSERT_OK(result->ValidateFull());
    ASSERT_EQ(input.data()->buffers.size(), result->data()->buffers.size());
    for (size_t i = 0; i < input.data()->buffers.size(); ++i) {
      AssertBufferSame(input, *result, static_cast<int>(i));
    }
  }

  template <typename InType, typename I_TYPE, typename OutType, typename O_TYPE>
  void CheckCase(const std::shared_ptr<DataType>& in_type,
                 const std::vector<I_TYPE>& in_values, const std::vector<bool>& is_valid,
                 const std::shared_ptr<DataType>& out_type,
                 const std::vector<O_TYPE>& out_values, const CastOptions& options) {
    ASSERT_EQ(in_values.size(), out_values.size());
    std::shared_ptr<Array> input, expected;
    if (is_valid.size() > 0) {
      ASSERT_EQ(is_valid.size(), out_values.size());
      ArrayFromVector<InType, I_TYPE>(in_type, is_valid, in_values, &input);
      ArrayFromVector<OutType, O_TYPE>(out_type, is_valid, out_values, &expected);
    } else {
      ArrayFromVector<InType, I_TYPE>(in_type, in_values, &input);
      ArrayFromVector<OutType, O_TYPE>(out_type, out_values, &expected);
    }
    CheckPass(*input, *expected, out_type, options);

    // Check a sliced variant
    if (input->length() > 1) {
      CheckPass(*input->Slice(1), *expected->Slice(1), out_type, options);
    }
  }

  void CheckCaseJSON(const std::shared_ptr<DataType>& in_type,
                     const std::shared_ptr<DataType>& out_type,
                     const std::string& in_json, const std::string& expected_json,
                     const CastOptions& options = CastOptions()) {
    std::shared_ptr<Array> input = ArrayFromJSON(in_type, in_json);
    std::shared_ptr<Array> expected = ArrayFromJSON(out_type, expected_json);
    ASSERT_EQ(input->length(), expected->length());
    CheckPass(*input, *expected, out_type, options);

    // Check a sliced variant
    if (input->length() > 1) {
      CheckPass(*input->Slice(1), *expected->Slice(1), out_type, options);
    }
  }

  template <typename SourceType, typename DestType>
  void TestCastBinaryToString() {
    CastOptions options;
    auto src_type = TypeTraits<SourceType>::type_singleton();
    auto dest_type = TypeTraits<DestType>::type_singleton();

    // All valid except the last one
    std::vector<bool> all = {1, 1, 1, 1, 1};
    std::vector<bool> valid = {1, 1, 1, 1, 0};
    std::vector<std::string> strings = {"Hi", "olá mundo", "你好世界", "", kInvalidUtf8};

    std::shared_ptr<Array> array;

    // Should accept when invalid but null.
    ArrayFromVector<SourceType, std::string>(src_type, valid, strings, &array);
    CheckZeroCopy(*array, dest_type);

    // Should refuse due to invalid utf8 payload
    CheckFails<SourceType, std::string>(src_type, strings, all, dest_type, options);

    // Should accept due to option override
    options.allow_invalid_utf8 = true;
    CheckCase<SourceType, std::string, DestType, std::string>(
        src_type, strings, all, dest_type, strings, options);
  }

  template <typename DestType>
  void TestCastNumberToString() {
    auto dest_type = TypeTraits<DestType>::type_singleton();

    CheckCaseJSON(int8(), dest_type, "[0, 1, 127, -128, null]",
                  R"(["0", "1", "127", "-128", null])");
    CheckCaseJSON(uint8(), dest_type, "[0, 1, 255, null]", R"(["0", "1", "255", null])");
    CheckCaseJSON(int16(), dest_type, "[0, 1, 32767, -32768, null]",
                  R"(["0", "1", "32767", "-32768", null])");
    CheckCaseJSON(uint16(), dest_type, "[0, 1, 65535, null]",
                  R"(["0", "1", "65535", null])");
    CheckCaseJSON(int32(), dest_type, "[0, 1, 2147483647, -2147483648, null]",
                  R"(["0", "1", "2147483647", "-2147483648", null])");
    CheckCaseJSON(uint32(), dest_type, "[0, 1, 4294967295, null]",
                  R"(["0", "1", "4294967295", null])");
    CheckCaseJSON(int64(), dest_type,
                  "[0, 1, 9223372036854775807, -9223372036854775808, null]",
                  R"(["0", "1", "9223372036854775807", "-9223372036854775808", null])");
    CheckCaseJSON(uint64(), dest_type, "[0, 1, 18446744073709551615, null]",
                  R"(["0", "1", "18446744073709551615", null])");

    CheckCaseJSON(float32(), dest_type, "[0.0, -0.0, 1.5, -Inf, Inf, NaN, null]",
                  R"(["0", "-0", "1.5", "-inf", "inf", "nan", null])");
    CheckCaseJSON(float64(), dest_type, "[0.0, -0.0, 1.5, -Inf, Inf, NaN, null]",
                  R"(["0", "-0", "1.5", "-inf", "inf", "nan", null])");
  }

  template <typename DestType>
  void TestCastBooleanToString() {
    auto dest_type = TypeTraits<DestType>::type_singleton();

    CheckCaseJSON(boolean(), dest_type, "[true, true, false, null]",
                  R"(["true", "true", "false", null])");
  }

  template <typename SourceType>
  void TestCastStringToNumber() {
    CastOptions options;
    auto src_type = TypeTraits<SourceType>::type_singleton();

    std::vector<bool> is_valid = {true, false, true, true, true};

    // string to int
    std::vector<std::string> v_int = {"0", "1", "127", "-1", "0"};
    std::vector<int8_t> e_int8 = {0, 1, 127, -1, 0};
    std::vector<int16_t> e_int16 = {0, 1, 127, -1, 0};
    std::vector<int32_t> e_int32 = {0, 1, 127, -1, 0};
    std::vector<int64_t> e_int64 = {0, 1, 127, -1, 0};
    CheckCase<SourceType, std::string, Int8Type, int8_t>(src_type, v_int, is_valid,
                                                         int8(), e_int8, options);
    CheckCase<SourceType, std::string, Int16Type, int16_t>(src_type, v_int, is_valid,
                                                           int16(), e_int16, options);
    CheckCase<SourceType, std::string, Int32Type, int32_t>(src_type, v_int, is_valid,
                                                           int32(), e_int32, options);
    CheckCase<SourceType, std::string, Int64Type, int64_t>(src_type, v_int, is_valid,
                                                           int64(), e_int64, options);

    v_int = {"2147483647", "0", "-2147483648", "0", "0"};
    e_int32 = {2147483647, 0, -2147483648LL, 0, 0};
    CheckCase<SourceType, std::string, Int32Type, int32_t>(src_type, v_int, is_valid,
                                                           int32(), e_int32, options);
    v_int = {"9223372036854775807", "0", "-9223372036854775808", "0", "0"};
    e_int64 = {9223372036854775807LL, 0, (-9223372036854775807LL - 1), 0, 0};
    CheckCase<SourceType, std::string, Int64Type, int64_t>(src_type, v_int, is_valid,
                                                           int64(), e_int64, options);

    // string to uint
    std::vector<std::string> v_uint = {"0", "1", "127", "255", "0"};
    std::vector<uint8_t> e_uint8 = {0, 1, 127, 255, 0};
    std::vector<uint16_t> e_uint16 = {0, 1, 127, 255, 0};
    std::vector<uint32_t> e_uint32 = {0, 1, 127, 255, 0};
    std::vector<uint64_t> e_uint64 = {0, 1, 127, 255, 0};
    CheckCase<SourceType, std::string, UInt8Type, uint8_t>(src_type, v_uint, is_valid,
                                                           uint8(), e_uint8, options);
    CheckCase<SourceType, std::string, UInt16Type, uint16_t>(src_type, v_uint, is_valid,
                                                             uint16(), e_uint16, options);
    CheckCase<SourceType, std::string, UInt32Type, uint32_t>(src_type, v_uint, is_valid,
                                                             uint32(), e_uint32, options);
    CheckCase<SourceType, std::string, UInt64Type, uint64_t>(src_type, v_uint, is_valid,
                                                             uint64(), e_uint64, options);

    v_uint = {"4294967295", "0", "0", "0", "0"};
    e_uint32 = {4294967295, 0, 0, 0, 0};
    CheckCase<SourceType, std::string, UInt32Type, uint32_t>(src_type, v_uint, is_valid,
                                                             uint32(), e_uint32, options);
    v_uint = {"18446744073709551615", "0", "0", "0", "0"};
    e_uint64 = {18446744073709551615ULL, 0, 0, 0, 0};
    CheckCase<SourceType, std::string, UInt64Type, uint64_t>(src_type, v_uint, is_valid,
                                                             uint64(), e_uint64, options);

    // string to float
    std::vector<std::string> v_float = {"0.1", "1.2", "127.3", "200.4", "0.5"};
    std::vector<float> e_float = {0.1f, 1.2f, 127.3f, 200.4f, 0.5f};
    std::vector<double> e_double = {0.1, 1.2, 127.3, 200.4, 0.5};
    CheckCase<SourceType, std::string, FloatType, float>(src_type, v_float, is_valid,
                                                         float32(), e_float, options);
    CheckCase<SourceType, std::string, DoubleType, double>(src_type, v_float, is_valid,
                                                           float64(), e_double, options);

#if !defined(_WIN32) || defined(NDEBUG)
    // Test that casting is locale-independent
    {
      // French locale uses the comma as decimal point
      LocaleGuard locale_guard("fr_FR.UTF-8");
      CheckCase<SourceType, std::string, FloatType, float>(src_type, v_float, is_valid,
                                                           float32(), e_float, options);
      CheckCase<SourceType, std::string, DoubleType, double>(
          src_type, v_float, is_valid, float64(), e_double, options);
    }
#endif
  }

  template <typename SourceType>
  void TestCastStringToTimestamp() {
    CastOptions options;
    auto src_type = TypeTraits<SourceType>::type_singleton();

    std::vector<bool> is_valid = {true, false, true};
    std::vector<std::string> strings = {"1970-01-01", "xxx", "2000-02-29"};

    auto type = timestamp(TimeUnit::SECOND);
    std::vector<int64_t> e = {0, 0, 951782400};
    CheckCase<SourceType, std::string, TimestampType, int64_t>(
        src_type, strings, is_valid, type, e, options);

    type = timestamp(TimeUnit::MICRO);
    e = {0, 0, 951782400000000LL};
    CheckCase<SourceType, std::string, TimestampType, int64_t>(
        src_type, strings, is_valid, type, e, options);

    // NOTE: timestamp parsing is tested comprehensively in parsing-util-test.cc
  }
};

TEST_F(TestCast, SameTypeZeroCopy) {
  std::shared_ptr<Array> arr = ArrayFromJSON(int32(), "[0, null, 2, 3, 4]");
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> result, Cast(*arr, int32()));

  AssertBufferSame(*arr, *result, 0);
  AssertBufferSame(*arr, *result, 1);
}

TEST_F(TestCast, ZeroChunks) {
  auto chunked_i32 = std::make_shared<ChunkedArray>(ArrayVector{}, int32());
  ASSERT_OK_AND_ASSIGN(Datum result, Cast(chunked_i32, utf8()));

  ASSERT_EQ(result.kind(), Datum::CHUNKED_ARRAY);
  AssertChunkedEqual(*result.chunked_array(), ChunkedArray({}, utf8()));
}

TEST_F(TestCast, FromBoolean) {
  CastOptions options;

  std::vector<bool> is_valid(20, true);
  is_valid[3] = false;

  std::vector<bool> v1(is_valid.size(), true);
  std::vector<int32_t> e1(is_valid.size(), 1);
  for (size_t i = 0; i < v1.size(); ++i) {
    if (i % 3 == 1) {
      v1[i] = false;
      e1[i] = 0;
    }
  }

  CheckCase<BooleanType, bool, Int32Type, int32_t>(boolean(), v1, is_valid, int32(), e1,
                                                   options);
}

TEST_F(TestCast, ToBoolean) {
  CastOptions options;
  for (auto type : kNumericTypes) {
    CheckCaseJSON(type, boolean(), "[0, null, 127, 1, 0]",
                  "[false, null, true, true, false]");
  }

  // Check negative numbers
  CheckCaseJSON(int8(), boolean(), "[0, null, 127, -1, 0]",
                "[false, null, true, true, false]");
  CheckCaseJSON(float64(), boolean(), "[0, null, 127, -1, 0]",
                "[false, null, true, true, false]");
}

TEST_F(TestCast, ToIntUpcast) {
  CastOptions options;
  options.allow_int_overflow = false;

  std::vector<bool> is_valid = {true, false, true, true, true};

  // int8 to int32
  std::vector<int8_t> v1 = {0, 1, 127, -1, 0};
  std::vector<int32_t> e1 = {0, 1, 127, -1, 0};
  CheckCase<Int8Type, int8_t, Int32Type, int32_t>(int8(), v1, is_valid, int32(), e1,
                                                  options);

  // bool to int8
  std::vector<bool> v2 = {false, true, false, true, true};
  std::vector<int8_t> e2 = {0, 1, 0, 1, 1};
  CheckCase<BooleanType, bool, Int8Type, int8_t>(boolean(), v2, is_valid, int8(), e2,
                                                 options);

  // uint8 to int16, no overflow/underrun
  std::vector<uint8_t> v3 = {0, 100, 200, 255, 0};
  std::vector<int16_t> e3 = {0, 100, 200, 255, 0};
  CheckCase<UInt8Type, uint8_t, Int16Type, int16_t>(uint8(), v3, is_valid, int16(), e3,
                                                    options);
}

TEST_F(TestCast, OverflowInNullSlot) {
  CastOptions options;
  options.allow_int_overflow = false;

  std::vector<bool> is_valid = {true, false, true, true, true};

  std::vector<int32_t> v11 = {0, 70000, 2000, 1000, 0};
  std::vector<int16_t> e11 = {0, 0, 2000, 1000, 0};

  std::shared_ptr<Array> expected;
  ArrayFromVector<Int16Type, int16_t>(int16(), is_valid, e11, &expected);

  auto buf = Buffer::Wrap(v11.data(), v11.size());
  Int32Array tmp11(5, buf, expected->null_bitmap(), -1);

  CheckPass(tmp11, *expected, int16(), options);
}

TEST_F(TestCast, ToIntDowncastSafe) {
  CastOptions options;
  options.allow_int_overflow = false;

  std::vector<bool> is_valid = {true, false, true, true, true};

  // int16 to uint8, no overflow/underrun
  std::vector<int16_t> v1 = {0, 100, 200, 1, 2};
  std::vector<uint8_t> e1 = {0, 100, 200, 1, 2};
  CheckCase<Int16Type, int16_t, UInt8Type, uint8_t>(int16(), v1, is_valid, uint8(), e1,
                                                    options);

  // int16 to uint8, with overflow
  std::vector<int16_t> v2 = {0, 100, 256, 0, 0};
  CheckFails<Int16Type>(int16(), v2, is_valid, uint8(), options);

  // underflow
  std::vector<int16_t> v3 = {0, 100, -1, 0, 0};
  CheckFails<Int16Type>(int16(), v3, is_valid, uint8(), options);

  // int32 to int16, no overflow
  std::vector<int32_t> v4 = {0, 1000, 2000, 1, 2};
  std::vector<int16_t> e4 = {0, 1000, 2000, 1, 2};
  CheckCase<Int32Type, int32_t, Int16Type, int16_t>(int32(), v4, is_valid, int16(), e4,
                                                    options);

  // int32 to int16, overflow
  std::vector<int32_t> v5 = {0, 1000, 2000, 70000, 0};
  CheckFails<Int32Type>(int32(), v5, is_valid, int16(), options);

  // underflow
  std::vector<int32_t> v6 = {0, 1000, 2000, -70000, 0};
  CheckFails<Int32Type>(int32(), v6, is_valid, int16(), options);

  std::vector<int32_t> v7 = {0, 1000, 2000, -70000, 0};
  CheckFails<Int32Type>(int32(), v7, is_valid, uint8(), options);
}

template <typename O, typename I>
std::vector<O> UnsafeVectorCast(const std::vector<I>& v) {
  size_t n_elems = v.size();
  std::vector<O> result(n_elems);

  for (size_t i = 0; i < v.size(); i++) result[i] = static_cast<O>(v[i]);

  return result;
}

TEST_F(TestCast, IntegerSignedToUnsigned) {
  CastOptions options;
  options.allow_int_overflow = false;

  std::vector<bool> is_valid = {true, false, true, true, true};

  std::vector<int32_t> v1 = {INT32_MIN, 100, -1, UINT16_MAX, INT32_MAX};

  // Same width
  CheckFails<Int32Type>(int32(), v1, is_valid, uint32(), options);
  // Wider
  CheckFails<Int32Type>(int32(), v1, is_valid, uint64(), options);
  // Narrower
  CheckFails<Int32Type>(int32(), v1, is_valid, uint16(), options);
  // Fail because of overflow (instead of underflow).
  std::vector<int32_t> over = {0, -11, 0, UINT16_MAX + 1, INT32_MAX};
  CheckFails<Int32Type>(int32(), over, is_valid, uint16(), options);

  options.allow_int_overflow = true;

  CheckCase<Int32Type, int32_t, UInt32Type, uint32_t>(
      int32(), v1, is_valid, uint32(), UnsafeVectorCast<uint32_t, int32_t>(v1), options);
  CheckCase<Int32Type, int32_t, UInt64Type, uint64_t>(
      int32(), v1, is_valid, uint64(), UnsafeVectorCast<uint64_t, int32_t>(v1), options);
  CheckCase<Int32Type, int32_t, UInt16Type, uint16_t>(
      int32(), v1, is_valid, uint16(), UnsafeVectorCast<uint16_t, int32_t>(v1), options);
  CheckCase<Int32Type, int32_t, UInt16Type, uint16_t>(
      int32(), over, is_valid, uint16(), UnsafeVectorCast<uint16_t, int32_t>(over),
      options);
}

TEST_F(TestCast, IntegerUnsignedToSigned) {
  CastOptions options;
  options.allow_int_overflow = false;

  std::vector<bool> is_valid = {true, true, true};

  std::vector<uint32_t> v1 = {0, INT16_MAX + 1, UINT32_MAX};
  std::vector<uint32_t> v2 = {0, INT16_MAX + 1, 2};
  // Same width
  CheckFails<UInt32Type>(uint32(), v1, is_valid, int32(), options);
  // Narrower
  CheckFails<UInt32Type>(uint32(), v1, is_valid, int16(), options);
  CheckFails<UInt32Type>(uint32(), v2, is_valid, int16(), options);

  options.allow_int_overflow = true;

  CheckCase<UInt32Type, uint32_t, Int32Type, int32_t>(
      uint32(), v1, is_valid, int32(), UnsafeVectorCast<int32_t, uint32_t>(v1), options);
  CheckCase<UInt32Type, uint32_t, Int64Type, int64_t>(
      uint32(), v1, is_valid, int64(), UnsafeVectorCast<int64_t, uint32_t>(v1), options);
  CheckCase<UInt32Type, uint32_t, Int16Type, int16_t>(
      uint32(), v1, is_valid, int16(), UnsafeVectorCast<int16_t, uint32_t>(v1), options);
  CheckCase<UInt32Type, uint32_t, Int16Type, int16_t>(
      uint32(), v2, is_valid, int16(), UnsafeVectorCast<int16_t, uint32_t>(v2), options);
}

TEST_F(TestCast, ToIntDowncastUnsafe) {
  CastOptions options;
  options.allow_int_overflow = true;

  std::vector<bool> is_valid = {true, false, true, true, true};

  // int16 to uint8, no overflow/underrun
  std::vector<int16_t> v1 = {0, 100, 200, 1, 2};
  std::vector<uint8_t> e1 = {0, 100, 200, 1, 2};
  CheckCase<Int16Type, int16_t, UInt8Type, uint8_t>(int16(), v1, is_valid, uint8(), e1,
                                                    options);

  // int16 to uint8, with overflow
  std::vector<int16_t> v2 = {0, 100, 256, 0, 0};
  std::vector<uint8_t> e2 = {0, 100, 0, 0, 0};
  CheckCase<Int16Type, int16_t, UInt8Type, uint8_t>(int16(), v2, is_valid, uint8(), e2,
                                                    options);

  // underflow
  std::vector<int16_t> v3 = {0, 100, -1, 0, 0};
  std::vector<uint8_t> e3 = {0, 100, 255, 0, 0};
  CheckCase<Int16Type, int16_t, UInt8Type, uint8_t>(int16(), v3, is_valid, uint8(), e3,
                                                    options);

  // int32 to int16, no overflow
  std::vector<int32_t> v4 = {0, 1000, 2000, 1, 2};
  std::vector<int16_t> e4 = {0, 1000, 2000, 1, 2};
  CheckCase<Int32Type, int32_t, Int16Type, int16_t>(int32(), v4, is_valid, int16(), e4,
                                                    options);

  // int32 to int16, overflow
  // TODO(wesm): do we want to allow this? we could set to null
  std::vector<int32_t> v5 = {0, 1000, 2000, 70000, 0};
  std::vector<int16_t> e5 = {0, 1000, 2000, 4464, 0};
  CheckCase<Int32Type, int32_t, Int16Type, int16_t>(int32(), v5, is_valid, int16(), e5,
                                                    options);

  // underflow
  // TODO(wesm): do we want to allow this? we could set overflow to null
  std::vector<int32_t> v6 = {0, 1000, 2000, -70000, 0};
  std::vector<int16_t> e6 = {0, 1000, 2000, -4464, 0};
  CheckCase<Int32Type, int32_t, Int16Type, int16_t>(int32(), v6, is_valid, int16(), e6,
                                                    options);
}

TEST_F(TestCast, FloatingPointToInt) {
  // which means allow_float_truncate == false
  auto options = CastOptions::Safe();

  std::vector<bool> is_valid = {true, false, true, true, true};
  std::vector<bool> all_valid = {true, true, true, true, true};

  // float32 to int32 no truncation
  std::vector<float> v1 = {1.0, 0, 0.0, -1.0, 5.0};
  std::vector<int32_t> e1 = {1, 0, 0, -1, 5};
  CheckCase<FloatType, float, Int32Type, int32_t>(float32(), v1, is_valid, int32(), e1,
                                                  options);
  CheckCase<FloatType, float, Int32Type, int32_t>(float32(), v1, all_valid, int32(), e1,
                                                  options);

  // float64 to int32 no truncation
  std::vector<double> v2 = {1.0, 0, 0.0, -1.0, 5.0};
  std::vector<int32_t> e2 = {1, 0, 0, -1, 5};
  CheckCase<DoubleType, double, Int32Type, int32_t>(float64(), v2, is_valid, int32(), e2,
                                                    options);
  CheckCase<DoubleType, double, Int32Type, int32_t>(float64(), v2, all_valid, int32(), e2,
                                                    options);

  // float64 to int64 no truncation
  std::vector<double> v3 = {1.0, 0, 0.0, -1.0, 5.0};
  std::vector<int64_t> e3 = {1, 0, 0, -1, 5};
  CheckCase<DoubleType, double, Int64Type, int64_t>(float64(), v3, is_valid, int64(), e3,
                                                    options);
  CheckCase<DoubleType, double, Int64Type, int64_t>(float64(), v3, all_valid, int64(), e3,
                                                    options);

  // float64 to int32 truncate
  std::vector<double> v4 = {1.5, 0, 0.5, -1.5, 5.5};
  std::vector<int32_t> e4 = {1, 0, 0, -1, 5};

  options.allow_float_truncate = false;
  CheckFails<DoubleType>(float64(), v4, is_valid, int32(), options);
  CheckFails<DoubleType>(float64(), v4, all_valid, int32(), options);

  options.allow_float_truncate = true;
  CheckCase<DoubleType, double, Int32Type, int32_t>(float64(), v4, is_valid, int32(), e4,
                                                    options);
  CheckCase<DoubleType, double, Int32Type, int32_t>(float64(), v4, all_valid, int32(), e4,
                                                    options);

  // float64 to int64 truncate
  std::vector<double> v5 = {1.5, 0, 0.5, -1.5, 5.5};
  std::vector<int64_t> e5 = {1, 0, 0, -1, 5};

  options.allow_float_truncate = false;
  CheckFails<DoubleType>(float64(), v5, is_valid, int64(), options);
  CheckFails<DoubleType>(float64(), v5, all_valid, int64(), options);

  options.allow_float_truncate = true;
  CheckCase<DoubleType, double, Int64Type, int64_t>(float64(), v5, is_valid, int64(), e5,
                                                    options);
  CheckCase<DoubleType, double, Int64Type, int64_t>(float64(), v5, all_valid, int64(), e5,
                                                    options);
}

#if ARROW_BITNESS >= 64
TEST_F(TestCast, IntToFloatingPoint) {
  auto options = CastOptions::Safe();

  std::vector<bool> all_valid = {true, true, true, true, true};
  std::vector<bool> all_invalid = {false, false, false, false, false};

  std::vector<int64_t> v1 = {INT64_MIN, INT64_MIN + 1, 0, INT64_MAX - 1, INT64_MAX};
  CheckFails<Int64Type>(int64(), v1, all_valid, float32(), options);

  // While it's not safe to convert, all values are null.
  CheckCase<Int64Type, int64_t, DoubleType, double>(int64(), v1, all_invalid, float64(),
                                                    UnsafeVectorCast<double, int64_t>(v1),
                                                    options);
}
#endif

TEST_F(TestCast, DecimalToInt) {
  CastOptions options;
  std::vector<bool> is_valid2 = {true, true};
  std::vector<bool> is_valid3 = {true, true, false};

  // no overflow no truncation
  std::vector<Decimal128> v12 = {Decimal128("02.0000000000"),
                                 Decimal128("-11.0000000000")};
  std::vector<Decimal128> v13 = {Decimal128("02.0000000000"),
                                 Decimal128("-11.0000000000"),
                                 Decimal128("-12.0000000000")};
  std::vector<int64_t> e12 = {2, -11};
  std::vector<int64_t> e13 = {2, -11, 0};

  for (bool allow_int_overflow : {false, true}) {
    for (bool allow_decimal_truncate : {false, true}) {
      options.allow_int_overflow = allow_int_overflow;
      options.allow_decimal_truncate = allow_decimal_truncate;
      CheckCase<Decimal128Type, Decimal128, Int64Type, int64_t>(
          decimal(38, 10), v12, is_valid2, int64(), e12, options);
      CheckCase<Decimal128Type, Decimal128, Int64Type, int64_t>(
          decimal(38, 10), v13, is_valid3, int64(), e13, options);
    }
  }

  // truncation, no overflow
  std::vector<Decimal128> v22 = {Decimal128("02.1000000000"),
                                 Decimal128("-11.0000004500")};
  std::vector<Decimal128> v23 = {Decimal128("02.1000000000"),
                                 Decimal128("-11.0000004500"),
                                 Decimal128("-12.0000004500")};
  std::vector<int64_t> e22 = {2, -11};
  std::vector<int64_t> e23 = {2, -11, 0};

  for (bool allow_int_overflow : {false, true}) {
    options.allow_int_overflow = allow_int_overflow;
    options.allow_decimal_truncate = true;
    CheckCase<Decimal128Type, Decimal128, Int64Type, int64_t>(
        decimal(38, 10), v22, is_valid2, int64(), e22, options);
    CheckCase<Decimal128Type, Decimal128, Int64Type, int64_t>(
        decimal(38, 10), v23, is_valid3, int64(), e23, options);
    options.allow_decimal_truncate = false;
    CheckFails<Decimal128Type>(decimal(38, 10), v22, is_valid2, int64(), options);
    CheckFails<Decimal128Type>(decimal(38, 10), v23, is_valid3, int64(), options);
  }

  // overflow, no truncation
  std::vector<Decimal128> v32 = {Decimal128("12345678901234567890000.0000000000"),
                                 Decimal128("99999999999999999999999.0000000000")};
  std::vector<Decimal128> v33 = {Decimal128("12345678901234567890000.0000000000"),
                                 Decimal128("99999999999999999999999.0000000000"),
                                 Decimal128("99999999999999999999999.0000000000")};
  // 12345678901234567890000 % 2**64, 99999999999999999999999 % 2**64
  std::vector<int64_t> e32 = {4807115922877858896, 200376420520689663};
  std::vector<int64_t> e33 = {4807115922877858896, 200376420520689663, -2};

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;
    options.allow_int_overflow = true;
    CheckCase<Decimal128Type, Decimal128, Int64Type, int64_t>(
        decimal(38, 10), v32, is_valid2, int64(), e32, options);
    CheckCase<Decimal128Type, Decimal128, Int64Type, int64_t>(
        decimal(38, 10), v33, is_valid3, int64(), e33, options);
    options.allow_int_overflow = false;
    CheckFails<Decimal128Type>(decimal(38, 10), v32, is_valid2, int64(), options);
    CheckFails<Decimal128Type>(decimal(38, 10), v33, is_valid3, int64(), options);
  }

  // overflow, truncation
  std::vector<Decimal128> v42 = {Decimal128("12345678901234567890000.0045345000"),
                                 Decimal128("99999999999999999999999.0000005430")};
  std::vector<Decimal128> v43 = {Decimal128("12345678901234567890000.0005345340"),
                                 Decimal128("99999999999999999999999.0000344300"),
                                 Decimal128("99999999999999999999999.0004354000")};
  // 12345678901234567890000 % 2**64, 99999999999999999999999 % 2**64
  std::vector<int64_t> e42 = {4807115922877858896, 200376420520689663};
  std::vector<int64_t> e43 = {4807115922877858896, 200376420520689663, -2};

  for (bool allow_int_overflow : {false, true}) {
    for (bool allow_decimal_truncate : {false, true}) {
      options.allow_int_overflow = allow_int_overflow;
      options.allow_decimal_truncate = allow_decimal_truncate;
      if (options.allow_int_overflow && options.allow_decimal_truncate) {
        CheckCase<Decimal128Type, Decimal128, Int64Type, int64_t>(
            decimal(38, 10), v42, is_valid2, int64(), e42, options);
        CheckCase<Decimal128Type, Decimal128, Int64Type, int64_t>(
            decimal(38, 10), v43, is_valid3, int64(), e43, options);
      } else {
        CheckFails<Decimal128Type>(decimal(38, 10), v42, is_valid2, int64(), options);
        CheckFails<Decimal128Type>(decimal(38, 10), v43, is_valid3, int64(), options);
      }
    }
  }

  // negative scale
  std::vector<Decimal128> v5 = {Decimal128("1234567890000."), Decimal128("-120000.")};
  for (int i = 0; i < 2; i++) v5[i] = v5[i].Rescale(0, -4).ValueOrDie();
  std::vector<int64_t> e5 = {1234567890000, -120000};
  CheckCase<Decimal128Type, Decimal128, Int64Type, int64_t>(
      decimal(38, -4), v5, is_valid2, int64(), e5, options);
}

TEST_F(TestCast, DecimalToDecimal) {
  CastOptions options;

  std::vector<bool> is_valid1 = {true};
  std::vector<bool> is_valid2 = {true, true};
  std::vector<bool> is_valid3 = {true, true, false};

  // Non-truncating

  std::vector<Decimal128> v12 = {Decimal128("02.0000000000"),
                                 Decimal128("30.0000000000")};
  std::vector<Decimal128> e12 = {Decimal128("02."), Decimal128("30.")};
  std::vector<Decimal128> v13 = {Decimal128("02.0000000000"), Decimal128("30.0000000000"),
                                 Decimal128("30.0000000000")};
  std::vector<Decimal128> e13 = {Decimal128("02."), Decimal128("30."), Decimal128("-1.")};

  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;
    CheckCase<Decimal128Type, Decimal128, Decimal128Type, Decimal128>(
        decimal(38, 10), v12, is_valid2, decimal(28, 0), e12, options);
    CheckCase<Decimal128Type, Decimal128, Decimal128Type, Decimal128>(
        decimal(38, 10), v13, is_valid3, decimal(28, 0), e13, options);
    // and back
    CheckCase<Decimal128Type, Decimal128, Decimal128Type, Decimal128>(
        decimal(28, 0), e12, is_valid2, decimal(38, 10), v12, options);
    CheckCase<Decimal128Type, Decimal128, Decimal128Type, Decimal128>(
        decimal(28, 0), e13, is_valid3, decimal(38, 10), v13, options);
  }

  // Same scale, different precision
  std::vector<Decimal128> v14 = {Decimal128("12.34"), Decimal128("0.56")};
  for (bool allow_decimal_truncate : {false, true}) {
    options.allow_decimal_truncate = allow_decimal_truncate;
    CheckCase<Decimal128Type, Decimal128, Decimal128Type, Decimal128>(
        decimal(5, 2), v14, is_valid2, decimal(4, 2), v14, options);
    // and back
    CheckCase<Decimal128Type, Decimal128, Decimal128Type, Decimal128>(
        decimal(4, 2), v14, is_valid2, decimal(5, 2), v14, options);
  }

  auto check_truncate = [this](const std::shared_ptr<DataType>& input_type,
                               const std::vector<Decimal128>& input,
                               const std::vector<bool>& is_valid,
                               const std::shared_ptr<DataType>& output_type,
                               const std::vector<Decimal128>& expected_output) {
    CastOptions options;

    options.allow_decimal_truncate = true;
    CheckCase<Decimal128Type, Decimal128, Decimal128Type, Decimal128>(
        input_type, input, is_valid, output_type, expected_output, options);
    options.allow_decimal_truncate = false;
    CheckFails<Decimal128Type>(input_type, input, is_valid, output_type, options);
  };

  auto check_truncate_and_back =
      [this](const std::shared_ptr<DataType>& input_type,
             const std::vector<Decimal128>& input, const std::vector<bool>& is_valid,
             const std::shared_ptr<DataType>& output_type,
             const std::vector<Decimal128>& expected_output,
             const std::vector<Decimal128>& expected_back_convert) {
        CastOptions options;

        options.allow_decimal_truncate = true;
        CheckCase<Decimal128Type, Decimal128, Decimal128Type, Decimal128>(
            input_type, input, is_valid, output_type, expected_output, options);
        // and back
        CheckCase<Decimal128Type, Decimal128, Decimal128Type, Decimal128>(
            output_type, expected_output, is_valid, input_type, expected_back_convert,
            options);

        options.allow_decimal_truncate = false;
        CheckFails<Decimal128Type>(input_type, input, is_valid, output_type, options);
        // back case is valid
        CheckCase<Decimal128Type, Decimal128, Decimal128Type, Decimal128>(
            output_type, expected_output, is_valid, input_type, expected_back_convert,
            options);
      };

  // Rescale leads to truncation

  std::vector<Decimal128> v22 = {Decimal128("-02.1234567890"),
                                 Decimal128("30.1234567890")};
  std::vector<Decimal128> e22 = {Decimal128("-02."), Decimal128("30.")};
  std::vector<Decimal128> f22 = {Decimal128("-02.0000000000"),
                                 Decimal128("30.0000000000")};
  std::vector<Decimal128> v23 = {Decimal128("-02.1234567890"),
                                 Decimal128("30.1234567890"),
                                 Decimal128("30.1234567890")};
  std::vector<Decimal128> e23 = {Decimal128("-02."), Decimal128("30."),
                                 Decimal128("-70.")};
  std::vector<Decimal128> f23 = {Decimal128("-02.0000000000"),
                                 Decimal128("30.0000000000"),
                                 Decimal128("80.0000000000")};

  check_truncate_and_back(decimal(38, 10), v22, is_valid2, decimal(28, 0), e22, f22);
  check_truncate_and_back(decimal(38, 10), v23, is_valid3, decimal(28, 0), e23, f23);

  // Precision loss without rescale leads to truncation

  std::vector<Decimal128> v3 = {Decimal128("12.34")};
  std::vector<Decimal128> e3 = {Decimal128("12.34")};

  check_truncate(decimal(4, 2), v3, is_valid1, decimal(3, 2), e3);

  // Precision loss with rescale leads to truncation

  std::vector<Decimal128> v4 = {Decimal128("12.34")};
  std::vector<Decimal128> e4 = {Decimal128("12.340")};
  std::vector<Decimal128> v5 = {Decimal128("12.34")};
  std::vector<Decimal128> e5 = {Decimal128("12.3")};

  check_truncate(decimal(4, 2), v4, is_valid1, decimal(4, 3), e4);
  check_truncate(decimal(4, 2), v5, is_valid1, decimal(2, 1), e5);
}

TEST_F(TestCast, TimestampToTimestamp) {
  CastOptions options;

  auto CheckTimestampCast =
      [this](const CastOptions& options, TimeUnit::type from_unit, TimeUnit::type to_unit,
             const std::vector<int64_t>& from_values,
             const std::vector<int64_t>& to_values, const std::vector<bool>& is_valid) {
        CheckCase<TimestampType, int64_t, TimestampType, int64_t>(
            timestamp(from_unit), from_values, is_valid, timestamp(to_unit), to_values,
            options);
      };

  std::vector<bool> is_valid = {true, false, true, true, true};

  // Multiply promotions
  std::vector<int64_t> v1 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e1 = {0, 100000, 200000, 1000, 2000};
  CheckTimestampCast(options, TimeUnit::SECOND, TimeUnit::MILLI, v1, e1, is_valid);

  std::vector<int64_t> v2 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e2 = {0, 100000000L, 200000000L, 1000000, 2000000};
  CheckTimestampCast(options, TimeUnit::SECOND, TimeUnit::MICRO, v2, e2, is_valid);

  std::vector<int64_t> v3 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e3 = {0, 100000000000L, 200000000000L, 1000000000L, 2000000000L};
  CheckTimestampCast(options, TimeUnit::SECOND, TimeUnit::NANO, v3, e3, is_valid);

  std::vector<int64_t> v4 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e4 = {0, 100000, 200000, 1000, 2000};
  CheckTimestampCast(options, TimeUnit::MILLI, TimeUnit::MICRO, v4, e4, is_valid);

  std::vector<int64_t> v5 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e5 = {0, 100000000L, 200000000L, 1000000, 2000000};
  CheckTimestampCast(options, TimeUnit::MILLI, TimeUnit::NANO, v5, e5, is_valid);

  std::vector<int64_t> v6 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e6 = {0, 100000, 200000, 1000, 2000};
  CheckTimestampCast(options, TimeUnit::MICRO, TimeUnit::NANO, v6, e6, is_valid);

  // Zero copy
  std::vector<int64_t> v7 = {0, 70000, 2000, 1000, 0};
  std::shared_ptr<Array> arr;
  ArrayFromVector<TimestampType, int64_t>(timestamp(TimeUnit::SECOND), is_valid, v7,
                                          &arr);
  CheckZeroCopy(*arr, timestamp(TimeUnit::SECOND));

  // ARROW-1773, cast to integer
  CheckZeroCopy(*arr, int64());

  // Divide, truncate
  std::vector<int64_t> v8 = {0, 100123, 200456, 1123, 2456};
  std::vector<int64_t> e8 = {0, 100, 200, 1, 2};

  options.allow_time_truncate = true;
  CheckTimestampCast(options, TimeUnit::MILLI, TimeUnit::SECOND, v8, e8, is_valid);
  CheckTimestampCast(options, TimeUnit::MICRO, TimeUnit::MILLI, v8, e8, is_valid);
  CheckTimestampCast(options, TimeUnit::NANO, TimeUnit::MICRO, v8, e8, is_valid);

  std::vector<int64_t> v9 = {0, 100123000, 200456000, 1123000, 2456000};
  std::vector<int64_t> e9 = {0, 100, 200, 1, 2};
  CheckTimestampCast(options, TimeUnit::MICRO, TimeUnit::SECOND, v9, e9, is_valid);
  CheckTimestampCast(options, TimeUnit::NANO, TimeUnit::MILLI, v9, e9, is_valid);

  std::vector<int64_t> v10 = {0, 100123000000L, 200456000000L, 1123000000L, 2456000000};
  std::vector<int64_t> e10 = {0, 100, 200, 1, 2};
  CheckTimestampCast(options, TimeUnit::NANO, TimeUnit::SECOND, v10, e10, is_valid);

  // Disallow truncate, failures
  options.allow_time_truncate = false;
  CheckFails<TimestampType>(timestamp(TimeUnit::MILLI), v8, is_valid,
                            timestamp(TimeUnit::SECOND), options);
  CheckFails<TimestampType>(timestamp(TimeUnit::MICRO), v8, is_valid,
                            timestamp(TimeUnit::MILLI), options);
  CheckFails<TimestampType>(timestamp(TimeUnit::NANO), v8, is_valid,
                            timestamp(TimeUnit::MICRO), options);
  CheckFails<TimestampType>(timestamp(TimeUnit::MICRO), v9, is_valid,
                            timestamp(TimeUnit::SECOND), options);
  CheckFails<TimestampType>(timestamp(TimeUnit::NANO), v9, is_valid,
                            timestamp(TimeUnit::MILLI), options);
  CheckFails<TimestampType>(timestamp(TimeUnit::NANO), v10, is_valid,
                            timestamp(TimeUnit::SECOND), options);

  // Multiply overflow

  // 1000-01-01, 1800-01-01 , 2000-01-01, 2300-01-01, 3000-01-01
  std::vector<int64_t> v11 = {-30610224000, -5364662400, 946684800, 10413792000,
                              32503680000};

  options.allow_time_overflow = false;
  CheckFails<TimestampType>(timestamp(TimeUnit::SECOND), v11, is_valid,
                            timestamp(TimeUnit::NANO), options);
}

TEST_F(TestCast, TimestampToDate32_Date64) {
  CastOptions options;

  std::vector<bool> is_valid = {true, true, false};

  // 2000-01-01, 2000-01-02, null
  std::vector<int64_t> v_nano = {946684800000000000, 946771200000000000, 0};
  std::vector<int64_t> v_micro = {946684800000000, 946771200000000, 0};
  std::vector<int64_t> v_milli = {946684800000, 946771200000, 0};
  std::vector<int64_t> v_second = {946684800, 946771200, 0};
  std::vector<int32_t> v_day = {10957, 10958, 0};

  // Simple conversions
  CheckCase<TimestampType, int64_t, Date64Type, int64_t>(
      timestamp(TimeUnit::NANO), v_nano, is_valid, date64(), v_milli, options);
  CheckCase<TimestampType, int64_t, Date64Type, int64_t>(
      timestamp(TimeUnit::MICRO), v_micro, is_valid, date64(), v_milli, options);
  CheckCase<TimestampType, int64_t, Date64Type, int64_t>(
      timestamp(TimeUnit::MILLI), v_milli, is_valid, date64(), v_milli, options);
  CheckCase<TimestampType, int64_t, Date64Type, int64_t>(
      timestamp(TimeUnit::SECOND), v_second, is_valid, date64(), v_milli, options);

  CheckCase<TimestampType, int64_t, Date32Type, int32_t>(
      timestamp(TimeUnit::NANO), v_nano, is_valid, date32(), v_day, options);
  CheckCase<TimestampType, int64_t, Date32Type, int32_t>(
      timestamp(TimeUnit::MICRO), v_micro, is_valid, date32(), v_day, options);
  CheckCase<TimestampType, int64_t, Date32Type, int32_t>(
      timestamp(TimeUnit::MILLI), v_milli, is_valid, date32(), v_day, options);
  CheckCase<TimestampType, int64_t, Date32Type, int32_t>(
      timestamp(TimeUnit::SECOND), v_second, is_valid, date32(), v_day, options);

  // Disallow truncate, failures
  std::vector<int64_t> v_nano_fail = {946684800000000001, 946771200000000001, 0};
  std::vector<int64_t> v_micro_fail = {946684800000001, 946771200000001, 0};
  std::vector<int64_t> v_milli_fail = {946684800001, 946771200001, 0};
  std::vector<int64_t> v_second_fail = {946684801, 946771201, 0};

  options.allow_time_truncate = false;
  CheckFails<TimestampType>(timestamp(TimeUnit::NANO), v_nano_fail, is_valid, date64(),
                            options);
  CheckFails<TimestampType>(timestamp(TimeUnit::MICRO), v_micro_fail, is_valid, date64(),
                            options);
  CheckFails<TimestampType>(timestamp(TimeUnit::MILLI), v_milli_fail, is_valid, date64(),
                            options);
  CheckFails<TimestampType>(timestamp(TimeUnit::SECOND), v_second_fail, is_valid,
                            date64(), options);

  CheckFails<TimestampType>(timestamp(TimeUnit::NANO), v_nano_fail, is_valid, date32(),
                            options);
  CheckFails<TimestampType>(timestamp(TimeUnit::MICRO), v_micro_fail, is_valid, date32(),
                            options);
  CheckFails<TimestampType>(timestamp(TimeUnit::MILLI), v_milli_fail, is_valid, date32(),
                            options);
  CheckFails<TimestampType>(timestamp(TimeUnit::SECOND), v_second_fail, is_valid,
                            date32(), options);

  // Make sure that nulls are excluded from the truncation checks
  std::vector<int64_t> v_second_nofail = {946684800, 946771200, 1};
  CheckCase<TimestampType, int64_t, Date64Type, int64_t>(
      timestamp(TimeUnit::SECOND), v_second_nofail, is_valid, date64(), v_milli, options);
  CheckCase<TimestampType, int64_t, Date32Type, int32_t>(
      timestamp(TimeUnit::SECOND), v_second_nofail, is_valid, date32(), v_day, options);
}

TEST_F(TestCast, TimeToCompatible) {
  CastOptions options;

  std::vector<bool> is_valid = {true, false, true, true, true};

  // Multiply promotions
  std::vector<int32_t> v1 = {0, 100, 200, 1, 2};
  std::vector<int32_t> e1 = {0, 100000, 200000, 1000, 2000};
  CheckCase<Time32Type, int32_t, Time32Type, int32_t>(
      time32(TimeUnit::SECOND), v1, is_valid, time32(TimeUnit::MILLI), e1, options);

  std::vector<int32_t> v2 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e2 = {0, 100000000L, 200000000L, 1000000, 2000000};
  CheckCase<Time32Type, int32_t, Time64Type, int64_t>(
      time32(TimeUnit::SECOND), v2, is_valid, time64(TimeUnit::MICRO), e2, options);

  std::vector<int32_t> v3 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e3 = {0, 100000000000L, 200000000000L, 1000000000L, 2000000000L};
  CheckCase<Time32Type, int32_t, Time64Type, int64_t>(
      time32(TimeUnit::SECOND), v3, is_valid, time64(TimeUnit::NANO), e3, options);

  std::vector<int32_t> v4 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e4 = {0, 100000, 200000, 1000, 2000};
  CheckCase<Time32Type, int32_t, Time64Type, int64_t>(
      time32(TimeUnit::MILLI), v4, is_valid, time64(TimeUnit::MICRO), e4, options);

  std::vector<int32_t> v5 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e5 = {0, 100000000L, 200000000L, 1000000, 2000000};
  CheckCase<Time32Type, int32_t, Time64Type, int64_t>(
      time32(TimeUnit::MILLI), v5, is_valid, time64(TimeUnit::NANO), e5, options);

  std::vector<int64_t> v6 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e6 = {0, 100000, 200000, 1000, 2000};
  CheckCase<Time64Type, int64_t, Time64Type, int64_t>(
      time64(TimeUnit::MICRO), v6, is_valid, time64(TimeUnit::NANO), e6, options);

  // Zero copy
  std::vector<int64_t> v7 = {0, 70000, 2000, 1000, 0};
  std::shared_ptr<Array> arr;
  ArrayFromVector<Time64Type, int64_t>(time64(TimeUnit::MICRO), is_valid, v7, &arr);
  CheckZeroCopy(*arr, time64(TimeUnit::MICRO));

  // ARROW-1773: cast to int64
  CheckZeroCopy(*arr, int64());

  std::vector<int32_t> v7_2 = {0, 70000, 2000, 1000, 0};
  ArrayFromVector<Time32Type, int32_t>(time32(TimeUnit::SECOND), is_valid, v7_2, &arr);
  CheckZeroCopy(*arr, time32(TimeUnit::SECOND));

  // ARROW-1773: cast to int64
  CheckZeroCopy(*arr, int32());

  // Divide, truncate
  std::vector<int32_t> v8 = {0, 100123, 200456, 1123, 2456};
  std::vector<int32_t> e8 = {0, 100, 200, 1, 2};

  options.allow_time_truncate = true;
  CheckCase<Time32Type, int32_t, Time32Type, int32_t>(
      time32(TimeUnit::MILLI), v8, is_valid, time32(TimeUnit::SECOND), e8, options);
  CheckCase<Time64Type, int32_t, Time32Type, int32_t>(
      time64(TimeUnit::MICRO), v8, is_valid, time32(TimeUnit::MILLI), e8, options);
  CheckCase<Time64Type, int32_t, Time64Type, int32_t>(
      time64(TimeUnit::NANO), v8, is_valid, time64(TimeUnit::MICRO), e8, options);

  std::vector<int64_t> v9 = {0, 100123000, 200456000, 1123000, 2456000};
  std::vector<int32_t> e9 = {0, 100, 200, 1, 2};
  CheckCase<Time64Type, int64_t, Time32Type, int32_t>(
      time64(TimeUnit::MICRO), v9, is_valid, time32(TimeUnit::SECOND), e9, options);
  CheckCase<Time64Type, int64_t, Time32Type, int32_t>(
      time64(TimeUnit::NANO), v9, is_valid, time32(TimeUnit::MILLI), e9, options);

  std::vector<int64_t> v10 = {0, 100123000000L, 200456000000L, 1123000000L, 2456000000};
  std::vector<int32_t> e10 = {0, 100, 200, 1, 2};
  CheckCase<Time64Type, int64_t, Time32Type, int32_t>(
      time64(TimeUnit::NANO), v10, is_valid, time32(TimeUnit::SECOND), e10, options);

  // Disallow truncate, failures

  options.allow_time_truncate = false;
  CheckFails<Time32Type>(time32(TimeUnit::MILLI), v8, is_valid, time32(TimeUnit::SECOND),
                         options);
  CheckFails<Time64Type>(time64(TimeUnit::MICRO), v8, is_valid, time32(TimeUnit::MILLI),
                         options);
  CheckFails<Time64Type>(time64(TimeUnit::NANO), v8, is_valid, time64(TimeUnit::MICRO),
                         options);
  CheckFails<Time64Type>(time64(TimeUnit::MICRO), v9, is_valid, time32(TimeUnit::SECOND),
                         options);
  CheckFails<Time64Type>(time64(TimeUnit::NANO), v9, is_valid, time32(TimeUnit::MILLI),
                         options);
  CheckFails<Time64Type>(time64(TimeUnit::NANO), v10, is_valid, time32(TimeUnit::SECOND),
                         options);
}

TEST_F(TestCast, DateToCompatible) {
  CastOptions options;

  std::vector<bool> is_valid = {true, false, true, true, true};

  constexpr int64_t F = 86400000;

  // Multiply promotion
  std::vector<int32_t> v1 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e1 = {0, 100 * F, 200 * F, F, 2 * F};
  CheckCase<Date32Type, int32_t, Date64Type, int64_t>(date32(), v1, is_valid, date64(),
                                                      e1, options);

  // Zero copy
  std::vector<int32_t> v2 = {0, 70000, 2000, 1000, 0};
  std::vector<int64_t> v3 = {0, 70000, 2000, 1000, 0};
  std::shared_ptr<Array> arr;
  ArrayFromVector<Date32Type, int32_t>(date32(), is_valid, v2, &arr);
  CheckZeroCopy(*arr, date32());

  // ARROW-1773: zero copy cast to integer
  CheckZeroCopy(*arr, int32());

  ArrayFromVector<Date64Type, int64_t>(date64(), is_valid, v3, &arr);
  CheckZeroCopy(*arr, date64());

  // ARROW-1773: zero copy cast to integer
  CheckZeroCopy(*arr, int64());

  // Divide, truncate
  std::vector<int64_t> v8 = {0, 100 * F + 123, 200 * F + 456, F + 123, 2 * F + 456};
  std::vector<int32_t> e8 = {0, 100, 200, 1, 2};

  options.allow_time_truncate = true;
  CheckCase<Date64Type, int64_t, Date32Type, int32_t>(date64(), v8, is_valid, date32(),
                                                      e8, options);

  // Disallow truncate, failures
  options.allow_time_truncate = false;
  CheckFails<Date64Type>(date64(), v8, is_valid, date32(), options);
}

TEST_F(TestCast, DurationToCompatible) {
  CastOptions options;

  auto CheckDurationCast =
      [this](const CastOptions& options, TimeUnit::type from_unit, TimeUnit::type to_unit,
             const std::vector<int64_t>& from_values,
             const std::vector<int64_t>& to_values, const std::vector<bool>& is_valid) {
        CheckCase<DurationType, int64_t, DurationType, int64_t>(
            duration(from_unit), from_values, is_valid, duration(to_unit), to_values,
            options);
      };

  std::vector<bool> is_valid = {true, false, true, true, true};

  // Multiply promotions
  std::vector<int64_t> v1 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e1 = {0, 100000, 200000, 1000, 2000};
  CheckDurationCast(options, TimeUnit::SECOND, TimeUnit::MILLI, v1, e1, is_valid);

  std::vector<int64_t> v2 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e2 = {0, 100000000L, 200000000L, 1000000, 2000000};
  CheckDurationCast(options, TimeUnit::SECOND, TimeUnit::MICRO, v2, e2, is_valid);

  std::vector<int64_t> v3 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e3 = {0, 100000000000L, 200000000000L, 1000000000L, 2000000000L};
  CheckDurationCast(options, TimeUnit::SECOND, TimeUnit::NANO, v3, e3, is_valid);

  std::vector<int64_t> v4 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e4 = {0, 100000, 200000, 1000, 2000};
  CheckDurationCast(options, TimeUnit::MILLI, TimeUnit::MICRO, v4, e4, is_valid);

  std::vector<int64_t> v5 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e5 = {0, 100000000L, 200000000L, 1000000, 2000000};
  CheckDurationCast(options, TimeUnit::MILLI, TimeUnit::NANO, v5, e5, is_valid);

  std::vector<int64_t> v6 = {0, 100, 200, 1, 2};
  std::vector<int64_t> e6 = {0, 100000, 200000, 1000, 2000};
  CheckDurationCast(options, TimeUnit::MICRO, TimeUnit::NANO, v6, e6, is_valid);

  // Zero copy
  std::vector<int64_t> v7 = {0, 70000, 2000, 1000, 0};
  std::shared_ptr<Array> arr;
  ArrayFromVector<DurationType, int64_t>(duration(TimeUnit::SECOND), is_valid, v7, &arr);
  CheckZeroCopy(*arr, duration(TimeUnit::SECOND));
  CheckZeroCopy(*arr, int64());

  // Divide, truncate
  std::vector<int64_t> v8 = {0, 100123, 200456, 1123, 2456};
  std::vector<int64_t> e8 = {0, 100, 200, 1, 2};

  options.allow_time_truncate = true;
  CheckDurationCast(options, TimeUnit::MILLI, TimeUnit::SECOND, v8, e8, is_valid);
  CheckDurationCast(options, TimeUnit::MICRO, TimeUnit::MILLI, v8, e8, is_valid);
  CheckDurationCast(options, TimeUnit::NANO, TimeUnit::MICRO, v8, e8, is_valid);

  std::vector<int64_t> v9 = {0, 100123000, 200456000, 1123000, 2456000};
  std::vector<int64_t> e9 = {0, 100, 200, 1, 2};
  CheckDurationCast(options, TimeUnit::MICRO, TimeUnit::SECOND, v9, e9, is_valid);
  CheckDurationCast(options, TimeUnit::NANO, TimeUnit::MILLI, v9, e9, is_valid);

  std::vector<int64_t> v10 = {0, 100123000000L, 200456000000L, 1123000000L, 2456000000};
  std::vector<int64_t> e10 = {0, 100, 200, 1, 2};
  CheckDurationCast(options, TimeUnit::NANO, TimeUnit::SECOND, v10, e10, is_valid);

  // Disallow truncate, failures
  options.allow_time_truncate = false;
  CheckFails<DurationType>(duration(TimeUnit::MILLI), v8, is_valid,
                           duration(TimeUnit::SECOND), options);
  CheckFails<DurationType>(duration(TimeUnit::MICRO), v8, is_valid,
                           duration(TimeUnit::MILLI), options);
  CheckFails<DurationType>(duration(TimeUnit::NANO), v8, is_valid,
                           duration(TimeUnit::MICRO), options);
  CheckFails<DurationType>(duration(TimeUnit::MICRO), v9, is_valid,
                           duration(TimeUnit::SECOND), options);
  CheckFails<DurationType>(duration(TimeUnit::NANO), v9, is_valid,
                           duration(TimeUnit::MILLI), options);
  CheckFails<DurationType>(duration(TimeUnit::NANO), v10, is_valid,
                           duration(TimeUnit::SECOND), options);

  // Multiply overflow

  // 1000-01-01, 1800-01-01 , 2000-01-01, 2300-01-01, 3000-01-01
  std::vector<int64_t> v11 = {10000000000, 1, 2, 3, 10000000000};

  options.allow_time_overflow = false;
  CheckFails<DurationType>(duration(TimeUnit::SECOND), v11, is_valid,
                           duration(TimeUnit::NANO), options);
}

TEST_F(TestCast, ToDouble) {
  CastOptions options;
  std::vector<bool> is_valid = {true, false, true, true, true};

  // int16 to double
  std::vector<int16_t> v1 = {0, 100, 200, 1, 2};
  std::vector<double> e1 = {0, 100, 200, 1, 2};
  CheckCase<Int16Type, int16_t, DoubleType, double>(int16(), v1, is_valid, float64(), e1,
                                                    options);

  // float to double
  std::vector<float> v2 = {0, 100, 200, 1, 2};
  std::vector<double> e2 = {0, 100, 200, 1, 2};
  CheckCase<FloatType, float, DoubleType, double>(float32(), v2, is_valid, float64(), e2,
                                                  options);

  // bool to double
  std::vector<bool> v3 = {true, true, false, false, true};
  std::vector<double> e3 = {1, 1, 0, 0, 1};
  CheckCase<BooleanType, bool, DoubleType, double>(boolean(), v3, is_valid, float64(), e3,
                                                   options);
}

TEST_F(TestCast, ChunkedArray) {
  std::vector<int16_t> values1 = {0, 1, 2};
  std::vector<int16_t> values2 = {3, 4, 5};

  auto type = int16();
  auto out_type = int64();

  auto a1 = _MakeArray<Int16Type, int16_t>(type, values1, {});
  auto a2 = _MakeArray<Int16Type, int16_t>(type, values2, {});

  ArrayVector arrays = {a1, a2};
  auto carr = std::make_shared<ChunkedArray>(arrays);

  CastOptions options;

  ASSERT_OK_AND_ASSIGN(Datum out, Cast(carr, out_type, options));
  ASSERT_EQ(Datum::CHUNKED_ARRAY, out.kind());

  auto out_carr = out.chunked_array();

  std::vector<int64_t> ex_values1 = {0, 1, 2};
  std::vector<int64_t> ex_values2 = {3, 4, 5};
  auto a3 = _MakeArray<Int64Type, int64_t>(out_type, ex_values1, {});
  auto a4 = _MakeArray<Int64Type, int64_t>(out_type, ex_values2, {});

  ArrayVector ex_arrays = {a3, a4};
  auto ex_carr = std::make_shared<ChunkedArray>(ex_arrays);

  ASSERT_TRUE(out.chunked_array()->Equals(*ex_carr));
}

TEST_F(TestCast, UnsupportedTarget) {
  std::vector<bool> is_valid = {true, false, true, true, true};
  std::vector<int32_t> v1 = {0, 1, 2, 3, 4};

  std::shared_ptr<Array> arr;
  ArrayFromVector<Int32Type, int32_t>(int32(), is_valid, v1, &arr);

  ASSERT_RAISES(NotImplemented, Cast(*arr, list(utf8())));
}

TEST_F(TestCast, DateTimeZeroCopy) {
  std::vector<bool> is_valid = {true, false, true, true, true};

  std::vector<int32_t> v1 = {0, 70000, 2000, 1000, 0};
  std::shared_ptr<Array> arr;
  ArrayFromVector<Int32Type, int32_t>(int32(), is_valid, v1, &arr);

  CheckZeroCopy(*arr, time32(TimeUnit::SECOND));
  CheckZeroCopy(*arr, date32());

  std::vector<int64_t> v2 = {0, 70000, 2000, 1000, 0};
  ArrayFromVector<Int64Type, int64_t>(int64(), is_valid, v2, &arr);

  CheckZeroCopy(*arr, time64(TimeUnit::MICRO));
  CheckZeroCopy(*arr, date64());
  CheckZeroCopy(*arr, timestamp(TimeUnit::NANO));
  CheckZeroCopy(*arr, duration(TimeUnit::MILLI));
}

TEST_F(TestCast, StringToBoolean) {
  CastOptions options;

  std::vector<bool> is_valid = {true, false, true, true, true};

  std::vector<std::string> v1 = {"False", "true", "true", "True", "false"};
  std::vector<std::string> v2 = {"0", "1", "1", "1", "0"};
  std::vector<bool> e = {false, true, true, true, false};
  CheckCase<StringType, std::string, BooleanType, bool>(utf8(), v1, is_valid, boolean(),
                                                        e, options);
  CheckCase<StringType, std::string, BooleanType, bool>(utf8(), v2, is_valid, boolean(),
                                                        e, options);

  // Same with LargeStringType
  CheckCase<LargeStringType, std::string, BooleanType, bool>(large_utf8(), v1, is_valid,
                                                             boolean(), e, options);
}

TEST_F(TestCast, StringToBooleanErrors) {
  CastOptions options;

  std::vector<bool> is_valid = {true};

  CheckFails<StringType, std::string>(utf8(), {"false "}, is_valid, boolean(), options);
  CheckFails<StringType, std::string>(utf8(), {"T"}, is_valid, boolean(), options);
  CheckFails<LargeStringType, std::string>(large_utf8(), {"T"}, is_valid, boolean(),
                                           options);
}

TEST_F(TestCast, StringToNumber) { TestCastStringToNumber<StringType>(); }

TEST_F(TestCast, LargeStringToNumber) { TestCastStringToNumber<LargeStringType>(); }

TEST_F(TestCast, StringToNumberErrors) {
  CastOptions options;

  std::vector<bool> is_valid = {true};

  CheckFails<StringType, std::string>(utf8(), {"z"}, is_valid, int8(), options);
  CheckFails<StringType, std::string>(utf8(), {"12 z"}, is_valid, int8(), options);
  CheckFails<StringType, std::string>(utf8(), {"128"}, is_valid, int8(), options);
  CheckFails<StringType, std::string>(utf8(), {"-129"}, is_valid, int8(), options);
  CheckFails<StringType, std::string>(utf8(), {"0.5"}, is_valid, int8(), options);

  CheckFails<StringType, std::string>(utf8(), {"256"}, is_valid, uint8(), options);
  CheckFails<StringType, std::string>(utf8(), {"-1"}, is_valid, uint8(), options);

  CheckFails<StringType, std::string>(utf8(), {"z"}, is_valid, float32(), options);
}

TEST_F(TestCast, StringToTimestamp) { TestCastStringToTimestamp<StringType>(); }

TEST_F(TestCast, LargeStringToTimestamp) { TestCastStringToTimestamp<LargeStringType>(); }

TEST_F(TestCast, StringToTimestampErrors) {
  CastOptions options;

  std::vector<bool> is_valid = {true};

  for (auto unit : {TimeUnit::SECOND, TimeUnit::MILLI, TimeUnit::MICRO, TimeUnit::NANO}) {
    auto type = timestamp(unit);
    CheckFails<StringType, std::string>(utf8(), {""}, is_valid, type, options);
    CheckFails<StringType, std::string>(utf8(), {"xxx"}, is_valid, type, options);
  }
}

TEST_F(TestCast, BinaryToString) { TestCastBinaryToString<BinaryType, StringType>(); }

TEST_F(TestCast, LargeBinaryToLargeString) {
  TestCastBinaryToString<LargeBinaryType, LargeStringType>();
}

TEST_F(TestCast, NumberToString) { TestCastNumberToString<StringType>(); }

TEST_F(TestCast, NumberToLargeString) { TestCastNumberToString<LargeStringType>(); }

TEST_F(TestCast, BooleanToString) { TestCastBooleanToString<StringType>(); }

TEST_F(TestCast, BooleanToLargeString) { TestCastBooleanToString<LargeStringType>(); }

TEST_F(TestCast, ListToPrimitive) {
  auto from_int = ArrayFromJSON(list(int8()), "[[1, 2], [3, 4]]");
  auto from_binary = ArrayFromJSON(list(binary()), "[[\"1\", \"2\"], [\"3\", \"4\"]]");

  ASSERT_RAISES(NotImplemented, Cast(*from_int, uint8()));
  ASSERT_RAISES(NotImplemented, Cast(*from_binary, utf8()));
}

TEST_F(TestCast, ListToList) {
  CastOptions options;
  std::shared_ptr<Array> offsets;

  std::vector<int32_t> offsets_values = {0, 1, 2, 5, 7, 7, 8, 10};
  std::vector<bool> offsets_is_valid = {true, true, true, true, false, true, true, true};
  ArrayFromVector<Int32Type, int32_t>(offsets_is_valid, offsets_values, &offsets);

  std::shared_ptr<Array> int32_plain_array =
      TestBase::MakeRandomArray<typename TypeTraits<Int32Type>::ArrayType>(10, 2);
  ASSERT_OK_AND_ASSIGN(auto int32_list_array,
                       ListArray::FromArrays(*offsets, *int32_plain_array, pool_));

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> int64_plain_array,
                       Cast(*int32_plain_array, int64(), options));
  ASSERT_OK_AND_ASSIGN(auto int64_list_array,
                       ListArray::FromArrays(*offsets, *int64_plain_array, pool_));

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> float64_plain_array,
                       Cast(*int32_plain_array, float64(), options));
  ASSERT_OK_AND_ASSIGN(auto float64_list_array,
                       ListArray::FromArrays(*offsets, *float64_plain_array, pool_));

  CheckPass(*int32_list_array, *int64_list_array, int64_list_array->type(), options);
  CheckPass(*int32_list_array, *float64_list_array, float64_list_array->type(), options);
  CheckPass(*int64_list_array, *int32_list_array, int32_list_array->type(), options);
  CheckPass(*int64_list_array, *float64_list_array, float64_list_array->type(), options);

  options.allow_float_truncate = true;
  CheckPass(*float64_list_array, *int32_list_array, int32_list_array->type(), options);
  CheckPass(*float64_list_array, *int64_list_array, int64_list_array->type(), options);
}

TEST_F(TestCast, LargeListToLargeList) {
  // Like ListToList above, only testing the basics
  CastOptions options;
  std::shared_ptr<Array> offsets;

  std::vector<int64_t> offsets_values = {0, 1, 2, 5, 7, 7, 8, 10};
  std::vector<bool> offsets_is_valid = {true, true, true, true, false, true, true, true};
  ArrayFromVector<Int64Type, int64_t>(offsets_is_valid, offsets_values, &offsets);

  std::shared_ptr<Array> int32_plain_array =
      TestBase::MakeRandomArray<typename TypeTraits<Int32Type>::ArrayType>(10, 2);
  ASSERT_OK_AND_ASSIGN(auto int32_list_array,
                       LargeListArray::FromArrays(*offsets, *int32_plain_array, pool_));

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> float64_plain_array,
                       Cast(*int32_plain_array, float64(), options));
  ASSERT_OK_AND_ASSIGN(auto float64_list_array,
                       LargeListArray::FromArrays(*offsets, *float64_plain_array, pool_));

  CheckPass(*int32_list_array, *float64_list_array, float64_list_array->type(), options);

  options.allow_float_truncate = true;
  CheckPass(*float64_list_array, *int32_list_array, int32_list_array->type(), options);
}

TEST_F(TestCast, IdentityCasts) {
  // ARROW-4102
  auto CheckIdentityCast = [this](std::shared_ptr<DataType> type,
                                  const std::string& json) {
    auto arr = ArrayFromJSON(type, json);
    CheckZeroCopy(*arr, type);
  };

  CheckIdentityCast(null(), "[null, null, null]");
  CheckIdentityCast(boolean(), "[false, true, null, false]");

  for (auto type : kNumericTypes) {
    CheckIdentityCast(type, "[1, 2, null, 4]");
  }
  CheckIdentityCast(binary(), "[\"foo\", \"bar\"]");
  CheckIdentityCast(utf8(), "[\"foo\", \"bar\"]");
  CheckIdentityCast(fixed_size_binary(3), "[\"foo\", \"bar\"]");

  CheckIdentityCast(list(int8()), "[[1, 2], [null], [], [3]]");

  CheckIdentityCast(time32(TimeUnit::MILLI), "[1, 2, 3, 4]");
  CheckIdentityCast(time64(TimeUnit::MICRO), "[1, 2, 3, 4]");
  CheckIdentityCast(date32(), "[1, 2, 3, 4]");
  CheckIdentityCast(date64(), "[86400000, 0]");
  CheckIdentityCast(timestamp(TimeUnit::SECOND), "[1, 2, 3, 4]");

  {
    auto dict_values = ArrayFromJSON(int8(), "[1, 2, 3]");
    auto dict_type = dictionary(int8(), dict_values->type());
    auto dict_indices = ArrayFromJSON(int8(), "[0, 1, 2, 0, null, 2]");
    auto dict_array =
        std::make_shared<DictionaryArray>(dict_type, dict_indices, dict_values);
    CheckZeroCopy(*dict_array, dict_type);
  }
}

TEST_F(TestCast, EmptyCasts) {
  // ARROW-4766: 0-length arrays should not segfault
  auto CheckEmptyCast = [this](std::shared_ptr<DataType> from,
                               std::shared_ptr<DataType> to) {
    CastOptions options;

    // Python creates array with nullptr instead of 0-length (valid) buffers.
    auto data = ArrayData::Make(from, /* length */ 0, /* buffers */ {nullptr, nullptr});
    auto input = MakeArray(data);
    auto expected = ArrayFromJSON(to, "[]");
    CheckPass(*input, *expected, to, CastOptions{});
  };

  for (auto numeric : kNumericTypes) {
    CheckEmptyCast(boolean(), numeric);
    CheckEmptyCast(numeric, boolean());
  }
}

// ----------------------------------------------------------------------
// Test casting from NullType

template <typename TestType>
class TestNullCast : public TestCast {};

typedef ::testing::Types<NullType, UInt8Type, Int8Type, UInt16Type, Int16Type, Int32Type,
                         UInt32Type, UInt64Type, Int64Type, FloatType, DoubleType,
                         Date32Type, Date64Type, FixedSizeBinaryType, BinaryType>
    TestTypes;

TYPED_TEST_SUITE(TestNullCast, TestTypes);

TYPED_TEST(TestNullCast, FromNull) {
  // Null casts to everything
  const int length = 10;

  // Hack to get a DataType including for parametric types
  std::shared_ptr<DataType> out_type =
      TestBase::MakeRandomArray<typename TypeTraits<TypeParam>::ArrayType>(0, 0)->type();

  NullArray arr(length);

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> result, Cast(arr, out_type));
  ASSERT_OK(result->ValidateFull());

  ASSERT_TRUE(result->type()->Equals(*out_type));
  ASSERT_EQ(length, result->length());
  ASSERT_EQ(length, result->null_count());
}

// ----------------------------------------------------------------------
// Test casting from DictionaryType

template <typename TestType>
class TestDictionaryCast : public TestCast {};

typedef ::testing::Types<NullType, UInt8Type, Int8Type, UInt16Type, Int16Type, Int32Type,
                         UInt32Type, UInt64Type, Int64Type, FloatType, DoubleType,
                         Date32Type, Date64Type, FixedSizeBinaryType, BinaryType>
    TestTypes;

TYPED_TEST_SUITE(TestDictionaryCast, TestTypes);

TYPED_TEST(TestDictionaryCast, Basic) {
  CastOptions options;
  std::shared_ptr<Array> plain_array =
      TestBase::MakeRandomArray<typename TypeTraits<TypeParam>::ArrayType>(10, 2);

  ASSERT_OK_AND_ASSIGN(Datum encoded, DictionaryEncode(plain_array->data()));
  ASSERT_EQ(encoded.array()->type->id(), Type::DICTIONARY);

  this->CheckPass(*MakeArray(encoded.array()), *plain_array, plain_array->type(),
                  options);
}

TYPED_TEST(TestDictionaryCast, NoNulls) {
  // Test with a nullptr bitmap buffer (ARROW-3208)
  if (TypeParam::type_id == Type::NA) {
    // Skip, but gtest doesn't support skipping :-/
    return;
  }

  CastOptions options;
  std::shared_ptr<Array> plain_array =
      TestBase::MakeRandomArray<typename TypeTraits<TypeParam>::ArrayType>(10, 0);
  ASSERT_EQ(plain_array->null_count(), 0);

  // Dict-encode the plain array
  ASSERT_OK_AND_ASSIGN(Datum encoded, DictionaryEncode(plain_array->data()));

  // Make a new dict array with nullptr bitmap buffer
  auto data = encoded.array()->Copy();
  data->buffers[0] = nullptr;
  data->null_count = 0;
  std::shared_ptr<Array> dict_array = std::make_shared<DictionaryArray>(data);
  ASSERT_OK(dict_array->ValidateFull());

  this->CheckPass(*dict_array, *plain_array, plain_array->type(), options);
}

// TODO: See how this might cause problems post-refactor
TYPED_TEST(TestDictionaryCast, DISABLED_OutTypeError) {
  // ARROW-7077: unsupported out type should return an error
  std::shared_ptr<Array> plain_array =
      TestBase::MakeRandomArray<typename TypeTraits<TypeParam>::ArrayType>(0, 0);
  auto in_type = dictionary(int32(), plain_array->type());

  auto out_type = (plain_array->type()->id() == Type::INT8) ? binary() : int8();
  // Test an output type that's not part of TestTypes.
  out_type = list(in_type);
  ASSERT_RAISES(NotImplemented, GetCastFunction(out_type));
}

std::shared_ptr<Array> SmallintArrayFromJSON(const std::string& json_data) {
  auto arr = ArrayFromJSON(int16(), json_data);
  auto ext_data = arr->data()->Copy();
  ext_data->type = smallint();
  return MakeArray(ext_data);
}

TEST_F(TestCast, ExtensionTypeToIntDowncast) {
  auto smallint = std::make_shared<SmallintType>();
  ASSERT_OK(RegisterExtensionType(smallint));

  CastOptions options;
  options.allow_int_overflow = false;

  std::shared_ptr<Array> result;
  std::vector<bool> is_valid = {true, false, true, true, true};

  // Smallint(int16) to int16
  auto v0 = SmallintArrayFromJSON("[0, 100, 200, 1, 2]");
  CheckZeroCopy(*v0, int16());

  // Smallint(int16) to uint8, no overflow/underrun
  auto v1 = SmallintArrayFromJSON("[0, 100, 200, 1, 2]");
  auto e1 = ArrayFromJSON(uint8(), "[0, 100, 200, 1, 2]");
  CheckPass(*v1, *e1, uint8(), options);

  // Smallint(int16) to uint8, with overflow
  auto v2 = SmallintArrayFromJSON("[0, null, 256, 1, 3]");
  auto e2 = ArrayFromJSON(uint8(), "[0, null, 0, 1, 3]");
  // allow overflow
  options.allow_int_overflow = true;
  CheckPass(*v2, *e2, uint8(), options);
  // disallow overflow
  options.allow_int_overflow = false;
  ASSERT_RAISES(Invalid, Cast(*v2, uint8(), options));

  // Smallint(int16) to uint8, with underflow
  auto v3 = SmallintArrayFromJSON("[0, null, -1, 1, 0]");
  auto e3 = ArrayFromJSON(uint8(), "[0, null, 255, 1, 0]");
  // allow overflow
  options.allow_int_overflow = true;
  CheckPass(*v3, *e3, uint8(), options);
  // disallow overflow
  options.allow_int_overflow = false;
  ASSERT_RAISES(Invalid, Cast(*v3, uint8(), options));

  ASSERT_OK(UnregisterExtensionType("smallint"));
}

}  // namespace compute
}  // namespace arrow
