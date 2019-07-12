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

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/ipc/json-simple.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"

#if defined(_MSC_VER)
// "warning C4307: '+': integral constant overflow"
#pragma warning(disable : 4307)
#endif

namespace arrow {
namespace ipc {
namespace internal {
namespace json {

using ::arrow::internal::checked_cast;
using ::arrow::internal::checked_pointer_cast;

// Avoid undefined behaviour on signed overflow
template <typename Signed>
Signed SafeSignedAdd(Signed u, Signed v) {
  using Unsigned = typename std::make_unsigned<Signed>::type;
  return static_cast<Signed>(static_cast<Unsigned>(u) + static_cast<Unsigned>(v));
}

// Special case for 8-bit ints (must output their decimal value, not the
// corresponding ASCII character)
void JSONArrayInternal(std::ostream* ss, int8_t value) {
  *ss << static_cast<int16_t>(value);
}

void JSONArrayInternal(std::ostream* ss, uint8_t value) {
  *ss << static_cast<int16_t>(value);
}

template <typename Value>
void JSONArrayInternal(std::ostream* ss, Value&& value) {
  *ss << value;
}

template <typename Value, typename... Tail>
void JSONArrayInternal(std::ostream* ss, Value&& value, Tail&&... tail) {
  JSONArrayInternal(ss, std::forward<Value>(value));
  *ss << ", ";
  JSONArrayInternal(ss, std::forward<Tail>(tail)...);
}

template <typename... Args>
std::string JSONArray(Args&&... args) {
  std::stringstream ss;
  ss << "[";
  JSONArrayInternal(&ss, std::forward<Args>(args)...);
  ss << "]";
  return ss.str();
}

template <typename T, typename C_TYPE = typename T::c_type>
void AssertJSONArray(const std::shared_ptr<DataType>& type, const std::string& json,
                     const std::vector<C_TYPE>& values) {
  std::shared_ptr<Array> actual, expected;

  ASSERT_OK(ArrayFromJSON(type, json, &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<T, C_TYPE>(type, values, &expected);
  AssertArraysEqual(*expected, *actual);
}

template <typename T, typename C_TYPE = typename T::c_type>
void AssertJSONArray(const std::shared_ptr<DataType>& type, const std::string& json,
                     const std::vector<bool>& is_valid,
                     const std::vector<C_TYPE>& values) {
  std::shared_ptr<Array> actual, expected;

  ASSERT_OK(ArrayFromJSON(type, json, &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<T, C_TYPE>(type, is_valid, values, &expected);
  AssertArraysEqual(*expected, *actual);
}

TEST(TestHelper, JSONArray) {
  // Test the JSONArray helper func
  std::string s =
      JSONArray(123, -4.5, static_cast<int8_t>(-12), static_cast<uint8_t>(34));
  ASSERT_EQ(s, "[123, -4.5, -12, 34]");
  s = JSONArray(9223372036854775807LL, 9223372036854775808ULL, -9223372036854775807LL - 1,
                18446744073709551615ULL);
  ASSERT_EQ(s,
            "[9223372036854775807, 9223372036854775808, -9223372036854775808, "
            "18446744073709551615]");
}

TEST(TestHelper, SafeSignedAdd) {
  ASSERT_EQ(0, SafeSignedAdd<int8_t>(-128, -128));
  ASSERT_EQ(1, SafeSignedAdd<int8_t>(-128, -127));
  ASSERT_EQ(-128, SafeSignedAdd<int8_t>(1, 127));
  ASSERT_EQ(-2147483648LL, SafeSignedAdd<int32_t>(1, 2147483647));
}

template <typename T>
class TestIntegers : public ::testing::Test {};

TYPED_TEST_CASE_P(TestIntegers);

TYPED_TEST_P(TestIntegers, Basics) {
  using T = TypeParam;
  using c_type = typename T::c_type;

  std::shared_ptr<Array> expected, actual;
  std::shared_ptr<DataType> type = TypeTraits<T>::type_singleton();

  AssertJSONArray<T>(type, "[]", {});
  AssertJSONArray<T>(type, "[4, 0, 5]", {4, 0, 5});
  AssertJSONArray<T>(type, "[4, null, 5]", {true, false, true}, {4, 0, 5});

  // Test limits
  const auto min_val = std::numeric_limits<c_type>::min();
  const auto max_val = std::numeric_limits<c_type>::max();
  std::string json_string = JSONArray(0, 1, min_val);
  AssertJSONArray<T>(type, json_string, {0, 1, min_val});
  json_string = JSONArray(0, 1, max_val);
  AssertJSONArray<T>(type, json_string, {0, 1, max_val});
}

TYPED_TEST_P(TestIntegers, Errors) {
  using T = TypeParam;

  std::shared_ptr<Array> array;
  std::shared_ptr<DataType> type = TypeTraits<T>::type_singleton();

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "0", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "{}", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0.0]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"0\"]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0]]", &array));
}

TYPED_TEST_P(TestIntegers, OutOfBounds) {
  using T = TypeParam;
  using c_type = typename T::c_type;

  std::shared_ptr<Array> array;
  std::shared_ptr<DataType> type = TypeTraits<T>::type_singleton();

  if (type->id() == Type::UINT64) {
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[18446744073709551616]", &array));
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[-1]", &array));
  } else if (type->id() == Type::INT64) {
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[9223372036854775808]", &array));
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[-9223372036854775809]", &array));
  } else if (std::is_signed<c_type>::value) {
    const auto lower = SafeSignedAdd<int64_t>(std::numeric_limits<c_type>::min(), -1);
    const auto upper = SafeSignedAdd<int64_t>(std::numeric_limits<c_type>::max(), +1);
    auto json_string = JSONArray(lower);
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, json_string, &array));
    json_string = JSONArray(upper);
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, json_string, &array));
  } else {
    const auto upper = static_cast<uint64_t>(std::numeric_limits<c_type>::max()) + 1;
    auto json_string = JSONArray(upper);
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, json_string, &array));
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[-1]", &array));
  }
}

REGISTER_TYPED_TEST_CASE_P(TestIntegers, Basics, Errors, OutOfBounds);

INSTANTIATE_TYPED_TEST_CASE_P(TestInt8, TestIntegers, Int8Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestInt16, TestIntegers, Int16Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestInt32, TestIntegers, Int32Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestInt64, TestIntegers, Int64Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt8, TestIntegers, UInt8Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt16, TestIntegers, UInt16Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt32, TestIntegers, UInt32Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt64, TestIntegers, UInt64Type);

TEST(TestNull, Basics) {
  std::shared_ptr<DataType> type = null();
  std::shared_ptr<Array> expected, actual;

  AssertJSONArray<NullType, std::nullptr_t>(type, "[]", {});
  AssertJSONArray<NullType, std::nullptr_t>(type, "[null, null]", {nullptr, nullptr});
}

TEST(TestNull, Errors) {
  std::shared_ptr<DataType> type = null();
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[NaN]", &array));
}

TEST(TestBoolean, Basics) {
  std::shared_ptr<DataType> type = boolean();
  std::shared_ptr<Array> expected, actual;

  AssertJSONArray<BooleanType, bool>(type, "[]", {});
  AssertJSONArray<BooleanType, bool>(type, "[false, true, false]", {false, true, false});
  AssertJSONArray<BooleanType, bool>(type, "[false, true, null]", {true, true, false},
                                     {false, true, false});
  // Supports integer literal casting
  AssertJSONArray<BooleanType, bool>(type, "[0, 1, 0]", {false, true, false});
  AssertJSONArray<BooleanType, bool>(type, "[0, 1, null]", {true, true, false},
                                     {false, true, false});
}

TEST(TestBoolean, Errors) {
  std::shared_ptr<DataType> type = boolean();
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0.0]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"true\"]", &array));
}

TEST(TestFloat, Basics) {
  std::shared_ptr<DataType> type = float32();
  std::shared_ptr<Array> expected, actual;

  AssertJSONArray<FloatType>(type, "[]", {});
  AssertJSONArray<FloatType>(type, "[1, 2.5, -3e4]", {1.0f, 2.5f, -3.0e4f});
  AssertJSONArray<FloatType>(type, "[-0.0, Inf, -Inf, null]", {true, true, true, false},
                             {-0.0f, INFINITY, -INFINITY, 0.0f});

  // Check NaN separately as AssertArraysEqual simply memcmp's array contents
  // and NaNs can have many bit representations.
  ASSERT_OK(ArrayFromJSON(type, "[NaN]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  float value = checked_cast<FloatArray&>(*actual).Value(0);
  ASSERT_TRUE(std::isnan(value));
}

TEST(TestFloat, Errors) {
  std::shared_ptr<DataType> type = float32();
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[true]", &array));
}

TEST(TestDouble, Basics) {
  std::shared_ptr<DataType> type = float64();
  std::shared_ptr<Array> expected, actual;

  AssertJSONArray<DoubleType>(type, "[]", {});
  AssertJSONArray<DoubleType>(type, "[1, 2.5, -3e4]", {1.0, 2.5, -3.0e4});
  AssertJSONArray<DoubleType>(type, "[-0.0, Inf, -Inf, null]", {true, true, true, false},
                              {-0.0, INFINITY, -INFINITY, 0.0});

  ASSERT_OK(ArrayFromJSON(type, "[NaN]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  double value = checked_cast<DoubleArray&>(*actual).Value(0);
  ASSERT_TRUE(std::isnan(value));
}

TEST(TestDouble, Errors) {
  std::shared_ptr<DataType> type = float64();
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[true]", &array));
}

TEST(TestString, Basics) {
  // String type
  std::shared_ptr<DataType> type = utf8();
  std::shared_ptr<Array> expected, actual;

  AssertJSONArray<StringType, std::string>(type, "[]", {});
  AssertJSONArray<StringType, std::string>(type, "[\"\", \"foo\"]", {"", "foo"});
  AssertJSONArray<StringType, std::string>(type, "[\"\", null]", {true, false}, {"", ""});
  // NUL character in string
  std::string s = "some";
  s += '\x00';
  s += "char";
  AssertJSONArray<StringType, std::string>(type, "[\"\", \"some\\u0000char\"]", {"", s});
  // UTF8 sequence in string
  AssertJSONArray<StringType, std::string>(type, "[\"\xc3\xa9\"]", {"\xc3\xa9"});

  // Binary type
  type = binary();
  AssertJSONArray<BinaryType, std::string>(type, "[\"\", \"foo\", null]",
                                           {true, true, false}, {"", "foo", ""});
  // Arbitrary binary (non-UTF8) sequence in string
  s = "\xff\x9f";
  AssertJSONArray<BinaryType, std::string>(type, "[\"" + s + "\"]", {s});
  // Bytes < 0x20 can be represented as JSON unicode escapes
  s = '\x00';
  s += "\x1f";
  AssertJSONArray<BinaryType, std::string>(type, "[\"\\u0000\\u001f\"]", {s});
}

TEST(TestTimestamp, Basics) {
  // Timestamp type
  auto type = timestamp(TimeUnit::SECOND);
  AssertJSONArray<TimestampType, int64_t>(
      type, R"(["1970-01-01","2000-02-29","3989-07-14","1900-02-28"])",
      {0, 951782400, 63730281600LL, -2203977600LL});

  type = timestamp(TimeUnit::NANO);
  AssertJSONArray<TimestampType, int64_t>(
      type, R"(["1970-01-01","2000-02-29","1900-02-28"])",
      {0, 951782400000000000LL, -2203977600000000000LL});
}

TEST(TestString, Errors) {
  std::shared_ptr<DataType> type = utf8();
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[]]", &array));
}

TEST(TestFixedSizeBinary, Basics) {
  std::shared_ptr<DataType> type = fixed_size_binary(3);
  std::shared_ptr<Array> expected, actual;

  AssertJSONArray<FixedSizeBinaryType, std::string>(type, "[]", {});
  AssertJSONArray<FixedSizeBinaryType, std::string>(type, "[\"foo\", \"bar\"]",
                                                    {"foo", "bar"});
  AssertJSONArray<FixedSizeBinaryType, std::string>(type, "[null, \"foo\"]",
                                                    {false, true}, {"", "foo"});
  // Arbitrary binary (non-UTF8) sequence in string
  std::string s = "\xff\x9f\xcc";
  AssertJSONArray<FixedSizeBinaryType, std::string>(type, "[\"" + s + "\"]", {s});
}

TEST(TestFixedSizeBinary, Errors) {
  std::shared_ptr<DataType> type = fixed_size_binary(3);
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[]]", &array));
  // Invalid length
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"\"]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"abcd\"]", &array));
}

TEST(TestDecimal, Basics) {
  std::shared_ptr<DataType> type = decimal(10, 4);
  std::shared_ptr<Array> expected, actual;

  ASSERT_OK(ArrayFromJSON(type, "[]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  {
    Decimal128Builder builder(type);
    ASSERT_OK(builder.Finish(&expected));
  }
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[\"123.4567\", \"-78.9000\"]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  {
    Decimal128Builder builder(type);
    ASSERT_OK(builder.Append(Decimal128(1234567)));
    ASSERT_OK(builder.Append(Decimal128(-789000)));
    ASSERT_OK(builder.Finish(&expected));
  }
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[\"123.4567\", null]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  {
    Decimal128Builder builder(type);
    ASSERT_OK(builder.Append(Decimal128(1234567)));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK(builder.Finish(&expected));
  }
  AssertArraysEqual(*expected, *actual);
}

TEST(TestDecimal, Errors) {
  std::shared_ptr<DataType> type = decimal(10, 4);
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[12.3456]", &array));
  // Bad scale
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"12.345\"]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"12.34560\"]", &array));
}

TEST(TestList, IntegerList) {
  auto pool = default_memory_pool();
  std::shared_ptr<DataType> type = list(int64());
  std::shared_ptr<Array> offsets, values, expected, actual;

  ASSERT_OK(ArrayFromJSON(type, "[]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<Int32Type>({0}, &offsets);
  ArrayFromVector<Int64Type>({}, &values);
  ASSERT_OK(ListArray::FromArrays(*offsets, *values, pool, &expected));
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[[4, 5], [], [6]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<Int32Type>({0, 2, 2, 3}, &offsets);
  ArrayFromVector<Int64Type>({4, 5, 6}, &values);
  ASSERT_OK(ListArray::FromArrays(*offsets, *values, pool, &expected));
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[[], [null], [6, null]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<Int32Type>({0, 0, 1, 3}, &offsets);
  auto is_valid = std::vector<bool>{false, true, false};
  ArrayFromVector<Int64Type>(is_valid, {0, 6, 0}, &values);
  ASSERT_OK(ListArray::FromArrays(*offsets, *values, pool, &expected));
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[null, [], null]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  {
    std::unique_ptr<ArrayBuilder> builder;
    ASSERT_OK(MakeBuilder(pool, type, &builder));
    auto& list_builder = checked_cast<ListBuilder&>(*builder);
    ASSERT_OK(list_builder.AppendNull());
    ASSERT_OK(list_builder.Append());
    ASSERT_OK(list_builder.AppendNull());
    ASSERT_OK(list_builder.Finish(&expected));
  }
  AssertArraysEqual(*expected, *actual);
}

TEST(TestList, IntegerListErrors) {
  std::shared_ptr<DataType> type = list(int64());
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0.0]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[9223372036854775808]]", &array));
}

TEST(TestList, NullList) {
  auto pool = default_memory_pool();
  std::shared_ptr<DataType> type = list(null());
  std::shared_ptr<Array> offsets, values, expected, actual;

  ASSERT_OK(ArrayFromJSON(type, "[]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<Int32Type>({0}, &offsets);
  values = std::make_shared<NullArray>(0);
  ASSERT_OK(ListArray::FromArrays(*offsets, *values, pool, &expected));
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[[], [null], [null, null]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<Int32Type>({0, 0, 1, 3}, &offsets);
  values = std::make_shared<NullArray>(3);
  ASSERT_OK(ListArray::FromArrays(*offsets, *values, pool, &expected));
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[null, [], null]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  {
    std::unique_ptr<ArrayBuilder> builder;
    ASSERT_OK(MakeBuilder(pool, type, &builder));
    auto& list_builder = checked_cast<ListBuilder&>(*builder);
    ASSERT_OK(list_builder.AppendNull());
    ASSERT_OK(list_builder.Append());
    ASSERT_OK(list_builder.AppendNull());
    ASSERT_OK(list_builder.Finish(&expected));
  }
  AssertArraysEqual(*expected, *actual);
}

TEST(TestList, IntegerListList) {
  auto pool = default_memory_pool();
  std::shared_ptr<DataType> type = list(list(uint8()));
  std::shared_ptr<Array> offsets, values, nested, expected, actual;

  ASSERT_OK(ArrayFromJSON(type, "[[[4], [5, 6]], [[7, 8, 9]]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<Int32Type>({0, 1, 3, 6}, &offsets);
  ArrayFromVector<UInt8Type>({4, 5, 6, 7, 8, 9}, &values);
  ASSERT_OK(ListArray::FromArrays(*offsets, *values, pool, &nested));
  ArrayFromVector<Int32Type>({0, 2, 3}, &offsets);
  ASSERT_OK(ListArray::FromArrays(*offsets, *nested, pool, &expected));
  ASSERT_EQ(actual->length(), 2);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[[], [[]], [[4], [], [5, 6]], [[7, 8, 9]]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<Int32Type>({0, 0, 1, 1, 3, 6}, &offsets);
  ArrayFromVector<UInt8Type>({4, 5, 6, 7, 8, 9}, &values);
  ASSERT_OK(ListArray::FromArrays(*offsets, *values, pool, &nested));
  ArrayFromVector<Int32Type>({0, 0, 1, 4, 5}, &offsets);
  ASSERT_OK(ListArray::FromArrays(*offsets, *nested, pool, &expected));
  ASSERT_EQ(actual->length(), 4);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[null, [null], [[null]]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  {
    std::unique_ptr<ArrayBuilder> builder;
    ASSERT_OK(MakeBuilder(pool, type, &builder));
    auto& list_builder = checked_cast<ListBuilder&>(*builder);
    auto& child_builder = checked_cast<ListBuilder&>(*list_builder.value_builder());
    ASSERT_OK(list_builder.AppendNull());
    ASSERT_OK(list_builder.Append());
    ASSERT_OK(child_builder.AppendNull());
    ASSERT_OK(list_builder.Append());
    ASSERT_OK(child_builder.Append());
    ASSERT_OK(list_builder.Finish(&expected));
  }
}

TEST(TestMap, IntegerToInteger) {
  auto type = map(int16(), int16());
  std::shared_ptr<Array> expected, actual;

  const char* input = R"(
[
    [[0, 1], [1, 1], [2, 2], [3, 3], [4, 5], [5, 8]],
    null,
    [[0, null], [1, null], [2, 0], [3, 1], [4, null], [5, 2]],
    []
  ]
)";
  ASSERT_OK(ArrayFromJSON(type, input, &actual));

  std::unique_ptr<ArrayBuilder> builder;
  ASSERT_OK(MakeBuilder(default_memory_pool(), type, &builder));
  auto& map_builder = checked_cast<MapBuilder&>(*builder);
  auto& key_builder = checked_cast<Int16Builder&>(*map_builder.key_builder());
  auto& item_builder = checked_cast<Int16Builder&>(*map_builder.item_builder());

  ASSERT_OK(map_builder.Append());
  ASSERT_OK(key_builder.AppendValues({0, 1, 2, 3, 4, 5}));
  ASSERT_OK(item_builder.AppendValues({1, 1, 2, 3, 5, 8}));
  ASSERT_OK(map_builder.AppendNull());
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(key_builder.AppendValues({0, 1, 2, 3, 4, 5}));
  ASSERT_OK(item_builder.AppendValues({-1, -1, 0, 1, -1, 2}, {0, 0, 1, 1, 0, 1}));
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(map_builder.Finish(&expected));

  ASSERT_ARRAYS_EQUAL(*actual, *expected);
}

TEST(TestMap, StringToInteger) {
  auto type = map(utf8(), int32());
  const char* input = R"(
[
    [["joe", 0], ["mark", null]],
    null,
    [["cap", 8]],
    []
  ]
)";
  auto actual = ArrayFromJSON(type, input);
  std::vector<int32_t> offsets = {0, 2, 2, 3, 3};
  auto expected_keys = ArrayFromJSON(utf8(), R"(["joe", "mark", "cap"])");
  auto expected_values = ArrayFromJSON(int32(), "[0, null, 8]");
  std::shared_ptr<Buffer> expected_null_bitmap;
  ASSERT_OK(
      BitUtil::BytesToBits({1, 0, 1, 1}, default_memory_pool(), &expected_null_bitmap));
  auto expected =
      std::make_shared<MapArray>(type, 4, Buffer::Wrap(offsets), expected_keys,
                                 expected_values, expected_null_bitmap, 1);
  ASSERT_ARRAYS_EQUAL(*actual, *expected);
}

TEST(TestMap, Errors) {
  auto type = map(int16(), int16());
  std::shared_ptr<Array> array;

  // list of pairs isn't an array
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]", &array));
  // pair isn't an array
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[null]]", &array));
  // pair with length != 2
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[[0]]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[[0, 1, 2]]]", &array));
  // null key
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[[null, 0]]]", &array));
  // key or value fails to convert
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[[0.0, 0]]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[[0, 0.0]]]", &array));
}

TEST(TestMap, IntegerMapToStringList) {
  auto type = map(map(int16(), int16()), list(utf8()));
  std::shared_ptr<Array> expected, actual;

  const char* input = R"(
[
    [
      [
        [],
        [null, "empty"]
      ],
      [
        [[0, 1]],
        null
      ],
      [
        [[0, 0], [1, 1]],
        ["bootstrapping tautology?", "lispy", null, "i can see eternity"]
      ]
    ],
    null
  ]
)";
  ASSERT_OK(ArrayFromJSON(type, input, &actual));

  std::unique_ptr<ArrayBuilder> builder;
  ASSERT_OK(MakeBuilder(default_memory_pool(), type, &builder));
  auto& map_builder = checked_cast<MapBuilder&>(*builder);
  auto& key_builder = checked_cast<MapBuilder&>(*map_builder.key_builder());
  auto& key_key_builder = checked_cast<Int16Builder&>(*key_builder.key_builder());
  auto& key_item_builder = checked_cast<Int16Builder&>(*key_builder.item_builder());
  auto& item_builder = checked_cast<ListBuilder&>(*map_builder.item_builder());
  auto& item_value_builder = checked_cast<StringBuilder&>(*item_builder.value_builder());

  ASSERT_OK(map_builder.Append());
  ASSERT_OK(key_builder.Append());
  ASSERT_OK(item_builder.Append());
  ASSERT_OK(item_value_builder.AppendNull());
  ASSERT_OK(item_value_builder.Append("empty"));

  ASSERT_OK(key_builder.Append());
  ASSERT_OK(item_builder.AppendNull());
  ASSERT_OK(key_key_builder.AppendValues({0}));
  ASSERT_OK(key_item_builder.AppendValues({1}));

  ASSERT_OK(key_builder.Append());
  ASSERT_OK(item_builder.Append());
  ASSERT_OK(key_key_builder.AppendValues({0, 1}));
  ASSERT_OK(key_item_builder.AppendValues({0, 1}));
  ASSERT_OK(item_value_builder.Append("bootstrapping tautology?"));
  ASSERT_OK(item_value_builder.Append("lispy"));
  ASSERT_OK(item_value_builder.AppendNull());
  ASSERT_OK(item_value_builder.Append("i can see eternity"));

  ASSERT_OK(map_builder.AppendNull());

  ASSERT_OK(map_builder.Finish(&expected));
  ASSERT_ARRAYS_EQUAL(*actual, *expected);
}

TEST(TestFixedSizeList, IntegerList) {
  auto pool = default_memory_pool();
  auto type = fixed_size_list(int64(), 2);
  std::shared_ptr<Array> values, expected, actual;

  ASSERT_OK(ArrayFromJSON(type, "[]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<Int64Type>({}, &values);
  expected = std::make_shared<FixedSizeListArray>(type, 0, values);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[[4, 5], [0, 0], [6, 7]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<Int64Type>({4, 5, 0, 0, 6, 7}, &values);
  expected = std::make_shared<FixedSizeListArray>(type, 3, values);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[[null, null], [0, null], [6, null]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  auto is_valid = std::vector<bool>{false, false, true, false, true, false};
  ArrayFromVector<Int64Type>(is_valid, {0, 0, 0, 0, 6, 0}, &values);
  expected = std::make_shared<FixedSizeListArray>(type, 3, values);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[null, [null, null], null]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  {
    std::unique_ptr<ArrayBuilder> builder;
    ASSERT_OK(MakeBuilder(pool, type, &builder));
    auto& list_builder = checked_cast<FixedSizeListBuilder&>(*builder);
    auto value_builder = checked_cast<Int64Builder*>(list_builder.value_builder());
    ASSERT_OK(list_builder.AppendNull());
    ASSERT_OK(list_builder.Append());
    ASSERT_OK(value_builder->AppendNull());
    ASSERT_OK(value_builder->AppendNull());
    ASSERT_OK(list_builder.AppendNull());
    ASSERT_OK(list_builder.Finish(&expected));
  }
  AssertArraysEqual(*expected, *actual);
}

TEST(TestFixedSizeList, IntegerListErrors) {
  std::shared_ptr<DataType> type = fixed_size_list(int64(), 2);
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0.0, 1.0]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[9223372036854775808, 0]]", &array));
}

TEST(TestFixedSizeList, NullList) {
  auto pool = default_memory_pool();
  std::shared_ptr<DataType> type = fixed_size_list(null(), 2);
  std::shared_ptr<Array> values, expected, actual;

  ASSERT_OK(ArrayFromJSON(type, "[]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  values = std::make_shared<NullArray>(0);
  expected = std::make_shared<FixedSizeListArray>(type, 0, values);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[[null, null], [null, null], [null, null]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  values = std::make_shared<NullArray>(6);
  expected = std::make_shared<FixedSizeListArray>(type, 3, values);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[null, [null, null], null]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  {
    std::unique_ptr<ArrayBuilder> builder;
    ASSERT_OK(MakeBuilder(pool, type, &builder));
    auto& list_builder = checked_cast<FixedSizeListBuilder&>(*builder);
    auto value_builder = checked_cast<NullBuilder*>(list_builder.value_builder());
    ASSERT_OK(list_builder.AppendNull());
    ASSERT_OK(list_builder.Append());
    ASSERT_OK(value_builder->AppendNull());
    ASSERT_OK(value_builder->AppendNull());
    ASSERT_OK(list_builder.AppendNull());
    ASSERT_OK(list_builder.Finish(&expected));
  }
  AssertArraysEqual(*expected, *actual);
}

TEST(TestFixedSizeList, IntegerListList) {
  auto pool = default_memory_pool();
  auto nested_type = fixed_size_list(uint8(), 2);
  std::shared_ptr<DataType> type = fixed_size_list(nested_type, 1);
  std::shared_ptr<Array> values, nested, expected, actual;

  ASSERT_OK(ArrayFromJSON(type, "[[[1, 4]], [[2, 5]], [[3, 6]]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<UInt8Type>({1, 4, 2, 5, 3, 6}, &values);
  nested = std::make_shared<FixedSizeListArray>(nested_type, 3, values);
  expected = std::make_shared<FixedSizeListArray>(type, 3, nested);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[[[1, null]], [null], null]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  {
    std::unique_ptr<ArrayBuilder> builder;
    ASSERT_OK(MakeBuilder(pool, type, &builder));
    auto& list_builder = checked_cast<FixedSizeListBuilder&>(*builder);
    auto nested_builder =
        checked_cast<FixedSizeListBuilder*>(list_builder.value_builder());
    auto value_builder = checked_cast<UInt8Builder*>(nested_builder->value_builder());

    ASSERT_OK(list_builder.Append());
    ASSERT_OK(nested_builder->Append());
    ASSERT_OK(value_builder->Append(1));
    ASSERT_OK(value_builder->AppendNull());

    ASSERT_OK(list_builder.Append());
    ASSERT_OK(nested_builder->AppendNull());

    ASSERT_OK(list_builder.AppendNull());

    ASSERT_OK(list_builder.Finish(&expected));
  }
  AssertArraysEqual(*expected, *actual);
}

TEST(TestStruct, SimpleStruct) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());
  std::shared_ptr<DataType> type = struct_({field_a, field_b});
  std::shared_ptr<Array> a, b, expected, actual;
  std::shared_ptr<Buffer> null_bitmap;
  std::vector<bool> is_valid;
  std::vector<std::shared_ptr<Array>> children;

  // Trivial
  ASSERT_OK(ArrayFromJSON(type, "[]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<Int8Type>({}, &a);
  ArrayFromVector<BooleanType, bool>({}, &b);
  children.assign({a, b});
  expected = std::make_shared<StructArray>(type, 0, children);
  AssertArraysEqual(*expected, *actual);

  // Non-empty
  ArrayFromVector<Int8Type>({5, 6}, &a);
  ArrayFromVector<BooleanType, bool>({true, false}, &b);
  children.assign({a, b});
  expected = std::make_shared<StructArray>(type, 2, children);

  ASSERT_OK(ArrayFromJSON(type, "[[5, true], [6, false]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  AssertArraysEqual(*expected, *actual);
  ASSERT_OK(ArrayFromJSON(type, "[{\"a\": 5, \"b\": true}, {\"b\": false, \"a\": 6}]",
                          &actual));
  ASSERT_OK(ValidateArray(*actual));
  AssertArraysEqual(*expected, *actual);

  // With nulls
  is_valid = {false, true, false, false};
  ArrayFromVector<Int8Type>(is_valid, {0, 5, 6, 0}, &a);
  is_valid = {false, false, true, false};
  ArrayFromVector<BooleanType, bool>(is_valid, {false, true, false, false}, &b);
  children.assign({a, b});
  BitmapFromVector<bool>({false, true, true, true}, &null_bitmap);
  expected = std::make_shared<StructArray>(type, 4, children, null_bitmap, 1);

  ASSERT_OK(
      ArrayFromJSON(type, "[null, [5, null], [null, false], [null, null]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  AssertArraysEqual(*expected, *actual);
  // When using object notation, null members can be omitted
  ASSERT_OK(ArrayFromJSON(type, "[null, {\"a\": 5, \"b\": null}, {\"b\": false}, {}]",
                          &actual));
  ASSERT_OK(ValidateArray(*actual));
  AssertArraysEqual(*expected, *actual);
}

TEST(TestStruct, NestedStruct) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());
  auto field_c = field("c", float64());
  std::shared_ptr<DataType> nested_type = struct_({field_a, field_b});
  auto field_nested = field("nested", nested_type);
  std::shared_ptr<DataType> type = struct_({field_nested, field_c});
  std::shared_ptr<Array> expected, actual;
  std::shared_ptr<Buffer> null_bitmap;
  std::vector<bool> is_valid;
  std::vector<std::shared_ptr<Array>> children(2);

  ASSERT_OK(ArrayFromJSON(type, "[]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<Int8Type>({}, &children[0]);
  ArrayFromVector<BooleanType, bool>({}, &children[1]);
  children[0] = std::make_shared<StructArray>(nested_type, 0, children);
  ArrayFromVector<DoubleType>({}, &children[1]);
  expected = std::make_shared<StructArray>(type, 0, children);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[[[5, true], 1.5], [[6, false], -3e2]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  ArrayFromVector<Int8Type>({5, 6}, &children[0]);
  ArrayFromVector<BooleanType, bool>({true, false}, &children[1]);
  children[0] = std::make_shared<StructArray>(nested_type, 2, children);
  ArrayFromVector<DoubleType>({1.5, -300.0}, &children[1]);
  expected = std::make_shared<StructArray>(type, 2, children);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK(ArrayFromJSON(type, "[null, [[5, null], null], [null, -3e2]]", &actual));
  ASSERT_OK(ValidateArray(*actual));
  is_valid = {false, true, false};
  ArrayFromVector<Int8Type>(is_valid, {0, 5, 0}, &children[0]);
  is_valid = {false, false, false};
  ArrayFromVector<BooleanType, bool>(is_valid, {false, false, false}, &children[1]);
  BitmapFromVector<bool>({false, true, false}, &null_bitmap);
  children[0] = std::make_shared<StructArray>(nested_type, 3, children, null_bitmap, 2);
  is_valid = {false, false, true};
  ArrayFromVector<DoubleType>(is_valid, {0.0, 0.0, -300.0}, &children[1]);
  BitmapFromVector<bool>({false, true, true}, &null_bitmap);
  expected = std::make_shared<StructArray>(type, 3, children, null_bitmap, 1);
  AssertArraysEqual(*expected, *actual);
}

TEST(TestStruct, Errors) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());
  std::shared_ptr<DataType> type = struct_({field_a, field_b});
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0, true]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0, true, 1]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[true, 0]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[{\"b\": 0, \"a\": true}]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[{\"c\": 0}]", &array));
}

TEST(TestDenseUnion, Basics) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());

  auto type = union_({field_a, field_b}, {4, 8}, UnionMode::DENSE);
  auto array = ArrayFromJSON(type, "[[4, 122], [8, true], [4, null], null, [8, false]]");

  auto expected_types = ArrayFromJSON(int8(), "[4, 8, 4, null, 8]");
  auto expected_offsets = ArrayFromJSON(int32(), "[0, 0, 1, 0, 1]");
  auto expected_a = ArrayFromJSON(int8(), "[122, null]");
  auto expected_b = ArrayFromJSON(boolean(), "[true, false]");

  std::shared_ptr<Array> expected;
  ASSERT_OK(UnionArray::MakeDense(*expected_types, *expected_offsets,
                                  {expected_a, expected_b}, {"a", "b"}, {4, 8},
                                  &expected));

  ASSERT_ARRAYS_EQUAL(*expected, *array);
}

TEST(TestSparseUnion, Basics) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());

  auto type = union_({field_a, field_b}, {4, 8}, UnionMode::SPARSE);
  auto array = ArrayFromJSON(type, "[[4, 122], [8, true], [4, null], null, [8, false]]");

  auto expected_types = ArrayFromJSON(int8(), "[4, 8, 4, null, 8]");
  auto expected_a = ArrayFromJSON(int8(), "[122, null, null, null, null]");
  auto expected_b = ArrayFromJSON(boolean(), "[null, true, null, null, false]");

  std::shared_ptr<Array> expected;
  ASSERT_OK(UnionArray::MakeSparse(*expected_types, {expected_a, expected_b}, {"a", "b"},
                                   {4, 8}, &expected));

  ASSERT_ARRAYS_EQUAL(*expected, *array);
}

TEST(TestDenseUnion, ListOfUnion) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());
  auto union_type = union_({field_a, field_b}, {4, 8}, UnionMode::DENSE);
  auto list_type = list(union_type);
  auto array = ArrayFromJSON(list_type,
                             "["
                             "[[4, 122], [8, true]],"
                             "[[4, null], null, [8, false]]"
                             "]");

  auto expected_types = ArrayFromJSON(int8(), "[4, 8, 4, null, 8]");
  auto expected_offsets = ArrayFromJSON(int32(), "[0, 0, 1, 0, 1]");
  auto expected_a = ArrayFromJSON(int8(), "[122, null]");
  auto expected_b = ArrayFromJSON(boolean(), "[true, false]");

  std::shared_ptr<Array> expected_values, expected;
  ASSERT_OK(UnionArray::MakeDense(*expected_types, *expected_offsets,
                                  {expected_a, expected_b}, {"a", "b"}, {4, 8},
                                  &expected_values));
  auto expected_list_offsets = ArrayFromJSON(int32(), "[0, 2, 5]");
  ASSERT_OK(ListArray::FromArrays(*expected_list_offsets, *expected_values,
                                  default_memory_pool(), &expected));

  ASSERT_ARRAYS_EQUAL(*expected, *array);
}

TEST(TestSparseUnion, ListOfUnion) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());
  auto union_type = union_({field_a, field_b}, {4, 8}, UnionMode::SPARSE);
  auto list_type = list(union_type);
  auto array = ArrayFromJSON(list_type,
                             "["
                             "[[4, 122], [8, true]],"
                             "[[4, null], null, [8, false]]"
                             "]");

  auto expected_types = ArrayFromJSON(int8(), "[4, 8, 4, null, 8]");
  auto expected_a = ArrayFromJSON(int8(), "[122, null, null, null, null]");
  auto expected_b = ArrayFromJSON(boolean(), "[null, true, null, null, false]");

  std::shared_ptr<Array> expected_values, expected;
  ASSERT_OK(UnionArray::MakeSparse(*expected_types, {expected_a, expected_b}, {"a", "b"},
                                   {4, 8}, &expected_values));
  auto expected_list_offsets = ArrayFromJSON(int32(), "[0, 2, 5]");
  ASSERT_OK(ListArray::FromArrays(*expected_list_offsets, *expected_values,
                                  default_memory_pool(), &expected));

  ASSERT_ARRAYS_EQUAL(*expected, *array);
}

TEST(TestDenseUnion, UnionOfStructs) {
  std::vector<std::shared_ptr<Field>> fields = {
      field("ab", struct_({field("alpha", float64()), field("bravo", utf8())})),
      field("wtf", struct_({field("whiskey", int8()), field("tango", float64()),
                            field("foxtrot", list(int8()))})),
      field("q", struct_({field("quebec", utf8())}))};
  auto type = union_(fields, {0, 23, 47}, UnionMode::DENSE);
  auto array = ArrayFromJSON(type, R"([
    [0, {"alpha": 0.0, "bravo": "charlie"}],
    [23, {"whiskey": 99}],
    [0, {"bravo": "mike"}],
    null,
    [23, {"tango": 8.25, "foxtrot": [0, 2, 3]}]
  ])");

  auto expected_types = ArrayFromJSON(int8(), "[0, 23, 0, null, 23]");
  auto expected_offsets = ArrayFromJSON(int32(), "[0, 0, 1, 0, 1]");
  ArrayVector expected_fields = {ArrayFromJSON(fields[0]->type(), R"([
      {"alpha": 0.0, "bravo": "charlie"},
      {"bravo": "mike"}
    ])"),
                                 ArrayFromJSON(fields[1]->type(), R"([
      {"whiskey": 99},
      {"tango": 8.25, "foxtrot": [0, 2, 3]}
    ])"),
                                 ArrayFromJSON(fields[2]->type(), "[]")};

  std::shared_ptr<Array> expected;
  ASSERT_OK(UnionArray::MakeDense(*expected_types, *expected_offsets, expected_fields,
                                  {"ab", "wtf", "q"}, {0, 23, 47}, &expected));

  ASSERT_ARRAYS_EQUAL(*expected, *array);
}

TEST(TestSparseUnion, UnionOfStructs) {
  std::vector<std::shared_ptr<Field>> fields = {
      field("ab", struct_({field("alpha", float64()), field("bravo", utf8())})),
      field("wtf", struct_({field("whiskey", int8()), field("tango", float64()),
                            field("foxtrot", list(int8()))})),
      field("q", struct_({field("quebec", utf8())}))};
  auto type = union_(fields, {0, 23, 47}, UnionMode::SPARSE);
  auto array = ArrayFromJSON(type, R"([
    [0, {"alpha": 0.0, "bravo": "charlie"}],
    [23, {"whiskey": 99}],
    [0, {"bravo": "mike"}],
    null,
    [23, {"tango": 8.25, "foxtrot": [0, 2, 3]}]
  ])");

  auto expected_types = ArrayFromJSON(int8(), "[0, 23, 0, null, 23]");
  ArrayVector expected_fields = {
      ArrayFromJSON(fields[0]->type(), R"([
      {"alpha": 0.0, "bravo": "charlie"},
      null,
      {"bravo": "mike"},
      null,
      null
    ])"),
      ArrayFromJSON(fields[1]->type(), R"([
      null,
      {"whiskey": 99},
      null,
      null,
      {"tango": 8.25, "foxtrot": [0, 2, 3]}
    ])"),
      ArrayFromJSON(fields[2]->type(), "[null, null, null, null, null]")};

  std::shared_ptr<Array> expected;
  ASSERT_OK(UnionArray::MakeSparse(*expected_types, expected_fields, {"ab", "wtf", "q"},
                                   {0, 23, 47}, &expected));

  ASSERT_ARRAYS_EQUAL(*expected, *array);
}

TEST(TestDenseUnion, Errors) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());
  std::shared_ptr<DataType> type = union_({field_a, field_b}, {4, 8}, UnionMode::DENSE);
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"\"]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0, 8]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[4, \"\"]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[8, true, 1]]", &array));
}

TEST(TestSparseUnion, Errors) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());
  std::shared_ptr<DataType> type = union_({field_a, field_b}, {4, 8}, UnionMode::SPARSE);
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"\"]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0, 8]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[4, \"\"]]", &array));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[8, true, 1]]", &array));
}

}  // namespace json
}  // namespace internal
}  // namespace ipc
}  // namespace arrow
