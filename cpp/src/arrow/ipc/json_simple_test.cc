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
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/chunked_array.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/scalar.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_builders.h"
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

using ::arrow::internal::BytesToBits;
using ::arrow::internal::checked_cast;
using ::arrow::internal::checked_pointer_cast;

using ListAndListViewTypes =
    ::testing::Types<ListType, LargeListType, ListViewType, LargeListViewType>;

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
  std::shared_ptr<Array> expected;

  ASSERT_OK_AND_ASSIGN(auto actual, ArrayFromJSON(type, json));
  ASSERT_OK(actual->ValidateFull());
  ArrayFromVector<T, C_TYPE>(type, values, &expected);
  AssertArraysEqual(*expected, *actual);
}

template <typename T, typename C_TYPE = typename T::c_type>
void AssertJSONArray(const std::shared_ptr<DataType>& type, const std::string& json,
                     const std::vector<bool>& is_valid,
                     const std::vector<C_TYPE>& values) {
  std::shared_ptr<Array> expected;

  ASSERT_OK_AND_ASSIGN(auto actual, ArrayFromJSON(type, json));
  ASSERT_OK(actual->ValidateFull());
  ArrayFromVector<T, C_TYPE>(type, is_valid, values, &expected);
  AssertArraysEqual(*expected, *actual);
}

void AssertJSONDictArray(const std::shared_ptr<DataType>& index_type,
                         const std::shared_ptr<DataType>& value_type,
                         const std::string& json,
                         const std::string& expected_indices_json,
                         const std::string& expected_values_json) {
  auto type = dictionary(index_type, value_type);

  ASSERT_OK_AND_ASSIGN(auto expected_indices,
                       ArrayFromJSON(index_type, expected_indices_json));
  ASSERT_OK_AND_ASSIGN(auto expected_values,
                       ArrayFromJSON(value_type, expected_values_json));

  ASSERT_OK_AND_ASSIGN(auto actual, ArrayFromJSON(type, json));
  ASSERT_OK(actual->ValidateFull());

  const auto& dict_array = checked_cast<const DictionaryArray&>(*actual);
  AssertArraysEqual(*expected_indices, *dict_array.indices());
  AssertArraysEqual(*expected_values, *dict_array.dictionary());
}

template <typename T, typename C_TYPE = typename T::c_type>
void AssertJSONScalar(const std::shared_ptr<DataType>& type, const std::string& json,
                      const bool is_valid, const C_TYPE value) {
  SCOPED_TRACE(json);
  std::shared_ptr<Scalar> actual, expected;

  ASSERT_OK(ScalarFromJSON(type, json, &actual));
  if (is_valid) {
    ASSERT_OK_AND_ASSIGN(expected, MakeScalar(type, value));
  } else {
    expected = MakeNullScalar(type);
  }
  AssertScalarsEqual(*expected, *actual, /*verbose=*/true);
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
class TestIntegers : public ::testing::Test {
 public:
  std::shared_ptr<DataType> type() { return TypeTraits<T>::type_singleton(); }
};

TYPED_TEST_SUITE_P(TestIntegers);

TYPED_TEST_P(TestIntegers, Basics) {
  using T = TypeParam;
  using c_type = typename T::c_type;

  std::shared_ptr<Array> expected, actual;
  auto type = this->type();

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
  std::shared_ptr<Array> array;
  auto type = this->type();
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, ""));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "["));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "0"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "{}"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0.0]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"0\"]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0]]"));
}

TYPED_TEST_P(TestIntegers, OutOfBounds) {
  using T = TypeParam;
  using c_type = typename T::c_type;

  std::shared_ptr<Array> array;
  auto type = this->type();

  if (type->id() == Type::UINT64) {
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[18446744073709551616]"));
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[-1]"));
  } else if (type->id() == Type::INT64) {
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[9223372036854775808]"));
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[-9223372036854775809]"));
  } else if (std::is_signed<c_type>::value) {
    const auto lower = SafeSignedAdd<int64_t>(std::numeric_limits<c_type>::min(), -1);
    const auto upper = SafeSignedAdd<int64_t>(std::numeric_limits<c_type>::max(), +1);
    auto json_string = JSONArray(lower);
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, json_string));
    json_string = JSONArray(upper);
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, json_string));
  } else {
    const auto upper = static_cast<uint64_t>(std::numeric_limits<c_type>::max()) + 1;
    auto json_string = JSONArray(upper);
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, json_string));
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[-1]"));
  }
}

TYPED_TEST_P(TestIntegers, Dictionary) {
  std::shared_ptr<Array> array;
  std::shared_ptr<DataType> value_type = this->type();

  if (value_type->id() == Type::HALF_FLOAT) {
    // Unsupported, skip
    return;
  }

  AssertJSONDictArray(int8(), value_type, "[1, 2, 3, null, 3, 1]",
                      /*indices=*/"[0, 1, 2, null, 2, 0]",
                      /*values=*/"[1, 2, 3]");
}

REGISTER_TYPED_TEST_SUITE_P(TestIntegers, Basics, Errors, OutOfBounds, Dictionary);

INSTANTIATE_TYPED_TEST_SUITE_P(TestInt8, TestIntegers, Int8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt16, TestIntegers, Int16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt32, TestIntegers, Int32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt64, TestIntegers, Int64Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt8, TestIntegers, UInt8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt16, TestIntegers, UInt16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt32, TestIntegers, UInt32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt64, TestIntegers, UInt64Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestHalfFloat, TestIntegers, HalfFloatType);

template <typename T>
class TestStrings : public ::testing::Test {
 public:
  std::shared_ptr<DataType> type() const {
    if constexpr (is_binary_view_like_type<T>::value) {
      return T::is_utf8 ? utf8_view() : binary_view();
    } else {
      return TypeTraits<T>::type_singleton();
    }
  }
};

TYPED_TEST_SUITE_P(TestStrings);

TYPED_TEST_P(TestStrings, Basics) {
  using T = TypeParam;
  auto type = this->type();

  std::shared_ptr<Array> expected, actual;

  AssertJSONArray<T, std::string>(type, "[]", {});
  AssertJSONArray<T, std::string>(type, "[\"\", \"foo\"]", {"", "foo"});
  AssertJSONArray<T, std::string>(type, "[\"\", null]", {true, false}, {"", ""});
  // NUL character in string
  std::string s = "some";
  s += '\x00';
  s += "char";
  AssertJSONArray<T, std::string>(type, "[\"\", \"some\\u0000char\"]", {"", s});
  // UTF8 sequence in string
  AssertJSONArray<T, std::string>(type, "[\"\xc3\xa9\"]", {"\xc3\xa9"});

  if (!T::is_utf8) {
    // Arbitrary binary (non-UTF8) sequence in string
    s = "\xff\x9f";
    AssertJSONArray<T, std::string>(type, "[\"" + s + "\"]", {s});
  }

  // Bytes < 0x20 can be represented as JSON unicode escapes
  s = '\x00';
  s += "\x1f";
  AssertJSONArray<T, std::string>(type, "[\"\\u0000\\u001f\"]", {s});
}

TYPED_TEST_P(TestStrings, Errors) {
  auto type = this->type();
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[]]"));
}

TYPED_TEST_P(TestStrings, Dictionary) {
  auto value_type = this->type();

  AssertJSONDictArray(int16(), value_type, R"(["foo", "bar", null, "bar", "foo"])",
                      /*indices=*/"[0, 1, null, 1, 0]",
                      /*values=*/R"(["foo", "bar"])");
}

REGISTER_TYPED_TEST_SUITE_P(TestStrings, Basics, Errors, Dictionary);

INSTANTIATE_TYPED_TEST_SUITE_P(TestString, TestStrings, StringType);
INSTANTIATE_TYPED_TEST_SUITE_P(TestBinary, TestStrings, BinaryType);
INSTANTIATE_TYPED_TEST_SUITE_P(TestLargeString, TestStrings, LargeStringType);
INSTANTIATE_TYPED_TEST_SUITE_P(TestLargeBinary, TestStrings, LargeBinaryType);
INSTANTIATE_TYPED_TEST_SUITE_P(TestStringView, TestStrings, StringViewType);
INSTANTIATE_TYPED_TEST_SUITE_P(TestBinaryView, TestStrings, BinaryViewType);

TEST(TestNull, Basics) {
  std::shared_ptr<DataType> type = null();
  std::shared_ptr<Array> expected, actual;

  AssertJSONArray<NullType, std::nullptr_t>(type, "[]", {});
  AssertJSONArray<NullType, std::nullptr_t>(type, "[null, null]", {nullptr, nullptr});
}

TEST(TestNull, Errors) {
  std::shared_ptr<DataType> type = null();
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[]]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[NaN]"));
}

TEST(TestBoolean, Basics) {
  std::shared_ptr<DataType> type = boolean();

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

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0.0]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"true\"]"));
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
  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[NaN]"));
  ASSERT_OK(actual->ValidateFull());
  float value = checked_cast<FloatArray&>(*actual).Value(0);
  ASSERT_TRUE(std::isnan(value));
}

TEST(TestFloat, Errors) {
  std::shared_ptr<DataType> type = float32();
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[true]"));
}

TEST(TestDouble, Basics) {
  std::shared_ptr<DataType> type = float64();
  std::shared_ptr<Array> expected, actual;

  AssertJSONArray<DoubleType>(type, "[]", {});
  AssertJSONArray<DoubleType>(type, "[1, 2.5, -3e4]", {1.0, 2.5, -3.0e4});
  AssertJSONArray<DoubleType>(type, "[-0.0, Inf, -Inf, null]", {true, true, true, false},
                              {-0.0, INFINITY, -INFINITY, 0.0});

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[NaN]"));
  ASSERT_OK(actual->ValidateFull());
  double value = checked_cast<DoubleArray&>(*actual).Value(0);
  ASSERT_TRUE(std::isnan(value));
}

TEST(TestDouble, Errors) {
  std::shared_ptr<DataType> type = float64();
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[true]"));
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

TEST(TestDate, Basics) {
  auto type = date32();
  AssertJSONArray<Date32Type>(type, R"([5, null, 42])", {true, false, true}, {5, 0, 42});
  type = date64();
  AssertJSONArray<Date64Type>(type, R"([86400000, null, 172800000])", {true, false, true},
                              {86400000, 0, 172800000});
}

TEST(TestTime, Basics) {
  auto type = time32(TimeUnit::SECOND);
  AssertJSONArray<Time32Type>(type, R"([5, null, 42])", {true, false, true}, {5, 0, 42});
  type = time32(TimeUnit::MILLI);
  AssertJSONArray<Time32Type>(type, R"([5, null, 42])", {true, false, true}, {5, 0, 42});

  type = time64(TimeUnit::MICRO);
  AssertJSONArray<Time64Type>(type, R"([1, null, 9999999999])", {true, false, true},
                              {1, 0, 9999999999LL});
  type = time64(TimeUnit::NANO);
  AssertJSONArray<Time64Type>(type, R"([1, null, 9999999999999])", {true, false, true},
                              {1, 0, 9999999999999LL});
}

TEST(TestDuration, Basics) {
  auto type = duration(TimeUnit::SECOND);
  AssertJSONArray<DurationType>(type, R"([null, -7777777777777, 9999999999999])",
                                {false, true, true},
                                {0, -7777777777777LL, 9999999999999LL});
  type = duration(TimeUnit::MILLI);
  AssertJSONArray<DurationType>(type, R"([null, -7777777777777, 9999999999999])",
                                {false, true, true},
                                {0, -7777777777777LL, 9999999999999LL});
  type = duration(TimeUnit::MICRO);
  AssertJSONArray<DurationType>(type, R"([null, -7777777777777, 9999999999999])",
                                {false, true, true},
                                {0, -7777777777777LL, 9999999999999LL});
  type = duration(TimeUnit::NANO);
  AssertJSONArray<DurationType>(type, R"([null, -7777777777777, 9999999999999])",
                                {false, true, true},
                                {0, -7777777777777LL, 9999999999999LL});
}

TEST(TestMonthInterval, Basics) {
  auto type = month_interval();
  AssertJSONArray<MonthIntervalType>(type, R"([123, -456, null])", {true, true, false},
                                     {123, -456, 0});
}

TEST(TestDayTimeInterval, Basics) {
  auto type = day_time_interval();
  AssertJSONArray<DayTimeIntervalType>(type, R"([[1, -600], null])", {true, false},
                                       {{1, -600}, {}});
}

TEST(MonthDayNanoInterval, Basics) {
  auto type = month_day_nano_interval();
  AssertJSONArray<MonthDayNanoIntervalType>(type, R"([[1, -600, 5000], null])",
                                            {true, false}, {{1, -600, 5000}, {}});
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

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[]]"));
  // Invalid length
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"\"]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"abcd\"]"));
}

TEST(TestFixedSizeBinary, Dictionary) {
  std::shared_ptr<DataType> type = fixed_size_binary(3);

  AssertJSONDictArray(int8(), type, R"(["foo", "bar", "foo", null])",
                      /*indices=*/"[0, 1, 0, null]",
                      /*values=*/R"(["foo", "bar"])");

  // Invalid length
  std::shared_ptr<Array> array;
  ASSERT_RAISES(Invalid, ArrayFromJSON(dictionary(int8(), type), R"(["x"])"));
}

template <typename DecimalValue, typename DecimalBuilder>
void TestDecimalBasic(std::shared_ptr<DataType> type) {
  std::shared_ptr<Array> expected, actual;

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[]"));
  ASSERT_OK(actual->ValidateFull());
  {
    DecimalBuilder builder(type);
    ASSERT_OK(builder.Finish(&expected));
  }
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[\"123.4567\", \"-78.9000\"]"));
  ASSERT_OK(actual->ValidateFull());
  {
    DecimalBuilder builder(type);
    ASSERT_OK(builder.Append(DecimalValue(1234567)));
    ASSERT_OK(builder.Append(DecimalValue(-789000)));
    ASSERT_OK(builder.Finish(&expected));
  }
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[\"123.4567\", null]"));
  ASSERT_OK(actual->ValidateFull());
  {
    DecimalBuilder builder(type);
    ASSERT_OK(builder.Append(DecimalValue(1234567)));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK(builder.Finish(&expected));
  }
  AssertArraysEqual(*expected, *actual);
}

TEST(TestDecimal128, Basics) {
  TestDecimalBasic<Decimal128, Decimal128Builder>(decimal128(10, 4));
}

TEST(TestDecimal256, Basics) {
  TestDecimalBasic<Decimal256, Decimal256Builder>(decimal256(10, 4));
}

TEST(TestDecimal, Errors) {
  for (std::shared_ptr<DataType> type : {decimal128(10, 4), decimal256(10, 4)}) {
    std::shared_ptr<Array> array;

    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]"));
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[12.3456]"));
    // Bad scale
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"12.345\"]"));
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"12.34560\"]"));
  }
}

TEST(TestDecimal, Dictionary) {
  for (std::shared_ptr<DataType> type : {decimal128(10, 2), decimal256(10, 2)}) {
    AssertJSONDictArray(int32(), type,
                        R"(["123.45", "-78.90", "-78.90", null, "123.45"])",
                        /*indices=*/"[0, 1, 1, null, 0]",
                        /*values=*/R"(["123.45", "-78.90"])");
  }
}

template <typename T>
class TestVarLengthListArray : public ::testing::Test {
 public:
  using TypeClass = T;
  using offset_type = typename TypeClass::offset_type;
  using ArrayType = typename TypeTraits<TypeClass>::ArrayType;
  using BuilderType = typename TypeTraits<TypeClass>::BuilderType;
  using OffsetType = typename TypeTraits<TypeClass>::OffsetType;

  static constexpr bool is_list_view_type = is_list_view(TypeClass::type_id);

  void TestIntegerList() {
    auto pool = default_memory_pool();
    std::shared_ptr<DataType> type = std::make_shared<TypeClass>(int64());
    std::shared_ptr<Array> offsets, sizes, values, expected, actual;

    ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[]"));
    ASSERT_OK(actual->ValidateFull());
    ArrayFromVector<OffsetType>({0}, &offsets);
    ArrayFromVector<Int64Type>({}, &values);
    if constexpr (is_list_view_type) {
      ArrayFromVector<OffsetType>({}, &sizes);
      ASSERT_OK_AND_ASSIGN(expected,
                           ArrayType::FromArrays(*offsets, *sizes, *values, pool));
    } else {
      ASSERT_OK_AND_ASSIGN(expected, ArrayType::FromArrays(*offsets, *values, pool));
    }
    AssertArraysEqual(*expected, *actual);

    ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[[4, 5], [], [6]]"));
    ASSERT_OK(actual->ValidateFull());
    ArrayFromVector<OffsetType>({0, 2, 2, 3}, &offsets);
    ArrayFromVector<Int64Type>({4, 5, 6}, &values);
    if constexpr (is_list_view_type) {
      ArrayFromVector<OffsetType>({2, 0, 1}, &sizes);
      ASSERT_OK_AND_ASSIGN(expected,
                           ArrayType::FromArrays(*offsets, *sizes, *values, pool));
    } else {
      ASSERT_OK_AND_ASSIGN(expected, ArrayType::FromArrays(*offsets, *values, pool));
    }
    AssertArraysEqual(*expected, *actual);

    ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[[], [null], [6, null]]"));
    ASSERT_OK(actual->ValidateFull());
    ArrayFromVector<OffsetType>({0, 0, 1, 3}, &offsets);
    auto is_valid = std::vector<bool>{false, true, false};
    ArrayFromVector<Int64Type>(is_valid, {0, 6, 0}, &values);
    if constexpr (is_list_view_type) {
      ArrayFromVector<OffsetType>({0, 1, 2}, &sizes);
      ASSERT_OK_AND_ASSIGN(expected,
                           ArrayType::FromArrays(*offsets, *sizes, *values, pool));
    } else {
      ASSERT_OK_AND_ASSIGN(expected, ArrayType::FromArrays(*offsets, *values, pool));
    }
    AssertArraysEqual(*expected, *actual);

    ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[null, [], null]"));
    ASSERT_OK(actual->ValidateFull());
    {
      std::unique_ptr<ArrayBuilder> builder;
      ASSERT_OK(MakeBuilder(pool, type, &builder));
      auto& list_builder = checked_cast<BuilderType&>(*builder);
      ASSERT_OK(list_builder.AppendNull());
      ASSERT_OK(list_builder.Append(true, 0));
      ASSERT_OK(list_builder.AppendNull());
      ASSERT_OK(list_builder.Finish(&expected));
    }
    AssertArraysEqual(*expected, *actual);
  }

  void TestIntegerListErrors() {
    std::shared_ptr<DataType> type = std::make_shared<TypeClass>(int64());
    std::shared_ptr<Array> array;

    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]"));
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0.0]]"));
    ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[9223372036854775808]]"));
  }

  void TestNullList() {
    auto pool = default_memory_pool();
    std::shared_ptr<DataType> type = std::make_shared<TypeClass>(null());
    std::shared_ptr<Array> offsets, sizes, values, expected, actual;

    ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[]"));
    ASSERT_OK(actual->ValidateFull());
    ArrayFromVector<OffsetType>({0}, &offsets);
    values = std::make_shared<NullArray>(0);
    if constexpr (is_list_view_type) {
      ArrayFromVector<OffsetType>({}, &sizes);
      ASSERT_OK_AND_ASSIGN(expected,
                           ArrayType::FromArrays(*offsets, *sizes, *values, pool));
    } else {
      ASSERT_OK_AND_ASSIGN(expected, ArrayType::FromArrays(*offsets, *values, pool));
    }
    AssertArraysEqual(*expected, *actual);

    ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[[], [null], [null, null]]"));
    ASSERT_OK(actual->ValidateFull());
    ArrayFromVector<OffsetType>({0, 0, 1, 3}, &offsets);
    values = std::make_shared<NullArray>(3);
    if constexpr (is_list_view_type) {
      ArrayFromVector<OffsetType>({0, 1, 2}, &sizes);
      ASSERT_OK_AND_ASSIGN(expected,
                           ArrayType::FromArrays(*offsets, *sizes, *values, pool));
    } else {
      ASSERT_OK_AND_ASSIGN(expected, ArrayType::FromArrays(*offsets, *values, pool));
    }
    AssertArraysEqual(*expected, *actual);

    ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[null, [], null]"));
    ASSERT_OK(actual->ValidateFull());
    {
      std::unique_ptr<ArrayBuilder> builder;
      ASSERT_OK(MakeBuilder(pool, type, &builder));
      auto& list_builder = checked_cast<BuilderType&>(*builder);
      ASSERT_OK(list_builder.AppendNull());
      ASSERT_OK(list_builder.Append(true, 0));
      ASSERT_OK(list_builder.AppendNull());
      ASSERT_OK(list_builder.Finish(&expected));
    }
    AssertArraysEqual(*expected, *actual);
  }

  void TestIntegerListList() {
    auto pool = default_memory_pool();
    std::shared_ptr<DataType> type =
        std::make_shared<TypeClass>(std::make_shared<TypeClass>(uint8()));
    std::shared_ptr<Array> offsets, sizes, values, nested, expected, actual;

    ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[[[4], [5, 6]], [[7, 8, 9]]]"));
    ASSERT_OK(actual->ValidateFull());
    ArrayFromVector<OffsetType>({0, 1, 3, 6}, &offsets);
    ArrayFromVector<UInt8Type>({4, 5, 6, 7, 8, 9}, &values);
    if constexpr (is_list_view_type) {
      ArrayFromVector<OffsetType>({1, 2, 3}, &sizes);
      ASSERT_OK_AND_ASSIGN(nested,
                           ArrayType::FromArrays(*offsets, *sizes, *values, pool));
    } else {
      ASSERT_OK_AND_ASSIGN(nested, ArrayType::FromArrays(*offsets, *values, pool));
    }
    ArrayFromVector<OffsetType>({0, 2, 3}, &offsets);
    if constexpr (is_list_view_type) {
      ArrayFromVector<OffsetType>({2, 1}, &sizes);
      ASSERT_OK_AND_ASSIGN(expected,
                           ArrayType::FromArrays(*offsets, *sizes, *nested, pool));
    } else {
      ASSERT_OK_AND_ASSIGN(expected, ArrayType::FromArrays(*offsets, *nested, pool));
    }
    ASSERT_EQ(actual->length(), 2);
    AssertArraysEqual(*expected, *actual);

    ASSERT_OK_AND_ASSIGN(
        actual, ArrayFromJSON(type, "[[], [[]], [[4], [], [5, 6]], [[7, 8, 9]]]"));
    ASSERT_OK(actual->ValidateFull());
    ArrayFromVector<OffsetType>({0, 0, 1, 1, 3, 6}, &offsets);
    ArrayFromVector<UInt8Type>({4, 5, 6, 7, 8, 9}, &values);
    if constexpr (is_list_view_type) {
      ArrayFromVector<OffsetType>({0, 1, 0, 2, 3}, &sizes);
      ASSERT_OK_AND_ASSIGN(nested,
                           ArrayType::FromArrays(*offsets, *sizes, *values, pool));
    } else {
      ASSERT_OK_AND_ASSIGN(nested, ArrayType::FromArrays(*offsets, *values, pool));
    }
    ArrayFromVector<OffsetType>({0, 0, 1, 4, 5}, &offsets);
    if constexpr (is_list_view_type) {
      ArrayFromVector<OffsetType>({0, 1, 3, 1}, &sizes);
      ASSERT_OK_AND_ASSIGN(expected,
                           ArrayType::FromArrays(*offsets, *sizes, *nested, pool));
    } else {
      ASSERT_OK_AND_ASSIGN(expected, ArrayType::FromArrays(*offsets, *nested, pool));
    }
    ASSERT_EQ(actual->length(), 4);
    AssertArraysEqual(*expected, *actual);

    ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[null, [null], [[null]]]"));
    ASSERT_OK(actual->ValidateFull());
    {
      std::unique_ptr<ArrayBuilder> builder;
      ASSERT_OK(MakeBuilder(pool, type, &builder));
      auto& list_builder = checked_cast<BuilderType&>(*builder);
      auto& child_builder = checked_cast<BuilderType&>(*list_builder.value_builder());
      ASSERT_OK(list_builder.AppendNull());
      ASSERT_OK(list_builder.Append(true, 0));
      ASSERT_OK(child_builder.AppendNull());
      ASSERT_OK(list_builder.Append(true, 0));
      ASSERT_OK(child_builder.Append(true, 0));
      ASSERT_OK(list_builder.Finish(&expected));
    }
  }
};

TYPED_TEST_SUITE(TestVarLengthListArray, ListAndListViewTypes);

TYPED_TEST(TestVarLengthListArray, IntegerList) { this->TestIntegerList(); }

TYPED_TEST(TestVarLengthListArray, IntegerListErrors) { this->TestIntegerListErrors(); }

TYPED_TEST(TestVarLengthListArray, NullList) { this->TestNullList(); }

TYPED_TEST(TestVarLengthListArray, IntegerListList) { this->TestIntegerListList(); }

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
  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, input));

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
  ASSERT_OK_AND_ASSIGN(auto actual, ArrayFromJSON(type, input));
  std::vector<int32_t> offsets = {0, 2, 2, 3, 3};
  ASSERT_OK_AND_ASSIGN(auto expected_keys,
                       ArrayFromJSON(utf8(), R"(["joe", "mark", "cap"])"));
  ASSERT_OK_AND_ASSIGN(auto expected_values, ArrayFromJSON(int32(), "[0, null, 8]"));
  ASSERT_OK_AND_ASSIGN(auto expected_null_bitmap, BytesToBits({1, 0, 1, 1}));
  auto expected =
      std::make_shared<MapArray>(type, 4, Buffer::Wrap(offsets), expected_keys,
                                 expected_values, expected_null_bitmap, 1);
  ASSERT_ARRAYS_EQUAL(*actual, *expected);
}

TEST(TestMap, Errors) {
  auto type = map(int16(), int16());
  std::shared_ptr<Array> array;

  // list of pairs isn't an array
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]"));
  // pair isn't an array
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0]]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[null]]"));
  // pair with length != 2
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[[0]]]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[[0, 1, 2]]]"));
  // null key
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[[null, 0]]]"));
  // key or value fails to convert
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[[0.0, 0]]]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[[0, 0.0]]]"));
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
  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, input));

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

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[]"));
  ASSERT_OK(actual->ValidateFull());
  ArrayFromVector<Int64Type>({}, &values);
  expected = std::make_shared<FixedSizeListArray>(type, 0, values);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[[4, 5], [0, 0], [6, 7]]"));
  ASSERT_OK(actual->ValidateFull());
  ArrayFromVector<Int64Type>({4, 5, 0, 0, 6, 7}, &values);
  expected = std::make_shared<FixedSizeListArray>(type, 3, values);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(actual,
                       ArrayFromJSON(type, "[[null, null], [0, null], [6, null]]"));
  ASSERT_OK(actual->ValidateFull());
  auto is_valid = std::vector<bool>{false, false, true, false, true, false};
  ArrayFromVector<Int64Type>(is_valid, {0, 0, 0, 0, 6, 0}, &values);
  expected = std::make_shared<FixedSizeListArray>(type, 3, values);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[null, [null, null], null]"));
  ASSERT_OK(actual->ValidateFull());
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

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0.0, 1.0]]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0]]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[9223372036854775808, 0]]"));
}

TEST(TestFixedSizeList, NullList) {
  auto pool = default_memory_pool();
  std::shared_ptr<DataType> type = fixed_size_list(null(), 2);
  std::shared_ptr<Array> values, expected, actual;

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[]"));
  ASSERT_OK(actual->ValidateFull());
  values = std::make_shared<NullArray>(0);
  expected = std::make_shared<FixedSizeListArray>(type, 0, values);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(actual,
                       ArrayFromJSON(type, "[[null, null], [null, null], [null, null]]"));
  ASSERT_OK(actual->ValidateFull());
  values = std::make_shared<NullArray>(6);
  expected = std::make_shared<FixedSizeListArray>(type, 3, values);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[null, [null, null], null]"));
  ASSERT_OK(actual->ValidateFull());
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

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[[[1, 4]], [[2, 5]], [[3, 6]]]"));
  ASSERT_OK(actual->ValidateFull());
  ArrayFromVector<UInt8Type>({1, 4, 2, 5, 3, 6}, &values);
  nested = std::make_shared<FixedSizeListArray>(nested_type, 3, values);
  expected = std::make_shared<FixedSizeListArray>(type, 3, nested);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[[[1, null]], [null], null]"));
  ASSERT_OK(actual->ValidateFull());
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
  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[]"));
  ASSERT_OK(actual->ValidateFull());
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

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[[5, true], [6, false]]"));
  ASSERT_OK(actual->ValidateFull());
  AssertArraysEqual(*expected, *actual);
  ASSERT_OK_AND_ASSIGN(
      actual, ArrayFromJSON(type, "[{\"a\": 5, \"b\": true}, {\"b\": false, \"a\": 6}]"));
  ASSERT_OK(actual->ValidateFull());
  AssertArraysEqual(*expected, *actual);

  // With nulls
  is_valid = {false, true, false, false};
  ArrayFromVector<Int8Type>(is_valid, {0, 5, 6, 0}, &a);
  is_valid = {false, false, true, false};
  ArrayFromVector<BooleanType, bool>(is_valid, {false, true, false, false}, &b);
  children.assign({a, b});
  BitmapFromVector<bool>({false, true, true, true}, &null_bitmap);
  expected = std::make_shared<StructArray>(type, 4, children, null_bitmap, 1);

  ASSERT_OK_AND_ASSIGN(
      actual, ArrayFromJSON(type, "[null, [5, null], [null, false], [null, null]]"));
  ASSERT_OK(actual->ValidateFull());
  AssertArraysEqual(*expected, *actual);
  // When using object notation, null members can be omitted
  ASSERT_OK_AND_ASSIGN(
      actual, ArrayFromJSON(type, "[null, {\"a\": 5, \"b\": null}, {\"b\": false}, {}]"));
  ASSERT_OK(actual->ValidateFull());
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

  ASSERT_OK_AND_ASSIGN(actual, ArrayFromJSON(type, "[]"));
  ASSERT_OK(actual->ValidateFull());
  ArrayFromVector<Int8Type>({}, &children[0]);
  ArrayFromVector<BooleanType, bool>({}, &children[1]);
  children[0] = std::make_shared<StructArray>(nested_type, 0, children);
  ArrayFromVector<DoubleType>({}, &children[1]);
  expected = std::make_shared<StructArray>(type, 0, children);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(actual,
                       ArrayFromJSON(type, "[[[5, true], 1.5], [[6, false], -3e2]]"));
  ASSERT_OK(actual->ValidateFull());
  ArrayFromVector<Int8Type>({5, 6}, &children[0]);
  ArrayFromVector<BooleanType, bool>({true, false}, &children[1]);
  children[0] = std::make_shared<StructArray>(nested_type, 2, children);
  ArrayFromVector<DoubleType>({1.5, -300.0}, &children[1]);
  expected = std::make_shared<StructArray>(type, 2, children);
  AssertArraysEqual(*expected, *actual);

  ASSERT_OK_AND_ASSIGN(actual,
                       ArrayFromJSON(type, "[null, [[5, null], null], [null, -3e2]]"));
  ASSERT_OK(actual->ValidateFull());
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

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[0, true]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0]]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0, true, 1]]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[true, 0]]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[{\"b\": 0, \"a\": true}]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[{\"c\": 0}]"));
}

TEST(TestDenseUnion, Basics) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());

  auto type = dense_union({field_a, field_b}, {4, 8});
  ASSERT_OK_AND_ASSIGN(
      auto array_parsed,
      ArrayFromJSON(type, "[null, [4, 122], [8, true], [4, null], null, [8, false]]"));
  auto array = checked_pointer_cast<DenseUnionArray>(array_parsed);

  ASSERT_OK_AND_ASSIGN(auto expected_types, ArrayFromJSON(int8(), "[4, 4, 8, 4, 4, 8]"));
  ASSERT_OK_AND_ASSIGN(auto expected_offsets,
                       ArrayFromJSON(int32(), "[0, 1, 0, 2, 3, 1]"));
  ASSERT_OK_AND_ASSIGN(auto expected_a, ArrayFromJSON(int8(), "[null, 122, null, null]"));
  ASSERT_OK_AND_ASSIGN(auto expected_b, ArrayFromJSON(boolean(), "[true, false]"));

  ASSERT_OK_AND_ASSIGN(
      auto expected, DenseUnionArray::Make(*expected_types, *expected_offsets,
                                           {expected_a, expected_b}, {"a", "b"}, {4, 8}));

  ASSERT_ARRAYS_EQUAL(*expected, *array);

  // ensure that the array is as dense as we expect
  ASSERT_TRUE(array->value_offsets()->Equals(*expected_offsets->data()->buffers[1]));
  ASSERT_ARRAYS_EQUAL(*expected_a, *array->field(0));
  ASSERT_ARRAYS_EQUAL(*expected_b, *array->field(1));
}

TEST(TestSparseUnion, Basics) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());

  auto type = sparse_union({field_a, field_b}, {4, 8});
  ASSERT_OK_AND_ASSIGN(
      auto array,
      ArrayFromJSON(type, "[[4, 122], [8, true], [4, null], null, [8, false]]"));

  ASSERT_OK_AND_ASSIGN(auto expected_types, ArrayFromJSON(int8(), "[4, 8, 4, 4, 8]"));
  ASSERT_OK_AND_ASSIGN(auto expected_a,
                       ArrayFromJSON(int8(), "[122, null, null, null, null]"));
  ASSERT_OK_AND_ASSIGN(auto expected_b,
                       ArrayFromJSON(boolean(), "[null, true, null, null, false]"));

  ASSERT_OK_AND_ASSIGN(auto expected,
                       SparseUnionArray::Make(*expected_types, {expected_a, expected_b},
                                              {"a", "b"}, {4, 8}));

  ASSERT_ARRAYS_EQUAL(*expected, *array);
}

TEST(TestDenseUnion, ListOfUnion) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());
  auto union_type = dense_union({field_a, field_b}, {4, 8});
  auto list_type = list(union_type);
  ASSERT_OK_AND_ASSIGN(auto parsed_array, ArrayFromJSON(list_type,
                                                        "["
                                                        "[[4, 122], [8, true]],"
                                                        "[[4, null], null, [8, false]]"
                                                        "]"));
  auto array = checked_pointer_cast<ListArray>(parsed_array);

  ASSERT_OK_AND_ASSIGN(auto expected_types, ArrayFromJSON(int8(), "[4, 8, 4, 4, 8]"));
  ASSERT_OK_AND_ASSIGN(auto expected_offsets, ArrayFromJSON(int32(), "[0, 0, 1, 2, 1]"));
  ASSERT_OK_AND_ASSIGN(auto expected_a, ArrayFromJSON(int8(), "[122, null, null]"));
  ASSERT_OK_AND_ASSIGN(auto expected_b, ArrayFromJSON(boolean(), "[true, false]"));

  ASSERT_OK_AND_ASSIGN(
      auto expected_values,
      DenseUnionArray::Make(*expected_types, *expected_offsets, {expected_a, expected_b},
                            {"a", "b"}, {4, 8}));
  ASSERT_OK_AND_ASSIGN(auto expected_list_offsets, ArrayFromJSON(int32(), "[0, 2, 5]"));
  ASSERT_OK_AND_ASSIGN(auto expected,
                       ListArray::FromArrays(*expected_list_offsets, *expected_values));

  ASSERT_ARRAYS_EQUAL(*expected, *array);

  // ensure that the array is as dense as we expect
  auto array_values = checked_pointer_cast<DenseUnionArray>(array->values());
  ASSERT_TRUE(array_values->value_offsets()->Equals(
      *checked_pointer_cast<DenseUnionArray>(expected_values)->value_offsets()));
  ASSERT_ARRAYS_EQUAL(*expected_a, *array_values->field(0));
  ASSERT_ARRAYS_EQUAL(*expected_b, *array_values->field(1));
}

TEST(TestSparseUnion, ListOfUnion) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());
  auto union_type = sparse_union({field_a, field_b}, {4, 8});
  auto list_type = list(union_type);
  ASSERT_OK_AND_ASSIGN(auto array, ArrayFromJSON(list_type,
                                                 "["
                                                 "[[4, 122], [8, true]],"
                                                 "[[4, null], null, [8, false]]"
                                                 "]"));

  ASSERT_OK_AND_ASSIGN(auto expected_types, ArrayFromJSON(int8(), "[4, 8, 4, 4, 8]"));
  ASSERT_OK_AND_ASSIGN(auto expected_a,
                       ArrayFromJSON(int8(), "[122, null, null, null, null]"));
  ASSERT_OK_AND_ASSIGN(auto expected_b,
                       ArrayFromJSON(boolean(), "[null, true, null, null, false]"));

  ASSERT_OK_AND_ASSIGN(auto expected_values,
                       SparseUnionArray::Make(*expected_types, {expected_a, expected_b},
                                              {"a", "b"}, {4, 8}));
  ASSERT_OK_AND_ASSIGN(auto expected_list_offsets, ArrayFromJSON(int32(), "[0, 2, 5]"));
  ASSERT_OK_AND_ASSIGN(auto expected,
                       ListArray::FromArrays(*expected_list_offsets, *expected_values));

  ASSERT_ARRAYS_EQUAL(*expected, *array);
}

TEST(TestDenseUnion, UnionOfStructs) {
  std::vector<std::shared_ptr<Field>> fields = {
      field("ab", struct_({field("alpha", float64()), field("bravo", utf8())})),
      field("wtf", struct_({field("whiskey", int8()), field("tango", float64()),
                            field("foxtrot", list(int8()))})),
      field("q", struct_({field("quebec", utf8())}))};
  auto type = dense_union(fields, {0, 23, 47});
  ASSERT_OK_AND_ASSIGN(auto array_parsed,
                       ArrayFromJSON(type, R"([[0, {"alpha": 0.0, "bravo": "charlie"}],
                                      [23, {"whiskey": 99}],
                                      [0, {"bravo": "mike"}],
                                      null,
                                      [23, {"tango": 8.25, "foxtrot": [0, 2, 3]}]
                                      ])"));
  auto array = checked_pointer_cast<DenseUnionArray>(array_parsed);

  ASSERT_OK_AND_ASSIGN(auto expected_types, ArrayFromJSON(int8(), "[0, 23, 0, 0, 23]"));
  ASSERT_OK_AND_ASSIGN(auto expected_offsets, ArrayFromJSON(int32(), "[0, 0, 1, 2, 1]"));
  ASSERT_OK_AND_ASSIGN(auto expected_fields_0, ArrayFromJSON(fields[0]->type(), R"([
    {"alpha": 0.0, "bravo": "charlie"},
    {"bravo": "mike"},
    null
    ])"));
  ASSERT_OK_AND_ASSIGN(auto expected_fields_1, ArrayFromJSON(fields[1]->type(), R"([
    {"whiskey": 99},
    {"tango": 8.25, "foxtrot": [0, 2, 3]}
    ])"));
  ASSERT_OK_AND_ASSIGN(auto expected_fields_2, ArrayFromJSON(fields[2]->type(), "[]"));
  ArrayVector expected_fields = {expected_fields_0, expected_fields_1, expected_fields_2};

  ASSERT_OK_AND_ASSIGN(
      auto expected,
      DenseUnionArray::Make(*expected_types, *expected_offsets, expected_fields,
                            {"ab", "wtf", "q"}, {0, 23, 47}));

  ASSERT_ARRAYS_EQUAL(*expected, *array);

  // ensure that the array is as dense as we expect
  ASSERT_TRUE(array->value_offsets()->Equals(*expected_offsets->data()->buffers[1]));
  for (int i = 0; i < type->num_fields(); ++i) {
    ASSERT_ARRAYS_EQUAL(*checked_cast<const UnionArray&>(*expected).field(i),
                        *array->field(i));
  }
}

TEST(TestSparseUnion, UnionOfStructs) {
  std::vector<std::shared_ptr<Field>> fields = {
      field("ab", struct_({field("alpha", float64()), field("bravo", utf8())})),
      field("wtf", struct_({field("whiskey", int8()), field("tango", float64()),
                            field("foxtrot", list(int8()))})),
      field("q", struct_({field("quebec", utf8())}))};
  auto type = sparse_union(fields, {0, 23, 47});
  ASSERT_OK_AND_ASSIGN(auto array, ArrayFromJSON(type, R"([
    [0, {"alpha": 0.0, "bravo": "charlie"}],
    [23, {"whiskey": 99}],
    [0, {"bravo": "mike"}],
    null,
    [23, {"tango": 8.25, "foxtrot": [0, 2, 3]}]
  ])"));

  ASSERT_OK_AND_ASSIGN(auto expected_types, ArrayFromJSON(int8(), "[0, 23, 0, 0, 23]"));
  ASSERT_OK_AND_ASSIGN(auto expected_fields_0, ArrayFromJSON(fields[0]->type(), R"([
    {"alpha": 0.0, "bravo": "charlie"},
    null,
    {"bravo": "mike"},
    null,
    null
    ])"));
  ASSERT_OK_AND_ASSIGN(auto expected_fields_1, ArrayFromJSON(fields[1]->type(), R"([
    null,
    {"whiskey": 99},
    null,
    null,
    {"tango": 8.25, "foxtrot": [0, 2, 3]}
    ])"));
  ASSERT_OK_AND_ASSIGN(auto expected_fields_2,
                       ArrayFromJSON(fields[2]->type(), "[null, null, null, null, null]"))
  ArrayVector expected_fields = {expected_fields_0, expected_fields_1, expected_fields_2};

  ASSERT_OK_AND_ASSIGN(auto expected,
                       SparseUnionArray::Make(*expected_types, expected_fields,
                                              {"ab", "wtf", "q"}, {0, 23, 47}));

  ASSERT_ARRAYS_EQUAL(*expected, *array);
}

TEST(TestDenseUnion, Errors) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());
  std::shared_ptr<DataType> type = dense_union({field_a, field_b}, {4, 8});
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"not a valid type_id\"]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0, 99]]"));    // 0 is not one of {4, 8}
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[4, \"\"]]"));  // "" is not a valid int8()

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"not a pair\"]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0]]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[8, true, 1]]"));
}

TEST(TestSparseUnion, Errors) {
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean());
  std::shared_ptr<DataType> type = sparse_union({field_a, field_b}, {4, 8});
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"not a valid type_id\"]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0, 99]]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[4, \"\"]]"));

  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[\"not a pair\"]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[0]]"));
  ASSERT_RAISES(Invalid, ArrayFromJSON(type, "[[8, true, 1]]"));
}

TEST(TestNestedDictionary, ListOfDict) {
  auto index_type = int8();
  auto value_type = utf8();
  auto dict_type = dictionary(index_type, value_type);
  auto type = list(dict_type);

  std::shared_ptr<Array> array, expected, indices, values, dicts, offsets;

  ASSERT_OK_AND_ASSIGN(
      array, ArrayFromJSON(type, R"([["ab", "cd", null], null, ["cd", "cd"]])"));
  ASSERT_OK(array->ValidateFull());

  // Build expected array
  ASSERT_OK_AND_ASSIGN(indices, ArrayFromJSON(index_type, "[0, 1, null, 1, 1]"));
  ASSERT_OK_AND_ASSIGN(values, ArrayFromJSON(value_type, R"(["ab", "cd"])"));
  ASSERT_OK_AND_ASSIGN(dicts, DictionaryArray::FromArrays(dict_type, indices, values));
  ASSERT_OK_AND_ASSIGN(offsets, ArrayFromJSON(int32(), "[0, null, 3, 5]"));
  ASSERT_OK_AND_ASSIGN(expected, ListArray::FromArrays(*offsets, *dicts));

  AssertArraysEqual(*expected, *array, /*verbose=*/true);
}

TEST(TestDictArrayFromJSON, Basics) {
  auto type = dictionary(int32(), utf8());
  auto array =
      DictArrayFromJSON(type, "[null, 2, 1, 0]", R"(["whiskey", "tango", "foxtrot"])");

  ASSERT_OK_AND_ASSIGN(auto expected_indices, ArrayFromJSON(int32(), "[null, 2, 1, 0]"));
  ASSERT_OK_AND_ASSIGN(auto expected_dictionary,
                       ArrayFromJSON(utf8(), R"(["whiskey", "tango", "foxtrot"])"));

  ASSERT_ARRAYS_EQUAL(DictionaryArray(type, expected_indices, expected_dictionary),
                      *array);
}

TEST(TestDictArrayFromJSON, Errors) {
  auto type = dictionary(int32(), utf8());
  std::shared_ptr<Array> array;

  ASSERT_RAISES(Invalid,
                DictArrayFromJSON(type, "[\"not a valid index\"]", "[\"\"]", &array));
  ASSERT_RAISES(Invalid, DictArrayFromJSON(type, "[0, 1]", "[1]",
                                           &array));  // dict value isn't string
}

TEST(TestChunkedArrayFromJSON, Basics) {
  auto type = int32();
  std::shared_ptr<ChunkedArray> chunked_array;
  ASSERT_OK(ChunkedArrayFromJSON(type, {}, &chunked_array));
  ASSERT_OK(chunked_array->ValidateFull());
  ASSERT_EQ(chunked_array->num_chunks(), 0);
  AssertTypeEqual(type, chunked_array->type());

  ASSERT_OK(ChunkedArrayFromJSON(type, {"[1, 2]", "[3, null, 4]"}, &chunked_array));
  ASSERT_OK(chunked_array->ValidateFull());
  ASSERT_EQ(chunked_array->num_chunks(), 2);
  std::shared_ptr<Array> expected_chunk;
  ASSERT_OK_AND_ASSIGN(expected_chunk, ArrayFromJSON(type, "[1, 2]"));
  AssertArraysEqual(*expected_chunk, *chunked_array->chunk(0), /*verbose=*/true);
  ASSERT_OK_AND_ASSIGN(expected_chunk, ArrayFromJSON(type, "[3, null, 4]"));
  AssertArraysEqual(*expected_chunk, *chunked_array->chunk(1), /*verbose=*/true);
}

TEST(TestScalarFromJSON, Basics) {
  // Sanity check for common types (not exhaustive)
  std::shared_ptr<Scalar> scalar;
  AssertJSONScalar<Int64Type>(int64(), "4", true, 4);
  AssertJSONScalar<Int64Type>(int64(), "null", false, 0);
  AssertJSONScalar<StringType, std::shared_ptr<Buffer>>(utf8(), R"("")", true,
                                                        Buffer::FromString(""));
  AssertJSONScalar<StringType, std::shared_ptr<Buffer>>(utf8(), R"("foo")", true,
                                                        Buffer::FromString("foo"));
  AssertJSONScalar<StringType, std::shared_ptr<Buffer>>(utf8(), R"(null)", false,
                                                        Buffer::FromString(""));
  AssertJSONScalar<NullType, std::nullptr_t>(null(), "null", false, nullptr);
  AssertJSONScalar<BooleanType, bool>(boolean(), "true", true, true);
  AssertJSONScalar<BooleanType, bool>(boolean(), "false", true, false);
  AssertJSONScalar<BooleanType, bool>(boolean(), "null", false, false);
  AssertJSONScalar<BooleanType, bool>(boolean(), "0", true, false);
  AssertJSONScalar<BooleanType, bool>(boolean(), "1", true, true);
  AssertJSONScalar<DoubleType>(float64(), "1.0", true, 1.0);
  AssertJSONScalar<DoubleType>(float64(), "-0.0", true, -0.0);
  ASSERT_OK(ScalarFromJSON(float64(), "NaN", &scalar));
  ASSERT_TRUE(std::isnan(checked_cast<DoubleScalar&>(*scalar).value));
  ASSERT_OK(ScalarFromJSON(float64(), "Inf", &scalar));
  ASSERT_TRUE(std::isinf(checked_cast<DoubleScalar&>(*scalar).value));
}

TEST(TestScalarFromJSON, Errors) {
  std::shared_ptr<Scalar> scalar;
  ASSERT_RAISES(Invalid, ScalarFromJSON(int64(), "[0]", &scalar));
  ASSERT_RAISES(Invalid, ScalarFromJSON(int64(), "[9223372036854775808]", &scalar));
  ASSERT_RAISES(Invalid, ScalarFromJSON(int64(), "[-9223372036854775809]", &scalar));
  ASSERT_RAISES(Invalid, ScalarFromJSON(uint64(), "[18446744073709551616]", &scalar));
  ASSERT_RAISES(Invalid, ScalarFromJSON(uint64(), "[-1]", &scalar));
  ASSERT_RAISES(Invalid, ScalarFromJSON(binary(), "0", &scalar));
  ASSERT_RAISES(Invalid, ScalarFromJSON(binary(), "[]", &scalar));
  ASSERT_RAISES(Invalid, ScalarFromJSON(boolean(), "0.0", &scalar));
  ASSERT_RAISES(Invalid, ScalarFromJSON(boolean(), "\"true\"", &scalar));
}

TEST(TestDictScalarFromJSON, Basics) {
  auto type = dictionary(int32(), utf8());
  auto dict = R"(["whiskey", "tango", "foxtrot"])";
  ASSERT_OK_AND_ASSIGN(auto expected_dictionary, ArrayFromJSON(utf8(), dict));

  for (auto index : {"null", "2", "1", "0"}) {
    auto scalar = DictScalarFromJSON(type, index, dict);
    auto expected_index = ScalarFromJSON(int32(), index);
    AssertScalarsEqual(*DictionaryScalar::Make(expected_index, expected_dictionary),
                       *scalar, /*verbose=*/true);
    ASSERT_OK(scalar->ValidateFull());
  }
}

TEST(TestDictScalarFromJSON, Errors) {
  auto type = dictionary(int32(), utf8());
  std::shared_ptr<Scalar> scalar;

  ASSERT_RAISES(Invalid,
                DictScalarFromJSON(type, "\"not a valid index\"", "[\"\"]", &scalar));
  ASSERT_RAISES(Invalid, DictScalarFromJSON(type, "0", "[1]",
                                            &scalar));  // dict value isn't string
}

}  // namespace json
}  // namespace internal
}  // namespace ipc
}  // namespace arrow
