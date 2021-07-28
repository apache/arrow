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

#include "arrow/csv/converter.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/csv/options.h"
#include "arrow/csv/test_common.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/value_parsing.h"

namespace arrow {
namespace csv {

class BlockParser;

// All recognized (non-empty) null values
std::vector<std::string> AllNulls() {
  return {"#N/A\n", "#N/A N/A\n", "#NA\n",     "-1.#IND\n", "-1.#QNAN\n", "-NaN\n",
          "-nan\n", "1.#IND\n",   "1.#QNAN\n", "N/A\n",     "NA\n",       "NULL\n",
          "NaN\n",  "n/a\n",      "nan\n",     "null\n"};
}

template <typename DATA_TYPE, typename C_TYPE>
void AssertConversion(const std::shared_ptr<DataType>& type,
                      const std::vector<std::string>& csv_string,
                      const std::vector<std::vector<C_TYPE>>& expected,
                      ConvertOptions options = ConvertOptions::Defaults(),
                      bool validate_full = true) {
  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<Converter> converter;
  std::shared_ptr<Array> array, expected_array;

  ASSERT_OK_AND_ASSIGN(converter, Converter::Make(type, options));

  MakeCSVParser(csv_string, &parser);
  for (int32_t col_index = 0; col_index < static_cast<int32_t>(expected.size());
       ++col_index) {
    ASSERT_OK_AND_ASSIGN(array, converter->Convert(*parser, col_index));
    if (validate_full) {
      ASSERT_OK(array->ValidateFull());
    } else {
      ASSERT_OK(array->Validate());
    }
    ArrayFromVector<DATA_TYPE, C_TYPE>(type, expected[col_index], &expected_array);
    AssertArraysEqual(*expected_array, *array);
  }
}

template <typename DATA_TYPE, typename C_TYPE>
void AssertConversion(const std::shared_ptr<DataType>& type,
                      const std::vector<std::string>& csv_string,
                      const std::vector<std::vector<C_TYPE>>& expected,
                      const std::vector<std::vector<bool>>& is_valid,
                      ConvertOptions options = ConvertOptions::Defaults()) {
  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<Converter> converter;
  std::shared_ptr<Array> array, expected_array;

  ASSERT_OK_AND_ASSIGN(converter, Converter::Make(type, options));

  MakeCSVParser(csv_string, &parser);
  for (int32_t col_index = 0; col_index < static_cast<int32_t>(expected.size());
       ++col_index) {
    ASSERT_OK_AND_ASSIGN(array, converter->Convert(*parser, col_index));
    ASSERT_OK(array->ValidateFull());
    ArrayFromVector<DATA_TYPE, C_TYPE>(type, is_valid[col_index], expected[col_index],
                                       &expected_array);
    AssertArraysEqual(*expected_array, *array);
  }
}

Result<std::shared_ptr<Array>> DictConversion(
    const std::shared_ptr<DataType>& value_type, const std::string& csv_string,
    int32_t max_cardinality = -1, ConvertOptions options = ConvertOptions::Defaults()) {
  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<DictionaryConverter> converter;

  ARROW_ASSIGN_OR_RAISE(converter, DictionaryConverter::Make(value_type, options));
  if (max_cardinality >= 0) {
    converter->SetMaxCardinality(max_cardinality);
  }

  ParseOptions parse_options;
  parse_options.ignore_empty_lines = false;
  MakeCSVParser({csv_string}, parse_options, &parser);

  const int32_t col_index = 0;
  return converter->Convert(*parser, col_index);
}

void AssertDictConversion(const std::string& csv_string,
                          const std::shared_ptr<Array>& expected_indices,
                          const std::shared_ptr<Array>& expected_dict,
                          int32_t max_cardinality = -1,
                          ConvertOptions options = ConvertOptions::Defaults(),
                          bool validate_full = true) {
  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<DictionaryConverter> converter;
  std::shared_ptr<Array> array, expected_array;
  std::shared_ptr<DataType> expected_type;

  ASSERT_OK_AND_ASSIGN(
      array, DictConversion(expected_dict->type(), csv_string, max_cardinality, options));
  if (validate_full) {
    ASSERT_OK(array->ValidateFull());
  } else {
    ASSERT_OK(array->Validate());
  }
  expected_type = dictionary(expected_indices->type(), expected_dict->type());
  ASSERT_TRUE(array->type()->Equals(*expected_type));
  const auto& dict_array = internal::checked_cast<const DictionaryArray&>(*array);
  AssertArraysEqual(*dict_array.dictionary(), *expected_dict);
  AssertArraysEqual(*dict_array.indices(), *expected_indices);
}

template <typename DATA_TYPE, typename C_TYPE>
void AssertConversionAllNulls(const std::shared_ptr<DataType>& type) {
  std::vector<std::string> nulls = AllNulls();
  std::vector<bool> is_valid(nulls.size(), false);
  std::vector<C_TYPE> values(nulls.size());
  AssertConversion<DATA_TYPE, C_TYPE>(type, nulls, {values}, {is_valid});
}

void AssertConversionError(const std::shared_ptr<DataType>& type,
                           const std::vector<std::string>& csv_string,
                           const std::set<int32_t>& invalid_columns,
                           ConvertOptions options = ConvertOptions::Defaults()) {
  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<Converter> converter;

  ASSERT_OK_AND_ASSIGN(converter, Converter::Make(type, options));

  MakeCSVParser(csv_string, &parser);
  for (int32_t i = 0; i < parser->num_cols(); ++i) {
    if (invalid_columns.find(i) == invalid_columns.end()) {
      ASSERT_OK(converter->Convert(*parser, i));
    } else {
      ASSERT_RAISES(Invalid, converter->Convert(*parser, i));
    }
  }
}

//////////////////////////////////////////////////////////////////////////
// Converter tests

template <typename T>
class BinaryConversionTestBase : public testing::Test {
 public:
  std::shared_ptr<DataType> type() { return TypeTraits<T>::type_singleton(); }

  void TestNulls() {
    auto type = this->type();
    AssertConversion<T, std::string>(type, {"ab,N/A\n", "NULL,\n"},
                                     {{"ab", "NULL"}, {"N/A", ""}},
                                     {{true, true}, {true, true}});

    auto options = ConvertOptions::Defaults();
    options.strings_can_be_null = true;
    AssertConversion<T, std::string>(type, {"ab,N/A\n", "NULL,\n"},
                                     {{"ab", ""}, {"", ""}},
                                     {{true, false}, {false, false}}, options);
    AssertConversion<T, std::string>(type, {"ab,\"N/A\"\n", "\"NULL\",\"\"\n"},
                                     {{"ab", ""}, {"", ""}},
                                     {{true, false}, {false, false}}, options);
    options.quoted_strings_can_be_null = false;
    AssertConversion<T, std::string>(type, {"ab,N/A\n", "NULL,\n"},
                                     {{"ab", ""}, {"", ""}},
                                     {{true, false}, {false, false}}, options);
    AssertConversion<T, std::string>(type, {"ab,\"N/A\"\n", "\"NULL\",\"\"\n"},
                                     {{"ab", "NULL"}, {"N/A", ""}},
                                     {{true, true}, {true, true}}, options);
  }

  void TestCustomNulls() {
    auto type = this->type();
    auto options = ConvertOptions::Defaults();
    options.null_values = {"xxx", "zzz"};
    AssertConversion<T, std::string>(type, {"ab,N/A\n", "xxx,\"zzz\"\n"},
                                     {{"ab", "xxx"}, {"N/A", "zzz"}},
                                     {{true, true}, {true, true}}, options);

    options.strings_can_be_null = true;
    AssertConversion<T, std::string>(type, {"ab,N/A\n", "xxx,\"zzz\"\n"},
                                     {{"ab", ""}, {"N/A", ""}},
                                     {{true, false}, {true, false}}, options);
    options.quoted_strings_can_be_null = false;
    AssertConversion<T, std::string>(type, {"ab,N/A\n", "xxx,\"zzz\"\n"},
                                     {{"ab", ""}, {"N/A", "zzz"}},
                                     {{true, false}, {true, true}}, options);
  }
};

template <typename T>
class BinaryConversionTest : public BinaryConversionTestBase<T> {
 public:
  void TestBasics() {
    auto type = this->type();
    AssertConversion<T, std::string>(type, {"ab,cdé\n", ",\xffgh\n"},
                                     {{"ab", ""}, {"cdé", "\xffgh"}});
  }
};

using BinaryTestTypes = ::testing::Types<BinaryType, LargeBinaryType>;

TYPED_TEST_SUITE(BinaryConversionTest, BinaryTestTypes);

TYPED_TEST(BinaryConversionTest, Basics) { this->TestBasics(); }

TYPED_TEST(BinaryConversionTest, Nulls) { this->TestNulls(); }

TYPED_TEST(BinaryConversionTest, CustomNulls) { this->TestNulls(); }

template <typename T>
class StringConversionTest : public BinaryConversionTestBase<T> {
 public:
  void TestBasics() {
    auto type = TypeTraits<T>::type_singleton();
    AssertConversion<T, std::string>(type, {"ab,cdé\n", ",gh\n"},
                                     {{"ab", ""}, {"cdé", "gh"}});
  }

  void TestInvalidUtf8() {
    auto type = TypeTraits<T>::type_singleton();
    // Invalid UTF8 in column 0
    AssertConversionError(type, {"ab,cdé\n", "\xff,gh\n"}, {0});

    auto options = ConvertOptions::Defaults();
    options.check_utf8 = false;
    AssertConversion<T, std::string>(type, {"ab,cdé\n", ",\xffgh\n"},
                                     {{"ab", ""}, {"cdé", "\xffgh"}}, options,
                                     /*validate_full=*/false);
  }
};

using StringTestTypes = ::testing::Types<StringType, LargeStringType>;

TYPED_TEST_SUITE(StringConversionTest, StringTestTypes);

TYPED_TEST(StringConversionTest, Basics) { this->TestBasics(); }

TYPED_TEST(StringConversionTest, Nulls) { this->TestNulls(); }

TYPED_TEST(StringConversionTest, CustomNulls) { this->TestCustomNulls(); }

TYPED_TEST(StringConversionTest, InvalidUtf8) { this->TestInvalidUtf8(); }

TEST(FixedSizeBinaryConversion, Basics) {
  AssertConversion<FixedSizeBinaryType, std::string>(
      fixed_size_binary(2), {"ab,cd\n", "gh,ij\n"}, {{"ab", "gh"}, {"cd", "ij"}});
}

TEST(FixedSizeBinaryConversion, Errors) {
  // Wrong-sized string in column 0
  AssertConversionError(fixed_size_binary(2), {"ab,cd\n", "g,ij\n"}, {0});
}

TEST(FixedSizeBinaryConversion, Nulls) {
  AssertConversion<FixedSizeBinaryType, std::string>(
      fixed_size_binary(2), {"ab,N/A\n", ",ij\n"}, {{"ab", "\0\0"}, {"\0\0", "ij"}},
      {{true, false}, {false, true}});

  AssertConversionAllNulls<FixedSizeBinaryType, std::string>(fixed_size_binary(2));
}

TEST(FixedSizeBinaryConversion, CustomNulls) {
  auto options = ConvertOptions::Defaults();
  options.null_values = {"xxx", "zzz"};

  AssertConversion<FixedSizeBinaryType, std::string>(
      fixed_size_binary(2), {"ab,xxx\n", "zzz,ij\n"}, {{"ab", "\0\0"}, {"\0\0", "ij"}},
      {{true, false}, {false, true}}, options);

  AssertConversionError(fixed_size_binary(2), {",xxx,N/A\n"}, {0, 2}, options);

  // Duplicate nulls allowed
  options.null_values = {"xxx", "zzz", "xxx"};
  AssertConversion<FixedSizeBinaryType, std::string>(
      fixed_size_binary(2), {"ab,xxx\n", "zzz,ij\n"}, {{"ab", "\0,\0"}, {"\0\0", "ij"}},
      {{true, false}, {false, true}}, options);
}

TEST(NullConversion, Basics) {
  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<Converter> converter;
  std::shared_ptr<Array> array;
  std::shared_ptr<DataType> type = null();

  auto options = ConvertOptions::Defaults();
  ASSERT_OK_AND_ASSIGN(converter, Converter::Make(type, options));

  MakeCSVParser({"NA,z\n", ",0\n"}, &parser);
  ASSERT_OK_AND_ASSIGN(array, converter->Convert(*parser, 0));
  ASSERT_EQ(array->type()->id(), Type::NA);
  ASSERT_EQ(array->length(), 2);
  ASSERT_RAISES(Invalid, converter->Convert(*parser, 1));
}

TEST(IntegerConversion, Basics) {
  AssertConversion<Int8Type, int8_t>(int8(), {"12,34\n", "0,-128\n"},
                                     {{12, 0}, {34, -128}});
  AssertConversion<Int64Type, int64_t>(
      int64(), {"12,34\n", "9223372036854775807,-9223372036854775808\n"},
      {{12, 9223372036854775807LL}, {34, -9223372036854775807LL - 1}});

  AssertConversion<UInt16Type, uint16_t>(uint16(), {"12,34\n", "0,65535\n"},
                                         {{12, 0}, {34, 65535}});
  AssertConversion<UInt64Type, uint64_t>(uint64(),
                                         {"12,34\n", "0,18446744073709551615\n"},
                                         {{12, 0}, {34, 18446744073709551615ULL}});
}

TEST(IntegerConversion, Nulls) {
  AssertConversion<Int8Type, int8_t>(int8(), {"12,N/A\n", ",-128\n"},
                                     {{12, 0}, {0, -128}},
                                     {{true, false}, {false, true}});

  AssertConversionAllNulls<Int8Type, int8_t>(int8());
}

TEST(IntegerConversion, CustomNulls) {
  auto options = ConvertOptions::Defaults();
  options.null_values = {"xxx", "zzz"};

  AssertConversion<Int8Type, int8_t>(int8(), {"12,xxx\n", "zzz,-128\n"},
                                     {{12, 0}, {0, -128}}, {{true, false}, {false, true}},
                                     options);

  AssertConversionError(int8(), {",xxx,N/A\n"}, {0, 2}, options);

  // Duplicate nulls allowed
  options.null_values = {"xxx", "zzz", "xxx"};
  AssertConversion<Int8Type, int8_t>(int8(), {"12,xxx\n", "zzz,-128\n"},
                                     {{12, 0}, {0, -128}}, {{true, false}, {false, true}},
                                     options);
}

TEST(IntegerConversion, Whitespace) {
  AssertConversion<Int32Type, int32_t>(int32(), {" 12,34 \n", " 56 ,78\n"},
                                       {{12, 56}, {34, 78}});
}

TEST(FloatingPointConversion, Basics) {
  AssertConversion<FloatType, float>(float32(), {"12,34.5\n", "0,-1e30\n"},
                                     {{12., 0.}, {34.5, -1e30f}});
  AssertConversion<DoubleType, double>(float64(), {"12,34.5\n", "0,-1e100\n"},
                                       {{12., 0.}, {34.5, -1e100}});
}

TEST(FloatingPointConversion, Nulls) {
  AssertConversion<FloatType, float>(float32(), {"1.5,0.\n", ",-1e10\n"},
                                     {{1.5, 0.}, {0., -1e10f}},
                                     {{true, false}, {true, true}});

  AssertConversionAllNulls<DoubleType, double>(float64());
}

TEST(FloatingPointConversion, CustomNulls) {
  auto options = ConvertOptions::Defaults();
  options.null_values = {"xxx", "zzz"};

  AssertConversion<FloatType, float>(float32(), {"1.5,xxx\n", "zzz,-1e10\n"},
                                     {{1.5, 0.}, {0., -1e10f}},
                                     {{true, false}, {false, true}}, options);
}

TEST(FloatingPointConversion, Whitespace) {
  AssertConversion<DoubleType, double>(float64(), {" 12,34.5\n", " 0 ,-1e100 \n"},
                                       {{12., 0.}, {34.5, -1e100}});
}

TEST(BooleanConversion, Basics) {
  // XXX we may want to accept more bool-like values
  AssertConversion<BooleanType, bool>(boolean(), {"true,false\n", "1,0\n"},
                                      {{true, true}, {false, false}});
}

TEST(BooleanConversion, Nulls) {
  AssertConversion<BooleanType, bool>(boolean(), {"true,\n", "1,0\n"},
                                      {{true, true}, {false, false}},
                                      {{true, true}, {false, true}});
}

TEST(BooleanConversion, CustomNulls) {
  auto options = ConvertOptions::Defaults();
  options.null_values = {"xxx", "zzz"};

  AssertConversion<BooleanType, bool>(boolean(), {"true,xxx\n", "zzz,0\n"},
                                      {{true, false}, {false, false}},
                                      {{true, false}, {false, true}}, options);
}

TEST(Date32Conversion, Basics) {
  AssertConversion<Date32Type, int32_t>(date32(), {"1945-05-08\n", "2020-03-15\n"},
                                        {{-9004, 18336}});
}

TEST(Date32Conversion, Nulls) {
  AssertConversion<Date32Type, int32_t>(date32(), {"N/A\n", "2020-03-15\n"}, {{0, 18336}},
                                        {{false, true}});
}

TEST(Date32Conversion, Errors) {
  AssertConversionError(date32(), {"1945-06-31\n"}, {0});
  AssertConversionError(date32(), {"2020-13-01\n"}, {0});
}

TEST(Date64Conversion, Basics) {
  AssertConversion<Date64Type, int64_t>(date64(), {"1945-05-08\n", "2020-03-15\n"},
                                        {{-777945600000LL, 1584230400000LL}});
}

TEST(Date64Conversion, Nulls) {
  AssertConversion<Date64Type, int64_t>(date64(), {"N/A\n", "2020-03-15\n"},
                                        {{0, 1584230400000LL}}, {{false, true}});
}

TEST(Date64Conversion, Errors) {
  AssertConversionError(date64(), {"1945-06-31\n"}, {0});
  AssertConversionError(date64(), {"2020-13-01\n"}, {0});
}

TEST(Time32Conversion, Seconds) {
  const auto type = time32(TimeUnit::SECOND);

  AssertConversion<Time32Type, int32_t>(type, {"00:00\n", "00:00:00\n"}, {{0, 0}});
  AssertConversion<Time32Type, int32_t>(type, {"01:23:45\n", "23:45:43\n"},
                                        {{5025, 85543}});
  AssertConversion<Time32Type, int32_t>(type, {"N/A\n", "23:59:59\n"}, {{0, 86399}},
                                        {{false, true}});

  AssertConversionError(type, {"24:00\n"}, {0});
  AssertConversionError(type, {"23:59:60\n"}, {0});
}

TEST(Time32Conversion, Millis) {
  const auto type = time32(TimeUnit::MILLI);

  AssertConversion<Time32Type, int32_t>(type, {"00:00\n", "00:00:00\n"}, {{0, 0}});
  AssertConversion<Time32Type, int32_t>(type, {"01:23:45.1\n", "23:45:43.789\n"},
                                        {{5025100, 85543789}});
  AssertConversion<Time32Type, int32_t>(type, {"N/A\n", "23:59:59.999\n"},
                                        {{0, 86399999}}, {{false, true}});

  AssertConversionError(type, {"24:00\n"}, {0});
  AssertConversionError(type, {"23:59:60\n"}, {0});
}

TEST(Time64Conversion, Micros) {
  const auto type = time64(TimeUnit::MICRO);

  AssertConversion<Time64Type, int64_t>(type, {"00:00\n", "00:00:00\n"}, {{0LL, 0LL}});
  AssertConversion<Time64Type, int64_t>(type, {"01:23:45.1\n", "23:45:43.456789\n"},
                                        {{5025100000LL, 85543456789LL}});
  AssertConversion<Time64Type, int64_t>(type, {"N/A\n", "23:59:59.999999\n"},
                                        {{0, 86399999999LL}}, {{false, true}});

  AssertConversionError(type, {"24:00\n"}, {0});
  AssertConversionError(type, {"23:59:60\n"}, {0});
}

TEST(Time64Conversion, Nanos) {
  const auto type = time64(TimeUnit::NANO);

  AssertConversion<Time64Type, int64_t>(type, {"00:00\n", "00:00:00\n"}, {{0LL, 0LL}});
  AssertConversion<Time64Type, int64_t>(type, {"01:23:45.1\n", "23:45:43.123456789\n"},
                                        {{5025100000000LL, 85543123456789LL}});
  AssertConversion<Time64Type, int64_t>(type, {"N/A\n", "23:59:59.999999999\n"},
                                        {{0, 86399999999999LL}}, {{false, true}});

  AssertConversionError(type, {"24:00\n"}, {0});
  AssertConversionError(type, {"23:59:60\n"}, {0});
}

TEST(TimestampConversion, Basics) {
  auto type = timestamp(TimeUnit::SECOND);

  AssertConversion<TimestampType, int64_t>(
      type, {"1970-01-01\n2000-02-29\n3989-07-14\n1900-02-28\n"},
      {{0, 951782400, 63730281600LL, -2203977600LL}});
  AssertConversion<TimestampType, int64_t>(type,
                                           {"2018-11-13 17:11:10\n1900-02-28 12:34:56\n"},
                                           {{1542129070, -2203932304LL}});

  type = timestamp(TimeUnit::NANO);
  AssertConversion<TimestampType, int64_t>(
      type, {"1970-01-01\n2000-02-29\n1900-02-28\n"},
      {{0, 951782400000000000LL, -2203977600000000000LL}});
}

TEST(TimestampConversion, Nulls) {
  auto type = timestamp(TimeUnit::MILLI);
  AssertConversion<TimestampType, int64_t>(
      type, {"1970-01-01 00:01:00,,N/A\n"}, {{60000}, {0}, {0}},
      {{true}, {false}, {false}}, ConvertOptions::Defaults());
}

TEST(TimestampConversion, CustomNulls) {
  auto options = ConvertOptions::Defaults();
  options.null_values = {"xxx", "zzz"};

  auto type = timestamp(TimeUnit::MILLI);
  AssertConversion<TimestampType, int64_t>(type, {"1970-01-01 00:01:00,xxx,zzz\n"},
                                           {{60000}, {0}, {0}},
                                           {{true}, {false}, {false}}, options);
}

TEST(TimestampConversion, UserDefinedParsers) {
  auto options = ConvertOptions::Defaults();
  auto type = timestamp(TimeUnit::MILLI);

  // Test a single parser
  options.timestamp_parsers = {TimestampParser::MakeStrptime("%m/%d/%Y")};
  AssertConversion<TimestampType, int64_t>(type, {"01/02/1970,01/03/1970\n"},
                                           {{86400000}, {172800000}}, options);

  // Test multiple parsers
  options.timestamp_parsers.push_back(TimestampParser::MakeISO8601());
  AssertConversion<TimestampType, int64_t>(type, {"01/02/1970,1970-01-03\n"},
                                           {{86400000}, {172800000}}, options);
}

Decimal128 Dec128(util::string_view value) {
  Decimal128 dec;
  int32_t scale = 0;
  int32_t precision = 0;
  DCHECK_OK(Decimal128::FromString(value, &dec, &precision, &scale));
  return dec;
}

TEST(DecimalConversion, Basics) {
  AssertConversion<Decimal128Type, Decimal128>(
      decimal(23, 2), {"12,34.5\n", "36.37,-1e5\n"},
      {{Dec128("12.00"), Dec128("36.37")}, {Dec128("34.50"), Dec128("-100000.00")}});
}

TEST(DecimalConversion, Nulls) {
  AssertConversion<Decimal128Type, Decimal128>(
      decimal(14, 3), {"1.5,0.\n", ",-1e3\n"},
      {{Dec128("1.500"), Decimal128()}, {Decimal128(), Dec128("-1000.000")}},
      {{true, false}, {true, true}});

  AssertConversionAllNulls<Decimal128Type, Decimal128>(decimal(14, 2));
}

TEST(DecimalConversion, CustomNulls) {
  auto options = ConvertOptions::Defaults();
  options.null_values = {"xxx", "zzz"};

  AssertConversion<Decimal128Type, Decimal128>(
      decimal(14, 3), {"1.5,xxx\n", "zzz,-1e3\n"},
      {{Dec128("1.500"), Decimal128()}, {Decimal128(), Dec128("-1000.000")}},
      {{true, false}, {false, true}}, options);
}

TEST(DecimalConversion, Whitespace) {
  AssertConversion<Decimal128Type, Decimal128>(
      decimal(5, 1), {" 12.00,34.5\n", " 0 ,-1e2 \n"},
      {{Dec128("12.0"), Decimal128()}, {Dec128("34.5"), Dec128("-100.0")}});
}

TEST(DecimalConversion, OverflowFails) {
  AssertConversionError(decimal(5, 0), {"1e6,0\n"}, {0});

  AssertConversionError(decimal(5, 1), {"123.22\n"}, {0});
  AssertConversionError(decimal(5, 1), {"12345.6\n"}, {0});
  AssertConversionError(decimal(5, 1), {"1.61\n"}, {0});
}

//////////////////////////////////////////////////////////////////////////
// DictionaryConverter tests

template <typename T>
class TestNumericDictConverter : public ::testing::Test {
 public:
  std::shared_ptr<DataType> type() const { return TypeTraits<T>::type_singleton(); }
};

using NumericDictConversionTypes =
    ::testing::Types<Int32Type, UInt32Type, Int64Type, UInt64Type, FloatType, DoubleType>;

TYPED_TEST_SUITE(TestNumericDictConverter, NumericDictConversionTypes);

TYPED_TEST(TestNumericDictConverter, Basics) {
  auto expected_dict = ArrayFromJSON(this->type(), "[4, 5]");
  auto expected_indices = ArrayFromJSON(int32(), "[0, 1, 0, 0]");

  AssertDictConversion("4\n5\n4\n4\n", expected_indices, expected_dict);
}

TYPED_TEST(TestNumericDictConverter, Nulls) {
  auto expected_dict = ArrayFromJSON(this->type(), "[4, 5]");
  auto expected_indices = ArrayFromJSON(int32(), "[0, 1, null, 0]");

  AssertDictConversion("4\n5\nN/A\n4\n", expected_indices, expected_dict);
}

TYPED_TEST(TestNumericDictConverter, Errors) {
  auto value_type = this->type();
  ASSERT_RAISES(Invalid, DictConversion(value_type, "xxx\n"));

  // Overflow
  if (is_integer(value_type->id())) {
    ASSERT_RAISES(Invalid, DictConversion(value_type, "99999999999999999999999\n"));
  }
  if (is_unsigned_integer(value_type->id())) {
    ASSERT_RAISES(Invalid, DictConversion(value_type, "-1\n"));
  }
}

template <typename T>
class TestStringDictConverter : public ::testing::Test {
 public:
  std::shared_ptr<DataType> type() const { return TypeTraits<T>::type_singleton(); }

  bool is_utf8_type() const {
    return T::type_id == Type::STRING || T::type_id == Type::LARGE_STRING;
  }
};

using StringDictConversionTypes =
    ::testing::Types<BinaryType, LargeBinaryType, StringType, LargeStringType>;

TYPED_TEST_SUITE(TestStringDictConverter, StringDictConversionTypes);

TYPED_TEST(TestStringDictConverter, Basics) {
  auto expected_dict = ArrayFromJSON(this->type(), R"(["ab", "cdé", ""])");
  auto expected_indices = ArrayFromJSON(int32(), "[0, 1, 2, 0]");

  AssertDictConversion("ab\ncdé\n\nab\n", expected_indices, expected_dict);
}

TYPED_TEST(TestStringDictConverter, Nulls) {
  auto expected_dict = ArrayFromJSON(this->type(), R"(["ab", "N/A", ""])");
  auto expected_indices = ArrayFromJSON(int32(), "[0, 1, 2, 0]");

  AssertDictConversion("ab\nN/A\n\nab\n", expected_indices, expected_dict);

  auto options = ConvertOptions::Defaults();
  options.strings_can_be_null = true;
  expected_dict = ArrayFromJSON(this->type(), R"(["ab"])");
  expected_indices = ArrayFromJSON(int32(), "[0, null, null, 0]");
  AssertDictConversion("ab\nN/A\n\nab\n", expected_indices, expected_dict, -1, options);
}

TYPED_TEST(TestStringDictConverter, NonUTF8) {
  auto expected_indices = ArrayFromJSON(int32(), "[0, 1, 2, 0]");
  std::shared_ptr<Array> expected_dict;
  ArrayFromVector<TypeParam, std::string>({"ab", "cd\xff", ""}, &expected_dict);
  std::string csv_string = "ab\ncd\xff\n\nab\n";

  if (this->is_utf8_type()) {
    ASSERT_RAISES(Invalid, DictConversion(this->type(), "ab\ncd\xff\n\nab\n"));

    auto options = ConvertOptions::Defaults();
    options.check_utf8 = false;
    AssertDictConversion(csv_string, expected_indices, expected_dict, -1, options,
                         /*validate_full=*/false);
  } else {
    AssertDictConversion(csv_string, expected_indices, expected_dict);
  }
}

TYPED_TEST(TestStringDictConverter, MaxCardinality) {
  auto expected_dict = ArrayFromJSON(this->type(), R"(["ab", "cd", "ef"])");
  auto expected_indices = ArrayFromJSON(int32(), "[0, 1, 2, 1]");
  std::string csv_string = "ab\ncd\nef\ncd\n";

  AssertDictConversion(csv_string, expected_indices, expected_dict, 3);
  ASSERT_RAISES(IndexError, DictConversion(this->type(), csv_string, 2));
}

TEST(TestFixedSizeBinaryDictConverter, Basics) {
  auto value_type = fixed_size_binary(3);

  auto expected_dict = ArrayFromJSON(value_type, R"(["abc", "def"])");
  auto expected_indices = ArrayFromJSON(int32(), "[0, 1, 0, 1]");

  AssertDictConversion("abc\ndef\nabc\ndef\n", expected_indices, expected_dict);
}

TEST(TestFixedSizeBinaryDictConverter, Errors) {
  auto value_type = fixed_size_binary(3);

  // Invalid string size
  ASSERT_RAISES(Invalid, DictConversion(value_type, "abc\nde\n"));
}

TEST(TestDecimalDictConverter, Basics) {
  auto value_type = decimal(9, 3);

  auto expected_dict = ArrayFromJSON(value_type, R"(["1.234", "456.789"])");
  auto expected_indices = ArrayFromJSON(int32(), "[0, 1, null, 1]");

  AssertDictConversion("1.234\n456.789\nN/A\n4.56789e2\n", expected_indices,
                       expected_dict);
}

TEST(TestDecimalDictConverter, Errors) {
  auto value_type = decimal(9, 3);

  // Overflow
  ASSERT_RAISES(Invalid, DictConversion(value_type, "1e10\n"));
}

}  // namespace csv
}  // namespace arrow
