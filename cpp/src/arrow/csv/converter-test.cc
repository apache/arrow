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
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/csv/converter.h"
#include "arrow/csv/options.h"
#include "arrow/csv/test-common.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/decimal.h"

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
                      ConvertOptions options = ConvertOptions::Defaults()) {
  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<Converter> converter;
  std::shared_ptr<Array> array, expected_array;

  ASSERT_OK(Converter::Make(type, options, &converter));

  MakeCSVParser(csv_string, &parser);
  for (int32_t col_index = 0; col_index < static_cast<int32_t>(expected.size());
       ++col_index) {
    ASSERT_OK(converter->Convert(*parser, col_index, &array));
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

  ASSERT_OK(Converter::Make(type, options, &converter));

  MakeCSVParser(csv_string, &parser);
  for (int32_t col_index = 0; col_index < static_cast<int32_t>(expected.size());
       ++col_index) {
    ASSERT_OK(converter->Convert(*parser, col_index, &array));
    ArrayFromVector<DATA_TYPE, C_TYPE>(type, is_valid[col_index], expected[col_index],
                                       &expected_array);
    AssertArraysEqual(*expected_array, *array);
  }
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
  std::shared_ptr<Array> array;

  ASSERT_OK(Converter::Make(type, options, &converter));

  MakeCSVParser(csv_string, &parser);
  for (int32_t i = 0; i < parser->num_cols(); ++i) {
    if (invalid_columns.find(i) == invalid_columns.end()) {
      ASSERT_OK(converter->Convert(*parser, i, &array));
    } else {
      ASSERT_RAISES(Invalid, converter->Convert(*parser, i, &array));
    }
  }
}

//////////////////////////////////////////////////////////////////////////
// Test functions begin here

TEST(BinaryConversion, Basics) {
  AssertConversion<BinaryType, std::string>(binary(), {"ab,cdé\n", ",\xffgh\n"},
                                            {{"ab", ""}, {"cdé", "\xffgh"}});
}

TEST(BinaryConversion, Nulls) {
  AssertConversion<BinaryType, std::string>(binary(), {"ab,N/A\n", "NULL,\n"},
                                            {{"ab", "NULL"}, {"N/A", ""}},
                                            {{true, true}, {true, true}});

  auto options = ConvertOptions::Defaults();
  options.strings_can_be_null = true;
  AssertConversion<BinaryType, std::string>(binary(), {"ab,N/A\n", "NULL,\n"},
                                            {{"ab", ""}, {"", ""}},
                                            {{true, false}, {false, false}}, options);
}

TEST(StringConversion, Basics) {
  AssertConversion<StringType, std::string>(utf8(), {"ab,cdé\n", ",gh\n"},
                                            {{"ab", ""}, {"cdé", "gh"}});

  auto options = ConvertOptions::Defaults();
  options.check_utf8 = false;
  AssertConversion<StringType, std::string>(utf8(), {"ab,cdé\n", ",\xffgh\n"},
                                            {{"ab", ""}, {"cdé", "\xffgh"}}, options);
}

TEST(StringConversion, Nulls) {
  AssertConversion<StringType, std::string>(utf8(), {"ab,N/A\n", "NULL,\n"},
                                            {{"ab", "NULL"}, {"N/A", ""}},
                                            {{true, true}, {true, true}});

  auto options = ConvertOptions::Defaults();
  options.strings_can_be_null = true;
  AssertConversion<StringType, std::string>(utf8(), {"ab,N/A\n", "NULL,\n"},
                                            {{"ab", ""}, {"", ""}},
                                            {{true, false}, {false, false}}, options);
}

TEST(StringConversion, Errors) {
  // Invalid UTF8 in column 0
  AssertConversionError(utf8(), {"ab,cdé\n", "\xff,gh\n"}, {0});
}

TEST(FixedSizeBinaryConversion, Basics) {
  AssertConversion<FixedSizeBinaryType, std::string>(
      fixed_size_binary(2), {"ab,cd\n", "gh,ij\n"}, {{"ab", "gh"}, {"cd", "ij"}});
}

TEST(FixedSizeBinaryConversion, Errors) {
  // Wrong-sized string in column 0
  AssertConversionError(fixed_size_binary(2), {"ab,cd\n", "g,ij\n"}, {0});
}

TEST(NullConversion, Basics) {
  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<Converter> converter;
  std::shared_ptr<Array> array;
  std::shared_ptr<DataType> type = null();

  ASSERT_OK(Converter::Make(type, ConvertOptions::Defaults(), &converter));

  MakeCSVParser({"NA,z\n", ",0\n"}, &parser);
  ASSERT_OK(converter->Convert(*parser, 0, &array));
  ASSERT_EQ(array->type()->id(), Type::NA);
  ASSERT_EQ(array->length(), 2);
  ASSERT_RAISES(Invalid, converter->Convert(*parser, 1, &array));
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
  AssertConversion<TimestampType, int64_t>(type, {"1970-01-01 00:01:00,,N/A\n"},
                                           {{60000}, {0}, {0}},
                                           {{true}, {false}, {false}});
}

TEST(TimestampConversion, CustomNulls) {
  auto options = ConvertOptions::Defaults();
  options.null_values = {"xxx", "zzz"};

  auto type = timestamp(TimeUnit::MILLI);
  AssertConversion<TimestampType, int64_t>(type, {"1970-01-01 00:01:00,xxx,zzz\n"},
                                           {{60000}, {0}, {0}},
                                           {{true}, {false}, {false}}, options);
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

}  // namespace csv
}  // namespace arrow
