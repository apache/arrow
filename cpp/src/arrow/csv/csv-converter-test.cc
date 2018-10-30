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
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/csv/converter.h"
#include "arrow/csv/options.h"
#include "arrow/csv/test-common.h"
#include "arrow/status.h"
#include "arrow/test-util.h"
#include "arrow/type.h"

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
                      const std::vector<std::vector<C_TYPE>>& expected) {
  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<Converter> converter;
  std::shared_ptr<Array> array, expected_array;

  ASSERT_OK(Converter::Make(type, ConvertOptions::Defaults(), &converter));

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
                      const std::vector<std::vector<bool>>& is_valid) {
  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<Converter> converter;
  std::shared_ptr<Array> array, expected_array;

  ASSERT_OK(Converter::Make(type, ConvertOptions::Defaults(), &converter));

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

//////////////////////////////////////////////////////////////////////////
// Test functions begin here

TEST(BinaryConversion, Basics) {
  AssertConversion<BinaryType, std::string>(binary(), {"ab,cde\n", ",gh\n"},
                                            {{"ab", ""}, {"cde", "gh"}});
}

TEST(StringConversion, Basics) {
  AssertConversion<StringType, std::string>(utf8(), {"ab,cde\n", ",gh\n"},
                                            {{"ab", ""}, {"cde", "gh"}});
}

TEST(FixedSizeBinaryConversion, Basics) {
  AssertConversion<FixedSizeBinaryType, std::string>(
      fixed_size_binary(2), {"ab,cd\n", "gh,ij\n"}, {{"ab", "gh"}, {"cd", "ij"}});
}

TEST(FixedSizeBinaryConversion, Errors) {
  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<Converter> converter;
  std::shared_ptr<Array> array;
  std::shared_ptr<DataType> type = fixed_size_binary(2);

  ASSERT_OK(Converter::Make(type, ConvertOptions::Defaults(), &converter));

  MakeCSVParser({"ab,cd\n", "g,ij\n"}, &parser);
  ASSERT_RAISES(Invalid, converter->Convert(*parser, 0, &array));
  ASSERT_OK(converter->Convert(*parser, 1, &array));
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
  AssertConversion<Int8Type, int8_t>(int8(), {"12,34\n", ",-128\n"},
                                     {{12, 0}, {34, -128}},
                                     {{true, false}, {true, true}});

  AssertConversionAllNulls<Int8Type, int8_t>(int8());
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

TEST(DecimalConversion, NotImplemented) {
  std::shared_ptr<Converter> converter;
  ASSERT_RAISES(NotImplemented,
                Converter::Make(decimal(12, 3), ConvertOptions::Defaults(), &converter));
}

}  // namespace csv
}  // namespace arrow
