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

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/status.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/decimal.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"

namespace arrow {

namespace compute {

class TestDictionaryCompactionKernel : public ::testing::Test {};

void CheckDictionaryCompaction(const std::shared_ptr<DataType>& dict_type,
                               const std::string& input_dictionary_json,
                               const std::string& input_index_json,
                               const std::string& expected_dictionary_json,
                               const std::string& expected_index_json) {
  auto input = DictArrayFromJSON(dict_type, input_index_json, input_dictionary_json);
  auto expected =
      DictArrayFromJSON(dict_type, expected_index_json, expected_dictionary_json);

  ASSERT_OK_AND_ASSIGN(Datum actual_datum, DictionaryCompaction(input));
  ValidateOutput(actual_datum);
  std::shared_ptr<Array> actual = actual_datum.make_array();
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

void CheckDictionaryCompactionOnChunks(const std::shared_ptr<DataType>& dict_type,
                                       const ArrayVector& input,
                                       const ArrayVector& expected) {
  auto input_chunked_array = std::make_shared<ChunkedArray>(input, dict_type);

  ASSERT_OK_AND_ASSIGN(Datum actual_datum, DictionaryCompaction(input_chunked_array));
  ValidateOutput(actual_datum);
  auto actual = actual_datum.chunked_array();
  AssertChunkedEqual(*actual, expected);
}

TEST_F(TestDictionaryCompactionKernel, DictionaryArray) {
  std::shared_ptr<arrow::DataType> type;
  std::shared_ptr<arrow::DataType> dict_type;

  for (const auto& index_type : all_dictionary_index_types()) {
    ARROW_SCOPED_TRACE("index_type = ", index_type->ToString());

    type = boolean();
    dict_type = dictionary(index_type, type);
    // input is compacted
    CheckDictionaryCompaction(dict_type, "[]", "[]", "[]", "[]");
    CheckDictionaryCompaction(dict_type, "[true, false]", "[0, 1, 0]", "[true, false]",
                              "[0, 1, 0]");
    CheckDictionaryCompaction(dict_type, "[true, null, false]", "[2, 1, 0]",
                              "[true, null, false]", "[2, 1, 0]");
    CheckDictionaryCompaction(dict_type, "[true, false]", "[0, null, 1, 0]",
                              "[true, false]", "[0, null, 1, 0]");
    CheckDictionaryCompaction(dict_type, "[true, null, false]", "[2, null, 1, 0]",
                              "[true, null, false]", "[2, null, 1, 0]");
    // input isn't compacted
    CheckDictionaryCompaction(dict_type, "[null]", "[]", "[]", "[]");
    CheckDictionaryCompaction(dict_type, "[false]", "[null]", "[]", "[null]");
    CheckDictionaryCompaction(dict_type, "[true, false]", "[0]", "[true]", "[0]");
    CheckDictionaryCompaction(dict_type, "[true, null, false]", "[2, 1]", "[null, false]",
                              "[1, 0]");
    CheckDictionaryCompaction(dict_type, "[true, false]", "[0, null]", "[true]",
                              "[0, null]");
    CheckDictionaryCompaction(dict_type, "[true, null, false]", "[2, null, 1]",
                              "[null, false]", "[1, null, 0]");
  }
}

TEST_F(TestDictionaryCompactionKernel, DictionaryArrayChunks) {
  ArrayVector input;
  ArrayVector expected;
  std::shared_ptr<arrow::DataType> type;
  std::shared_ptr<arrow::DataType> dict_type;

  for (const auto& index_type : all_dictionary_index_types()) {
    ARROW_SCOPED_TRACE("index_type = ", index_type->ToString());

    type = boolean();
    dict_type = dictionary(index_type, type);
    // input is compacted
    input = {
        DictArrayFromJSON(dict_type, "[]", "[]"),
        DictArrayFromJSON(dict_type, "[0, 1, 0]", "[true, false]"),
        DictArrayFromJSON(dict_type, "[2, 1, 0]", "[true, null, false]"),
        DictArrayFromJSON(dict_type, "[0, null, 1, 0]", "[true, false]"),
        DictArrayFromJSON(dict_type, "[2, null, 1, 0]", "[true, null, false]"),
    };
    expected = {
        DictArrayFromJSON(dict_type, "[0, 1, 0]", "[true, false]"),
        DictArrayFromJSON(dict_type, "[2, 1, 0]", "[true, null, false]"),
        DictArrayFromJSON(dict_type, "[0, null, 1, 0]", "[true, false]"),
        DictArrayFromJSON(dict_type, "[2, null, 1, 0]", "[true, null, false]"),
    };
    CheckDictionaryCompactionOnChunks(dict_type, input, expected);
    // input isn't compacted
    input = {
        DictArrayFromJSON(dict_type, "[null]", "[false]"),
        DictArrayFromJSON(dict_type, "[0]", "[true, false]"),
        DictArrayFromJSON(dict_type, "[2, 1]", "[true, null, false]"),
        DictArrayFromJSON(dict_type, "[0, null]", "[true, false]"),
        DictArrayFromJSON(dict_type, "[2, null, 1]", "[true, null, false]"),
    };
    expected = {
        DictArrayFromJSON(dict_type, "[null]", "[]"),
        DictArrayFromJSON(dict_type, "[0]", "[true]"),
        DictArrayFromJSON(dict_type, "[1, 0]", "[null, false]"),
        DictArrayFromJSON(dict_type, "[0, null]", "[true]"),
        DictArrayFromJSON(dict_type, "[1, null, 0]", "[null, false]"),
    };
    CheckDictionaryCompactionOnChunks(dict_type, input, expected);
  }
}

}  // namespace compute
}  // namespace arrow
