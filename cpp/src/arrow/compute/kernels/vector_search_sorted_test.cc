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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util_internal.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace {

Result<std::shared_ptr<Array>> REEFromJSON(const std::shared_ptr<DataType>& ree_type,
                                           const std::string& json) {
  auto ree_type_ptr = checked_cast<const RunEndEncodedType*>(ree_type.get());
  auto array = ArrayFromJSON(ree_type_ptr->value_type(), json);
  ARROW_ASSIGN_OR_RAISE(
      auto datum, RunEndEncode(array, RunEndEncodeOptions{ree_type_ptr->run_end_type()}));
  return datum.make_array();
}

void CheckSimpleSearchSorted(const std::shared_ptr<DataType>& type,
                             const std::string& values_json,
                             const std::string& needles_json,
                             const std::string& expected_left_json,
                             const std::string& expected_right_json) {
  auto values = ArrayFromJSON(type, values_json);
  auto needles = ArrayFromJSON(type, needles_json);

  ASSERT_OK_AND_ASSIGN(auto left,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Left)));
  ASSERT_OK_AND_ASSIGN(auto right,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Right)));

  AssertArraysEqual(*ArrayFromJSON(uint64(), expected_left_json), *left.make_array());
  AssertArraysEqual(*ArrayFromJSON(uint64(), expected_right_json), *right.make_array());
}

void CheckSimpleScalarSearchSorted(const std::shared_ptr<DataType>& type,
                                   const std::string& values_json,
                                   const std::string& needle_json, uint64_t expected_left,
                                   uint64_t expected_right) {
  auto values = ArrayFromJSON(type, values_json);
  auto needle = ScalarFromJSON(type, needle_json);

  ASSERT_OK_AND_ASSIGN(auto left,
                       SearchSorted(Datum(values), Datum(needle),
                                    SearchSortedOptions(SearchSortedOptions::Left)));
  ASSERT_OK_AND_ASSIGN(auto right,
                       SearchSorted(Datum(values), Datum(needle),
                                    SearchSortedOptions(SearchSortedOptions::Right)));

  ASSERT_TRUE(left.is_scalar());
  ASSERT_TRUE(right.is_scalar());
  ASSERT_EQ(checked_cast<const UInt64Scalar&>(*left.scalar()).value, expected_left);
  ASSERT_EQ(checked_cast<const UInt64Scalar&>(*right.scalar()).value, expected_right);
}

struct SearchSortedSmokeCase {
  std::string name;
  std::shared_ptr<DataType> type;
  std::string values_json;
  std::string needles_json;
  std::string expected_left_json;
  std::string expected_right_json;
  std::string scalar_needle_json;
  uint64_t expected_scalar_left;
  uint64_t expected_scalar_right;
};

std::vector<SearchSortedSmokeCase> SupportedTypeSmokeCases() {
  return {
      {"Boolean", boolean(), "[false, false, true, true]", "[false, true]", "[0, 2]",
       "[2, 4]", "true", 2, 4},
      {"Int8", int8(), "[1, 3, 3, 5]", "[0, 3, 4, 6]", "[0, 1, 3, 4]", "[0, 3, 3, 4]",
       "3", 1, 3},
      {"Int16", int16(), "[1, 3, 3, 5]", "[0, 3, 4, 6]", "[0, 1, 3, 4]", "[0, 3, 3, 4]",
       "3", 1, 3},
      {"Int32", int32(), "[1, 3, 3, 5]", "[0, 3, 4, 6]", "[0, 1, 3, 4]", "[0, 3, 3, 4]",
       "3", 1, 3},
      {"Int64", int64(), "[1, 3, 3, 5]", "[0, 3, 4, 6]", "[0, 1, 3, 4]", "[0, 3, 3, 4]",
       "3", 1, 3},
      {"UInt8", uint8(), "[1, 3, 3, 5]", "[0, 3, 4, 6]", "[0, 1, 3, 4]", "[0, 3, 3, 4]",
       "3", 1, 3},
      {"UInt16", uint16(), "[1, 3, 3, 5]", "[0, 3, 4, 6]", "[0, 1, 3, 4]", "[0, 3, 3, 4]",
       "3", 1, 3},
      {"UInt32", uint32(), "[1, 3, 3, 5]", "[0, 3, 4, 6]", "[0, 1, 3, 4]", "[0, 3, 3, 4]",
       "3", 1, 3},
      {"UInt64", uint64(), "[1, 3, 3, 5]", "[0, 3, 4, 6]", "[0, 1, 3, 4]", "[0, 3, 3, 4]",
       "3", 1, 3},
      {"Float32", float32(), "[1.0, 3.0, 3.0, 5.0]", "[0.0, 3.0, 4.0, 6.0]",
       "[0, 1, 3, 4]", "[0, 3, 3, 4]", "3.0", 1, 3},
      {"Float64", float64(), "[1.0, 3.0, 3.0, 5.0]", "[0.0, 3.0, 4.0, 6.0]",
       "[0, 1, 3, 4]", "[0, 3, 3, 4]", "3.0", 1, 3},
      {"Date32", date32(), "[1, 3, 3, 5]", "[0, 3, 4, 6]", "[0, 1, 3, 4]", "[0, 3, 3, 4]",
       "3", 1, 3},
      {"Date64", date64(), "[86400000, 259200000, 259200000, 432000000]",
       "[0, 259200000, 345600000, 518400000]", "[0, 1, 3, 4]", "[0, 3, 3, 4]",
       "259200000", 1, 3},
      {"Time32", time32(TimeUnit::SECOND), "[1, 3, 3, 5]", "[0, 3, 4, 6]", "[0, 1, 3, 4]",
       "[0, 3, 3, 4]", "3", 1, 3},
      {"Time64", time64(TimeUnit::NANO), "[1, 3, 3, 5]", "[0, 3, 4, 6]", "[0, 1, 3, 4]",
       "[0, 3, 3, 4]", "3", 1, 3},
      {"Timestamp", timestamp(TimeUnit::SECOND),
       R"(["1970-01-02", "1970-01-04", "1970-01-04", "1970-01-06"])",
       R"(["1970-01-01", "1970-01-04", "1970-01-05", "1970-01-07"])", "[0, 1, 3, 4]",
       "[0, 3, 3, 4]", R"("1970-01-04")", 1, 3},
      {"Duration", duration(TimeUnit::NANO), "[1, 3, 3, 5]", "[0, 3, 4, 6]",
       "[0, 1, 3, 4]", "[0, 3, 3, 4]", "3", 1, 3},
      {"Binary", binary(), R"(["aa", "bb", "bb", "dd"])", R"(["a", "bb", "bc", "z"])",
       "[0, 1, 3, 4]", "[0, 3, 3, 4]", R"("bb")", 1, 3},
      {"String", utf8(), R"(["aa", "bb", "bb", "dd"])", R"(["a", "bb", "bc", "z"])",
       "[0, 1, 3, 4]", "[0, 3, 3, 4]", R"("bb")", 1, 3},
      {"LargeBinary", large_binary(), R"(["aa", "bb", "bb", "dd"])",
       R"(["a", "bb", "bc", "z"])", "[0, 1, 3, 4]", "[0, 3, 3, 4]", R"("bb")", 1, 3},
      {"LargeString", large_utf8(), R"(["aa", "bb", "bb", "dd"])",
       R"(["a", "bb", "bc", "z"])", "[0, 1, 3, 4]", "[0, 3, 3, 4]", R"("bb")", 1, 3},
      {"BinaryView", binary_view(), R"(["aa", "bb", "bb", "dd"])",
       R"(["a", "bb", "bc", "z"])", "[0, 1, 3, 4]", "[0, 3, 3, 4]", R"("bb")", 1, 3},
      {"StringView", utf8_view(), R"(["aa", "bb", "bb", "dd"])",
       R"(["a", "bb", "bc", "z"])", "[0, 1, 3, 4]", "[0, 3, 3, 4]", R"("bb")", 1, 3},
  };
}

class SearchSortedSupportedTypesTest
    : public ::testing::TestWithParam<SearchSortedSmokeCase> {};

TEST(SearchSorted, BasicLeftRight) {
  auto values = ArrayFromJSON(int64(), "[100, 200, 200, 300, 300]");
  auto needles = ArrayFromJSON(int64(), "[50, 200, 250, 400]");

  ASSERT_OK_AND_ASSIGN(auto left,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Left)));
  ASSERT_OK_AND_ASSIGN(auto right,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Right)));

  AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 1, 3, 5]"), *left.make_array());
  AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 3, 3, 5]"), *right.make_array());
}

TEST(SearchSorted, ScalarNeedle) {
  auto values = ArrayFromJSON(int32(), "[1, 3, 5, 7]");

  ASSERT_OK_AND_ASSIGN(
      auto result, SearchSorted(Datum(values), Datum(std::make_shared<Int32Scalar>(5)),
                                SearchSortedOptions(SearchSortedOptions::Right)));

  ASSERT_TRUE(result.is_scalar());
  ASSERT_EQ(checked_cast<const UInt64Scalar&>(*result.scalar()).value, 3);
}

TEST(SearchSorted, ScalarStringNeedle) {
  auto values = ArrayFromJSON(utf8(), R"(["aa", "bb", "bb", "cc"])");

  ASSERT_OK_AND_ASSIGN(
      auto result,
      SearchSorted(Datum(values), Datum(std::make_shared<StringScalar>("bb")),
                   SearchSortedOptions(SearchSortedOptions::Right)));

  ASSERT_TRUE(result.is_scalar());
  ASSERT_EQ(checked_cast<const UInt64Scalar&>(*result.scalar()).value, 3);
}

TEST(SearchSorted, EmptyHaystack) {
  auto values = ArrayFromJSON(int16(), "[]");
  auto needles = ArrayFromJSON(int16(), "[1, 2, 3]");

  ASSERT_OK_AND_ASSIGN(auto result, SearchSorted(Datum(values), Datum(needles)));
  AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 0, 0]"), *result.make_array());
}

TEST(SearchSorted, ValuesWithLeadingNulls) {
  auto values = ArrayFromJSON(int32(), "[null, 200, 300, 300]");
  auto needles = ArrayFromJSON(int32(), "[50, 200, 250, 400]");

  ASSERT_OK_AND_ASSIGN(auto left,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Left)));
  ASSERT_OK_AND_ASSIGN(auto right,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Right)));

  AssertArraysEqual(*ArrayFromJSON(uint64(), "[1, 1, 2, 4]"), *left.make_array());
  AssertArraysEqual(*ArrayFromJSON(uint64(), "[1, 2, 2, 4]"), *right.make_array());
}

TEST(SearchSorted, ValuesAllNull) {
  auto values = ArrayFromJSON(int32(), "[null, null, null]");
  auto needles = ArrayFromJSON(int32(), "[50, 200, null]");

  ASSERT_OK_AND_ASSIGN(auto left,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Left)));
  ASSERT_OK_AND_ASSIGN(auto right,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Right)));

  AssertArraysEqual(*ArrayFromJSON(uint64(), "[3, 3, null]"), *left.make_array());
  AssertArraysEqual(*ArrayFromJSON(uint64(), "[3, 3, null]"), *right.make_array());
}

TEST(SearchSorted, ValuesWithTrailingNulls) {
  auto values = ArrayFromJSON(int32(), "[200, 300, 300, null, null]");
  auto needles = ArrayFromJSON(int32(), "[50, 200, 250, 400]");

  ASSERT_OK_AND_ASSIGN(auto left,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Left)));
  ASSERT_OK_AND_ASSIGN(auto right,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Right)));

  AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 0, 1, 3]"), *left.make_array());
  AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 1, 1, 3]"), *right.make_array());
}

TEST(SearchSorted, NullNeedlesEmitNull) {
  auto values = ArrayFromJSON(int32(), "[null, 200, 300, 300]");
  auto needles = ArrayFromJSON(int32(), "[null, 50, 200, null, 400]");

  ASSERT_OK_AND_ASSIGN(auto left,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Left)));
  ASSERT_OK_AND_ASSIGN(auto right,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Right)));

  AssertArraysEqual(*ArrayFromJSON(uint64(), "[null, 1, 1, null, 4]"),
                    *left.make_array());
  AssertArraysEqual(*ArrayFromJSON(uint64(), "[null, 1, 2, null, 4]"),
                    *right.make_array());

  ASSERT_OK_AND_ASSIGN(auto scalar_result,
                       SearchSorted(Datum(values), Datum(std::make_shared<Int32Scalar>()),
                                    SearchSortedOptions(SearchSortedOptions::Left)));
  ASSERT_TRUE(scalar_result.is_scalar());
  ASSERT_FALSE(scalar_result.scalar()->is_valid);
  ASSERT_TRUE(scalar_result.scalar()->type->Equals(uint64()));
}

TEST(SearchSorted, RejectUnclusteredNullValues) {
  auto values = ArrayFromJSON(int32(), "[null, 1, null, 3]");
  auto needles = ArrayFromJSON(int32(), "[2]");

  ASSERT_RAISES(Invalid, SearchSorted(Datum(values), Datum(needles)));
}

TEST(SearchSorted, RunEndEncodedNulls) {
  auto values_type = run_end_encoded(int16(), int32());
  ASSERT_OK_AND_ASSIGN(auto ree_values,
                       REEFromJSON(values_type, "[null, null, 2, 4, 4]"));
  auto needles_type = run_end_encoded(int16(), int32());
  ASSERT_OK_AND_ASSIGN(auto ree_needles,
                       REEFromJSON(needles_type, "[null, null, 1, 4, 4, null, 8]"));

  ASSERT_OK_AND_ASSIGN(auto result,
                       SearchSorted(Datum(ree_values), Datum(ree_needles),
                                    SearchSortedOptions(SearchSortedOptions::Left)));

  AssertArraysEqual(*ArrayFromJSON(uint64(), "[null, null, 2, 3, 3, null, 5]"),
                    *result.make_array());
}

TEST(SearchSorted, RunEndEncodedNeedlesWithNullRuns) {
  auto values = ArrayFromJSON(int32(), "[1, 1, 3, 5, 8]");
  auto needles_type = run_end_encoded(int32(), int32());
  ASSERT_OK_AND_ASSIGN(
      auto ree_needles,
      REEFromJSON(needles_type, "[null, null, 0, 0, 0, 1, 1, 4, 4, 4, null, 9, 9]"));

  ASSERT_OK_AND_ASSIGN(auto left,
                       SearchSorted(Datum(values), Datum(ree_needles),
                                    SearchSortedOptions(SearchSortedOptions::Left)));
  ASSERT_OK_AND_ASSIGN(auto right,
                       SearchSorted(Datum(values), Datum(ree_needles),
                                    SearchSortedOptions(SearchSortedOptions::Right)));

  AssertArraysEqual(
      *ArrayFromJSON(uint64(), "[null, null, 0, 0, 0, 0, 0, 3, 3, 3, null, 5, 5]"),
      *left.make_array());
  AssertArraysEqual(
      *ArrayFromJSON(uint64(), "[null, null, 0, 0, 0, 2, 2, 3, 3, 3, null, 5, 5]"),
      *right.make_array());
}

TEST(SearchSorted, RunEndEncodedAllNullValues) {
  auto values_type = run_end_encoded(int16(), int32());
  ASSERT_OK_AND_ASSIGN(auto ree_values,
                       REEFromJSON(values_type, "[null, null, null, null]"));
  auto needles = ArrayFromJSON(int32(), "[null, 1, 8]");

  ASSERT_OK_AND_ASSIGN(auto result,
                       SearchSorted(Datum(ree_values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Left)));

  AssertArraysEqual(*ArrayFromJSON(uint64(), "[null, 4, 4]"), *result.make_array());
}

TEST(SearchSorted, RejectMismatchedTypes) {
  auto values = ArrayFromJSON(int32(), "[1, 2, 3]");
  auto needles = ArrayFromJSON(int64(), "[2]");

  ASSERT_RAISES(TypeError, SearchSorted(Datum(values), Datum(needles)));
}

TEST(SearchSorted, RunEndEncodedValues) {
  auto values_type = run_end_encoded(int16(), int32());
  ASSERT_OK_AND_ASSIGN(auto ree_values, REEFromJSON(values_type, "[1, 1, 1, 3, 3, 5]"));
  auto needles = ArrayFromJSON(int32(), "[0, 1, 2, 3, 4, 5, 6]");

  ASSERT_OK_AND_ASSIGN(auto left,
                       SearchSorted(Datum(ree_values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Left)));
  ASSERT_OK_AND_ASSIGN(auto right,
                       SearchSorted(Datum(ree_values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Right)));

  AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 0, 3, 3, 5, 5, 6]"),
                    *left.make_array());
  AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 3, 3, 5, 5, 6, 6]"),
                    *right.make_array());
}

TEST(SearchSorted, RunEndEncodedNeedles) {
  auto values = ArrayFromJSON(int32(), "[1, 1, 3, 5, 8]");
  auto needles_type = run_end_encoded(int32(), int32());
  ASSERT_OK_AND_ASSIGN(auto ree_needles,
                       REEFromJSON(needles_type, "[0, 0, 1, 1, 4, 4, 9]"));

  ASSERT_OK_AND_ASSIGN(auto result,
                       SearchSorted(Datum(values), Datum(ree_needles),
                                    SearchSortedOptions(SearchSortedOptions::Right)));

  AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 0, 2, 2, 3, 3, 5]"),
                    *result.make_array());
}

TEST(SearchSorted, SlicedRunEndEncodedValues) {
  auto values_type = run_end_encoded(int32(), int32());
  ASSERT_OK_AND_ASSIGN(auto ree_values,
                       REEFromJSON(values_type, "[0, 0, 1, 1, 1, 4, 4, 9]"));
  auto sliced = ree_values->Slice(2, 5);
  auto needles = ArrayFromJSON(int32(), "[0, 1, 2, 4, 9]");

  ASSERT_OK_AND_ASSIGN(auto result,
                       SearchSorted(Datum(sliced), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Left)));

  AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 0, 3, 3, 5]"), *result.make_array());
}

TEST(SearchSorted, BinaryValues) {
  auto values = ArrayFromJSON(utf8(), R"(["aa", "bb", "bb", "cc"])");
  auto needles = ArrayFromJSON(utf8(), R"(["a", "bb", "bc", "z"])");

  ASSERT_OK_AND_ASSIGN(auto result,
                       SearchSorted(Datum(values), Datum(needles),
                                    SearchSortedOptions(SearchSortedOptions::Left)));

  AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 1, 3, 4]"), *result.make_array());
}

TEST_P(SearchSortedSupportedTypesTest, ArraySmoke) {
  const auto& param = GetParam();
  CheckSimpleSearchSorted(param.type, param.values_json, param.needles_json,
                          param.expected_left_json, param.expected_right_json);
}

TEST_P(SearchSortedSupportedTypesTest, ScalarSmoke) {
  const auto& param = GetParam();
  CheckSimpleScalarSearchSorted(param.type, param.values_json, param.scalar_needle_json,
                                param.expected_scalar_left, param.expected_scalar_right);
}

INSTANTIATE_TEST_SUITE_P(SupportedTypes, SearchSortedSupportedTypesTest,
                         ::testing::ValuesIn(SupportedTypeSmokeCases()),
                         [](const ::testing::TestParamInfo<SearchSortedSmokeCase>& info) {
                           return info.param.name;
                         });

}  // namespace
}  // namespace compute
}  // namespace arrow
