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

void CheckSearchSorted(const Datum& values, const Datum& needles,
                       SearchSortedOptions::Side side,
                       const std::string& expected_json) {
  ASSERT_OK_AND_ASSIGN(auto result, SearchSorted(values, needles, SearchSortedOptions(side)));
  ASSERT_TRUE(result.is_array());
  ASSERT_OK(result.make_array()->ValidateFull());

  AssertArraysEqual(*ArrayFromJSON(uint64(), expected_json), *result.make_array());
}

void CheckSearchSorted(const Datum& values, const Datum& needles,
                       const std::string& expected_left_json,
                       const std::string& expected_right_json) {
  CheckSearchSorted(values, needles, SearchSortedOptions::Left, expected_left_json);
  CheckSearchSorted(values, needles, SearchSortedOptions::Right, expected_right_json);
}

void CheckSimpleSearchSorted(const std::shared_ptr<DataType>& type,
                             const std::string& values_json,
                             const std::string& needles_json,
                             const std::string& expected_left_json,
                             const std::string& expected_right_json) {
  auto values = ArrayFromJSON(type, values_json);
  auto needles = ArrayFromJSON(type, needles_json);

  CheckSearchSorted(Datum(values), Datum(needles), expected_left_json, expected_right_json);
}

void CheckScalarSearchSorted(const Datum& values, const std::shared_ptr<Array>& needles,
                             const std::string& expected_left_json,
                             const std::string& expected_right_json) {
  auto expected_left = ArrayFromJSON(uint64(), expected_left_json);
  auto expected_right = ArrayFromJSON(uint64(), expected_right_json);

  ASSERT_EQ(needles->length(), expected_left->length());
  ASSERT_EQ(needles->length(), expected_right->length());

  for (int64_t index = 0; index < needles->length(); ++index) {
    ASSERT_OK_AND_ASSIGN(auto needle, needles->GetScalar(index));
    ASSERT_OK_AND_ASSIGN(auto left,
                         SearchSorted(values, Datum(needle),
                                      SearchSortedOptions(SearchSortedOptions::Left)));
    ASSERT_OK_AND_ASSIGN(auto right,
                         SearchSorted(values, Datum(needle),
                                      SearchSortedOptions(SearchSortedOptions::Right)));

    ASSERT_TRUE(left.is_scalar());
    ASSERT_TRUE(right.is_scalar());

    ASSERT_OK_AND_ASSIGN(auto expected_left_scalar, expected_left->GetScalar(index));
    ASSERT_OK_AND_ASSIGN(auto expected_right_scalar, expected_right->GetScalar(index));
    AssertScalarsEqual(*expected_left_scalar, *left.scalar());
    AssertScalarsEqual(*expected_right_scalar, *right.scalar());
  }
}

void CheckSimpleScalarSearchSorted(const std::shared_ptr<DataType>& type,
                                   const std::string& values_json,
                                   const std::string& needles_json,
                                   const std::string& expected_left_json,
                                   const std::string& expected_right_json) {
  auto values = ArrayFromJSON(type, values_json);
  auto needles = ArrayFromJSON(type, needles_json);
  CheckScalarSearchSorted(Datum(values), needles, expected_left_json, expected_right_json);
}

void CheckSimpleSearchSortedAndScalar(const std::shared_ptr<DataType>& type,
                                      const std::string& values_json,
                                      const std::string& needles_json,
                                      const std::string& expected_left_json,
                                      const std::string& expected_right_json) {
  auto values = ArrayFromJSON(type, values_json);
  auto needles = ArrayFromJSON(type, needles_json);

  CheckSearchSorted(Datum(values), Datum(needles), expected_left_json, expected_right_json);
  CheckScalarSearchSorted(Datum(values), needles, expected_left_json,
                          expected_right_json);
}

struct SearchSortedSmokeCase {
  std::string name;
  std::shared_ptr<DataType> type;
  std::string values_json;
  std::string needles_json;
  std::string expected_left_json;
  std::string expected_right_json;
};

std::vector<SearchSortedSmokeCase> SupportedTypeSmokeCases() {
  return {
    {"Boolean", boolean(), "[false, false, false, true, true]", "[false, true]",
     "[0, 3]", "[3, 5]"},
      {
          "Int8",
          int8(),
      "[1, 3, 3, 5, 8]",
      "[0, 3, 9]",
      "[0, 1, 5]",
      "[0, 3, 5]",
      },
      {
          "Int16",
          int16(),
      "[1, 3, 3, 5, 8]",
      "[0, 3, 9]",
      "[0, 1, 5]",
      "[0, 3, 5]",
      },
      {
          "Int32",
          int32(),
      "[1, 3, 3, 5, 8]",
      "[0, 3, 9]",
      "[0, 1, 5]",
      "[0, 3, 5]",
      },
      {
          "Int64",
          int64(),
      "[1, 3, 3, 5, 8]",
      "[0, 3, 9]",
      "[0, 1, 5]",
      "[0, 3, 5]",
      },
      {
          "UInt8",
          uint8(),
      "[1, 3, 3, 5, 8]",
      "[0, 3, 9]",
      "[0, 1, 5]",
      "[0, 3, 5]",
      },
      {
          "UInt16",
          uint16(),
      "[1, 3, 3, 5, 8]",
      "[0, 3, 9]",
      "[0, 1, 5]",
      "[0, 3, 5]",
      },
      {
          "UInt32",
          uint32(),
      "[1, 3, 3, 5, 8]",
      "[0, 3, 9]",
      "[0, 1, 5]",
      "[0, 3, 5]",
      },
      {
          "UInt64",
          uint64(),
      "[1, 3, 3, 5, 8]",
      "[0, 3, 9]",
      "[0, 1, 5]",
      "[0, 3, 5]",
      },
    {"Float32", float32(), "[1.0, 3.0, 3.0, 5.0, 8.0]", "[0.0, 3.0, 9.0]",
     "[0, 1, 5]", "[0, 3, 5]"},
    {"Float64", float64(), "[1.0, 3.0, 3.0, 5.0, 8.0]", "[0.0, 3.0, 9.0]",
     "[0, 1, 5]", "[0, 3, 5]"},
      {
          "Date32",
          date32(),
      "[1, 3, 3, 5, 8]",
      "[0, 3, 9]",
      "[0, 1, 5]",
      "[0, 3, 5]",
      },
      {
          "Date64",
          date64(),
      "[86400000, 259200000, 259200000, 432000000, 691200000]",
      "[0, 259200000, 777600000]",
      "[0, 1, 5]",
      "[0, 3, 5]",
      },
    {"Time32", time32(TimeUnit::SECOND), "[1, 3, 3, 5, 8]", "[0, 3, 9]", "[0, 1, 5]",
     "[0, 3, 5]"},
    {"Time64", time64(TimeUnit::NANO), "[1, 3, 3, 5, 8]", "[0, 3, 9]", "[0, 1, 5]",
     "[0, 3, 5]"},
      {"Timestamp", timestamp(TimeUnit::SECOND),
     R"(["1970-01-02", "1970-01-04", "1970-01-04", "1970-01-06", "1970-01-09"])",
     R"(["1970-01-01", "1970-01-04", "1970-01-10"])", "[0, 1, 5]",
     "[0, 3, 5]"},
    {"Duration", duration(TimeUnit::NANO), "[1, 3, 3, 5, 8]", "[0, 3, 9]",
     "[0, 1, 5]", "[0, 3, 5]"},
    {"Binary", binary(), R"(["aa", "bb", "bb", "dd", "ff"])", R"(["a", "bb", "z"])",
     "[0, 1, 5]", "[0, 3, 5]"},
    {"String", utf8(), R"(["aa", "bb", "bb", "dd", "ff"])", R"(["a", "bb", "z"])",
     "[0, 1, 5]", "[0, 3, 5]"},
    {"LargeBinary", large_binary(), R"(["aa", "bb", "bb", "dd", "ff"])",
     R"(["a", "bb", "z"])", "[0, 1, 5]", "[0, 3, 5]"},
    {"LargeString", large_utf8(), R"(["aa", "bb", "bb", "dd", "ff"])",
     R"(["a", "bb", "z"])", "[0, 1, 5]", "[0, 3, 5]"},
    {"BinaryView", binary_view(), R"(["aa", "bb", "bb", "dd", "ff"])",
     R"(["a", "bb", "z"])", "[0, 1, 5]", "[0, 3, 5]"},
    {"StringView", utf8_view(), R"(["aa", "bb", "bb", "dd", "ff"])",
     R"(["a", "bb", "z"])", "[0, 1, 5]", "[0, 3, 5]"},
  };
}

class SearchSortedSupportedTypesTest
    : public ::testing::TestWithParam<SearchSortedSmokeCase> {};

TEST(SearchSorted, BasicLeftRight) {
  CheckSimpleSearchSorted(int64(), "[100, 200, 200, 300, 300]", "[50, 200, 250, 400]",
                          "[0, 1, 3, 5]", "[0, 3, 3, 5]");
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
  CheckSimpleSearchSorted(int16(), "[]", "[1, 2, 3]", "[0, 0, 0]", "[0, 0, 0]");
}

TEST(SearchSorted, ValuesWithLeadingNulls) {
  CheckSimpleSearchSorted(int32(), "[null, 200, 300, 300]", "[50, 200, 250, 400]",
                          "[1, 1, 2, 4]", "[1, 2, 2, 4]");
}

TEST(SearchSorted, ValuesAllNull) {
  CheckSimpleSearchSorted(int32(), "[null, null, null]", "[50, 200, null]",
                          "[3, 3, null]", "[3, 3, null]");
}

TEST(SearchSorted, ValuesWithTrailingNulls) {
  CheckSimpleSearchSorted(int32(), "[200, 300, 300, null, null]",
                          "[50, 200, 250, 400]", "[0, 0, 1, 3]",
                          "[0, 1, 1, 3]");
}

TEST(SearchSorted, FloatValuesWithTrailingNaNsAndNulls) {
  CheckSimpleSearchSortedAndScalar(float64(),
                                   "[1.0, 3.0, 3.0, 5.0, NaN, NaN, null]",
                                   "[0.0, 3.0, 4.0, NaN]", "[0, 1, 3, 4]",
                                   "[0, 3, 3, 6]");
}

TEST(SearchSorted, FloatValuesWithLeadingNullsAndTrailingNaNs) {
  CheckSimpleSearchSortedAndScalar(float64(),
                                   "[null, 1.0, 3.0, 3.0, 5.0, NaN, NaN]",
                                   "[0.0, 3.0, 4.0, NaN]", "[1, 2, 4, 5]",
                                   "[1, 4, 4, 7]");
}

TEST(SearchSorted, NullNeedlesEmitNull) {
  CheckSimpleSearchSorted(int32(), "[null, 200, 300, 300]",
                          "[null, 50, 200, null, 400]", "[null, 1, 1, null, 4]",
                          "[null, 1, 2, null, 4]");

  auto values = ArrayFromJSON(int32(), "[null, 200, 300, 300]");

  ASSERT_OK_AND_ASSIGN(auto scalar_result,
                       SearchSorted(Datum(values), Datum(std::make_shared<Int32Scalar>()),
                                    SearchSortedOptions(SearchSortedOptions::Left)));
  ASSERT_TRUE(scalar_result.is_scalar());
  ASSERT_FALSE(scalar_result.scalar()->is_valid);
  ASSERT_TRUE(scalar_result.scalar()->type->Equals(uint64()));
}

TEST(SearchSorted, ChunkedValues) {
  auto values = std::make_shared<ChunkedArray>(ArrayVector{
      ArrayFromJSON(int32(), "[1, 1]"),
      ArrayFromJSON(int32(), "[1, 3, 5]"),
  });
  auto needles = ArrayFromJSON(int32(), "[1, 2, 6]");

  CheckSearchSorted(Datum(values), Datum(needles), "[0, 3, 5]", "[3, 3, 5]");
}

TEST(SearchSorted, ChunkedNeedles) {
  auto values = ArrayFromJSON(int32(), "[1, 1, 3, 5, 8]");
  auto needles = std::make_shared<ChunkedArray>(ArrayVector{
      ArrayFromJSON(int32(), "[null, 0, 1]"),
      ArrayFromJSON(int32(), "[4, null, 9]"),
  });

  CheckSearchSorted(Datum(values), Datum(needles), "[null, 0, 0, 3, null, 5]",
                    "[null, 0, 2, 3, null, 5]");
}

TEST(SearchSorted, ChunkedRunEndEncodedValues) {
  auto values_type = run_end_encoded(int16(), int32());
  ASSERT_OK_AND_ASSIGN(auto left_chunk, REEFromJSON(values_type, "[1, 1, 1]"));
  ASSERT_OK_AND_ASSIGN(auto right_chunk, REEFromJSON(values_type, "[3, 3, 5]"));
  auto values = std::make_shared<ChunkedArray>(ArrayVector{left_chunk, right_chunk});
  auto needles = ArrayFromJSON(int32(), "[0, 1, 2, 3, 4, 5, 6]");

  CheckSearchSorted(Datum(values), Datum(needles), "[0, 0, 3, 3, 5, 5, 6]",
                    "[0, 3, 3, 5, 5, 6, 6]");
}

TEST(SearchSorted, ChunkedRunEndEncodedNeedles) {
  auto values = ArrayFromJSON(int32(), "[1, 1, 3, 5, 8]");
  auto needles_type = run_end_encoded(int32(), int32());
  ASSERT_OK_AND_ASSIGN(auto left_chunk, REEFromJSON(needles_type, "[0, 0, 1, 1]"));
  ASSERT_OK_AND_ASSIGN(auto right_chunk, REEFromJSON(needles_type, "[4, 4, 9]"));
  auto needles = std::make_shared<ChunkedArray>(ArrayVector{left_chunk, right_chunk});

  CheckSearchSorted(Datum(values), Datum(needles), SearchSortedOptions::Right,
                    "[0, 0, 2, 2, 3, 3, 5]");
}

TEST(SearchSorted, ChunkedValuesLeadingNullsAcrossEmptyChunks) {
  auto values = std::make_shared<ChunkedArray>(ArrayVector{
      ArrayFromJSON(int32(), "[]"),
      ArrayFromJSON(int32(), "[null, null]"),
      ArrayFromJSON(int32(), "[]"),
      ArrayFromJSON(int32(), "[2, 4, 4]"),
  });
  auto needles = ArrayFromJSON(int32(), "[1, 4, 8]");

  CheckSearchSorted(Datum(values), Datum(needles), "[2, 3, 5]", "[2, 5, 5]");
}

TEST(SearchSorted, ChunkedValuesTrailingNullsAcrossEmptyChunks) {
  auto values = std::make_shared<ChunkedArray>(ArrayVector{
      ArrayFromJSON(int32(), "[2, 4, 4]"),
      ArrayFromJSON(int32(), "[]"),
      ArrayFromJSON(int32(), "[null, null]"),
      ArrayFromJSON(int32(), "[]"),
  });
  auto needles = ArrayFromJSON(int32(), "[1, 4, 8]");

  CheckSearchSorted(Datum(values), Datum(needles), "[0, 1, 3]", "[0, 3, 3]");
}

TEST(SearchSorted, RejectChunkedValuesUnclusteredNullsAcrossEmptyChunks) {
  auto values = std::make_shared<ChunkedArray>(ArrayVector{
      ArrayFromJSON(int32(), "[]"),
      ArrayFromJSON(int32(), "[null, 1]"),
      ArrayFromJSON(int32(), "[]"),
      ArrayFromJSON(int32(), "[null, 3]"),
  });
  auto needles = ArrayFromJSON(int32(), "[2]");

  ASSERT_RAISES(Invalid, SearchSorted(Datum(values), Datum(needles)));
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

  CheckSearchSorted(Datum(ree_values), Datum(ree_needles), SearchSortedOptions::Left,
                    "[null, null, 2, 3, 3, null, 5]");
}

TEST(SearchSorted, RunEndEncodedNeedlesWithNullRuns) {
  auto values = ArrayFromJSON(int32(), "[1, 1, 3, 5, 8]");
  auto needles_type = run_end_encoded(int32(), int32());
  ASSERT_OK_AND_ASSIGN(
      auto ree_needles,
      REEFromJSON(needles_type, "[null, null, 0, 0, 0, 1, 1, 4, 4, 4, null, 9, 9]"));

  CheckSearchSorted(Datum(values), Datum(ree_needles),
          "[null, null, 0, 0, 0, 0, 0, 3, 3, 3, null, 5, 5]",
          "[null, null, 0, 0, 0, 2, 2, 3, 3, 3, null, 5, 5]");
}

TEST(SearchSorted, RunEndEncodedAllNullValues) {
  auto values_type = run_end_encoded(int16(), int32());
  ASSERT_OK_AND_ASSIGN(auto ree_values,
                       REEFromJSON(values_type, "[null, null, null, null]"));
  auto needles = ArrayFromJSON(int32(), "[null, 1, 8]");

  CheckSearchSorted(Datum(ree_values), Datum(needles), SearchSortedOptions::Left,
                    "[null, 4, 4]");
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

  CheckSearchSorted(Datum(ree_values), Datum(needles), "[0, 0, 3, 3, 5, 5, 6]",
                    "[0, 3, 3, 5, 5, 6, 6]");
}

TEST(SearchSorted, RunEndEncodedNeedles) {
  auto values = ArrayFromJSON(int32(), "[1, 1, 3, 5, 8]");
  auto needles_type = run_end_encoded(int32(), int32());
  ASSERT_OK_AND_ASSIGN(auto ree_needles,
                       REEFromJSON(needles_type, "[0, 0, 1, 1, 4, 4, 9]"));

  CheckSearchSorted(Datum(values), Datum(ree_needles), SearchSortedOptions::Right,
                    "[0, 0, 2, 2, 3, 3, 5]");
}

TEST(SearchSorted, SlicedRunEndEncodedValues) {
  auto values_type = run_end_encoded(int32(), int32());
  ASSERT_OK_AND_ASSIGN(auto ree_values,
                       REEFromJSON(values_type, "[0, 0, 1, 1, 1, 4, 4, 9]"));
  auto sliced = ree_values->Slice(2, 5);
  auto needles = ArrayFromJSON(int32(), "[0, 1, 2, 4, 9]");

  CheckSearchSorted(Datum(sliced), Datum(needles), SearchSortedOptions::Left,
                    "[0, 0, 3, 3, 5]");
}

TEST(SearchSorted, BinaryValues) {
  CheckSimpleSearchSorted(utf8(), R"(["aa", "bb", "bb", "cc"])",
                          R"(["a", "bb", "bc", "z"])", "[0, 1, 3, 4]",
                          "[0, 3, 3, 4]");
}

TEST_P(SearchSortedSupportedTypesTest, ArraySmoke) {
  const auto& param = GetParam();
  CheckSimpleSearchSorted(param.type, param.values_json, param.needles_json,
                          param.expected_left_json, param.expected_right_json);
}

TEST_P(SearchSortedSupportedTypesTest, ScalarSmoke) {
  const auto& param = GetParam();
  CheckSimpleScalarSearchSorted(param.type, param.values_json, param.needles_json,
                                param.expected_left_json, param.expected_right_json);
}

INSTANTIATE_TEST_SUITE_P(SupportedTypes, SearchSortedSupportedTypesTest,
                         ::testing::ValuesIn(SupportedTypeSmokeCases()),
                         [](const ::testing::TestParamInfo<SearchSortedSmokeCase>& info) {
                           return info.param.name;
                         });

}  // namespace
}  // namespace compute
}  // namespace arrow
