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

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"

namespace arrow {
namespace compute {

class TestCumulativeOp : public ::testing::Test {
 protected:
  std::shared_ptr<Array> array(std::shared_ptr<DataType>& type,
                               const std::string& values) {
    return ArrayFromJSON(type, values);
  }

  std::shared_ptr<ChunkedArray> chunked_array(std::shared_ptr<DataType>& type,
                                              const std::vector<std::string>& values) {
    return ChunkedArrayFromJSON(type, values);
  }

  template <typename OptionsType>
  void Assert(const std::string func, const std::shared_ptr<Array>& input,
              const std::shared_ptr<Array>& expected, const OptionsType& options) {
    ASSERT_OK_AND_ASSIGN(auto result, CallFunction(func, {Datum(input)}, &options));

    AssertArraysApproxEqual(*expected, *result.make_array(), false,
                            EqualOptions::Defaults());
  }

  template <typename OptionsType>
  void Assert(std::shared_ptr<DataType>& type, const std::string func,
              const std::shared_ptr<ChunkedArray>& input,
              const std::shared_ptr<ChunkedArray>& expected, const OptionsType& options) {
    ASSERT_OK_AND_ASSIGN(auto result,
                         CallFunction(func, {Datum(input)}, &options, nullptr));

    ChunkedArray actual(result.chunks(), type);
    AssertChunkedApproxEquivalent(*expected, actual, EqualOptions::Defaults());
  }
};

struct CumulativeSumParam {
  std::string start;
  bool skip_nulls;
  std::shared_ptr<DataType> type;
  std::string json_arrays_input_no_nulls;
  std::string json_arrays_expected_no_nulls;
  std::string json_arrays_input_with_nulls;
  std::string json_arrays_expected_with_nulls;
  std::vector<std::string> json_chunked_arrays_input_no_nulls;
  std::vector<std::string> json_chunked_arrays_expected_no_nulls;
  std::vector<std::string> json_chunked_arrays_input_with_nulls;
  std::vector<std::string> json_chunked_arrays_expected_with_nulls;
};

std::vector<CumulativeSumParam> GenerateCumulativeSumParams(
    std::string start, bool skip_nulls, std::string json_arrays_input_no_nulls,
    std::string json_arrays_expected_no_nulls, std::string json_arrays_input_with_nulls,
    std::string json_arrays_expected_with_nulls,
    std::vector<std::string> json_chunked_arrays_input_no_nulls,
    std::vector<std::string> json_chunked_arrays_expected_no_nulls,
    std::vector<std::string> json_chunked_arrays_input_with_nulls,
    std::vector<std::string> json_chunked_arrays_expected_with_nulls) {
  std::vector<CumulativeSumParam> param_vector;

  for (auto ty : NumericTypes()) {
    param_vector.push_back(
        {start, skip_nulls, ty, json_arrays_input_no_nulls, json_arrays_expected_no_nulls,
         json_arrays_input_with_nulls, json_arrays_expected_with_nulls,
         json_chunked_arrays_input_no_nulls, json_chunked_arrays_expected_no_nulls,
         json_chunked_arrays_input_with_nulls, json_chunked_arrays_expected_with_nulls});
  }

  return param_vector;
}

class TestCumulativeSum : public TestCumulativeOp,
                          public testing::WithParamInterface<CumulativeSumParam> {
 protected:
  CumulativeSumOptions generate_options(std::shared_ptr<DataType> type,
                                        std::string start = "", bool skip_nulls = false,
                                        bool check_overflow = false) {
    return CumulativeSumOptions(ScalarFromJSON(type, start), skip_nulls, check_overflow);
  }

  void Assert(std::shared_ptr<DataType> type, const std::string& values,
              const std::string& expected, const CumulativeSumOptions& options) {
    auto values_arr = TestCumulativeOp::array(type, values);
    auto expected_arr = TestCumulativeOp::array(type, expected);
    auto func_name = options.check_overflow ? "cumulative_sum_checked" : "cumulative_sum";
    TestCumulativeOp::Assert(func_name, values_arr, expected_arr, options);
  }

  void Assert(std::shared_ptr<DataType> type, const std::vector<std::string>& values,
              const std::vector<std::string>& expected,
              const CumulativeSumOptions& options) {
    auto values_arr = TestCumulativeOp::chunked_array(type, values);
    auto expected_arr = TestCumulativeOp::chunked_array(type, expected);
    auto func_name = options.check_overflow ? "cumulative_sum_checked" : "cumulative_sum";
    TestCumulativeOp::Assert(type, func_name, values_arr, expected_arr, options);
  }
};

TEST_P(TestCumulativeSum, CheckResult) {
  const CumulativeSumParam param = GetParam();
  CumulativeSumOptions options =
      this->generate_options(param.type, param.start, param.skip_nulls);

  std::vector<std::string> empty_chunked_array;
  this->Assert(param.type, "[]", "[]", options);
  this->Assert(param.type, empty_chunked_array, empty_chunked_array, options);
  this->Assert(param.type, "[null, null, null]", "[null, null, null]", options);
  this->Assert(param.type, param.json_arrays_input_no_nulls,
               param.json_arrays_expected_no_nulls, options);
  this->Assert(param.type, param.json_arrays_input_with_nulls,
               param.json_arrays_expected_with_nulls, options);
  this->Assert(param.type, param.json_chunked_arrays_input_no_nulls,
               param.json_chunked_arrays_expected_no_nulls, options);
  this->Assert(param.type, param.json_chunked_arrays_input_with_nulls,
               param.json_chunked_arrays_expected_with_nulls, options);
}

INSTANTIATE_TEST_SUITE_P(NoStartNoSkip, TestCumulativeSum,
                         ::testing::ValuesIn(GenerateCumulativeSumParams(
                             "0", false, "[1, 2, 3, 4, 5, 6]", "[1, 3, 6, 10, 15, 21]",
                             "[1, 2, null, 4, null, 6]", "[1, 3, null, null, null, null]",
                             {"[1, 2, 3]", "[4, 5, 6]"}, {"[1, 3, 6, 10, 15, 21]"},
                             {"[1, 2, null]", "[4, null, 6]"},
                             {"[1, 3, null, null, null, null]"})));

INSTANTIATE_TEST_SUITE_P(NoStartDoSkip, TestCumulativeSum,
                         ::testing::ValuesIn(GenerateCumulativeSumParams(
                             "0", true, "[1, 2, 3, 4, 5, 6]", "[1, 3, 6, 10, 15, 21]",
                             "[1, 2, null, 4, null, 6]", "[1, 3, null, 7, null, 13]",
                             {"[1, 2, 3]", "[4, 5, 6]"}, {"[1, 3, 6, 10, 15, 21]"},
                             {"[1, 2, null]", "[4, null, 6]"},
                             {"[1, 3, null, 7, null, 13]"})));

INSTANTIATE_TEST_SUITE_P(
    HasStartNoSkip, TestCumulativeSum,
    ::testing::ValuesIn(GenerateCumulativeSumParams(
        "10", false, "[1, 2, 3, 4, 5, 6]", "[11, 13, 16, 20, 25, 31]",
        "[1, 2, null, 4, null, 6]", "[11, 13, null, null, null, null]",
        {"[1, 2, 3]", "[4, 5, 6]"}, {"[11, 13, 16, 20, 25, 31]"},
        {"[1, 2, null]", "[4, null, 6]"}, {"[11, 13, null, null, null, null]"})));

INSTANTIATE_TEST_SUITE_P(HasStartDoSkip, TestCumulativeSum,
                         ::testing::ValuesIn(GenerateCumulativeSumParams(
                             "10", true, "[1, 2, 3, 4, 5, 6]", "[11, 13, 16, 20, 25, 31]",
                             "[1, 2, null, 4, null, 6]", "[11, 13, null, 17, null, 23]",
                             {"[1, 2, 3]", "[4, 5, 6]"}, {"[11, 13, 16, 20, 25, 31]"},
                             {"[1, 2, null]", "[4, null, 6]"},
                             {"[11, 13, null, 17, null, 23]"})));

}  // namespace compute
}  // namespace arrow
