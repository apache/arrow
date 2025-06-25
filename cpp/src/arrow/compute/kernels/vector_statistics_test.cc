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
#include <string_view>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util_internal.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow::compute {

using ::arrow::internal::checked_cast;

class TestWinsorize : public ::testing::Test {
 public:
  void CheckWinsorize(const std::shared_ptr<DataType>& type, std::string_view json_input,
                      std::string_view json_expected) {
    auto input = ArrayFromJSON(type, json_input);
    auto expected = ArrayFromJSON(type, json_expected);
    CheckWinsorize(input, expected);
  }

  void CheckWinsorizeChunked(const std::shared_ptr<DataType>& type,
                             const std::vector<std::string>& json_input,
                             const std::vector<std::string>& json_expected) {
    auto input = ChunkedArrayFromJSON(type, json_input);
    auto expected = ChunkedArrayFromJSON(type, json_expected);
    CheckWinsorize(input, expected);
  }

  void CheckWinsorize(const Datum& input, const Datum& expected) {
    CheckVectorUnary("winsorize", input, expected, &options_);
  }

  WinsorizeOptions options_;
};

TEST_F(TestWinsorize, FloatingPoint) {
  for (auto type : FloatingPointTypes()) {
    options_.lower_limit = 0.25;
    options_.upper_limit = 0.75;
    CheckWinsorize(type, "[]", "[]");
    CheckWinsorize(type, "[null, null]", "[null, null]");
    CheckWinsorize(type, "[1.1, 2.2, 3.3, 44, 55]", "[2.2, 2.2, 3.3, 44, 44]");
    CheckWinsorize(type, "[2.2, 1.1, 44, 55, 3.3]", "[2.2, 2.2, 44, 44, 3.3]");
    CheckWinsorize(type, "[2.2, 1.1, null, null, 44, 55, 3.3]",
                   "[2.2, 2.2, null, null, 44, 44, 3.3]");
    CheckWinsorize(type, "[2.2, 1.1, null, null, NaN, 44, 55, 3.3]",
                   "[2.2, 2.2, null, null, NaN, 44, 44, 3.3]");
    // Chunked
    CheckWinsorizeChunked(type, {"[2.2, 1.1, null]", "[]", "[NaN, 44, 55, 3.3]"},
                          {"[2.2, 2.2, null]", "[]", "[NaN, 44, 44, 3.3]"});
    CheckWinsorizeChunked(type, {"[]", "[]"}, {"[]", "[]"});
    CheckWinsorize(ChunkedArrayFromJSON(type, {}), ChunkedArrayFromJSON(type, {}));

    options_.lower_limit = 0.05;
    CheckWinsorize(type, "[2.2, 1.1, 44, 55, 3.3]", "[2.2, 1.1, 44, 44, 3.3]");
    options_.upper_limit = 0.95;
    CheckWinsorize(type, "[2.2, 1.1, 44, 55, 3.3]", "[2.2, 1.1, 44, 55, 3.3]");
    options_.lower_limit = 0;
    options_.upper_limit = 1;
    CheckWinsorize(type, "[2.2, 1.1, 44, 55, 3.3]", "[2.2, 1.1, 44, 55, 3.3]");
    options_.lower_limit = options_.upper_limit = 0.5;
    CheckWinsorize(type, "[2.2, 1.1, 44, 55, 3.3]", "[3.3, 3.3, 3.3, 3.3, 3.3]");
  }
}

TEST_F(TestWinsorize, Integral) {
  for (auto type : IntTypes()) {
    options_.lower_limit = 0.25;
    options_.upper_limit = 0.75;
    CheckWinsorize(type, "[]", "[]");
    CheckWinsorize(type, "[null, null]", "[null, null]");
    CheckWinsorize(type, "[1, 2, 3, 44, 55]", "[2, 2, 3, 44, 44]");
    CheckWinsorize(type, "[2, 1, 44, 55, 3]", "[2, 2, 44, 44, 3]");
    CheckWinsorize(type, "[2, 1, null, null, 44, 55, 3]",
                   "[2, 2, null, null, 44, 44, 3]");
    // Chunked
    CheckWinsorizeChunked(type, {"[2, 1, null]", "[]", "[null, 44, 55, 3]"},
                          {"[2, 2, null]", "[]", "[null, 44, 44, 3]"});
    CheckWinsorizeChunked(type, {"[]", "[]"}, {"[]", "[]"});
    CheckWinsorize(ChunkedArrayFromJSON(type, {}), ChunkedArrayFromJSON(type, {}));

    options_.lower_limit = 0.05;
    CheckWinsorize(type, "[2, 1, 44, 55, 3]", "[2, 1, 44, 44, 3]");
    options_.upper_limit = 0.95;
    CheckWinsorize(type, "[2, 1, 44, 55, 3]", "[2, 1, 44, 55, 3]");
  }
}

TEST_F(TestWinsorize, Decimal) {
  for (auto type :
       {decimal32(3, 1), decimal64(3, 1), decimal128(3, 1), decimal256(3, 1)}) {
    options_.lower_limit = 0.25;
    options_.upper_limit = 0.75;
    CheckWinsorize(type, "[]", "[]");
    CheckWinsorize(type, "[null, null]", "[null, null]");
    CheckWinsorize(type, R"(["1.1", "2.2", "3.3", "44.4", "55.5"])",
                   R"(["2.2", "2.2", "3.3", "44.4", "44.4"])");
    CheckWinsorize(type, R"(["2.2", "1.1", "44.4", "55.5", "3.3"])",
                   R"(["2.2", "2.2", "44.4", "44.4", "3.3"])");
    CheckWinsorize(type, R"(["2.2", "1.1", null, null, "44.4", "55.5", "3.3"])",
                   R"(["2.2", "2.2", null, null, "44.4", "44.4", "3.3"])");
    // Chunked
    CheckWinsorizeChunked(
        type, {R"(["2.2", "1.1"])", R"([null, null, "44.4", "55.5", "3.3"])"},
        {R"(["2.2", "2.2"])", R"([null, null, "44.4", "44.4", "3.3"])"});
    CheckWinsorizeChunked(type, {"[]", "[]"}, {"[]", "[]"});
    CheckWinsorize(ChunkedArrayFromJSON(type, {}), ChunkedArrayFromJSON(type, {}));

    options_.lower_limit = 0.05;
    CheckWinsorize(type, R"(["2.2", "1.1", "44.4", "55.5", "3.3"])",
                   R"(["2.2", "1.1", "44.4", "44.4", "3.3"])");
    options_.upper_limit = 0.95;
    CheckWinsorize(type, R"(["2.2", "1.1", "44.4", "55.5", "3.3"])",
                   R"(["2.2", "1.1", "44.4", "55.5", "3.3"])");
  }
}

TEST_F(TestWinsorize, InvalidOptions) {
  auto input = ArrayFromJSON(float64(), "[]");

  options_.lower_limit = -0.1;
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("limits must be between 0 and 1"),
                                  CallFunction("winsorize", {input}, &options_));
  options_.lower_limit = 1.1;
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("limits must be between 0 and 1"),
                                  CallFunction("winsorize", {input}, &options_));
  options_.lower_limit = 0.1;
  options_.upper_limit = -0.1;
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("limits must be between 0 and 1"),
                                  CallFunction("winsorize", {input}, &options_));
  options_.upper_limit = 1.1;
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("limits must be between 0 and 1"),
                                  CallFunction("winsorize", {input}, &options_));
  options_.upper_limit = 0.1;
  options_.lower_limit = 0.9;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("upper limit must be equal or greater"),
      CallFunction("winsorize", {input}, &options_));
}

}  // namespace arrow::compute
