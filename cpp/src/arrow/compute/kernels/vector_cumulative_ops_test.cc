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

void Assert(const std::string func, const std::shared_ptr<Scalar>& input,
            const std::shared_ptr<Array>& expected, const FunctionOptions& options) {
  ASSERT_OK_AND_ASSIGN(auto result, CallFunction(func, {Datum(input)}, &options));

  AssertArraysApproxEqual(*expected, *result.make_array(), false,
                          EqualOptions::Defaults());
}

void Assert(const std::string func, const std::shared_ptr<Array>& input,
            const std::shared_ptr<Array>& expected, const FunctionOptions& options) {
  ASSERT_OK_AND_ASSIGN(auto result, CallFunction(func, {Datum(input)}, &options));

  AssertArraysApproxEqual(*expected, *result.make_array(), false,
                          EqualOptions::Defaults());
}

void Assert(const std::string func, std::shared_ptr<DataType>& type,
            const std::shared_ptr<ChunkedArray>& input,
            const std::shared_ptr<ChunkedArray>& expected,
            const FunctionOptions& options) {
  ASSERT_OK_AND_ASSIGN(auto result,
                       CallFunction(func, {Datum(input)}, &options, nullptr));

  ChunkedArray actual(result.chunks(), type);
  AssertChunkedApproxEquivalent(*expected, actual, EqualOptions::Defaults());
}

TEST(TestCumulativeSum, Empty) {
  CumulativeSumOptions options;
  for (auto ty : NumericTypes()) {
    auto empty_arr = ArrayFromJSON(ty, "[]");
    auto empty_chunked = ChunkedArrayFromJSON(ty, {"[]"});
    Assert("cumulative_sum", empty_arr, empty_arr, options);
    Assert("cumulative_sum", ty, empty_chunked, empty_chunked, options);
  }
}

TEST(TestCumulativeSum, AllNulls) {
  CumulativeSumOptions options;
  for (auto ty : NumericTypes()) {
    auto nulls_arr = ArrayFromJSON(ty, "[null, null, null]");
    auto nulls_one_chunk = ChunkedArrayFromJSON(ty, {"[null, null, null]"});
    auto nulls_three_chunks = ChunkedArrayFromJSON(ty, {"[null]", "[null]", "[null]"});
    Assert("cumulative_sum", nulls_arr, nulls_arr, options);
    Assert("cumulative_sum", ty, nulls_one_chunk, nulls_one_chunk, options);
    Assert("cumulative_sum", ty, nulls_three_chunks, nulls_one_chunk, options);
  }
}

TEST(TestCumulativeSum, ScalarInput) {
  CumulativeSumOptions no_start;
  CumulativeSumOptions with_start(10);
  for (auto ty : NumericTypes()) {
    Assert("cumulative_sum", ScalarFromJSON(ty, "10"), ArrayFromJSON(ty, "[10]"),
           no_start);
    Assert("cumulative_sum", ScalarFromJSON(ty, "10"), ArrayFromJSON(ty, "[20]"),
           with_start);
  }
}

TEST(TestCumulativeSum, NoStartNoSkip) {
  CumulativeSumOptions options;
  for (auto ty : NumericTypes()) {
    Assert("cumulative_sum", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6]"),
           ArrayFromJSON(ty, "[1, 3, 6, 10, 15, 21]"), options);

    Assert("cumulative_sum", ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
           ArrayFromJSON(ty, "[1, 3, null, null, null, null]"), options);

    Assert("cumulative_sum", ty, ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5, 6]"}),
           ChunkedArrayFromJSON(ty, {"[1, 3, 6, 10, 15, 21]"}), options);

    Assert("cumulative_sum", ty,
           ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
           ChunkedArrayFromJSON(ty, {"[1, 3, null, null, null, null]"}), options);
  }
}

TEST(TestCumulativeSum, NoStartDoSkip) {
  CumulativeSumOptions options(0, true);
  for (auto ty : NumericTypes()) {
    Assert("cumulative_sum", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6]"),
           ArrayFromJSON(ty, "[1, 3, 6, 10, 15, 21]"), options);

    Assert("cumulative_sum", ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
           ArrayFromJSON(ty, "[1, 3, null, 7, null, 13]"), options);

    Assert("cumulative_sum", ty, ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5, 6]"}),
           ChunkedArrayFromJSON(ty, {"[1, 3, 6, 10, 15, 21]"}), options);

    Assert("cumulative_sum", ty,
           ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
           ChunkedArrayFromJSON(ty, {"[1, 3, null, 7, null, 13]"}), options);
  }
}

TEST(TestCumulativeSum, HasStartNoSkip) {
  CumulativeSumOptions options(10);
  for (auto ty : NumericTypes()) {
    Assert("cumulative_sum", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6]"),
           ArrayFromJSON(ty, "[11, 13, 16, 20, 25, 31]"), options);

    Assert("cumulative_sum", ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
           ArrayFromJSON(ty, "[11, 13, null, null, null, null]"), options);

    Assert("cumulative_sum", ty, ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5, 6]"}),
           ChunkedArrayFromJSON(ty, {"[11, 13, 16, 20, 25, 31]"}), options);

    Assert("cumulative_sum", ty,
           ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
           ChunkedArrayFromJSON(ty, {"[11, 13, null, null, null, null]"}), options);
  }
}

TEST(TestCumulativeSum, HasStartDoSkip) {
  CumulativeSumOptions options(10, true);
  for (auto ty : NumericTypes()) {
    Assert("cumulative_sum", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6]"),
           ArrayFromJSON(ty, "[11, 13, 16, 20, 25, 31]"), options);

    Assert("cumulative_sum", ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
           ArrayFromJSON(ty, "[11, 13, null, 17, null, 23]"), options);

    Assert("cumulative_sum", ty, ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5, 6]"}),
           ChunkedArrayFromJSON(ty, {"[11, 13, 16, 20, 25, 31]"}), options);

    Assert("cumulative_sum", ty,
           ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
           ChunkedArrayFromJSON(ty, {"[11, 13, null, 17, null, 23]"}), options);
  }
}

}  // namespace compute
}  // namespace arrow
