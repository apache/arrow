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
#include "arrow/array/array_base.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/scalar.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/type_fwd.h"
#include "gmock/gmock.h"

namespace arrow::compute {
constexpr static std::array<const char*, 6> kRollingFunctionNames{
    "rolling_sum",          "rolling_sum_checked", "rolling_prod",
    "rolling_prod_checked", "rolling_min",         "rolling_max"};

TEST(TestRolling, Empty) {
  for (auto function : kRollingFunctionNames) {
    RollingOptions options(2);
    for (auto ty : NumericTypes()) {
      auto empty_arr = ArrayFromJSON(ty, "[]");
      auto empty_chunked = ChunkedArrayFromJSON(ty, {"[]"});
      CheckVectorUnary(function, empty_arr, empty_arr, &options);

      CheckVectorUnary(function, empty_chunked, empty_chunked, &options);
    }
  }
}

TEST(TestRolling, AllNulls) {
  for (auto function : kRollingFunctionNames) {
    RollingOptions options(2, 1, false);
    for (auto ty : NumericTypes()) {
      auto nulls_arr = ArrayFromJSON(ty, "[null, null, null]");
      auto nulls_one_chunk = ChunkedArrayFromJSON(ty, {"[null, null, null]"});
      auto nulls_three_chunks = ChunkedArrayFromJSON(ty, {"[null]", "[null]", "[null]"});
      CheckVectorUnary(function, nulls_arr, nulls_arr, &options);

      CheckVectorUnary(function, nulls_one_chunk, nulls_one_chunk, &options);

      CheckVectorUnary(function, nulls_three_chunks, nulls_one_chunk, &options);
    }
  }
  for (auto function : kRollingFunctionNames) {
    RollingOptions options(3, 2, true);
    for (auto ty : NumericTypes()) {
      auto nulls_arr = ArrayFromJSON(ty, "[null, null, null]");
      auto nulls_one_chunk = ChunkedArrayFromJSON(ty, {"[null, null, null]"});
      auto nulls_three_chunks = ChunkedArrayFromJSON(ty, {"[null]", "[null]", "[null]"});
      CheckVectorUnary(function, nulls_arr, nulls_arr, &options);

      CheckVectorUnary(function, nulls_one_chunk, nulls_one_chunk, &options);

      CheckVectorUnary(function, nulls_three_chunks, nulls_one_chunk, &options);
    }
  }
}

TEST(TestRolling, InvalidOptions) {
  std::vector<RollingOptions> invalid_options{RollingOptions(2, 3), RollingOptions(0, 0),
                                              RollingOptions(1, 0)};
  for (auto function : kRollingFunctionNames) {
    for (auto options : invalid_options) {
      for (auto ty : NumericTypes()) {
        auto arr = ArrayFromJSON(ty, "[1, 2, 3]");
        ASSERT_RAISES(Invalid, CallFunction(function, {arr}, &options));
      }
    }
  }
}

using testing::HasSubstr;
template <typename ArrowType>
void CheckRollingSumUnsignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  RollingOptions options(3, 2, true);
  auto max = std::numeric_limits<CType>::max();
  auto min = std::numeric_limits<CType>::min();

  BuilderType builder;
  std::shared_ptr<Array> max_arr;
  std::shared_ptr<Array> min_arr;
  std::shared_ptr<Array> expected_arr;
  ASSERT_OK(builder.Append(max));
  ASSERT_OK(builder.Append(1));
  ASSERT_OK(builder.Finish(&max_arr));
  builder.Reset();
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Append(min));
  ASSERT_OK(builder.Finish(&min_arr));
  builder.Reset();

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, HasSubstr("overflow"),
      CallFunction("rolling_sum_checked", {max_arr}, &options));
  CheckVectorUnary("rolling_sum", max_arr, min_arr, &options);
}

template <typename ArrowType>
void CheckRollingSumSignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  CheckRollingSumUnsignedOverflow<ArrowType>();

  RollingOptions options(3, 2, true);
  auto max = std::numeric_limits<CType>::max();
  auto min = std::numeric_limits<CType>::min();

  BuilderType builder;
  std::shared_ptr<Array> max_arr;
  std::shared_ptr<Array> min_arr;
  ASSERT_OK(builder.Append(min));
  ASSERT_OK(builder.Append(-1));
  ASSERT_OK(builder.Finish(&min_arr));
  builder.Reset();
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Append(max));
  ASSERT_OK(builder.Finish(&max_arr));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, HasSubstr("overflow"),
      CallFunction("rolling_sum_checked", {min_arr}, &options));
  CheckVectorUnary("rolling_sum", min_arr, max_arr, &options);
}

TEST(TestRollingSum, IntegerOverflow) {
  CheckRollingSumUnsignedOverflow<UInt8Type>();
  CheckRollingSumUnsignedOverflow<UInt16Type>();
  CheckRollingSumUnsignedOverflow<UInt32Type>();
  CheckRollingSumUnsignedOverflow<UInt64Type>();
  CheckRollingSumSignedOverflow<Int8Type>();
  CheckRollingSumSignedOverflow<Int16Type>();
  CheckRollingSumSignedOverflow<Int32Type>();
  CheckRollingSumSignedOverflow<Int64Type>();
}

template <typename ArrowType>
void CheckRollingProdUnsignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  RollingOptions options(3, 2, true);
  auto max = std::numeric_limits<CType>::max();
  auto min = std::numeric_limits<CType>::min();

  BuilderType builder;
  std::shared_ptr<Array> max_arr;
  std::shared_ptr<Array> min_arr;
  ASSERT_OK(builder.Append(max / 2 + 1));
  ASSERT_OK(builder.Append(2));
  ASSERT_OK(builder.Finish(&max_arr));
  builder.Reset();
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Append(min));
  ASSERT_OK(builder.Finish(&min_arr));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, HasSubstr("overflow"),
      CallFunction("rolling_prod_checked", {max_arr}, &options));
  CheckVectorUnary("rolling_prod", max_arr, min_arr, &options);
}

template <typename ArrowType>
void CheckRollingProdSignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  CheckRollingSumUnsignedOverflow<ArrowType>();

  RollingOptions options(3, 2, true);
  auto min = std::numeric_limits<CType>::min();

  BuilderType builder;
  std::shared_ptr<Array> min_arr;
  std::shared_ptr<Array> expected_arr;
  ASSERT_OK(builder.Append(min));
  ASSERT_OK(builder.Append(-1));
  ASSERT_OK(builder.Finish(&min_arr));
  builder.Reset();
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Append(min));
  ASSERT_OK(builder.Finish(&expected_arr));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, HasSubstr("overflow"),
      CallFunction("rolling_prod_checked", {min_arr}, &options));
  CheckVectorUnary("rolling_prod", min_arr, expected_arr, &options);
}

TEST(TestRollingProd, IntegerOverflow) {
  CheckRollingProdUnsignedOverflow<UInt8Type>();
  CheckRollingProdUnsignedOverflow<UInt16Type>();
  CheckRollingProdUnsignedOverflow<UInt32Type>();
  CheckRollingProdUnsignedOverflow<UInt64Type>();
  CheckRollingProdSignedOverflow<Int8Type>();
  CheckRollingProdSignedOverflow<Int16Type>();
  CheckRollingProdSignedOverflow<Int32Type>();
  CheckRollingProdSignedOverflow<Int64Type>();
}

void CheckRolling(const std::string& func_name, const std::shared_ptr<Array>& input,
                  const std::shared_ptr<Array>& expected, RollingOptions* options) {
  CheckVectorUnary(func_name, input, expected, options);

  if (func_name == "rolling_sum") {  // only check chunks for rolling_sum to reduce time
    // Test with 3 chunks
    for (int i = 0; i < input->length(); ++i) {
      for (int j = i; j < input->length(); ++j) {
        std::vector<std::shared_ptr<Array>> chunks{
            input->Slice(0, i), input->Slice(i, j - i), input->Slice(j)};
        auto chunked_input = std::make_shared<ChunkedArray>(chunks);
        auto chunked_expected = std::make_shared<ChunkedArray>(expected);
        CheckVectorUnary(func_name, chunked_input, chunked_expected, options);
      }
    }
  }
}

TEST(TestRollingSum, IgnoreNulls) {
  for (std::string func : {"rolling_sum", "rolling_sum_checked"}) {
    for (auto ty : NumericTypes()) {
      RollingOptions options(4, 3, true);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7]"),
                   ArrayFromJSON(ty, "[null, null, 6, 10, 14, 18, 22]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, null, 4, 5, 6, null]"),
                   ArrayFromJSON(ty, "[null, null, null, 7, 11, 15, 15]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, 4, 5, null, 7]"),
                   ArrayFromJSON(ty, "[null, null, null, 9, 14, 12, 16]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, null, 5, 6, null]"),
                   ArrayFromJSON(ty, "[null, null, null, null, 10, 14, null]"), &options);

      options = RollingOptions(2, 1, true);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7]"),
                   ArrayFromJSON(ty, "[1, 3, 5, 7, 9, 11, 13]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, null, 4, 5, 6, null]"),
                   ArrayFromJSON(ty, "[1, 3, 2, 4, 9, 11, 6]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, 4, 5, null, 7]"),
                   ArrayFromJSON(ty, "[null, 2, 5, 7, 9, 5, 7]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, null, 5, 6, null]"),
                   ArrayFromJSON(ty, "[null, 2, 5, 3, 5, 11, 6]"), &options);
    }
  }
}

TEST(TestRollingSum, NoIgnoreNulls) {
  for (std::string func : {"rolling_sum", "rolling_sum_checked"}) {
    for (auto ty : NumericTypes()) {
      RollingOptions options(4, 3, false);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7]"),
                   ArrayFromJSON(ty, "[null, null, 6, 10, 14, 18, 22]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, null, 4, 5, 6, null]"),
                   ArrayFromJSON(ty, "[null, null, null, null, null, null, null]"),
                   &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, 4, 5, null, 7]"),
                   ArrayFromJSON(ty, "[null, null, null, null, 14, null, null]"),
                   &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, null, 5, 6, null]"),
                   ArrayFromJSON(ty, "[null, null, null, null, null, null, null]"),
                   &options);

      options = RollingOptions(2, 1, false);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7]"),
                   ArrayFromJSON(ty, "[1, 3, 5, 7, 9, 11, 13]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, null, 4, 5, 6, null]"),
                   ArrayFromJSON(ty, "[1, 3, null, null, 9, 11, null]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, 4, 5, null, 7]"),
                   ArrayFromJSON(ty, "[null, null, 5, 7, 9, null, null]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, null, 5, 6, null]"),
                   ArrayFromJSON(ty, "[null, null, 5, null, null, 11, null]"), &options);
    }
  }
}

TEST(TestRollingProd, IgnoreNulls) {
  for (std::string func : {"rolling_prod", "rolling_prod_checked"}) {
    for (auto ty : NumericTypes()) {
      if (ty == uint8() || ty == int8()) {  // out of bounds
        continue;
      }
      RollingOptions options(4, 3, true);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7]"),
                   ArrayFromJSON(ty, "[null, null, 6, 24, 120, 360, 840]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, null, 4, 5, 6, null]"),
                   ArrayFromJSON(ty, "[null, null, null, 8, 40, 120, 120]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, 4, 5, null, 7]"),
                   ArrayFromJSON(ty, "[null, null, null, 24, 120, 60, 140]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, null, 5, 6, null]"),
                   ArrayFromJSON(ty, "[null, null, null, null, 30, 90, null]"), &options);

      options = RollingOptions(2, 1, true);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7]"),
                   ArrayFromJSON(ty, "[1, 2, 6, 12, 20, 30, 42]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, null, 4, 5, 6, null]"),
                   ArrayFromJSON(ty, "[1, 2, 2, 4, 20, 30, 6]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, 4, 5, null, 7]"),
                   ArrayFromJSON(ty, "[null, 2, 6, 12, 20, 5, 7]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, null, 5, 6, null]"),
                   ArrayFromJSON(ty, "[null, 2, 6, 3, 5, 30, 6]"), &options);
    }
  }
}

TEST(TestRollingProd, NoIgnoreNulls) {
  for (std::string func : {"rolling_prod", "rolling_prod_checked"}) {
    for (auto ty : NumericTypes()) {
      if (ty == uint8() || ty == int8()) {  // out of bounds
        continue;
      }
      RollingOptions options(4, 3, false);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7]"),
                   ArrayFromJSON(ty, "[null, null, 6, 24, 120, 360, 840]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, null, 4, 5, 6, null]"),
                   ArrayFromJSON(ty, "[null, null, null, null, null, null, null]"),
                   &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, 4, 5, null, 7]"),
                   ArrayFromJSON(ty, "[null, null, null, null, 120, null, null]"),
                   &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, null, 5, 6, null]"),
                   ArrayFromJSON(ty, "[null, null, null, null, null, null, null]"),
                   &options);

      options = RollingOptions(2, 1, false);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7]"),
                   ArrayFromJSON(ty, "[1, 2, 6, 12, 20, 30, 42]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[1, 2, null, 4, 5, 6, null]"),
                   ArrayFromJSON(ty, "[1, 2, null, null, 20, 30, null]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, 4, 5, null, 7]"),
                   ArrayFromJSON(ty, "[null, null, 6, 12, 20, null, null]"), &options);
      CheckRolling(func, ArrayFromJSON(ty, "[null, 2, 3, null, 5, 6, null]"),
                   ArrayFromJSON(ty, "[null, null, 6, null, null, 30, null]"), &options);
    }
  }
}

TEST(TestRollingMax, IgnoreNulls) {
  for (auto ty : NumericTypes()) {
    RollingOptions options(4, 3, true);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[2, 1, 3, 7, 5, 4, 6]"),
                 ArrayFromJSON(ty, "[null, null, 3, 7, 7, 7, 7]"), &options);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[2, 1, null, 7, 5, 4, null]"),
                 ArrayFromJSON(ty, "[null, null, null, 7, 7, 7, 7]"), &options);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[null, 1, 3, 7, 5, null, 6]"),
                 ArrayFromJSON(ty, "[null, null, null, 7, 7, 7, 7]"), &options);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[null, 1, 3, null, 5, 4, null]"),
                 ArrayFromJSON(ty, "[null, null, null, null, 5, 5, null]"), &options);

    options = RollingOptions(2, 1, true);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[2, 1, 3, 7, 5, 4, 6]"),
                 ArrayFromJSON(ty, "[2, 2, 3, 7, 7, 5, 6]"), &options);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[2, 1, null, 7, 5, 4, null]"),
                 ArrayFromJSON(ty, "[2, 2, 1, 7, 7, 5, 4]"), &options);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[null, 1, 3, 7, 5, null, 6]"),
                 ArrayFromJSON(ty, "[null, 1, 3, 7, 7, 5, 6]"), &options);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[null, 1, 3, null, 5, 4, null]"),
                 ArrayFromJSON(ty, "[null, 1, 3, 3, 5, 5, 4]"), &options);
  }
}

TEST(TestRollingMax, NoIgnoreNulls) {
  for (auto ty : NumericTypes()) {
    RollingOptions options(4, 3, false);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[2, 1, 3, 7, 5, 4, 6]"),
                 ArrayFromJSON(ty, "[null, null, 3, 7, 7, 7, 7]"), &options);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[2, 1, null, 7, 5, 4, null]"),
                 ArrayFromJSON(ty, "[null, null, null, null, null, null, null]"),
                 &options);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[null, 1, 3, 7, 5, null, 6]"),
                 ArrayFromJSON(ty, "[null, null, null, null, 7, null, null]"), &options);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[null, 1, 3, null, 5, 4, null]"),
                 ArrayFromJSON(ty, "[null, null, null, null, null, null, null]"),
                 &options);

    options = RollingOptions(2, 1, false);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[2, 1, 3, 7, 5, 4, 6]"),
                 ArrayFromJSON(ty, "[2, 2, 3, 7, 7, 5, 6]"), &options);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[2, 1, null, 7, 5, 4, null]"),
                 ArrayFromJSON(ty, "[2, 2, null, null, 7, 5, null]"), &options);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[null, 1, 3, 7, 5, null, 6]"),
                 ArrayFromJSON(ty, "[null, null, 3, 7, 7, null, null]"), &options);
    CheckRolling("rolling_max", ArrayFromJSON(ty, "[null, 1, 3, null, 5, 4, null]"),
                 ArrayFromJSON(ty, "[null, null, 3, null, null, 5, null]"), &options);
  }
}

TEST(TestRollingProd, ZeroAndNaN) {
  for (std::string func : {"rolling_prod", "rolling_prod_checked"}) {
    for (auto ty : NumericTypes()) {
      RollingOptions options(4, 3, true);
      CheckRolling(func, ArrayFromJSON(ty, "[0, 1, 2, 3, 4, 0]"),
                   ArrayFromJSON(ty, "[null, null, 0, 0, 24, 0]"), &options);
      options = RollingOptions(2, 1, true);
      CheckRolling(func, ArrayFromJSON(ty, "[0, 1, 2, 0, 3, 4, 0]"),
                   ArrayFromJSON(ty, "[0, 0, 2, 0, 0, 12, 0]"), &options);
    }
  }
  RollingOptions options(2, 1, true);
  CheckRolling(
      "rolling_prod", ArrayFromJSON(float32(), "[NaN, 0, 1, 2, NaN, 3, 4, 0, NaN]"),
      ArrayFromJSON(float32(), "[NaN, NaN, 0, 2, NaN, NaN, 12, 0, NaN]"), &options);
  CheckRolling("rolling_prod_checked",
               ArrayFromJSON(float32(), "[NaN, 0, 1, 2, NaN, 3, 4, 0, NaN]"),
               ArrayFromJSON(float32(), "[NaN, NaN, 0, 2, NaN, NaN, 12, 0, NaN]"),
               &options);
}

TEST(TestRollingMin, IgnoreNulls) {
  for (auto ty : NumericTypes()) {
    RollingOptions options(4, 3, true);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[2, 1, 3, 7, 5, 4, 6]"),
                 ArrayFromJSON(ty, "[null, null, 1, 1, 1, 3, 4]"), &options);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[2, 1, null, 7, 5, 4, null]"),
                 ArrayFromJSON(ty, "[null, null, null, 1, 1, 4, 4]"), &options);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[null, 1, 3, 7, 5, null, 6]"),
                 ArrayFromJSON(ty, "[null, null, null, 1, 1, 3, 5]"), &options);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[null, 1, 3, null, 5, 4, null]"),
                 ArrayFromJSON(ty, "[null, null, null, null, 1, 3, null]"), &options);

    options = RollingOptions(2, 1, true);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[2, 1, 3, 7, 5, 4, 6]"),
                 ArrayFromJSON(ty, "[2, 1, 1, 3, 5, 4, 4]"), &options);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[2, 1, null, 7, 5, 4, null]"),
                 ArrayFromJSON(ty, "[2, 1, 1, 7, 5, 4, 4]"), &options);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[null, 1, 3, 7, 5, null, 6]"),
                 ArrayFromJSON(ty, "[null, 1, 1, 3, 5, 5, 6]"), &options);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[null, 1, 3, null, 5, 4, null]"),
                 ArrayFromJSON(ty, "[null, 1, 1, 3, 5, 4, 4]"), &options);
  }
}

TEST(TestRollingMin, NoIgnoreNulls) {
  for (auto ty : NumericTypes()) {
    RollingOptions options(4, 3, false);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[2, 1, 3, 7, 5, 4, 6]"),
                 ArrayFromJSON(ty, "[null, null, 1, 1, 1, 3, 4]"), &options);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[2, 1, null, 7, 5, 4, null]"),
                 ArrayFromJSON(ty, "[null, null, null, null, null, null, null]"),
                 &options);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[null, 1, 3, 7, 5, null, 6]"),
                 ArrayFromJSON(ty, "[null, null, null, null, 1, null, null]"), &options);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[null, 1, 3, null, 5, 4, null]"),
                 ArrayFromJSON(ty, "[null, null, null, null, null, null, null]"),
                 &options);

    options = RollingOptions(2, 1, false);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[2, 1, 3, 7, 5, 4, 6]"),
                 ArrayFromJSON(ty, "[2, 1, 1, 3, 5, 4, 4]"), &options);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[2, 1, null, 7, 5, 4, null]"),
                 ArrayFromJSON(ty, "[2, 1, null, null, 5, 4, null]"), &options);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[null, 1, 3, 7, 5, null, 6]"),
                 ArrayFromJSON(ty, "[null, null, 1, 3, 5, null, null]"), &options);
    CheckRolling("rolling_min", ArrayFromJSON(ty, "[null, 1, 3, null, 5, 4, null]"),
                 ArrayFromJSON(ty, "[null, null, 1, null, null, 4, null]"), &options);
  }
}

TEST(TestRollingMean, IgnoreNulls) {
  for (auto ty : NumericTypes()) {
    RollingOptions options(4, 3, true);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7]"),
                 ArrayFromJSON(float64(), "[null, null, 2, 2.5, 3.5, 4.5, 5.5]"),
                 &options);
    CheckRolling(
        "rolling_mean", ArrayFromJSON(ty, "[1, 2, null, 4, 5, 6, null]"),
        ArrayFromJSON(
            float64(),
            "[null, null, null, 2.33333333333333333, 3.66666666666666667, 5, 5]"),
        &options);
    CheckRolling(
        "rolling_mean", ArrayFromJSON(ty, "[null, 2, 3, 4, 5, null, 7]"),
        ArrayFromJSON(float64(), "[null, null, null, 3, 3.5, 4, 5.33333333333333333]"),
        &options);
    CheckRolling(
        "rolling_mean", ArrayFromJSON(ty, "[null, 2, 3, null, 5, 6, null]"),
        ArrayFromJSON(
            float64(),
            "[null, null, null, null, 3.33333333333333333, 4.66666666666666667, null]"),
        &options);

    options = RollingOptions(2, 1, true);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7]"),
                 ArrayFromJSON(float64(), "[1, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5]"), &options);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[1, 2, null, 4, 5, 6, null]"),
                 ArrayFromJSON(float64(), "[1, 1.5, 2, 4, 4.5, 5.5, 6]"), &options);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[null, 2, 3, 4, 5, null, 7]"),
                 ArrayFromJSON(float64(), "[null, 2, 2.5, 3.5, 4.5, 5, 7]"), &options);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[null, 2, 3, null, 5, 6, null]"),
                 ArrayFromJSON(float64(), "[null, 2, 2.5, 3, 5, 5.5, 6]"), &options);
  }
}

TEST(TestRollingMean, NoIgnoreNulls) {
  for (auto ty : NumericTypes()) {
    RollingOptions options(4, 3, false);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7]"),
                 ArrayFromJSON(float64(), "[null, null, 2, 2.5, 3.5, 4.5, 5.5]"),
                 &options);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[1, 2, null, 4, 5, 6, null]"),
                 ArrayFromJSON(float64(), "[null, null, null, null, null, null, null]"),
                 &options);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[null, 2, 3, 4, 5, null, 7]"),
                 ArrayFromJSON(float64(), "[null, null, null, null, 3.5, null, null]"),
                 &options);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[null, 2, 3, null, 5, 6, null]"),
                 ArrayFromJSON(float64(), "[null, null, null, null, null, null, null]"),
                 &options);

    options = RollingOptions(2, 1, false);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7]"),
                 ArrayFromJSON(float64(), "[1, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5]"), &options);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[1, 2, null, 4, 5, 6, null]"),
                 ArrayFromJSON(float64(), "[1, 1.5, null, null, 4.5, 5.5, null]"),
                 &options);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[null, 2, 3, 4, 5, null, 7]"),
                 ArrayFromJSON(float64(), "[null, null, 2.5, 3.5, 4.5, null, null]"),
                 &options);
    CheckRolling("rolling_mean", ArrayFromJSON(ty, "[null, 2, 3, null, 5, 6, null]"),
                 ArrayFromJSON(float64(), "[null, null, 2.5, null, null, 5.5, null]"),
                 &options);
  }
}

TEST(TestRolling, AbnormalLength) {
  // Check array length < window length
  RollingOptions options(4, 2, true);
  CheckRolling("rolling_sum", ArrayFromJSON(int32(), "[]"), ArrayFromJSON(int32(), "[]"),
               &options);
  CheckRolling("rolling_sum", ArrayFromJSON(int32(), "[1]"),
               ArrayFromJSON(int32(), "[null]"), &options);
  CheckRolling("rolling_sum", ArrayFromJSON(int32(), "[1, 2]"),
               ArrayFromJSON(int32(), "[null, 3]"), &options);
  CheckRolling("rolling_sum", ArrayFromJSON(int32(), "[1, 2, 3]"),
               ArrayFromJSON(int32(), "[null, 3, 6]"), &options);
}
}  // namespace arrow::compute
