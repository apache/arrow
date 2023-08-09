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
#include "arrow/compute/api_vector.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace compute {

static const std::vector<std::string> kCumulativeFunctionNames{
    "cumulative_sum",          "cumulative_sum_checked", "cumulative_prod",
    "cumulative_prod_checked", "cumulative_min",         "cumulative_max",
    "cumulative_mean"};

TEST(TestCumulative, Empty) {
  for (auto function : kCumulativeFunctionNames) {
    CumulativeOptions options;
    for (auto ty : NumericTypes()) {
      auto return_ty = std::string(function) == "cumulative_mean" ? float64() : ty;
      auto empty_arr = ArrayFromJSON(ty, "[]");
      auto expected_arr = ArrayFromJSON(return_ty, "[]");
      auto empty_chunked = ChunkedArrayFromJSON(ty, {"[]"});
      auto expected_chunked = ChunkedArrayFromJSON(return_ty, {"[]"});
      CheckVectorUnary(function, empty_arr, expected_arr, &options);

      CheckVectorUnary(function, empty_chunked, expected_chunked, &options);
    }
  }
}

TEST(TestCumulative, AllNulls) {
  for (auto function : kCumulativeFunctionNames) {
    CumulativeOptions options;
    for (auto ty : NumericTypes()) {
      auto return_ty = std::string(function) == "cumulative_mean" ? float64() : ty;
      auto nulls_arr = ArrayFromJSON(ty, "[null, null, null]");
      auto expected_arr = ArrayFromJSON(return_ty, "[null, null, null]");
      auto nulls_one_chunk = ChunkedArrayFromJSON(ty, {"[null, null, null]"});
      auto expected_one_chunk = ChunkedArrayFromJSON(return_ty, {"[null, null, null]"});
      auto nulls_three_chunks = ChunkedArrayFromJSON(ty, {"[null]", "[null]", "[null]"});
      auto expected_three_chunks =
          ChunkedArrayFromJSON(return_ty, {"[null]", "[null]", "[null]"});
      CheckVectorUnary(function, nulls_arr, expected_arr, &options);

      CheckVectorUnary(function, nulls_one_chunk, expected_one_chunk, &options);

      CheckVectorUnary(function, nulls_three_chunks, expected_one_chunk, &options);
    }
  }
}

TEST(TestCumulativeSum, ScalarInput) {
  CumulativeOptions no_start_no_skip;
  CumulativeOptions no_start_do_skip(0, true);
  CumulativeOptions has_start_no_skip(10.0);
  CumulativeOptions has_start_do_skip(10, true);

  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_sum", ScalarFromJSON(ty, "10"),
                     ArrayFromJSON(ty, "[10]"), &no_start_no_skip);
    CheckVectorUnary("cumulative_sum_checked", ScalarFromJSON(ty, "10"),
                     ArrayFromJSON(ty, "[10]"), &no_start_no_skip);

    CheckVectorUnary("cumulative_sum", ScalarFromJSON(ty, "10"),
                     ArrayFromJSON(ty, "[20]"), &has_start_no_skip);
    CheckVectorUnary("cumulative_sum_checked", ScalarFromJSON(ty, "10"),
                     ArrayFromJSON(ty, "[20]"), &has_start_no_skip);

    CheckVectorUnary("cumulative_sum", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &no_start_no_skip);
    CheckVectorUnary("cumulative_sum_checked", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &no_start_no_skip);
    CheckVectorUnary("cumulative_sum", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &has_start_no_skip);
    CheckVectorUnary("cumulative_sum_checked", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &has_start_no_skip);

    CheckVectorUnary("cumulative_sum", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &no_start_do_skip);
    CheckVectorUnary("cumulative_sum_checked", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &no_start_do_skip);
    CheckVectorUnary("cumulative_sum", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &has_start_do_skip);
    CheckVectorUnary("cumulative_sum_checked", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &has_start_do_skip);
  }
}

TEST(TestCumulativeProd, ScalarInput) {
  CumulativeOptions no_start_no_skip;
  CumulativeOptions no_start_do_skip(1, true);
  CumulativeOptions has_start_no_skip(10.0);
  CumulativeOptions has_start_do_skip(10, true);

  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_prod", ScalarFromJSON(ty, "10"),
                     ArrayFromJSON(ty, "[10]"), &no_start_no_skip);
    CheckVectorUnary("cumulative_prod_checked", ScalarFromJSON(ty, "10"),
                     ArrayFromJSON(ty, "[10]"), &no_start_no_skip);

    CheckVectorUnary("cumulative_prod", ScalarFromJSON(ty, "10"),
                     ArrayFromJSON(ty, "[100]"), &has_start_no_skip);
    CheckVectorUnary("cumulative_prod_checked", ScalarFromJSON(ty, "10"),
                     ArrayFromJSON(ty, "[100]"), &has_start_no_skip);

    CheckVectorUnary("cumulative_prod", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &no_start_no_skip);
    CheckVectorUnary("cumulative_prod_checked", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &no_start_no_skip);
    CheckVectorUnary("cumulative_prod", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &has_start_no_skip);
    CheckVectorUnary("cumulative_prod_checked", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &has_start_no_skip);

    CheckVectorUnary("cumulative_prod", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &no_start_do_skip);
    CheckVectorUnary("cumulative_prod_checked", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &no_start_do_skip);
    CheckVectorUnary("cumulative_prod", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &has_start_do_skip);
    CheckVectorUnary("cumulative_prod_checked", ScalarFromJSON(ty, "null"),
                     ArrayFromJSON(ty, "[null]"), &has_start_do_skip);
  }
}

using testing::HasSubstr;

template <typename ArrowType>
void CheckCumulativeSumUnsignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  CumulativeOptions pos_overflow(1.0);
  auto max = std::numeric_limits<CType>::max();
  auto min = std::numeric_limits<CType>::lowest();

  BuilderType builder;
  std::shared_ptr<Array> max_arr;
  std::shared_ptr<Array> min_arr;
  ASSERT_OK(builder.Append(max));
  ASSERT_OK(builder.Finish(&max_arr));
  builder.Reset();
  ASSERT_OK(builder.Append(min));
  ASSERT_OK(builder.Finish(&min_arr));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, HasSubstr("overflow"),
      CallFunction("cumulative_sum_checked", {max_arr}, &pos_overflow));
  CheckVectorUnary("cumulative_sum", max_arr, min_arr, &pos_overflow);
}

template <typename ArrowType>
void CheckCumulativeSumSignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  CheckCumulativeSumUnsignedOverflow<ArrowType>();

  CumulativeOptions neg_overflow(-1.0);
  auto max = std::numeric_limits<CType>::max();
  auto min = std::numeric_limits<CType>::lowest();

  BuilderType builder;
  std::shared_ptr<Array> max_arr;
  std::shared_ptr<Array> min_arr;
  ASSERT_OK(builder.Append(max));
  ASSERT_OK(builder.Finish(&max_arr));
  builder.Reset();
  ASSERT_OK(builder.Append(min));
  ASSERT_OK(builder.Finish(&min_arr));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, HasSubstr("overflow"),
      CallFunction("cumulative_sum_checked", {min_arr}, &neg_overflow));
  CheckVectorUnary("cumulative_sum", min_arr, max_arr, &neg_overflow);
}

TEST(TestCumulativeSum, IntegerOverflow) {
  CheckCumulativeSumUnsignedOverflow<UInt8Type>();
  CheckCumulativeSumUnsignedOverflow<UInt16Type>();
  CheckCumulativeSumUnsignedOverflow<UInt32Type>();
  CheckCumulativeSumUnsignedOverflow<UInt64Type>();
  CheckCumulativeSumSignedOverflow<Int8Type>();
  CheckCumulativeSumSignedOverflow<Int16Type>();
  CheckCumulativeSumSignedOverflow<Int32Type>();
  CheckCumulativeSumSignedOverflow<Int64Type>();
}

template <typename ArrowType>
void CheckCumulativeProdUnsignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  CumulativeOptions pos_overflow(2.0);
  auto max = std::numeric_limits<CType>::max();
  auto min = std::numeric_limits<CType>::lowest();

  BuilderType builder;
  std::shared_ptr<Array> half_max_arr;
  std::shared_ptr<Array> min_arr;
  ASSERT_OK(builder.Append(max / 2 + 1));  // 2 * (max / 2 + 1) overflows to min
  ASSERT_OK(builder.Finish(&half_max_arr));
  builder.Reset();
  ASSERT_OK(builder.Append(min));
  ASSERT_OK(builder.Finish(&min_arr));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, HasSubstr("overflow"),
      CallFunction("cumulative_prod_checked", {half_max_arr}, &pos_overflow));
  CheckVectorUnary("cumulative_prod", half_max_arr, min_arr, &pos_overflow);
}

template <typename ArrowType>
void CheckCumulativeProdSignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  CheckCumulativeSumUnsignedOverflow<ArrowType>();

  CumulativeOptions neg_overflow(-1.0);  // min * -1 overflows to min
  auto min = std::numeric_limits<CType>::lowest();

  BuilderType builder;
  std::shared_ptr<Array> min_arr;
  builder.Reset();
  ASSERT_OK(builder.Append(min));
  ASSERT_OK(builder.Finish(&min_arr));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, HasSubstr("overflow"),
      CallFunction("cumulative_prod_checked", {min_arr}, &neg_overflow));
  CheckVectorUnary("cumulative_prod", min_arr, min_arr, &neg_overflow);
}

TEST(TestCumulativeProd, IntegerOverflow) {
  CheckCumulativeProdUnsignedOverflow<UInt8Type>();
  CheckCumulativeProdUnsignedOverflow<UInt16Type>();
  CheckCumulativeProdUnsignedOverflow<UInt32Type>();
  CheckCumulativeProdUnsignedOverflow<UInt64Type>();
  CheckCumulativeProdSignedOverflow<Int8Type>();
  CheckCumulativeProdSignedOverflow<Int16Type>();
  CheckCumulativeProdSignedOverflow<Int32Type>();
  CheckCumulativeProdSignedOverflow<Int64Type>();
}

TEST(TestCumulativeSum, NoStartNoSkip) {
  CumulativeOptions options;
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_sum", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6]"),
                     ArrayFromJSON(ty, "[1, 3, 6, 10, 15, 21]"), &options);
    CheckVectorUnary("cumulative_sum_checked", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6]"),
                     ArrayFromJSON(ty, "[1, 3, 6, 10, 15, 21]"), &options);

    CheckVectorUnary("cumulative_sum", ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[1, 3, null, null, null, null]"), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[1, 3, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_sum", ArrayFromJSON(ty, "[null, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[null, null, null, null, null, null]"), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ArrayFromJSON(ty, "[null, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[null, null, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_sum",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 3, 6, 10, 15, 21]"}), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 3, 6, 10, 15, 21]"}), &options);

    CheckVectorUnary(
        "cumulative_sum", ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[1, 3, null, null, null, null]"}), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 3, null, null, null, null]"}),
                     &options);

    CheckVectorUnary(
        "cumulative_sum", ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[null, null, null, null, null, null]"}), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[null, null, null, null, null, null]"}),
                     &options);
  }
}

TEST(TestCumulativeSum, HasStartNoSkip) {
  CumulativeOptions options(10.0);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_sum", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6]"),
                     ArrayFromJSON(ty, "[11, 13, 16, 20, 25, 31]"), &options);
    CheckVectorUnary("cumulative_sum_checked", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6]"),
                     ArrayFromJSON(ty, "[11, 13, 16, 20, 25, 31]"), &options);

    CheckVectorUnary("cumulative_sum", ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[11, 13, null, null, null, null]"), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[11, 13, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_sum", ArrayFromJSON(ty, "[null, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[null, null, null, null, null, null]"), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ArrayFromJSON(ty, "[null, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[null, null, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_sum",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[11, 13, 16, 20, 25, 31]"}), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[11, 13, 16, 20, 25, 31]"}), &options);

    CheckVectorUnary(
        "cumulative_sum", ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[11, 13, null, null, null, null]"}), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[11, 13, null, null, null, null]"}),
                     &options);

    CheckVectorUnary(
        "cumulative_sum", ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[null, null, null, null, null, null]"}), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[null, null, null, null, null, null]"}),
                     &options);
  }
}

TEST(TestCumulativeSum, HasStartDoSkip) {
  CumulativeOptions options(10, true);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_sum", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6]"),
                     ArrayFromJSON(ty, "[11, 13, 16, 20, 25, 31]"), &options);
    CheckVectorUnary("cumulative_sum_checked", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6]"),
                     ArrayFromJSON(ty, "[11, 13, 16, 20, 25, 31]"), &options);

    CheckVectorUnary("cumulative_sum", ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[11, 13, null, 17, null, 23]"), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[11, 13, null, 17, null, 23]"), &options);

    CheckVectorUnary("cumulative_sum", ArrayFromJSON(ty, "[null, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[null, 12, null, 16, null, 22]"), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ArrayFromJSON(ty, "[null, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[null, 12, null, 16, null, 22]"), &options);

    CheckVectorUnary("cumulative_sum",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[11, 13, 16, 20, 25, 31]"}), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[11, 13, 16, 20, 25, 31]"}), &options);

    CheckVectorUnary(
        "cumulative_sum", ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[11, 13, null, 17, null, 23]"}), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[11, 13, null, 17, null, 23]"}),
                     &options);

    CheckVectorUnary(
        "cumulative_sum", ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[null, 12, null, 16, null, 22]"}), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 12, null, 16, null, 22]"}),
                     &options);
  }
}

TEST(TestCumulativeSum, NoStartDoSkip) {
  CumulativeOptions options(0, true);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_sum", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6]"),
                     ArrayFromJSON(ty, "[1, 3, 6, 10, 15, 21]"), &options);
    CheckVectorUnary("cumulative_sum_checked", ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6]"),
                     ArrayFromJSON(ty, "[1, 3, 6, 10, 15, 21]"), &options);

    CheckVectorUnary("cumulative_sum", ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[1, 3, null, 7, null, 13]"), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[1, 3, null, 7, null, 13]"), &options);

    CheckVectorUnary("cumulative_sum", ArrayFromJSON(ty, "[null, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[null, 2, null, 6, null, 12]"), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ArrayFromJSON(ty, "[null, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[null, 2, null, 6, null, 12]"), &options);

    CheckVectorUnary("cumulative_sum",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 3, 6, 10, 15, 21]"}), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 3, 6, 10, 15, 21]"}), &options);

    CheckVectorUnary("cumulative_sum",
                     ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 3, null, 7, null, 13]"}), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 3, null, 7, null, 13]"}), &options);

    CheckVectorUnary(
        "cumulative_sum", ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[null, 2, null, 6, null, 12]"}), &options);
    CheckVectorUnary("cumulative_sum_checked",
                     ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 2, null, 6, null, 12]"}),
                     &options);
  }
}

TEST(TestCumulativeProd, NoStartNoSkip) {
  CumulativeOptions options;
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_prod", ArrayFromJSON(ty, "[1, 2, 3, 4, 5]"),
                     ArrayFromJSON(ty, "[1, 2, 6, 24, 120]"), &options);
    CheckVectorUnary("cumulative_prod_checked", ArrayFromJSON(ty, "[1, 2, 3, 4, 5]"),
                     ArrayFromJSON(ty, "[1, 2, 6, 24, 120]"), &options);

    CheckVectorUnary("cumulative_prod", ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[1, 2, null, null, null, null]"), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ArrayFromJSON(ty, "[1, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[1, 2, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_prod", ArrayFromJSON(ty, "[null, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[null, null, null, null, null, null]"), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ArrayFromJSON(ty, "[null, 2, null, 4, null, 6]"),
                     ArrayFromJSON(ty, "[null, null, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_prod", ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, 6, 24, 120]"}), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, 6, 24, 120]"}), &options);

    CheckVectorUnary(
        "cumulative_prod", ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[1, 2, null, null, null, null]"}), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, null, null, null, null]"}),
                     &options);

    CheckVectorUnary(
        "cumulative_prod", ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[null, null, null, null, null, null]"}), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[null, null, null, null, null, null]"}),
                     &options);
  }
}

TEST(TestCumulativeProd, HasStartNoSkip) {
  CumulativeOptions options(2.0);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_prod", ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                     ArrayFromJSON(ty, "[2, 4, 12, 48]"), &options);
    CheckVectorUnary("cumulative_prod_checked", ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                     ArrayFromJSON(ty, "[2, 4, 12, 48]"), &options);

    CheckVectorUnary("cumulative_prod", ArrayFromJSON(ty, "[1, 2, null, 4]"),
                     ArrayFromJSON(ty, "[2, 4, null, null]"), &options);
    CheckVectorUnary("cumulative_prod_checked", ArrayFromJSON(ty, "[1, 2, null, 4]"),
                     ArrayFromJSON(ty, "[2, 4, null, null]"), &options);

    CheckVectorUnary("cumulative_prod", ArrayFromJSON(ty, "[null, 2, null, 4]"),
                     ArrayFromJSON(ty, "[null, null, null, null]"), &options);
    CheckVectorUnary("cumulative_prod_checked", ArrayFromJSON(ty, "[null, 2, null, 4]"),
                     ArrayFromJSON(ty, "[null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_prod", ChunkedArrayFromJSON(ty, {"[1, 2]", "[3, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[2, 4, 12, 48]"}), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2]", "[3, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[2, 4, 12, 48]"}), &options);

    CheckVectorUnary("cumulative_prod", ChunkedArrayFromJSON(ty, {"[1, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[2, 4, null, null]"}), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[2, 4, null, null]"}), &options);

    CheckVectorUnary("cumulative_prod",
                     ChunkedArrayFromJSON(ty, {"[null, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[null, null, null, null]"}), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ChunkedArrayFromJSON(ty, {"[null, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[null, null, null, null]"}), &options);
  }
}

TEST(TestCumulativeProd, HasStartDoSkip) {
  CumulativeOptions options(2.0, true);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_prod", ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                     ArrayFromJSON(ty, "[2, 4, 12, 48]"), &options);
    CheckVectorUnary("cumulative_prod_checked", ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                     ArrayFromJSON(ty, "[2, 4, 12, 48]"), &options);

    CheckVectorUnary("cumulative_prod", ArrayFromJSON(ty, "[1, 2, null, 4]"),
                     ArrayFromJSON(ty, "[2, 4, null, 16]"), &options);
    CheckVectorUnary("cumulative_prod_checked", ArrayFromJSON(ty, "[1, 2, null, 4]"),
                     ArrayFromJSON(ty, "[2, 4, null, 16]"), &options);

    CheckVectorUnary("cumulative_prod", ArrayFromJSON(ty, "[null, 2, null, 4]"),
                     ArrayFromJSON(ty, "[null, 4, null, 16]"), &options);
    CheckVectorUnary("cumulative_prod_checked", ArrayFromJSON(ty, "[null, 2, null, 4]"),
                     ArrayFromJSON(ty, "[null, 4, null, 16]"), &options);

    CheckVectorUnary("cumulative_prod", ChunkedArrayFromJSON(ty, {"[1, 2]", "[3, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[2, 4, 12, 48]"}), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2]", "[3, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[2, 4, 12, 48]"}), &options);

    CheckVectorUnary("cumulative_prod", ChunkedArrayFromJSON(ty, {"[1, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[2, 4, null, 16]"}), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[2, 4, null, 16]"}), &options);

    CheckVectorUnary("cumulative_prod",
                     ChunkedArrayFromJSON(ty, {"[null, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 4, null, 16]"}), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ChunkedArrayFromJSON(ty, {"[null, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 4, null, 16]"}), &options);
  }
}

TEST(TestCumulativeProd, NoStartDoSkip) {
  CumulativeOptions options(true);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_prod", ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                     ArrayFromJSON(ty, "[1, 2, 6, 24]"), &options);
    CheckVectorUnary("cumulative_prod_checked", ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                     ArrayFromJSON(ty, "[1, 2, 6, 24]"), &options);

    CheckVectorUnary("cumulative_prod", ArrayFromJSON(ty, "[1, 2, null, 4]"),
                     ArrayFromJSON(ty, "[1, 2, null, 8]"), &options);
    CheckVectorUnary("cumulative_prod_checked", ArrayFromJSON(ty, "[1, 2, null, 4]"),
                     ArrayFromJSON(ty, "[1, 2, null, 8]"), &options);

    CheckVectorUnary("cumulative_prod", ArrayFromJSON(ty, "[null, 2, null, 4]"),
                     ArrayFromJSON(ty, "[null, 2, null, 8]"), &options);
    CheckVectorUnary("cumulative_prod_checked", ArrayFromJSON(ty, "[null, 2, null, 4]"),
                     ArrayFromJSON(ty, "[null, 2, null, 8]"), &options);

    CheckVectorUnary("cumulative_prod", ChunkedArrayFromJSON(ty, {"[1, 2]", "[3, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, 6, 24]"}), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2]", "[3, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, 6, 24]"}), &options);

    CheckVectorUnary("cumulative_prod", ChunkedArrayFromJSON(ty, {"[1, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, null, 8]"}), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, null, 8]"}), &options);

    CheckVectorUnary("cumulative_prod",
                     ChunkedArrayFromJSON(ty, {"[null, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 2, null, 8]"}), &options);
    CheckVectorUnary("cumulative_prod_checked",
                     ChunkedArrayFromJSON(ty, {"[null, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 2, null, 8]"}), &options);
  }
}

TEST(TestCumulativeMax, NoStartNoSkip) {
  CumulativeOptions options;
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_max", ArrayFromJSON(ty, "[2, 1, 3, 5, 4, 6]"),
                     ArrayFromJSON(ty, "[2, 2, 3, 5, 5, 6]"), &options);

    CheckVectorUnary("cumulative_max", ArrayFromJSON(ty, "[2, 1, null, 5, null, 6]"),
                     ArrayFromJSON(ty, "[2, 2, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_max", ArrayFromJSON(ty, "[null, 1, null, 5, null, 6]"),
                     ArrayFromJSON(ty, "[null, null, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_max",
                     ChunkedArrayFromJSON(ty, {"[2, 1, 3]", "[5, 4, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[2, 2, 3, 5, 5, 6]"}), &options);

    CheckVectorUnary(
        "cumulative_max", ChunkedArrayFromJSON(ty, {"[2, 1, null]", "[5, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[2, 2, null, null, null, null]"}), &options);

    CheckVectorUnary(
        "cumulative_max", ChunkedArrayFromJSON(ty, {"[null, 1, null]", "[5, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[null, null, null, null, null, null]"}), &options);
  }
}

TEST(TestCumulativeMax, HasStartNoSkip) {
  CumulativeOptions options(3.0);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_max", ArrayFromJSON(ty, "[2, 1, 3, 5, 4, 6]"),
                     ArrayFromJSON(ty, "[3, 3, 3, 5, 5, 6]"), &options);

    CheckVectorUnary("cumulative_max", ArrayFromJSON(ty, "[2, 1, null, 5, null, 6]"),
                     ArrayFromJSON(ty, "[3, 3, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_max", ArrayFromJSON(ty, "[null, 1, null, 5, null, 6]"),
                     ArrayFromJSON(ty, "[null, null, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_max",
                     ChunkedArrayFromJSON(ty, {"[2, 1, 3]", "[5, 4, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[3, 3, 3, 5, 5, 6]"}), &options);

    CheckVectorUnary(
        "cumulative_max", ChunkedArrayFromJSON(ty, {"[2, 1, null]", "[5, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[3, 3, null, null, null, null]"}), &options);

    CheckVectorUnary(
        "cumulative_max", ChunkedArrayFromJSON(ty, {"[null, 1, null]", "[5, null, 6]"}),
        ChunkedArrayFromJSON(ty, {"[null, null, null, null, null, null]"}), &options);
  }
}

TEST(TestCumulativeMax, HasStartDoSkip) {
  CumulativeOptions options(3.0, true);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_max", ArrayFromJSON(ty, "[2, 1, 3, 5, 4, 6]"),
                     ArrayFromJSON(ty, "[3, 3, 3, 5, 5, 6]"), &options);

    CheckVectorUnary("cumulative_max", ArrayFromJSON(ty, "[2, 1, null, 5, null, 6]"),
                     ArrayFromJSON(ty, "[3, 3, null, 5, null, 6]"), &options);

    CheckVectorUnary("cumulative_max", ArrayFromJSON(ty, "[null, 1, null, 5, null, 6]"),
                     ArrayFromJSON(ty, "[null, 3, null, 5, null, 6]"), &options);

    CheckVectorUnary("cumulative_max",
                     ChunkedArrayFromJSON(ty, {"[2, 1, 3]", "[5, 4, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[3, 3, 3, 5, 5, 6]"}), &options);

    CheckVectorUnary("cumulative_max",
                     ChunkedArrayFromJSON(ty, {"[2, 1, null]", "[5, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[3, 3, null, 5, null, 6]"}), &options);

    CheckVectorUnary("cumulative_max",
                     ChunkedArrayFromJSON(ty, {"[null, 1, null]", "[5, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 3, null, 5, null, 6]"}), &options);
  }
}

TEST(TestCumulativeMax, NoStartDoSkip) {
  CumulativeOptions options(true);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_max", ArrayFromJSON(ty, "[2, 1, 3, 5, 4, 6]"),
                     ArrayFromJSON(ty, "[2, 2, 3, 5, 5, 6]"), &options);

    CheckVectorUnary("cumulative_max", ArrayFromJSON(ty, "[2, 1, null, 5, null, 6]"),
                     ArrayFromJSON(ty, "[2, 2, null, 5, null, 6]"), &options);

    CheckVectorUnary("cumulative_max", ArrayFromJSON(ty, "[null, 1, null, 5, null, 6]"),
                     ArrayFromJSON(ty, "[null, 1, null, 5, null, 6]"), &options);

    CheckVectorUnary("cumulative_max",
                     ChunkedArrayFromJSON(ty, {"[2, 1, 3]", "[5, 4, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[2, 2, 3, 5, 5, 6]"}), &options);

    CheckVectorUnary("cumulative_max",
                     ChunkedArrayFromJSON(ty, {"[2, 1, null]", "[5, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[2, 2, null, 5, null, 6]"}), &options);

    CheckVectorUnary("cumulative_max",
                     ChunkedArrayFromJSON(ty, {"[null, 1, null]", "[5, null, 6]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 1, null, 5, null, 6]"}), &options);
  }
}

TEST(TestCumulativeMin, NoStartNoSkip) {
  CumulativeOptions options;
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_min", ArrayFromJSON(ty, "[5, 6, 4, 2, 3, 1]"),
                     ArrayFromJSON(ty, "[5, 5, 4, 2, 2, 1]"), &options);

    CheckVectorUnary("cumulative_min", ArrayFromJSON(ty, "[5, 6, null, 2, null, 1]"),
                     ArrayFromJSON(ty, "[5, 5, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_min", ArrayFromJSON(ty, "[null, 6, null, 2, null, 1]"),
                     ArrayFromJSON(ty, "[null, null, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_min",
                     ChunkedArrayFromJSON(ty, {"[5, 6, 4]", "[2, 3, 1]"}),
                     ChunkedArrayFromJSON(ty, {"[5, 5, 4, 2, 2, 1]"}), &options);

    CheckVectorUnary(
        "cumulative_min", ChunkedArrayFromJSON(ty, {"[5, 6, null]", "[2, null, 1]"}),
        ChunkedArrayFromJSON(ty, {"[5, 5, null, null, null, null]"}), &options);

    CheckVectorUnary(
        "cumulative_min", ChunkedArrayFromJSON(ty, {"[null, 6, null]", "[2, null, 1]"}),
        ChunkedArrayFromJSON(ty, {"[null, null, null, null, null, null]"}), &options);
  }
}

TEST(TestCumulativeMin, HasStartNoSkip) {
  CumulativeOptions options(3.0);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_min", ArrayFromJSON(ty, "[5, 6, 4, 2, 3, 1]"),
                     ArrayFromJSON(ty, "[3, 3, 3, 2, 2, 1]"), &options);

    CheckVectorUnary("cumulative_min", ArrayFromJSON(ty, "[5, 6, null, 2, null, 1]"),
                     ArrayFromJSON(ty, "[3, 3, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_min", ArrayFromJSON(ty, "[null, 6, null, 2, null, 1]"),
                     ArrayFromJSON(ty, "[null, null, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_min",
                     ChunkedArrayFromJSON(ty, {"[5, 6, 4]", "[2, 3, 1]"}),
                     ChunkedArrayFromJSON(ty, {"[3, 3, 3, 2, 2, 1]"}), &options);

    CheckVectorUnary(
        "cumulative_min", ChunkedArrayFromJSON(ty, {"[5, 6, null]", "[2, null, 1]"}),
        ChunkedArrayFromJSON(ty, {"[3, 3, null, null, null, null]"}), &options);

    CheckVectorUnary(
        "cumulative_min", ChunkedArrayFromJSON(ty, {"[null, 6, null]", "[2, null, 1]"}),
        ChunkedArrayFromJSON(ty, {"[null, null, null, null, null, null]"}), &options);
  }
}

TEST(TestCumulativeMin, HasStartDoSkip) {
  CumulativeOptions options(3.0, true);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_min", ArrayFromJSON(ty, "[5, 6, 4, 2, 3, 1]"),
                     ArrayFromJSON(ty, "[3, 3, 3, 2, 2, 1]"), &options);

    CheckVectorUnary("cumulative_min", ArrayFromJSON(ty, "[5, 6, null, 2, null, 1]"),
                     ArrayFromJSON(ty, "[3, 3, null, 2, null, 1]"), &options);

    CheckVectorUnary("cumulative_min", ArrayFromJSON(ty, "[null, 6, null, 2, null, 1]"),
                     ArrayFromJSON(ty, "[null, 3, null, 2, null, 1]"), &options);

    CheckVectorUnary("cumulative_min",
                     ChunkedArrayFromJSON(ty, {"[5, 6, 4]", "[2, 3, 1]"}),
                     ChunkedArrayFromJSON(ty, {"[3, 3, 3, 2, 2, 1]"}), &options);

    CheckVectorUnary("cumulative_min",
                     ChunkedArrayFromJSON(ty, {"[5, 6, null]", "[2, null, 1]"}),
                     ChunkedArrayFromJSON(ty, {"[3, 3, null, 2, null, 1]"}), &options);

    CheckVectorUnary("cumulative_min",
                     ChunkedArrayFromJSON(ty, {"[null, 6, null]", "[2, null, 1]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 3, null, 2, null, 1]"}), &options);
  }
}

TEST(TestCumulativeMin, NoStartDoSkip) {
  CumulativeOptions options(true);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_min", ArrayFromJSON(ty, "[5, 6, 4, 2, 3, 1]"),
                     ArrayFromJSON(ty, "[5, 5, 4, 2, 2, 1]"), &options);

    CheckVectorUnary("cumulative_min", ArrayFromJSON(ty, "[5, 6, null, 2, null, 1]"),
                     ArrayFromJSON(ty, "[5, 5, null, 2, null, 1]"), &options);

    CheckVectorUnary("cumulative_min", ArrayFromJSON(ty, "[null, 6, null, 2, null, 1]"),
                     ArrayFromJSON(ty, "[null, 6, null, 2, null, 1]"), &options);

    CheckVectorUnary("cumulative_min",
                     ChunkedArrayFromJSON(ty, {"[5, 6, 4]", "[2, 3, 1]"}),
                     ChunkedArrayFromJSON(ty, {"[5, 5, 4, 2, 2, 1]"}), &options);

    CheckVectorUnary("cumulative_min",
                     ChunkedArrayFromJSON(ty, {"[5, 6, null]", "[2, null, 1]"}),
                     ChunkedArrayFromJSON(ty, {"[5, 5, null, 2, null, 1]"}), &options);

    CheckVectorUnary("cumulative_min",
                     ChunkedArrayFromJSON(ty, {"[null, 6, null]", "[2, null, 1]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 6, null, 2, null, 1]"}), &options);
  }
}

TEST(TestCumulativeMean, NoSkip) {
  CumulativeOptions options(false);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_mean", ArrayFromJSON(ty, "[5, 6, 4, 2, 3, 1]"),
                     ArrayFromJSON(float64(), "[5, 5.5, 5, 4.25, 4, 3.5]"), &options);

    CheckVectorUnary("cumulative_mean", ArrayFromJSON(ty, "[5, 6, null, 2, null, 1]"),
                     ArrayFromJSON(float64(), "[5, 5.5, null, null, null, null]"),
                     &options);

    CheckVectorUnary("cumulative_mean", ArrayFromJSON(ty, "[null, 6, null, 2, null, 1]"),
                     ArrayFromJSON(float64(), "[null, null, null, null, null, null]"),
                     &options);

    CheckVectorUnary(
        "cumulative_mean", ChunkedArrayFromJSON(ty, {"[5, 6, 4]", "[2, 3, 1]"}),
        ChunkedArrayFromJSON(float64(), {"[5, 5.5, 5, 4.25, 4, 3.5]"}), &options);

    CheckVectorUnary(
        "cumulative_mean", ChunkedArrayFromJSON(ty, {"[5, 6, null]", "[2, null, 1]"}),
        ChunkedArrayFromJSON(float64(), {"[5, 5.5, null, null, null, null]"}), &options);

    CheckVectorUnary(
        "cumulative_mean", ChunkedArrayFromJSON(ty, {"[null, 6, null]", "[2, null, 1]"}),
        ChunkedArrayFromJSON(float64(), {"[null, null, null, null, null, null]"}),
        &options);
  }
}

TEST(TestCumulativeMean, DoSkip) {
  CumulativeOptions options(true);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_mean", ArrayFromJSON(ty, "[5, 6, 4, 2, 3, 1]"),
                     ArrayFromJSON(float64(), "[5, 5.5, 5, 4.25, 4, 3.5]"), &options);

    CheckVectorUnary(
        "cumulative_mean", ArrayFromJSON(ty, "[5, 6, null, 2, null, 1]"),
        ArrayFromJSON(float64(), "[5, 5.5, null, 4.333333333333333, null, 3.5]"),
        &options);

    CheckVectorUnary("cumulative_mean", ArrayFromJSON(ty, "[null, 6, null, 2, null, 1]"),
                     ArrayFromJSON(float64(), "[null, 6, null, 4, null, 3]"), &options);

    CheckVectorUnary(
        "cumulative_mean", ChunkedArrayFromJSON(ty, {"[5, 6, 4]", "[2, 3, 1]"}),
        ChunkedArrayFromJSON(float64(), {"[5, 5.5, 5, 4.25, 4, 3.5]"}), &options);

    CheckVectorUnary(
        "cumulative_mean", ChunkedArrayFromJSON(ty, {"[5, 6, null]", "[2, null, 1]"}),
        ChunkedArrayFromJSON(float64(), {"[5, 5.5, null, 4.333333333333333, null, 3.5]"}),
        &options);

    CheckVectorUnary(
        "cumulative_mean", ChunkedArrayFromJSON(ty, {"[null, 6, null]", "[2, null, 1]"}),
        ChunkedArrayFromJSON(float64(), {"[null, 6, null, 4, null, 3]"}), &options);
  }
}

TEST(TestCumulativeMean, StartValue) {
  CumulativeOptions options(3, true);  // start should be ignored
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_mean", ArrayFromJSON(ty, "[5, 6, 4, 2, 3, 1]"),
                     ArrayFromJSON(float64(), "[5, 5.5, 5, 4.25, 4, 3.5]"), &options);
  }
}

TEST(TestCumulativeSum, ConvenienceFunctionCheckOverflow) {
  ASSERT_ARRAYS_EQUAL(*CumulativeSum(ArrayFromJSON(int8(), "[127, 1]"),
                                     CumulativeOptions::Defaults(), false)
                           ->make_array(),
                      *ArrayFromJSON(int8(), "[127, -128]"));

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("overflow"),
                                  CumulativeSum(ArrayFromJSON(int8(), "[127, 1]"),
                                                CumulativeOptions::Defaults(), true));
}

TEST(TestCumulativeProd, ConvenienceFunctionCheckOverflow) {
  ASSERT_ARRAYS_EQUAL(*CumulativeProd(ArrayFromJSON(int8(), "[-128, -1]"),
                                      CumulativeOptions::Defaults(), false)
                           ->make_array(),
                      *ArrayFromJSON(int8(), "[-128, -128]"));

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("overflow"),
                                  CumulativeSum(ArrayFromJSON(int8(), "[-128, -1]"),
                                                CumulativeOptions::Defaults(), true));
}

TEST(TestCumulativeMax, ConvenienceFunction) {
  ASSERT_ARRAYS_EQUAL(
      *CumulativeMax(ArrayFromJSON(int8(), "[1, 2, 3]"), CumulativeOptions::Defaults())
           ->make_array(),
      *ArrayFromJSON(int8(), "[1, 2, 3]"));
}

TEST(TestCumulativeMin, ConvenienceFunction) {
  ASSERT_ARRAYS_EQUAL(
      *CumulativeMin(ArrayFromJSON(int8(), "[-1, -2, -3]"), CumulativeOptions::Defaults())
           ->make_array(),
      *ArrayFromJSON(int8(), "[-1, -2, -3]"));
}

TEST(TestCumulativeMean, ConvenienceFunction) {
  ASSERT_ARRAYS_EQUAL(*CumulativeMean(ArrayFromJSON(int8(), "[-1, -2, -3]"),
                                      CumulativeOptions::Defaults())
                           ->make_array(),
                      *ArrayFromJSON(float64(), "[-1, -1.5, -2]"));
}

TEST(TestCumulative, NaN) {
  // addition with NaN is always NaN
  CheckVectorUnary("cumulative_sum", ArrayFromJSON(float64(), "[1, 2, NaN, 4, 5]"),
                   ArrayFromJSON(float64(), "[1, 3, NaN, NaN, NaN]"));

  // multiply with Nan is always NaN
  CheckVectorUnary("cumulative_prod", ArrayFromJSON(float64(), "[1, 2, NaN, 4, 5]"),
                   ArrayFromJSON(float64(), "[1, 2, NaN, NaN, NaN]"));

  // max with NaN is always ignored because Nan > a always returns false
  CheckVectorUnary("cumulative_max", ArrayFromJSON(float64(), "[1, 2, NaN, 4, 5]"),
                   ArrayFromJSON(float64(), "[1, 2, 2, 4, 5]"));

  // min with NaN is always ignored because Nan < a always returns false
  CheckVectorUnary("cumulative_min", ArrayFromJSON(float64(), "[5, 4, NaN, 2, 1]"),
                   ArrayFromJSON(float64(), "[5, 4, 4, 2, 1]"));

  // mean with NaN is always Nan
  CheckVectorUnary("cumulative_mean", ArrayFromJSON(float64(), "[5, 4, NaN, 2, 1]"),
                   ArrayFromJSON(float64(), "[5, 4.5, NaN, NaN, NaN]"));
}
}  // namespace compute
}  // namespace arrow
