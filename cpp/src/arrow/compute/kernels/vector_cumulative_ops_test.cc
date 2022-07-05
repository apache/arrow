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

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"

namespace arrow {
namespace compute {

void TestEmpty(std::string func, FunctionOptions* options) {
  for (auto ty : NumericTypes()) {
    auto empty_arr = ArrayFromJSON(ty, "[]");
    auto empty_chunked = ChunkedArrayFromJSON(ty, {"[]"});
    CheckVectorUnary(func, empty_arr, empty_arr, options);
    CheckVectorUnary(func, empty_chunked, empty_chunked, options);
  }
}

void TestAllNulls(std::string func, FunctionOptions* options) {
  for (auto ty : NumericTypes()) {
    auto nulls_arr = ArrayFromJSON(ty, "[null, null, null]");
    auto nulls_one_chunk = ChunkedArrayFromJSON(ty, {"[null, null, null]"});
    auto nulls_three_chunks = ChunkedArrayFromJSON(ty, {"[null]", "[null]", "[null]"});
    CheckVectorUnary(func, nulls_arr, nulls_arr, options);
    CheckVectorUnary(func, nulls_one_chunk, nulls_one_chunk, options);
    CheckVectorUnary(func, nulls_three_chunks, nulls_one_chunk, options);
  }
}

using testing::HasSubstr;

void TestScalarNotSupported(std::string func, FunctionOptions* options) {
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      NotImplemented, HasSubstr("no kernel"),
      CallFunction(func, {std::make_shared<Int64Scalar>(5)}, options));
}

template <typename ArrowType>
void CheckCumulativeSumUnsignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  CumulativeSumOptions pos_overflow(1);
  auto max = std::numeric_limits<CType>::max();
  auto min = std::numeric_limits<CType>::lowest();

  BuilderType builder;
  std::shared_ptr<Array> max_arr;
  std::shared_ptr<Array> overflow_arr;
  ASSERT_OK(builder.Append(max));
  ASSERT_OK(builder.Finish(&max_arr));
  builder.Reset();
  ASSERT_OK(builder.Append(min));
  ASSERT_OK(builder.Finish(&overflow_arr));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, HasSubstr("overflow"),
      CallFunction("cumulative_sum_checked", {max_arr}, &pos_overflow));
  CheckVectorUnary("cumulative_sum", max_arr, overflow_arr, &pos_overflow);
}

template <typename ArrowType>
void CheckCumulativeSumSignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  CheckCumulativeSumUnsignedOverflow<ArrowType>();

  CumulativeSumOptions neg_overflow(-1);
  auto max = std::numeric_limits<CType>::max();
  auto min = std::numeric_limits<CType>::lowest();

  BuilderType builder;
  std::shared_ptr<Array> min_arr;
  std::shared_ptr<Array> overflow_arr;
  ASSERT_OK(builder.Append(min));
  ASSERT_OK(builder.Finish(&min_arr));
  builder.Reset();
  ASSERT_OK(builder.Append(max));
  ASSERT_OK(builder.Finish(&overflow_arr));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, HasSubstr("overflow"),
      CallFunction("cumulative_sum_checked", {min_arr}, &neg_overflow));
  CheckVectorUnary("cumulative_sum", min_arr, overflow_arr, &neg_overflow);
}

template <typename ArrowType>
void CheckCumulativeProductUnsignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  CumulativeProductOptions pos_overflow(2);
  auto max = std::numeric_limits<CType>::max();

  BuilderType builder;
  std::shared_ptr<Array> max_arr;
  std::shared_ptr<Array> overflow_arr;
  ASSERT_OK(builder.Append(max));
  ASSERT_OK(builder.Finish(&max_arr));
  builder.Reset();
  ASSERT_OK(builder.Append(max << 1));
  ASSERT_OK(builder.Finish(&overflow_arr));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, HasSubstr("overflow"),
      CallFunction("cumulative_product_checked", {max_arr}, &pos_overflow));
  CheckVectorUnary("cumulative_product", max_arr, overflow_arr, &pos_overflow);
}

template <typename ArrowType>
void CheckCumulativeProductSignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  CheckCumulativeProductUnsignedOverflow<ArrowType>();

  CumulativeProductOptions neg_overflow(2);
  auto min = std::numeric_limits<CType>::lowest();

  BuilderType builder;
  std::shared_ptr<Array> min_arr;
  std::shared_ptr<Array> overflow_arr;
  ASSERT_OK(builder.Append(min));
  ASSERT_OK(builder.Finish(&min_arr));
  builder.Reset();
  ASSERT_OK(builder.Append(0));
  ASSERT_OK(builder.Finish(&overflow_arr));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, HasSubstr("overflow"),
      CallFunction("cumulative_product_checked", {min_arr}, &neg_overflow));
  CheckVectorUnary("cumulative_product", min_arr, overflow_arr, &neg_overflow);
}

TEST(TestCumulativeOps, Empty) {
  CumulativeSumOptions sum_options;
  TestEmpty("cumulative_sum", &sum_options);
  TestEmpty("cumulative_sum_checked", &sum_options);

  CumulativeProductOptions product_options;
  TestEmpty("cumulative_product", &product_options);
  TestEmpty("cumulative_product_checked", &product_options);

  CumulativeMinOptions min_options;
  TestEmpty("cumulative_min", &min_options);

  CumulativeMaxOptions max_options;
  TestEmpty("cumulative_max", &max_options);
}

TEST(TestCumulativeOps, AllNulls) {
  CumulativeSumOptions sum_options;
  TestAllNulls("cumulative_sum", &sum_options);
  TestAllNulls("cumulative_sum_checked", &sum_options);

  CumulativeProductOptions product_options;
  TestAllNulls("cumulative_product", &product_options);
  TestAllNulls("cumulative_product_checked", &product_options);

  CumulativeMinOptions min_options;
  TestAllNulls("cumulative_min", &min_options);

  CumulativeMaxOptions max_options;
  TestAllNulls("cumulative_max", &max_options);
}

TEST(TestCumulativeOps, ScalarNotSupported) {
  CumulativeSumOptions sum_options;
  TestScalarNotSupported("cumulative_sum", &sum_options);
  TestScalarNotSupported("cumulative_sum_checked", &sum_options);

  CumulativeProductOptions product_options;
  TestScalarNotSupported("cumulative_product", &product_options);
  TestScalarNotSupported("cumulative_product_checked", &product_options);

  CumulativeMinOptions min_options;
  TestScalarNotSupported("cumulative_min", &min_options);

  CumulativeMaxOptions max_options;
  TestScalarNotSupported("cumulative_max", &max_options);
}

TEST(TestCumulativeOps, IntegerOverflow) {
  CheckCumulativeSumUnsignedOverflow<UInt8Type>();
  CheckCumulativeSumUnsignedOverflow<UInt16Type>();
  CheckCumulativeSumUnsignedOverflow<UInt32Type>();
  CheckCumulativeSumUnsignedOverflow<UInt64Type>();
  CheckCumulativeSumSignedOverflow<Int8Type>();
  CheckCumulativeSumSignedOverflow<Int16Type>();
  CheckCumulativeSumSignedOverflow<Int32Type>();
  CheckCumulativeSumSignedOverflow<Int64Type>();

  CheckCumulativeProductUnsignedOverflow<UInt8Type>();
  CheckCumulativeProductUnsignedOverflow<UInt16Type>();
  CheckCumulativeProductUnsignedOverflow<UInt32Type>();
  CheckCumulativeProductUnsignedOverflow<UInt64Type>();
  CheckCumulativeProductSignedOverflow<Int8Type>();
  CheckCumulativeProductSignedOverflow<Int16Type>();
  CheckCumulativeProductSignedOverflow<Int32Type>();
  CheckCumulativeProductSignedOverflow<Int64Type>();
}

TEST(TestCumulativeSum, NoStartNoSkip) {
  CumulativeSumOptions options;
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

TEST(TestCumulativeProduct, NoStartNoSkip) {
  CumulativeProductOptions options;
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_product", ArrayFromJSON(ty, "[1, 2, 3, 4, 5]"),
                     ArrayFromJSON(ty, "[1, 2, 6, 24, 120]"), &options);
    CheckVectorUnary("cumulative_product_checked", ArrayFromJSON(ty, "[1, 2, 3, 4, 5]"),
                     ArrayFromJSON(ty, "[1, 2, 6, 24, 120]"), &options);

    CheckVectorUnary("cumulative_product", ArrayFromJSON(ty, "[1, 2, null, 4, null]"),
                     ArrayFromJSON(ty, "[1, 2, null, null, null]"), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ArrayFromJSON(ty, "[1, 2, null, 4, null]"),
                     ArrayFromJSON(ty, "[1, 2, null, null, null]"), &options);

    CheckVectorUnary("cumulative_product", ArrayFromJSON(ty, "[null, 2, null, 4, null]"),
                     ArrayFromJSON(ty, "[null, null, null, null, null]"), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ArrayFromJSON(ty, "[null, 2, null, 4, null]"),
                     ArrayFromJSON(ty, "[null, null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_product",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, 6, 24, 120]"}), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, 6, 24, 120]"}), &options);

    CheckVectorUnary("cumulative_product",
                     ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, null, null, null]"}), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, null, null, null]"}), &options);

    CheckVectorUnary(
        "cumulative_product", ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null]"}),
        ChunkedArrayFromJSON(ty, {"[null, null, null, null, null]"}), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null]"}),
                     ChunkedArrayFromJSON(ty, {"[null, null, null, null, null]"}),
                     &options);
  }
}

TEST(TestCumulativeSum, NoStartDoSkip) {
  CumulativeSumOptions options(0, true);
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

TEST(TestCumulativeProduct, NoStartDoSkip) {
  CumulativeProductOptions options(1, true);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_product", ArrayFromJSON(ty, "[1, 2, 3, 4, 5]"),
                     ArrayFromJSON(ty, "[1, 2, 6, 24, 120]"), &options);
    CheckVectorUnary("cumulative_product_checked", ArrayFromJSON(ty, "[1, 2, 3, 4, 5]"),
                     ArrayFromJSON(ty, "[1, 2, 6, 24, 120]"), &options);

    CheckVectorUnary("cumulative_product", ArrayFromJSON(ty, "[1, 2, null, 4, null]"),
                     ArrayFromJSON(ty, "[1, 2, null, 8, null]"), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ArrayFromJSON(ty, "[1, 2, null, 4, null]"),
                     ArrayFromJSON(ty, "[1, 2, null, 8, null]"), &options);

    CheckVectorUnary("cumulative_product", ArrayFromJSON(ty, "[null, 2, null, 4, null]"),
                     ArrayFromJSON(ty, "[null, 2, null, 8, null]"), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ArrayFromJSON(ty, "[null, 2, null, 4, null]"),
                     ArrayFromJSON(ty, "[null, 2, null, 8, null]"), &options);

    CheckVectorUnary("cumulative_product",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, 6, 24, 120]"}), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, 3]", "[4, 5]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, 6, 24, 120]"}), &options);

    CheckVectorUnary("cumulative_product",
                     ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, null, 8, null]"}), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2, null]", "[4, null]"}),
                     ChunkedArrayFromJSON(ty, {"[1, 2, null, 8, null]"}), &options);

    CheckVectorUnary("cumulative_product",
                     ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 2, null, 8, null]"}), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ChunkedArrayFromJSON(ty, {"[null, 2, null]", "[4, null]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 2, null, 8, null]"}), &options);
  }
}

TEST(TestCumulativeSum, HasStartNoSkip) {
  CumulativeSumOptions options(10);
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

TEST(TestCumulativeProduct, HasStartNoSkip) {
  CumulativeProductOptions options(5);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_product", ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                     ArrayFromJSON(ty, "[5, 10, 30, 120]"), &options);
    CheckVectorUnary("cumulative_product_checked", ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                     ArrayFromJSON(ty, "[5, 10, 30, 120]"), &options);

    CheckVectorUnary("cumulative_product", ArrayFromJSON(ty, "[1, 2, null, 4]"),
                     ArrayFromJSON(ty, "[5, 10, null, null]"), &options);
    CheckVectorUnary("cumulative_product_checked", ArrayFromJSON(ty, "[1, 2, null, 4]"),
                     ArrayFromJSON(ty, "[5, 10, null, null]"), &options);

    CheckVectorUnary("cumulative_product", ArrayFromJSON(ty, "[null, 2, null, 4]"),
                     ArrayFromJSON(ty, "[null, null, null, null]"), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ArrayFromJSON(ty, "[null, 2, null, 4]"),
                     ArrayFromJSON(ty, "[null, null, null, null]"), &options);

    CheckVectorUnary("cumulative_product", ChunkedArrayFromJSON(ty, {"[1, 2]", "[3, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[5, 10, 30, 120]"}), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2]", "[3, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[5, 10, 30, 120]"}), &options);

    CheckVectorUnary("cumulative_product",
                     ChunkedArrayFromJSON(ty, {"[1, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[5, 10, null, null]"}), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[5, 10, null, null]"}), &options);

    CheckVectorUnary("cumulative_product",
                     ChunkedArrayFromJSON(ty, {"[null, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[null, null, null, null]"}), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ChunkedArrayFromJSON(ty, {"[null, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[null, null, null, null]"}), &options);
  }
}

TEST(TestCumulativeSum, HasStartDoSkip) {
  CumulativeSumOptions options(10, true);
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

TEST(TestCumulativeProduct, HasStartDoSkip) {
  CumulativeProductOptions options(5, true);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_product", ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                     ArrayFromJSON(ty, "[5, 10, 30, 120]"), &options);
    CheckVectorUnary("cumulative_product_checked", ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                     ArrayFromJSON(ty, "[5, 10, 30, 120]"), &options);

    CheckVectorUnary("cumulative_product", ArrayFromJSON(ty, "[1, 2, null, 4]"),
                     ArrayFromJSON(ty, "[5, 10, null, 40]"), &options);
    CheckVectorUnary("cumulative_product_checked", ArrayFromJSON(ty, "[1, 2, null, 4]"),
                     ArrayFromJSON(ty, "[5, 10, null, 40]"), &options);

    CheckVectorUnary("cumulative_product", ArrayFromJSON(ty, "[null, 2, null, 4]"),
                     ArrayFromJSON(ty, "[null, 10, null, 40]"), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ArrayFromJSON(ty, "[null, 2, null, 4]"),
                     ArrayFromJSON(ty, "[null, 10, null, 40]"), &options);

    CheckVectorUnary("cumulative_product", ChunkedArrayFromJSON(ty, {"[1, 2]", "[3, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[5, 10, 30, 120]"}), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2]", "[3, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[5, 10, 30, 120]"}), &options);

    CheckVectorUnary("cumulative_product",
                     ChunkedArrayFromJSON(ty, {"[1, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[5, 10, null, 40]"}), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ChunkedArrayFromJSON(ty, {"[1, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[5, 10, null, 40]"}), &options);

    CheckVectorUnary("cumulative_product",
                     ChunkedArrayFromJSON(ty, {"[null, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 10, null, 40]"}), &options);
    CheckVectorUnary("cumulative_product_checked",
                     ChunkedArrayFromJSON(ty, {"[null, 2]", "[null, 4]"}),
                     ChunkedArrayFromJSON(ty, {"[null, 10, null, 40]"}), &options);
  }
}

}  // namespace compute
}  // namespace arrow
