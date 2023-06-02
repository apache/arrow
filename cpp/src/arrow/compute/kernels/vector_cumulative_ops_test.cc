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
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace compute {

TEST(TestCumulativeSum, Empty) {
  CumulativeSumOptions options;
  for (auto ty : NumericTypes()) {
    auto empty_arr = ArrayFromJSON(ty, "[]");
    auto empty_chunked = ChunkedArrayFromJSON(ty, {"[]"});
    CheckVectorUnary("cumulative_sum", empty_arr, empty_arr, &options);
    CheckVectorUnary("cumulative_sum_checked", empty_arr, empty_arr, &options);

    CheckVectorUnary("cumulative_sum", empty_chunked, empty_chunked, &options);
    CheckVectorUnary("cumulative_sum_checked", empty_chunked, empty_chunked, &options);
  }
}

TEST(TestCumulativeSum, AllNulls) {
  CumulativeSumOptions options;
  for (auto ty : NumericTypes()) {
    auto nulls_arr = ArrayFromJSON(ty, "[null, null, null]");
    auto nulls_one_chunk = ChunkedArrayFromJSON(ty, {"[null, null, null]"});
    auto nulls_three_chunks = ChunkedArrayFromJSON(ty, {"[null]", "[null]", "[null]"});
    CheckVectorUnary("cumulative_sum", nulls_arr, nulls_arr, &options);
    CheckVectorUnary("cumulative_sum_checked", nulls_arr, nulls_arr, &options);

    CheckVectorUnary("cumulative_sum", nulls_one_chunk, nulls_one_chunk, &options);
    CheckVectorUnary("cumulative_sum_checked", nulls_one_chunk, nulls_one_chunk,
                     &options);

    CheckVectorUnary("cumulative_sum", nulls_three_chunks, nulls_one_chunk, &options);
    CheckVectorUnary("cumulative_sum_checked", nulls_three_chunks, nulls_one_chunk,
                     &options);
  }
}

TEST(TestCumulativeSum, ScalarInput) {
  CumulativeSumOptions no_start_no_skip;
  CumulativeSumOptions no_start_do_skip(0, true);
  CumulativeSumOptions has_start_no_skip(10);
  CumulativeSumOptions has_start_do_skip(10, true);

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

using testing::HasSubstr;

template <typename ArrowType>
void CheckCumulativeSumUnsignedOverflow() {
  using CType = typename TypeTraits<ArrowType>::CType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  CumulativeSumOptions pos_overflow(1);
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

  CumulativeSumOptions neg_overflow(-1);
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

TEST(TestCumulativeSum, ConvenienceFunctionCheckOverflow) {
  ASSERT_ARRAYS_EQUAL(*CumulativeSum(ArrayFromJSON(int8(), "[127, 1]"),
                                     CumulativeSumOptions::Defaults(), false)
                           ->make_array(),
                      *ArrayFromJSON(int8(), "[127, -128]"));

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("overflow"),
                                  CumulativeSum(ArrayFromJSON(int8(), "[127, 1]"),
                                                CumulativeSumOptions::Defaults(), true));
}
}  // namespace compute
}  // namespace arrow
