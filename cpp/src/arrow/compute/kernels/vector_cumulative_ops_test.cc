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
  CumulativeSumOptions no_start;
  CumulativeSumOptions with_start(10);
  for (auto ty : NumericTypes()) {
    CheckVectorUnary("cumulative_sum", ScalarFromJSON(ty, "10"),
                     ArrayFromJSON(ty, "[10]"), &no_start);
    CheckVectorUnary("cumulative_sum_checked", ScalarFromJSON(ty, "10"),
                     ArrayFromJSON(ty, "[10]"), &no_start);

    CheckVectorUnary("cumulative_sum", ScalarFromJSON(ty, "10"),
                     ArrayFromJSON(ty, "[20]"), &with_start);
    CheckVectorUnary("cumulative_sum_checked", ScalarFromJSON(ty, "10"),
                     ArrayFromJSON(ty, "[20]"), &with_start);
  }
}

using testing::HasSubstr;

#define CHECK_CUMULATIVE_SUM_UNSIGNED_OVERFLOW(FUNC, TYPENAME, CTYPE)            \
  {                                                                              \
    CumulativeSumOptions pos_overflow(1);                                        \
    auto max = std::numeric_limits<CTYPE>::max();                                \
    TYPENAME##Builder builder;                                                   \
    std::shared_ptr<Array> array;                                                \
    ASSERT_OK(builder.Append(max));                                              \
    ASSERT_OK(builder.Finish(&array));                                           \
    EXPECT_RAISES_WITH_MESSAGE_THAT(                                             \
        Invalid, HasSubstr("overflow"),                                          \
        CallFunction(FUNC, {TypeTraits<TYPENAME##Type>::ScalarType(max)},        \
                     &pos_overflow));                                            \
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("overflow"),              \
                                    CallFunction(FUNC, {array}, &pos_overflow)); \
  }

#define CHECK_CUMULATIVE_SUM_SIGNED_OVERFLOW(FUNC, TYPENAME, CTYPE)              \
  CHECK_CUMULATIVE_SUM_UNSIGNED_OVERFLOW(FUNC, TYPENAME, CTYPE);                 \
  {                                                                              \
    CumulativeSumOptions neg_overflow(-1);                                       \
    auto min = std::numeric_limits<CTYPE>::lowest();                             \
    TYPENAME##Builder builder;                                                   \
    std::shared_ptr<Array> array;                                                \
    ASSERT_OK(builder.Append(min));                                              \
    ASSERT_OK(builder.Finish(&array));                                           \
    EXPECT_RAISES_WITH_MESSAGE_THAT(                                             \
        Invalid, HasSubstr("overflow"),                                          \
        CallFunction(FUNC, {TypeTraits<TYPENAME##Type>::ScalarType(min)},        \
                     &neg_overflow));                                            \
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("overflow"),              \
                                    CallFunction(FUNC, {array}, &neg_overflow)); \
  }

TEST(TestCumulativeSum, IntegerOverflow) {
  CHECK_CUMULATIVE_SUM_UNSIGNED_OVERFLOW("cumulative_sum_checked", UInt8, uint8_t);
  CHECK_CUMULATIVE_SUM_UNSIGNED_OVERFLOW("cumulative_sum_checked", UInt16, uint16_t);
  CHECK_CUMULATIVE_SUM_UNSIGNED_OVERFLOW("cumulative_sum_checked", UInt32, uint32_t);
  CHECK_CUMULATIVE_SUM_UNSIGNED_OVERFLOW("cumulative_sum_checked", UInt64, uint64_t);
  CHECK_CUMULATIVE_SUM_SIGNED_OVERFLOW("cumulative_sum_checked", Int8, int8_t);
  CHECK_CUMULATIVE_SUM_SIGNED_OVERFLOW("cumulative_sum_checked", Int16, int16_t);
  CHECK_CUMULATIVE_SUM_SIGNED_OVERFLOW("cumulative_sum_checked", Int32, int32_t);
  CHECK_CUMULATIVE_SUM_SIGNED_OVERFLOW("cumulative_sum_checked", Int64, int64_t);
}

#undef CHECK_CUMULATIVE_SUM_UNSIGNED_OVERFLOW
#undef CHECK_CUMULATIVE_SUM_SIGNED_OVERFLOW

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

}  // namespace compute
}  // namespace arrow
