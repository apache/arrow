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

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"

namespace arrow {
namespace ree_util {

template <typename RunEndsType>
struct ReeUtilTest : public ::testing::Test {
  // Re-implementation of FindPhysicalIndex that uses trivial linear-search
  // instead of the more efficient implementation.
  int64_t FindPhysicalIndexTestImpl(const RunEndsType* run_ends, int64_t run_ends_size,
                                    int64_t i, int64_t absolute_offset = 0) {
    for (int64_t j = 0; j < run_ends_size; j++) {
      if (absolute_offset + i < run_ends[j]) {
        return j;
      }
    }
    return run_ends_size;
  }
};
TYPED_TEST_SUITE_P(ReeUtilTest);

TYPED_TEST_P(ReeUtilTest, PhysicalOffset) {
  using RE = TypeParam;  // Run-end type
  const RE run_ends1[] = {1};
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends1, 1, 0, 0), 0);
  const RE run_ends124[] = {1, 2, 4};
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends124, 3, 0, 0), 0);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends124, 3, 1, 0), 1);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends124, 3, 2, 0), 2);
  const RE run_ends234[] = {2, 3, 4};
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends234, 3, 0, 0), 0);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends234, 3, 1, 0), 0);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends234, 3, 2, 0), 1);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends234, 3, 3, 0), 2);
  const RE run_ends246[] = {2, 4, 6};
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends246, 3, 3, 0), 1);

  // Out-of-range logical offset should return run_ends size
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends246, 3, 6, 0), 3);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends246, 3, 1000, 0), 3);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends246, 0, 5, 0), 0);

  constexpr int64_t kMaxLogicalIndex = 150;
  const RE run_ends[] = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 100, 105, 115, 120, 125, kMaxLogicalIndex};
  for (int64_t i = 0; i <= kMaxLogicalIndex; i++) {
    DCHECK_EQ(internal::FindPhysicalIndex(run_ends, std::size(run_ends), i, 0),
              this->FindPhysicalIndexTestImpl(run_ends, std::size(run_ends), i, 0));
  }
}

TYPED_TEST_P(ReeUtilTest, PhysicalLength) {
  using RE = TypeParam;  // Run-end type

  const RE run_ends0[] = {-1};  // used as zero-length array
  ASSERT_EQ(internal::FindPhysicalLength(0, 0, run_ends0, 0), 0);

  const RE run_ends1[] = {1};
  ASSERT_EQ(internal::FindPhysicalLength(1, 0, run_ends1, 1), 1);
  ASSERT_EQ(internal::FindPhysicalLength(0, 1, run_ends1, 1), 0);

  const RE run_ends124[] = {1, 2, 4};  // run-lengths: 1, 1, 2
  ASSERT_EQ(internal::FindPhysicalLength(4, 0, run_ends124, 3), 3);
  ASSERT_EQ(internal::FindPhysicalLength(3, 1, run_ends124, 3), 2);
  ASSERT_EQ(internal::FindPhysicalLength(2, 2, run_ends124, 3), 1);
  ASSERT_EQ(internal::FindPhysicalLength(1, 3, run_ends124, 3), 1);
  ASSERT_EQ(internal::FindPhysicalLength(0, 4, run_ends124, 3), 0);
  const RE run_ends246[] = {2, 4, 6, 7};  // run-lengths: 2, 2, 2, 1
  ASSERT_EQ(internal::FindPhysicalLength(7, 0, run_ends246, 4), 4);
  ASSERT_EQ(internal::FindPhysicalLength(6, 1, run_ends246, 4), 4);
  ASSERT_EQ(internal::FindPhysicalLength(5, 2, run_ends246, 4), 3);
  ASSERT_EQ(internal::FindPhysicalLength(4, 3, run_ends246, 4), 3);
  ASSERT_EQ(internal::FindPhysicalLength(3, 4, run_ends246, 4), 2);
  ASSERT_EQ(internal::FindPhysicalLength(2, 5, run_ends246, 4), 2);
  ASSERT_EQ(internal::FindPhysicalLength(1, 6, run_ends246, 4), 1);
  ASSERT_EQ(internal::FindPhysicalLength(0, 7, run_ends246, 4), 0);
}

REGISTER_TYPED_TEST_SUITE_P(ReeUtilTest, PhysicalOffset, PhysicalLength);
using RunEndsTypes = testing::Types<int16_t, int32_t, int64_t>;
INSTANTIATE_TYPED_TEST_SUITE_P(ReeUtilTest, ReeUtilTest, RunEndsTypes);

}  // namespace ree_util
}  // namespace arrow
