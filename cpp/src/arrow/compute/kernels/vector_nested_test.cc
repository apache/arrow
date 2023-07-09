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

#include "arrow/chunked_array.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace compute {

using arrow::internal::checked_cast;

TEST(TestVectorNested, ListFlatten) {
  for (auto ty : {list(int16()), large_list(int16())}) {
    auto input = ArrayFromJSON(ty, "[[0, null, 1], null, [2, 3], []]");
    auto expected = ArrayFromJSON(int16(), "[0, null, 1, 2, 3]");
    CheckVectorUnary("list_flatten", input, expected);

    // Construct a list with a non-empty null slot
    auto tweaked = TweakValidityBit(input, 0, false);
    expected = ArrayFromJSON(int16(), "[2, 3]");
    CheckVectorUnary("list_flatten", tweaked, expected);
  }
}

TEST(TestVectorNested, ListFlattenNulls) {
  const auto ty = list(int32());
  auto input = ArrayFromJSON(ty, "[null, null]");
  auto expected = ArrayFromJSON(int32(), "[]");
  CheckVectorUnary("list_flatten", input, expected);
}

TEST(TestVectorNested, ListFlattenChunkedArray) {
  for (auto ty : {list(int16()), large_list(int16())}) {
    auto input = ChunkedArrayFromJSON(ty, {"[[0, null, 1], null]", "[[2, 3], []]"});
    auto expected = ChunkedArrayFromJSON(int16(), {"[0, null, 1]", "[2, 3]"});
    CheckVectorUnary("list_flatten", input, expected);

    input = ChunkedArrayFromJSON(ty, {});
    expected = ChunkedArrayFromJSON(int16(), {});
    CheckVectorUnary("list_flatten", input, expected);
  }
}

TEST(TestVectorNested, ListFlattenFixedSizeList) {
  for (auto ty : {fixed_size_list(int16(), 2), fixed_size_list(uint32(), 2)}) {
    const auto& out_ty = checked_cast<const FixedSizeListType&>(*ty).value_type();
    {
      auto input = ArrayFromJSON(ty, "[[0, null], null, [2, 3], [0, 42]]");
      auto expected = ArrayFromJSON(out_ty, "[0, null, 2, 3, 0, 42]");
      CheckVectorUnary("list_flatten", input, expected);
    }

    {
      // Test a chunked array
      auto input = ChunkedArrayFromJSON(ty, {"[[0, null], null]", "[[2, 3], [0, 42]]"});
      auto expected = ChunkedArrayFromJSON(out_ty, {"[0, null]", "[2, 3, 0, 42]"});
      CheckVectorUnary("list_flatten", input, expected);

      input = ChunkedArrayFromJSON(ty, {});
      expected = ChunkedArrayFromJSON(out_ty, {});
      CheckVectorUnary("list_flatten", input, expected);
    }
  }
}

TEST(TestVectorNested, ListFlattenFixedSizeListNulls) {
  const auto ty = fixed_size_list(int32(), 1);
  auto input = ArrayFromJSON(ty, "[null, null]");
  auto expected = ArrayFromJSON(int32(), "[]");
  CheckVectorUnary("list_flatten", input, expected);
}

TEST(TestVectorNested, ListParentIndices) {
  for (auto ty : {list(int16()), large_list(int16())}) {
    auto input = ArrayFromJSON(ty, "[[0, null, 1], null, [2, 3], [], [4, 5]]");

    auto expected = ArrayFromJSON(int64(), "[0, 0, 0, 2, 2, 4, 4]");
    CheckVectorUnary("list_parent_indices", input, expected);
  }

  // Construct a list with a non-empty null slot
  auto input = ArrayFromJSON(list(int16()), "[[0, null, 1], [0, 0], [2, 3], [], [4, 5]]");
  auto tweaked = TweakValidityBit(input, 1, false);
  auto expected = ArrayFromJSON(int64(), "[0, 0, 0, 1, 1, 2, 2, 4, 4]");
  CheckVectorUnary("list_parent_indices", tweaked, expected);
}

TEST(TestVectorNested, ListParentIndicesChunkedArray) {
  for (auto ty : {list(int16()), large_list(int16())}) {
    auto input =
        ChunkedArrayFromJSON(ty, {"[[0, null, 1], null]", "[[2, 3], [], [4, 5]]"});

    auto expected = ChunkedArrayFromJSON(int64(), {"[0, 0, 0]", "[2, 2, 4, 4]"});
    CheckVectorUnary("list_parent_indices", input, expected);

    input = ChunkedArrayFromJSON(ty, {});
    expected = ChunkedArrayFromJSON(int64(), {});
    CheckVectorUnary("list_parent_indices", input, expected);
  }
}

TEST(TestVectorNested, ListParentIndicesFixedSizeList) {
  for (auto ty : {fixed_size_list(int16(), 2), fixed_size_list(uint32(), 2)}) {
    {
      auto input = ArrayFromJSON(ty, "[[0, null], null, [1, 2], [3, 4], [null, 5]]");
      auto expected = ArrayFromJSON(int64(), "[0, 0, 2, 2, 3, 3, 4, 4]");
      CheckVectorUnary("list_parent_indices", input, expected);
    }
    {
      // Test a chunked array
      auto input =
          ChunkedArrayFromJSON(ty, {"[[0, null], null, [1, 2]]", "[[3, 4], [null, 5]]"});
      auto expected = ChunkedArrayFromJSON(int64(), {"[0, 0, 2, 2]", "[3, 3, 4, 4]"});
      CheckVectorUnary("list_parent_indices", input, expected);

      input = ChunkedArrayFromJSON(ty, {});
      expected = ChunkedArrayFromJSON(int64(), {});
      CheckVectorUnary("list_parent_indices", input, expected);
    }
  }
}

}  // namespace compute
}  // namespace arrow
