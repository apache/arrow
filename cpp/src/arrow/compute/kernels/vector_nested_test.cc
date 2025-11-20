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
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util_internal.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace compute {

using arrow::internal::checked_cast;

using ListAndListViewTypes =
    ::testing::Types<ListType, LargeListType, ListViewType, LargeListViewType>;

// ----------------------------------------------------------------------
// [Large]List and [Large]ListView tests
template <typename T>
class TestVectorNestedSpecialized : public ::testing::Test {
 public:
  using TypeClass = T;

  void SetUp() override {
    value_type_ = int16();
    type_ = std::make_shared<T>(value_type_);
  }

 public:
  void TestListFlatten() {
    auto input = ArrayFromJSON(type_, "[[0, null, 1], null, [2, 3], []]");
    auto expected = ArrayFromJSON(value_type_, "[0, null, 1, 2, 3]");
    CheckVectorUnary("list_flatten", input, expected);

    // Construct a list with a non-empty null slot
    auto tweaked = TweakValidityBit(input, 0, false);
    expected = ArrayFromJSON(value_type_, "[2, 3]");
    CheckVectorUnary("list_flatten", tweaked, expected);
  }

  void TestListFlattenNulls() {
    value_type_ = int32();
    type_ = std::make_shared<T>(value_type_);
    auto input = ArrayFromJSON(type_, "[null, null]");
    auto expected = ArrayFromJSON(value_type_, "[]");
    CheckVectorUnary("list_flatten", input, expected);
  }

  void TestListFlattenChunkedArray() {
    ARROW_SCOPED_TRACE(type_->ToString());
    auto input = ChunkedArrayFromJSON(type_, {"[[0, null, 1], null]", "[[2, 3], []]"});
    auto expected = ChunkedArrayFromJSON(value_type_, {"[0, null, 1]", "[2, 3]"});
    CheckVectorUnary("list_flatten", input, expected);

    ARROW_SCOPED_TRACE("empty");
    input = ChunkedArrayFromJSON(type_, {});
    expected = ChunkedArrayFromJSON(value_type_, {});
    CheckVectorUnary("list_flatten", input, expected);
  }

  void TestListFlattenRecursively() {
    auto inner_type = std::make_shared<T>(value_type_);
    type_ = std::make_shared<T>(inner_type);

    ListFlattenOptions opts;
    opts.recursive = true;

    // List types with two nesting levels: list<list<int16>>
    auto input = ArrayFromJSON(type_, R"([
        [[0, 1, 2], null, [3, null]],
        [null],
        [[2, 9], [4], [], [6, 5]]
      ])");
    auto expected = ArrayFromJSON(value_type_, "[0, 1, 2, 3, null, 2, 9, 4, 6, 5]");
    CheckVectorUnary("list_flatten", input, expected, &opts);

    // Empty nested list should flatten until non-list type is reached
    input = ArrayFromJSON(type_, R"([null])");
    expected = ArrayFromJSON(value_type_, "[]");
    CheckVectorUnary("list_flatten", input, expected, &opts);

    // List types with three nesting levels: list<list<fixed_size_list<int32, 2>>>
    type_ = std::make_shared<T>(std::make_shared<T>(fixed_size_list(value_type_, 2)));
    input = ArrayFromJSON(type_, R"([
        [
          [[null, 0]],
          [[3, 7], null]
        ],
        [
          [[4, null], [5, 8]],
          [[8, null]],
          null
        ],
        [
          null
        ]
      ])");
    expected = ArrayFromJSON(value_type_, "[null, 0, 3, 7, 4, null, 5, 8, 8, null]");
    CheckVectorUnary("list_flatten", input, expected, &opts);
  }

 protected:
  std::shared_ptr<DataType> type_;
  std::shared_ptr<DataType> value_type_;
};

TYPED_TEST_SUITE(TestVectorNestedSpecialized, ListAndListViewTypes);

TYPED_TEST(TestVectorNestedSpecialized, ListFlatten) { this->TestListFlatten(); }

TYPED_TEST(TestVectorNestedSpecialized, ListFlattenNulls) {
  this->TestListFlattenNulls();
}

TYPED_TEST(TestVectorNestedSpecialized, ListFlattenChunkedArray) {
  this->TestListFlattenChunkedArray();
}

TYPED_TEST(TestVectorNestedSpecialized, ListFlattenRecursively) {
  this->TestListFlattenRecursively();
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

TEST(TestVectorNested, ListFlattenFixedSizeListRecursively) {
  ListFlattenOptions opts;
  opts.recursive = true;

  auto inner_type = fixed_size_list(int32(), 2);
  auto type = fixed_size_list(inner_type, 2);
  auto input = ArrayFromJSON(type, R"([
    [[0, 1], [null, 3]],
    [[7, null], [2, 5]],
    [null, null]
  ])");
  auto expected = ArrayFromJSON(int32(), "[0, 1, null, 3, 7, null, 2, 5]");
  CheckVectorUnary("list_flatten", input, expected, &opts);
}

template <typename T = ListViewType, typename offset_type = typename T::offset_type>
void SwapListView(ArrayData* array, int64_t i, int64_t j) {
  ASSERT_TRUE(is_list_view(array->type->id()));
  ASSERT_EQ(array->type->id(), T::type_id);
  ASSERT_LT(i, array->length);
  ASSERT_LT(j, array->length);
  auto* validity = array->GetMutableValues<uint8_t>(0);
  if (validity) {
    const bool is_valid_i = bit_util::GetBit(validity, array->offset + i);
    const bool is_valid_j = bit_util::GetBit(validity, array->offset + j);
    if (is_valid_i ^ is_valid_j) {
      bit_util::SetBitTo(validity, array->offset + i, is_valid_j);
      bit_util::SetBitTo(validity, array->offset + j, is_valid_i);
    }
  }
  auto* offsets = array->GetMutableValues<offset_type>(1);
  auto* sizes = array->GetMutableValues<offset_type>(2);
  std::swap(offsets[i], offsets[j]);
  std::swap(sizes[i], sizes[j]);
}

template <typename T = ListViewType, typename offset_type = typename T::offset_type>
void SetListView(ArrayData* array, int64_t i, offset_type offset, offset_type size) {
  ASSERT_TRUE(is_list_view(array->type->id()));
  ASSERT_EQ(array->type->id(), T::type_id);
  ASSERT_LT(i, array->length);
  auto* validity = array->GetMutableValues<uint8_t>(0);
  if (validity) {
    bit_util::SetBit(validity, array->offset + i);
  }
  auto* offsets = array->GetMutableValues<offset_type>(1);
  auto* sizes = array->GetMutableValues<offset_type>(2);
  offsets[i] = offset;
  sizes[i] = size;
}

TEST(TestVectorNested, ListParentIndices) {
  const auto types = {
      list(int16()),
      large_list(int16()),
      list_view(int16()),
      large_list_view(int16()),
  };
  for (auto ty : types) {
    auto input = ArrayFromJSON(ty, "[[0, null, 1], null, [2, 3], [], [4, 5]]");

    auto expected = ArrayFromJSON(int64(), "[0, 0, 0, 2, 2, 4, 4]");
    CheckVectorUnary("list_parent_indices", input, expected);
  }

  // Construct a list with a non-empty null slot
  auto input = ArrayFromJSON(list(int16()), "[[0, null, 1], [0, 0], [2, 3], [], [4, 5]]");
  auto tweaked = TweakValidityBit(input, 1, false);
  auto expected = ArrayFromJSON(int64(), "[0, 0, 0, 1, 1, 2, 2, 4, 4]");
  CheckVectorUnary("list_parent_indices", tweaked, expected);

  {
    // Construct a list-view with a non-empty null slot
    auto input =
        ArrayFromJSON(list_view(int16()), "[[0, null, 1], [0, 0], [2, 3], [], [4, 5]]");
    auto tweaked = TweakValidityBit(input, 1, false);
    auto expected = ArrayFromJSON(int64(), "[0, 0, 0, null, null, 2, 2, 4, 4]");
    CheckVectorUnary("list_parent_indices", tweaked, expected);

    // Swap some list-view entries
    auto swapped = tweaked->data()->Copy();
    SwapListView(swapped.get(), 0, 2);
    SwapListView(swapped.get(), 1, 4);
    AssertDatumsEqual(
        swapped,
        ArrayFromJSON(list_view(int16()), "[[2, 3], [4, 5], [0, null, 1], [], null]"),
        /*verbose=*/true);
    expected = ArrayFromJSON(int64(), "[2, 2, 2, null, null, 0, 0, 1, 1]");
    CheckVectorUnary("list_parent_indices", swapped, expected);

    // Make one view use values that are used by other list-views
    SetListView(swapped.get(), 3, 1, 4);
    AssertDatumsEqual(
        swapped,
        ArrayFromJSON(list_view(int16()),
                      "[[2, 3], [4, 5], [0, null, 1], [null, 1, 0, 0], null]"),
        /*verbose=*/true);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("values used by more than one list-view"),
        CallFunction("list_parent_indices", {input}));
  }
}

TEST(TestVectorNested, ListParentIndicesChunkedArray) {
  const auto types = {
      list(int16()),
      large_list(int16()),
      list_view(int16()),
      large_list_view(int16()),
  };
  for (auto ty : types) {
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
