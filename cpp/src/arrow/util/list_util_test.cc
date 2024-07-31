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

#include "arrow/array/builder_nested.h"
#include "arrow/util/list_util.h"

#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

using ListAndListViewTypes =
    ::testing::Types<ListType, LargeListType, ListViewType, LargeListViewType>;

template <typename T>
class TestListUtils : public ::testing::Test {
 public:
  using TypeClass = T;
  using offset_type = typename TypeClass::offset_type;
  using BuilderType = typename TypeTraits<TypeClass>::BuilderType;

  void SetUp() override {
    value_type_ = int16();
    type_ = std::make_shared<T>(value_type_);

    std::unique_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_.reset(checked_cast<BuilderType*>(tmp.release()));
  }

  void TestRangeOfValuesUsed() {
    std::shared_ptr<ArrayData> result;

    // These list-views are built manually with the list-view builders instead
    // of using something like ArrayFromJSON() because we want to test the
    // RangeOfValuesUsed() function's ability to handle arrays containing
    // overlapping list-views.

    // Empty list-like array
    ASSERT_OK(builder_->FinishInternal(&result));
    builder_->Reset();
    ASSERT_OK_AND_ASSIGN(auto range, list_util::internal::RangeOfValuesUsed(*result));
    ASSERT_EQ(range.first, 0);
    ASSERT_EQ(range.second, 0);

    // List-like array with only nulls
    ASSERT_OK(builder_->AppendNulls(3));
    ASSERT_OK(builder_->FinishInternal(&result));
    builder_->Reset();
    ASSERT_OK_AND_ASSIGN(range, list_util::internal::RangeOfValuesUsed(*result));
    ASSERT_EQ(range.first, 0);
    ASSERT_EQ(range.second, 0);

    // Array with nulls and non-nulls (starting at a non-zero offset)
    Int16Builder* vb = checked_cast<Int16Builder*>(builder_->value_builder());
    ASSERT_OK(vb->Append(-2));
    ASSERT_OK(vb->Append(-1));
    ASSERT_OK(builder_->Append(/*is_valid=*/false, 0));
    ASSERT_OK(builder_->Append(/*is_valid=*/true, 2));
    ASSERT_OK(vb->Append(0));
    ASSERT_OK(vb->Append(1));
    ASSERT_OK(builder_->Append(/*is_valid=*/true, 3));
    ASSERT_OK(vb->Append(2));
    ASSERT_OK(vb->Append(3));
    ASSERT_OK(vb->Append(4));
    if constexpr (is_list_view_type<TypeClass>::value) {
      ASSERT_OK(vb->Append(10));
      ASSERT_OK(vb->Append(11));
    }
    std::shared_ptr<Array> array;
    ASSERT_OK(builder_->Finish(&array));
    builder_->Reset();
    ASSERT_OK(array->ValidateFull());
    ASSERT_OK_AND_ASSIGN(range, list_util::internal::RangeOfValuesUsed(*array->data()));
    ASSERT_EQ(range.first, 2);
    ASSERT_EQ(range.second, 5);

    // Overlapping list-views
    vb = checked_cast<Int16Builder*>(builder_->value_builder());
    ASSERT_OK(vb->Append(-2));
    ASSERT_OK(vb->Append(-1));
    ASSERT_OK(builder_->Append(/*is_valid=*/false, 0));
    if constexpr (is_list_view_type<TypeClass>::value) {
      ASSERT_OK(builder_->Append(/*is_valid=*/true, 6));
      ASSERT_OK(vb->Append(0));
      ASSERT_OK(builder_->Append(/*is_valid=*/true, 2));
      ASSERT_OK(vb->Append(1));
      ASSERT_OK(vb->Append(2));
      ASSERT_OK(vb->Append(3));
      ASSERT_OK(builder_->Append(/*is_valid=*/false, 0));
      ASSERT_OK(builder_->Append(/*is_valid=*/true, 1));
      ASSERT_OK(vb->Append(4));
      ASSERT_OK(vb->Append(5));
      // -- used range ends here --
      ASSERT_OK(vb->Append(10));
      ASSERT_OK(vb->Append(11));
    } else {
      ASSERT_OK(builder_->Append(/*is_valid=*/true, 6));
      ASSERT_OK(vb->Append(0));
      ASSERT_OK(vb->Append(1));
      ASSERT_OK(vb->Append(2));
      ASSERT_OK(vb->Append(3));
      ASSERT_OK(vb->Append(4));
      ASSERT_OK(vb->Append(5));
      ASSERT_OK(builder_->Append(/*is_valid=*/true, 2));
      ASSERT_OK(vb->Append(1));
      ASSERT_OK(vb->Append(2));
      ASSERT_OK(builder_->Append(/*is_valid=*/false, 0));
      ASSERT_OK(builder_->Append(/*is_valid=*/true, 1));
      ASSERT_OK(vb->Append(4));
    }
    ASSERT_OK(builder_->AppendNulls(2));
    ASSERT_OK(builder_->Finish(&array));
    builder_->Reset();
    ASSERT_OK(array->ValidateFull());
    ASSERT_ARRAYS_EQUAL(
        *array, *ArrayFromJSON(
                    type_, "[null, [0, 1, 2, 3, 4, 5], [1, 2], null, [4], null, null]"));
    // Check the range
    ASSERT_OK_AND_ASSIGN(range, list_util::internal::RangeOfValuesUsed(*array->data()));
    ASSERT_EQ(range.first, 2);
    if constexpr (is_list_view_type<TypeClass>::value) {
      ASSERT_EQ(range.second, 6);
    } else {
      ASSERT_EQ(range.second, 9);
    }
    // Check the sum of logical sizes as well
    ASSERT_OK_AND_ASSIGN(int64_t sum_of_logical_sizes,
                         list_util::internal::SumOfLogicalListSizes(*array->data()));
    ASSERT_EQ(sum_of_logical_sizes, 9);
  }

 protected:
  MemoryPool* pool_ = default_memory_pool();
  std::shared_ptr<DataType> type_;
  std::shared_ptr<DataType> value_type_;
  std::shared_ptr<BuilderType> builder_;
};

TYPED_TEST_SUITE(TestListUtils, ListAndListViewTypes);

TYPED_TEST(TestListUtils, RangeOfValuesUsed) { this->TestRangeOfValuesUsed(); }

}  // namespace arrow
