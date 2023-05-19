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

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/util.h"
#include "arrow/array/validate.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_builders.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

using ListTypes = ::testing::Types<ListType, LargeListType>;

// ----------------------------------------------------------------------
// List tests

template <typename T>
class TestListArray : public ::testing::Test {
 public:
  using TypeClass = T;
  using offset_type = typename TypeClass::offset_type;
  using ArrayType = typename TypeTraits<TypeClass>::ArrayType;
  using BuilderType = typename TypeTraits<TypeClass>::BuilderType;
  using OffsetType = typename TypeTraits<TypeClass>::OffsetType;
  using OffsetArrayType = typename TypeTraits<TypeClass>::OffsetArrayType;
  using OffsetBuilderType = typename TypeTraits<TypeClass>::OffsetBuilderType;

  void SetUp() {
    value_type_ = int16();
    type_ = std::make_shared<T>(value_type_);

    std::unique_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_.reset(checked_cast<BuilderType*>(tmp.release()));
  }

  void Done() {
    std::shared_ptr<Array> out;
    FinishAndCheckPadding(builder_.get(), &out);
    result_ = std::dynamic_pointer_cast<ArrayType>(out);
  }

  void ValidateBasicListArray(const ArrayType* result, const std::vector<int16_t>& values,
                              const std::vector<uint8_t>& is_valid) {
    ASSERT_OK(result->ValidateFull());
    ASSERT_EQ(1, result->null_count());
    ASSERT_EQ(0, result->values()->null_count());

    ASSERT_EQ(3, result->length());
    std::vector<offset_type> ex_offsets = {0, 3, 3, 7};
    for (size_t i = 0; i < ex_offsets.size(); ++i) {
      ASSERT_EQ(ex_offsets[i], result->value_offset(i));
    }

    for (int i = 0; i < result->length(); ++i) {
      ASSERT_EQ(is_valid[i] == 0, result->IsNull(i));
    }

    ASSERT_EQ(7, result->values()->length());
    auto varr = std::dynamic_pointer_cast<Int16Array>(result->values());

    for (size_t i = 0; i < values.size(); ++i) {
      ASSERT_EQ(values[i], varr->Value(i));
    }

    auto offsets = std::dynamic_pointer_cast<OffsetArrayType>(result->offsets());
    ASSERT_EQ(offsets->length(), result->length() + 1);
    ASSERT_EQ(offsets->null_count(), 0);
    AssertTypeEqual(*offsets->type(), OffsetType());

    for (int64_t i = 0; i < result->length(); ++i) {
      ASSERT_EQ(offsets->Value(i), result_->raw_value_offsets()[i]);
    }
    // last offset
    ASSERT_EQ(offsets->Value(result->length()),
              result_->raw_value_offsets()[result->length()]);
  }

  void TestBasics() {
    std::vector<int16_t> values = {0, 1, 2, 3, 4, 5, 6};
    std::vector<int> lengths = {3, 0, 4};
    std::vector<uint8_t> is_valid = {1, 0, 1};

    Int16Builder* vb = checked_cast<Int16Builder*>(builder_->value_builder());

    ASSERT_OK(builder_->Reserve(lengths.size()));
    ASSERT_OK(vb->Reserve(values.size()));

    int pos = 0;
    for (size_t i = 0; i < lengths.size(); ++i) {
      ASSERT_OK(builder_->Append(is_valid[i] > 0));
      for (int j = 0; j < lengths[i]; ++j) {
        ASSERT_OK(vb->Append(values[pos++]));
      }
    }

    Done();
    ValidateBasicListArray(result_.get(), values, is_valid);
  }

  void TestEquality() {
    auto vb = checked_cast<Int16Builder*>(builder_->value_builder());

    std::shared_ptr<Array> array, equal_array, unequal_array;
    std::vector<offset_type> equal_offsets = {0, 1, 2, 5, 6, 7, 8, 10};
    std::vector<int16_t> equal_values = {1, 2, 3, 4, 5, 2, 2, 2, 5, 6};
    std::vector<offset_type> unequal_offsets = {0, 1, 4, 7};
    std::vector<int16_t> unequal_values = {1, 2, 2, 2, 3, 4, 5};

    // setup two equal arrays
    ASSERT_OK(builder_->AppendValues(equal_offsets.data(), equal_offsets.size()));
    ASSERT_OK(vb->AppendValues(equal_values.data(), equal_values.size()));

    ASSERT_OK(builder_->Finish(&array));
    ASSERT_OK(builder_->AppendValues(equal_offsets.data(), equal_offsets.size()));
    ASSERT_OK(vb->AppendValues(equal_values.data(), equal_values.size()));

    ASSERT_OK(builder_->Finish(&equal_array));
    // now an unequal one
    ASSERT_OK(builder_->AppendValues(unequal_offsets.data(), unequal_offsets.size()));
    ASSERT_OK(vb->AppendValues(unequal_values.data(), unequal_values.size()));

    ASSERT_OK(builder_->Finish(&unequal_array));

    // Test array equality
    EXPECT_TRUE(array->Equals(array));
    EXPECT_TRUE(array->Equals(equal_array));
    EXPECT_TRUE(equal_array->Equals(array));
    EXPECT_FALSE(equal_array->Equals(unequal_array));
    EXPECT_FALSE(unequal_array->Equals(equal_array));

    // Test range equality
    EXPECT_TRUE(array->RangeEquals(0, 1, 0, unequal_array));
    EXPECT_FALSE(array->RangeEquals(0, 2, 0, unequal_array));
    EXPECT_FALSE(array->RangeEquals(1, 2, 1, unequal_array));
    EXPECT_TRUE(array->RangeEquals(2, 3, 2, unequal_array));

    // Check with slices, ARROW-33
    std::shared_ptr<Array> slice, slice2;

    slice = array->Slice(2);
    slice2 = array->Slice(2);
    ASSERT_EQ(array->length() - 2, slice->length());

    ASSERT_TRUE(slice->Equals(slice2));
    ASSERT_TRUE(array->RangeEquals(2, slice->length(), 0, slice));

    // Chained slices
    slice2 = array->Slice(1)->Slice(1);
    ASSERT_TRUE(slice->Equals(slice2));

    slice = array->Slice(1, 4);
    slice2 = array->Slice(1, 4);
    ASSERT_EQ(4, slice->length());

    ASSERT_TRUE(slice->Equals(slice2));
    ASSERT_TRUE(array->RangeEquals(1, 5, 0, slice));
  }

  void TestValuesEquality() {
    auto type = std::make_shared<T>(int32());
    auto left = ArrayFromJSON(type, "[[1, 2], [3], [0]]");
    auto right = ArrayFromJSON(type, "[[1, 2], [3], [100000]]");
    auto offset = 2;
    EXPECT_FALSE(left->Slice(offset)->Equals(right->Slice(offset)));
  }

  void TestFromArraysWithNullBitMap() {
    std::shared_ptr<Array> offsets_w_nulls, offsets_wo_nulls, values;

    std::vector<offset_type> offsets = {0, 1, 1, 3, 4};
    std::vector<bool> offsets_w_nulls_is_valid = {true, false, true, true, true};

    ArrayFromVector<OffsetType, offset_type>(offsets_w_nulls_is_valid, offsets,
                                             &offsets_w_nulls);
    ArrayFromVector<OffsetType, offset_type>(offsets, &offsets_wo_nulls);

    auto type = std::make_shared<T>(int32());
    auto expected = std::dynamic_pointer_cast<ArrayType>(
        ArrayFromJSON(type, "[[0], null, [0, null], [0]]"));
    values = expected->values();

    // Offsets with nulls will match.
    ASSERT_OK_AND_ASSIGN(auto result,
                         ArrayType::FromArrays(*offsets_w_nulls, *values, pool_));
    AssertArraysEqual(*result, *expected);

    // Offets without nulls, will replace null with empty list
    ASSERT_OK_AND_ASSIGN(result,
                         ArrayType::FromArrays(*offsets_wo_nulls, *values, pool_));
    AssertArraysEqual(*result, *std::dynamic_pointer_cast<ArrayType>(
                                   ArrayFromJSON(type, "[[0], [], [0, null], [0]]")));

    // Specify non-null offsets with null_bitmap
    ASSERT_OK_AND_ASSIGN(result, ArrayType::FromArrays(*offsets_wo_nulls, *values, pool_,
                                                       expected->null_bitmap()));
    AssertArraysEqual(*result, *expected);

    // Cannot specify both null offsets with null_bitmap
    ASSERT_RAISES(Invalid, ArrayType::FromArrays(*offsets_w_nulls, *values, pool_,
                                                 expected->null_bitmap()));
  }

  void TestFromArrays() {
    std::shared_ptr<Array> offsets1, offsets2, offsets3, offsets4, offsets5, values;

    std::vector<bool> offsets_is_valid3 = {true, false, true, true};
    std::vector<bool> offsets_is_valid4 = {true, true, false, true};
    std::vector<bool> offsets_is_valid5 = {true, true, false, false};

    std::vector<bool> values_is_valid = {true, false, true, true, true, true};

    std::vector<offset_type> offset1_values = {0, 2, 2, 6};
    std::vector<offset_type> offset2_values = {0, 2, 6, 6};

    std::vector<int8_t> values_values = {0, 1, 2, 3, 4, 5};
    const int length = 3;

    ArrayFromVector<OffsetType, offset_type>(offset1_values, &offsets1);
    ArrayFromVector<OffsetType, offset_type>(offset2_values, &offsets2);

    ArrayFromVector<OffsetType, offset_type>(offsets_is_valid3, offset1_values,
                                             &offsets3);
    ArrayFromVector<OffsetType, offset_type>(offsets_is_valid4, offset2_values,
                                             &offsets4);
    ArrayFromVector<OffsetType, offset_type>(offsets_is_valid5, offset2_values,
                                             &offsets5);

    ArrayFromVector<Int8Type, int8_t>(values_is_valid, values_values, &values);

    auto list_type = std::make_shared<T>(int8());

    ASSERT_OK_AND_ASSIGN(auto list1, ArrayType::FromArrays(*offsets1, *values, pool_));
    ASSERT_OK_AND_ASSIGN(auto list3, ArrayType::FromArrays(*offsets3, *values, pool_));
    ASSERT_OK_AND_ASSIGN(auto list4, ArrayType::FromArrays(*offsets4, *values, pool_));
    ASSERT_OK(list1->ValidateFull());
    ASSERT_OK(list3->ValidateFull());
    ASSERT_OK(list4->ValidateFull());

    ArrayType expected1(list_type, length, offsets1->data()->buffers[1], values,
                        offsets1->data()->buffers[0], 0);
    AssertArraysEqual(expected1, *list1);

    // Use null bitmap from offsets3, but clean offsets from non-null version
    ArrayType expected3(list_type, length, offsets1->data()->buffers[1], values,
                        offsets3->data()->buffers[0], 1);
    AssertArraysEqual(expected3, *list3);

    // Check that the last offset bit is zero
    ASSERT_FALSE(bit_util::GetBit(list3->null_bitmap()->data(), length + 1));

    ArrayType expected4(list_type, length, offsets2->data()->buffers[1], values,
                        offsets4->data()->buffers[0], 1);
    AssertArraysEqual(expected4, *list4);

    // Test failure modes

    std::shared_ptr<Array> tmp;

    // Zero-length offsets
    ASSERT_RAISES(Invalid, ArrayType::FromArrays(*offsets1->Slice(0, 0), *values, pool_));

    // Offsets not the right type
    ASSERT_RAISES(TypeError, ArrayType::FromArrays(*values, *offsets1, pool_));

    // Null final offset
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Last list offset should be non-null"),
        ArrayType::FromArrays(*offsets5, *values, pool_));

    // ARROW-12077: check for off-by-one in construction (need mimalloc/ASan/Valgrind)
    {
      std::shared_ptr<Array> offsets, values;
      // Length multiple of 8 - we'll allocate a validity buffer with exactly enough bits
      // (Need a large enough buffer or else ASan doesn't catch it)
      std::vector<bool> offsets_is_valid(4096);
      std::vector<offset_type> offset_values(4096);
      std::vector<int8_t> values_values(4096);
      std::fill(offsets_is_valid.begin(), offsets_is_valid.end(), true);
      offsets_is_valid[1] = false;
      std::fill(offset_values.begin(), offset_values.end(), 0);
      std::fill(values_values.begin(), values_values.end(), 0);
      ArrayFromVector<OffsetType, offset_type>(offsets_is_valid, offset_values, &offsets);
      ArrayFromVector<Int8Type, int8_t>(values_values, &values);
      ASSERT_OK_AND_ASSIGN(auto list, ArrayType::FromArrays(*offsets, *values, pool_));
    }
  }

  void TestAppendNull() {
    ASSERT_OK(builder_->AppendNull());
    ASSERT_OK(builder_->AppendNull());

    Done();

    ASSERT_OK(result_->ValidateFull());
    ASSERT_TRUE(result_->IsNull(0));
    ASSERT_TRUE(result_->IsNull(1));

    ASSERT_EQ(0, result_->raw_value_offsets()[0]);
    ASSERT_EQ(0, result_->value_offset(1));
    ASSERT_EQ(0, result_->value_offset(2));

    auto values = result_->values();
    ASSERT_EQ(0, values->length());
    // Values buffer should be non-null
    ASSERT_NE(nullptr, values->data()->buffers[1]);
  }

  void TestAppendNulls() {
    ASSERT_OK(builder_->AppendNulls(3));

    Done();

    ASSERT_OK(result_->ValidateFull());
    ASSERT_EQ(result_->length(), 3);
    ASSERT_EQ(result_->null_count(), 3);
    ASSERT_TRUE(result_->IsNull(0));
    ASSERT_TRUE(result_->IsNull(1));
    ASSERT_TRUE(result_->IsNull(2));

    ASSERT_EQ(0, result_->raw_value_offsets()[0]);
    ASSERT_EQ(0, result_->value_offset(1));
    ASSERT_EQ(0, result_->value_offset(2));
    ASSERT_EQ(0, result_->value_offset(3));

    auto values = result_->values();
    ASSERT_EQ(0, values->length());
    // Values buffer should be non-null
    ASSERT_NE(nullptr, values->data()->buffers[1]);
  }

  void TestBulkAppend() {
    std::vector<int16_t> values = {0, 1, 2, 3, 4, 5, 6};
    std::vector<uint8_t> is_valid = {1, 0, 1};
    std::vector<offset_type> offsets = {0, 3, 3};

    Int16Builder* vb = checked_cast<Int16Builder*>(builder_->value_builder());
    ASSERT_OK(vb->Reserve(values.size()));

    ASSERT_OK(builder_->AppendValues(offsets.data(), offsets.size(), is_valid.data()));
    for (int16_t value : values) {
      ASSERT_OK(vb->Append(value));
    }
    Done();
    ValidateBasicListArray(result_.get(), values, is_valid);
  }

  void TestBulkAppendInvalid() {
    std::vector<int16_t> values = {0, 1, 2, 3, 4, 5, 6};
    std::vector<int> lengths = {3, 0, 4};
    std::vector<uint8_t> is_valid = {1, 0, 1};
    // Should be {0, 3, 3} given the is_valid array
    std::vector<offset_type> offsets = {0, 2, 4};

    Int16Builder* vb = checked_cast<Int16Builder*>(builder_->value_builder());
    ASSERT_OK(vb->Reserve(values.size()));

    ASSERT_OK(builder_->AppendValues(offsets.data(), offsets.size(), is_valid.data()));
    ASSERT_OK(builder_->AppendValues(offsets.data(), offsets.size(), is_valid.data()));
    for (int16_t value : values) {
      ASSERT_OK(vb->Append(value));
    }

    Done();
    ASSERT_RAISES(Invalid, result_->ValidateFull());
  }

  void TestZeroLength() {
    // All buffers are null
    Done();
    ASSERT_OK(result_->ValidateFull());
  }

  void TestBuilderPreserveFieldName() {
    auto list_type_with_name = std::make_shared<T>(field("counts", int16()));

    std::unique_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, list_type_with_name, &tmp));
    builder_.reset(checked_cast<BuilderType*>(tmp.release()));

    std::vector<offset_type> offsets = {1, 2, 4, 8};
    ASSERT_OK(builder_->AppendValues(offsets.data(), offsets.size()));

    std::shared_ptr<Array> list_array;
    ASSERT_OK(builder_->Finish(&list_array));

    const auto& type = checked_cast<T&>(*list_array->type());
    ASSERT_EQ("counts", type.value_field()->name());
  }

  void TestFlattenZeroLength() {
    Done();
    ASSERT_OK_AND_ASSIGN(auto flattened, result_->Flatten());
    ASSERT_OK(flattened->ValidateFull());
    ASSERT_EQ(0, flattened->length());
  }

  void TestFlattenSimple() {
    auto type = std::make_shared<T>(int32());
    auto list_array = std::dynamic_pointer_cast<ArrayType>(
        ArrayFromJSON(type, "[[1, 2], [3], [4], null, [5], [], [6]]"));
    ASSERT_OK_AND_ASSIGN(auto flattened, list_array->Flatten());
    ASSERT_OK(flattened->ValidateFull());
    EXPECT_TRUE(flattened->Equals(ArrayFromJSON(int32(), "[1, 2, 3, 4, 5, 6]")));
  }

  void TestFlattenNulls() {
    ASSERT_OK(builder_->AppendNulls(2));
    Done();
    ASSERT_OK_AND_ASSIGN(auto flattened, result_->Flatten());
    ASSERT_OK(flattened->ValidateFull());
    ASSERT_EQ(0, flattened->length());
    AssertTypeEqual(*flattened->type(), *value_type_);
  }

  void TestFlattenSliced() {
    auto type = std::make_shared<T>(int32());
    auto list_array = std::dynamic_pointer_cast<ArrayType>(
        ArrayFromJSON(type, "[[1, 2], [3], [4], null, [5], [], [6]]"));
    auto sliced_list_array =
        std::dynamic_pointer_cast<ArrayType>(list_array->Slice(3, 4));
    ASSERT_OK_AND_ASSIGN(auto flattened, list_array->Flatten());
    ASSERT_OK(flattened->ValidateFull());
    // Note the difference between values() and Flatten().
    EXPECT_TRUE(flattened->Equals(ArrayFromJSON(int32(), "[5, 6]")));
    EXPECT_TRUE(sliced_list_array->values()->Equals(
        ArrayFromJSON(int32(), "[1, 2, 3, 4, 5, 6]")));
  }

  void TestFlattenNonEmptyBackingNulls() {
    auto type = std::make_shared<T>(int32());
    auto array_data =
        std::dynamic_pointer_cast<ArrayType>(
            ArrayFromJSON(type, "[[1, 2], [3], null, [5, 6], [7, 8], [], [9]]"))
            ->data();
    ASSERT_EQ(2, array_data->buffers.size());
    auto null_bitmap_buffer = array_data->buffers[0];
    ASSERT_NE(nullptr, null_bitmap_buffer);
    bit_util::ClearBit(null_bitmap_buffer->mutable_data(), 1);
    bit_util::ClearBit(null_bitmap_buffer->mutable_data(), 3);
    bit_util::ClearBit(null_bitmap_buffer->mutable_data(), 4);
    array_data->null_count += 3;
    auto list_array = std::dynamic_pointer_cast<ArrayType>(MakeArray(array_data));
    ASSERT_OK(list_array->ValidateFull());
    ASSERT_OK_AND_ASSIGN(auto flattened, list_array->Flatten());
    EXPECT_TRUE(flattened->Equals(ArrayFromJSON(int32(), "[1, 2, 9]")))
        << flattened->ToString();
  }

  Status ValidateOffsets(int64_t length, std::vector<offset_type> offsets,
                         const std::shared_ptr<Array>& values, int64_t offset = 0) {
    auto type = std::make_shared<TypeClass>(values->type());
    ArrayType arr(type, length, Buffer::Wrap(offsets), values,
                  /*null_bitmap=*/nullptr, /*null_count=*/0, offset);
    return arr.ValidateFull();
  }

  void TestValidateOffsets() {
    auto empty_values = ArrayFromJSON(int16(), "[]");
    auto values = ArrayFromJSON(int16(), "[1, 2, 3, 4, 5, 6, 7]");

    // An empty list array can have omitted or 0-length offsets
    ASSERT_OK(ValidateOffsets(0, {}, empty_values));

    ASSERT_OK(ValidateOffsets(0, {0}, empty_values));
    ASSERT_OK(ValidateOffsets(1, {0, 7}, values));
    ASSERT_OK(ValidateOffsets(2, {0, 4, 7}, values));
    ASSERT_OK(ValidateOffsets(3, {0, 4, 7, 7}, values));

    // Non-zero array offset
    ASSERT_OK(ValidateOffsets(1, {0, 4, 7}, values, 1));
    ASSERT_OK(ValidateOffsets(0, {0, 4, 7}, values, 2));

    // Not enough offsets
    ASSERT_RAISES(Invalid, ValidateOffsets(1, {0}, values));
    ASSERT_RAISES(Invalid, ValidateOffsets(2, {0, 0}, values, 1));

    // Offset out of bounds
    ASSERT_RAISES(Invalid, ValidateOffsets(1, {0, 8}, values));
    ASSERT_RAISES(Invalid, ValidateOffsets(1, {0, 8, 8}, values, 1));
    // Negative offset
    ASSERT_RAISES(Invalid, ValidateOffsets(1, {-1, 0}, values));
    ASSERT_RAISES(Invalid, ValidateOffsets(1, {0, -1}, values));
    ASSERT_RAISES(Invalid, ValidateOffsets(2, {0, -1, -1}, values, 1));
    // Offsets non-monotonic
    ASSERT_RAISES(Invalid, ValidateOffsets(2, {0, 7, 4}, values));
  }

  void TestCornerCases() {
    // ARROW-7985
    ASSERT_OK(builder_->AppendNull());
    Done();
    auto expected = ArrayFromJSON(type_, "[null]");
    AssertArraysEqual(*result_, *expected);

    SetUp();
    ASSERT_OK(builder_->Append());
    Done();
    expected = ArrayFromJSON(type_, "[[]]");
    AssertArraysEqual(*result_, *expected);

    SetUp();
    ASSERT_OK(builder_->AppendNull());
    ASSERT_OK(builder_->value_builder()->Reserve(100));
    Done();
    expected = ArrayFromJSON(type_, "[null]");
    AssertArraysEqual(*result_, *expected);
  }

  void TestOverflowCheck() {
    Int16Builder* vb = checked_cast<Int16Builder*>(builder_->value_builder());
    auto max_elements = builder_->maximum_elements();

    ASSERT_OK(builder_->ValidateOverflow(1));
    ASSERT_OK(builder_->ValidateOverflow(max_elements));
    ASSERT_RAISES(CapacityError, builder_->ValidateOverflow(max_elements + 1));

    ASSERT_OK(builder_->Append());
    ASSERT_OK(vb->Append(1));
    ASSERT_OK(vb->Append(2));
    ASSERT_OK(builder_->ValidateOverflow(max_elements - 2));
    ASSERT_RAISES(CapacityError, builder_->ValidateOverflow(max_elements - 1));

    ASSERT_OK(builder_->AppendNull());
    ASSERT_OK(builder_->ValidateOverflow(max_elements - 2));
    ASSERT_RAISES(CapacityError, builder_->ValidateOverflow(max_elements - 1));

    ASSERT_OK(builder_->Append());
    ASSERT_OK(vb->Append(1));
    ASSERT_OK(vb->Append(2));
    ASSERT_OK(vb->Append(3));
    ASSERT_OK(builder_->ValidateOverflow(max_elements - 5));
    ASSERT_RAISES(CapacityError, builder_->ValidateOverflow(max_elements - 4));
  }

 protected:
  MemoryPool* pool_ = default_memory_pool();
  std::shared_ptr<DataType> type_;
  std::shared_ptr<DataType> value_type_;

  std::shared_ptr<BuilderType> builder_;
  std::shared_ptr<ArrayType> result_;
};

TYPED_TEST_SUITE(TestListArray, ListTypes);

TYPED_TEST(TestListArray, Basics) { this->TestBasics(); }

TYPED_TEST(TestListArray, Equality) { this->TestEquality(); }

TYPED_TEST(TestListArray, ValuesEquality) { this->TestValuesEquality(); }

TYPED_TEST(TestListArray, FromArrays) { this->TestFromArrays(); }

TYPED_TEST(TestListArray, FromArraysWithNullBitMap) {
  this->TestFromArraysWithNullBitMap();
}

TYPED_TEST(TestListArray, AppendNull) { this->TestAppendNull(); }

TYPED_TEST(TestListArray, AppendNulls) { this->TestAppendNulls(); }

TYPED_TEST(TestListArray, BulkAppend) { this->TestBulkAppend(); }

TYPED_TEST(TestListArray, BulkAppendInvalid) { this->TestBulkAppendInvalid(); }

TYPED_TEST(TestListArray, ZeroLength) { this->TestZeroLength(); }

TYPED_TEST(TestListArray, BuilderPreserveFieldName) {
  this->TestBuilderPreserveFieldName();
}

TYPED_TEST(TestListArray, FlattenSimple) { this->TestFlattenSimple(); }
TYPED_TEST(TestListArray, FlattenNulls) { this->TestFlattenNulls(); }
TYPED_TEST(TestListArray, FlattenZeroLength) { this->TestFlattenZeroLength(); }
TYPED_TEST(TestListArray, TestFlattenNonEmptyBackingNulls) {
  this->TestFlattenNonEmptyBackingNulls();
}

TYPED_TEST(TestListArray, ValidateOffsets) { this->TestValidateOffsets(); }

TYPED_TEST(TestListArray, CornerCases) { this->TestCornerCases(); }

#ifndef ARROW_LARGE_MEMORY_TESTS
TYPED_TEST(TestListArray, DISABLED_TestOverflowCheck) { this->TestOverflowCheck(); }
#else
TYPED_TEST(TestListArray, TestOverflowCheck) { this->TestOverflowCheck(); }
#endif

// ----------------------------------------------------------------------
// Map tests

class TestMapArray : public ::testing::Test {
 public:
  using offset_type = typename MapType::offset_type;
  using OffsetType = typename TypeTraits<MapType>::OffsetType;

  void SetUp() {
    key_type_ = utf8();
    value_type_ = int32();
    type_ = map(key_type_, value_type_);

    std::unique_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_ = checked_pointer_cast<MapBuilder>(std::move(tmp));
  }

  void Done() {
    std::shared_ptr<Array> out;
    FinishAndCheckPadding(builder_.get(), &out);
    result_ = std::dynamic_pointer_cast<MapArray>(out);
  }

 protected:
  MemoryPool* pool_ = default_memory_pool();
  std::shared_ptr<DataType> type_, value_type_, key_type_;

  std::shared_ptr<MapBuilder> builder_;
  std::shared_ptr<MapArray> result_;
};

TEST_F(TestMapArray, Equality) {
  auto& kb = checked_cast<StringBuilder&>(*builder_->key_builder());
  auto& ib = checked_cast<Int32Builder&>(*builder_->item_builder());

  std::shared_ptr<Array> array, equal_array, unequal_array;
  std::vector<int32_t> equal_offsets = {0, 1, 2, 5, 6, 7, 8, 10};
  std::vector<std::string_view> equal_keys = {"a", "a", "a", "b", "c",
                                              "a", "a", "a", "a", "b"};
  std::vector<int32_t> equal_values = {1, 2, 3, 4, 5, 2, 2, 2, 5, 6};
  std::vector<int32_t> unequal_offsets = {0, 1, 4, 7};
  std::vector<std::string_view> unequal_keys = {"a", "a", "b", "c", "a", "b", "c"};
  std::vector<int32_t> unequal_values = {1, 2, 2, 2, 3, 4, 5};

  // setup two equal arrays
  for (auto out : {&array, &equal_array}) {
    ASSERT_OK(builder_->AppendValues(equal_offsets.data(), equal_offsets.size()));
    for (auto&& key : equal_keys) {
      ASSERT_OK(kb.Append(key));
    }
    ASSERT_OK(ib.AppendValues(equal_values.data(), equal_values.size()));
    ASSERT_OK(builder_->Finish(out));
    ASSERT_OK((*out)->ValidateFull());
  }

  // now an unequal one
  ASSERT_OK(builder_->AppendValues(unequal_offsets.data(), unequal_offsets.size()));
  for (auto&& key : unequal_keys) {
    ASSERT_OK(kb.Append(key));
  }
  ASSERT_OK(ib.AppendValues(unequal_values.data(), unequal_values.size()));
  ASSERT_OK(builder_->Finish(&unequal_array));
  ASSERT_OK(unequal_array->ValidateFull());

  // Test array equality
  EXPECT_TRUE(array->Equals(array));
  EXPECT_TRUE(array->Equals(equal_array));
  EXPECT_TRUE(equal_array->Equals(array));
  EXPECT_FALSE(equal_array->Equals(unequal_array));
  EXPECT_FALSE(unequal_array->Equals(equal_array));

  // Test range equality
  EXPECT_TRUE(array->RangeEquals(0, 1, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(0, 2, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(1, 2, 1, unequal_array));
  EXPECT_TRUE(array->RangeEquals(2, 3, 2, unequal_array));
}

TEST_F(TestMapArray, BuildingIntToInt) {
  auto type = map(int16(), int16());

  auto expected_keys = ArrayFromJSON(int16(), R"([
    0, 1, 2, 3, 4, 5,
    0, 1, 2, 3, 4, 5
  ])");
  auto expected_items = ArrayFromJSON(int16(), R"([
    1,    1,    2,  3,  5,    8,
    null, null, 0,  1,  null, 2
  ])");
  auto expected_offsets = ArrayFromJSON(int32(), "[0, 6, 6, 12, 12]")->data()->buffers[1];
  auto expected_null_bitmap =
      ArrayFromJSON(boolean(), "[1, 0, 1, 1]")->data()->buffers[1];

  MapArray expected(type, 4, expected_offsets, expected_keys, expected_items,
                    expected_null_bitmap, 1, 0);

  auto key_builder = std::make_shared<Int16Builder>();
  auto item_builder = std::make_shared<Int16Builder>();
  MapBuilder map_builder(default_memory_pool(), key_builder, item_builder);

  std::shared_ptr<Array> actual;
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(key_builder->AppendValues({0, 1, 2, 3, 4, 5}));
  ASSERT_OK(item_builder->AppendValues({1, 1, 2, 3, 5, 8}));
  ASSERT_OK(map_builder.AppendNull());
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(key_builder->AppendValues({0, 1, 2, 3, 4, 5}));
  ASSERT_OK(item_builder->AppendValues({-1, -1, 0, 1, -1, 2}, {0, 0, 1, 1, 0, 1}));
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(map_builder.Finish(&actual));
  ASSERT_OK(actual->ValidateFull());

  ASSERT_ARRAYS_EQUAL(*actual, expected);
}

TEST_F(TestMapArray, BuildingStringToInt) {
  auto type = map(utf8(), int32());

  std::vector<int32_t> offsets = {0, 2, 2, 3, 3};
  auto expected_keys = ArrayFromJSON(utf8(), R"(["joe", "mark", "cap"])");
  auto expected_values = ArrayFromJSON(int32(), "[0, null, 8]");
  ASSERT_OK_AND_ASSIGN(auto expected_null_bitmap, internal::BytesToBits({1, 0, 1, 1}));
  MapArray expected(type, 4, Buffer::Wrap(offsets), expected_keys, expected_values,
                    expected_null_bitmap, 1);

  auto key_builder = std::make_shared<StringBuilder>();
  auto item_builder = std::make_shared<Int32Builder>();
  MapBuilder map_builder(default_memory_pool(), key_builder, item_builder);

  std::shared_ptr<Array> actual;
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(key_builder->Append("joe"));
  ASSERT_OK(item_builder->Append(0));
  ASSERT_OK(key_builder->Append("mark"));
  ASSERT_OK(item_builder->AppendNull());
  ASSERT_OK(map_builder.AppendNull());
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(key_builder->Append("cap"));
  ASSERT_OK(item_builder->Append(8));
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(map_builder.Finish(&actual));
  ASSERT_OK(actual->ValidateFull());

  ASSERT_ARRAYS_EQUAL(*actual, expected);
}

TEST_F(TestMapArray, BuildingWithFieldNames) {
  // Builder should preserve field names in output Array
  ASSERT_OK_AND_ASSIGN(auto map_type,
                       MapType::Make(field("some_entries",
                                           struct_({field("some_key", int16(), false),
                                                    field("some_value", int16())}),
                                           false)));

  auto key_builder = std::make_shared<Int16Builder>();
  auto item_builder = std::make_shared<Int16Builder>();
  MapBuilder map_builder(default_memory_pool(), key_builder, item_builder, map_type);

  std::shared_ptr<Array> actual;
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(key_builder->AppendValues({0, 1, 2, 3, 4, 5}));
  ASSERT_OK(item_builder->AppendValues({1, 1, 2, 3, 5, 8}));
  ASSERT_OK(map_builder.AppendNull());
  ASSERT_OK(map_builder.Finish(&actual));
  ASSERT_OK(actual->ValidateFull());

  ASSERT_EQ(actual->type()->ToString(), map_type->ToString());
  ASSERT_EQ(map_builder.type()->ToString(), map_type->ToString());
}

TEST_F(TestMapArray, ValidateErrorNullStruct) {
  ASSERT_OK_AND_ASSIGN(
      auto values,
      MakeArrayOfNull(struct_({field("key", utf8()), field("value", int32())}), 1));

  Int32Builder offset_builder;
  ASSERT_OK(offset_builder.AppendNull());
  ASSERT_OK(offset_builder.Append(0));
  ASSERT_OK_AND_ASSIGN(auto offsets, offset_builder.Finish());

  ASSERT_OK_AND_ASSIGN(auto lists, ListArray::FromArrays(*offsets, *values));
  ASSERT_OK(lists->ValidateFull());
  ASSERT_EQ(lists->length(), 1);
  ASSERT_EQ(lists->null_count(), 1);

  // Make a Map ArrayData from the list array
  // Note we can't construct a MapArray as that would crash with an assertion.
  auto map_data = lists->data()->Copy();
  map_data->type = map(utf8(), int32());
  ASSERT_RAISES(Invalid, internal::ValidateArray(*map_data));
}

TEST_F(TestMapArray, ValidateErrorNullKey) {
  StringBuilder key_builder;
  ASSERT_OK(key_builder.AppendNull());
  ASSERT_OK_AND_ASSIGN(auto keys, key_builder.Finish());

  Int32Builder item_builder;
  ASSERT_OK(item_builder.Append(42));
  ASSERT_OK_AND_ASSIGN(auto items, item_builder.Finish());

  ASSERT_OK_AND_ASSIGN(
      auto values,
      StructArray::Make({keys, items}, std::vector<std::string>{"key", "value"}));

  Int32Builder offset_builder;
  ASSERT_OK(offset_builder.Append(0));
  ASSERT_OK(offset_builder.Append(1));
  ASSERT_OK_AND_ASSIGN(auto offsets, offset_builder.Finish());

  // The list array contains: [[null, 42]]
  ASSERT_OK_AND_ASSIGN(auto lists, ListArray::FromArrays(*offsets, *values));
  ASSERT_OK(lists->ValidateFull());

  // Make a Map ArrayData from the list array
  // Note we can't construct a MapArray as that would crash with an assertion.
  auto map_data = lists->data()->Copy();
  map_data->type = map(keys->type(), items->type());
  ASSERT_RAISES(Invalid, internal::ValidateArray(*map_data));
}

TEST_F(TestMapArray, FromArrays) {
  std::shared_ptr<Array> offsets1, offsets2, offsets3, offsets4, keys, items;

  std::vector<bool> offsets_is_valid3 = {true, false, true, true};
  std::vector<bool> offsets_is_valid4 = {true, true, false, true};

  std::vector<bool> items_is_valid = {true, false, true, true, true, true};

  std::vector<MapType::offset_type> offset1_values = {0, 2, 2, 6};
  std::vector<MapType::offset_type> offset2_values = {0, 2, 6, 6};

  std::vector<int8_t> key_values = {0, 1, 2, 3, 4, 5};
  std::vector<int16_t> item_values = {10, 9, 8, 7, 6, 5};
  const int length = 3;

  ArrayFromVector<OffsetType, offset_type>(offset1_values, &offsets1);
  ArrayFromVector<OffsetType, offset_type>(offset2_values, &offsets2);

  ArrayFromVector<OffsetType, offset_type>(offsets_is_valid3, offset1_values, &offsets3);
  ArrayFromVector<OffsetType, offset_type>(offsets_is_valid4, offset2_values, &offsets4);

  ArrayFromVector<Int8Type, int8_t>(key_values, &keys);
  ArrayFromVector<Int16Type, int16_t>(items_is_valid, item_values, &items);

  auto map_type = map(int8(), int16());

  ASSERT_OK_AND_ASSIGN(auto map1, MapArray::FromArrays(offsets1, keys, items, pool_));
  ASSERT_OK_AND_ASSIGN(auto map3, MapArray::FromArrays(offsets3, keys, items, pool_));
  ASSERT_OK_AND_ASSIGN(auto map4, MapArray::FromArrays(offsets4, keys, items, pool_));
  ASSERT_OK(map1->Validate());
  ASSERT_OK(map3->Validate());
  ASSERT_OK(map4->Validate());

  MapArray expected1(map_type, length, offsets1->data()->buffers[1], keys, items,
                     offsets1->data()->buffers[0], 0);
  AssertArraysEqual(expected1, *map1);

  // Use null bitmap from offsets3, but clean offsets from non-null version
  MapArray expected3(map_type, length, offsets1->data()->buffers[1], keys, items,
                     offsets3->data()->buffers[0], 1);
  AssertArraysEqual(expected3, *map3);

  // Check that the last offset bit is zero
  ASSERT_FALSE(bit_util::GetBit(map3->null_bitmap()->data(), length + 1));

  MapArray expected4(map_type, length, offsets2->data()->buffers[1], keys, items,
                     offsets4->data()->buffers[0], 1);
  AssertArraysEqual(expected4, *map4);

  // Test failure modes

  std::shared_ptr<Array> tmp;

  // Zero-length offsets
  ASSERT_RAISES(Invalid, MapArray::FromArrays(offsets1->Slice(0, 0), keys, items, pool_));

  // Offsets not the right type
  ASSERT_RAISES(TypeError, MapArray::FromArrays(keys, offsets1, items, pool_));

  // Keys and Items different lengths
  ASSERT_RAISES(Invalid, MapArray::FromArrays(offsets1, keys->Slice(0, 1), items, pool_));

  // Keys contains null values
  std::shared_ptr<Array> keys_with_null = offsets3;
  std::shared_ptr<Array> tmp_items = items->Slice(0, offsets3->length());
  ASSERT_EQ(keys_with_null->length(), tmp_items->length());
  ASSERT_RAISES(Invalid,
                MapArray::FromArrays(offsets1, keys_with_null, tmp_items, pool_));
}

TEST_F(TestMapArray, FromArraysEquality) {
  // More equality tests using MapArray::FromArrays
  auto keys1 = ArrayFromJSON(utf8(), R"(["ab", "cd", "ef", "gh", "ij", "kl"])");
  auto keys2 = ArrayFromJSON(utf8(), R"(["ab", "cd", "ef", "gh", "ij", "kl"])");
  auto keys3 = ArrayFromJSON(utf8(), R"(["ab", "cd", "ef", "gh", "zz", "kl"])");
  auto items1 = ArrayFromJSON(int16(), "[1, 2, 3, 4, 5, 6]");
  auto items2 = ArrayFromJSON(int16(), "[1, 2, 3, 4, 5, 6]");
  auto items3 = ArrayFromJSON(int16(), "[1, 2, 3, null, 5, 6]");
  auto offsets1 = ArrayFromJSON(int32(), "[0, 1, 3, null, 5, 6]");
  auto offsets2 = ArrayFromJSON(int32(), "[0, 1, 3, null, 5, 6]");
  auto offsets3 = ArrayFromJSON(int32(), "[0, 1, 3, 3, 5, 6]");

  ASSERT_OK_AND_ASSIGN(auto array1, MapArray::FromArrays(offsets1, keys1, items1));
  ASSERT_OK_AND_ASSIGN(auto array2, MapArray::FromArrays(offsets2, keys2, items2));
  ASSERT_OK_AND_ASSIGN(auto array3, MapArray::FromArrays(offsets3, keys2, items2));
  ASSERT_OK_AND_ASSIGN(auto array4, MapArray::FromArrays(offsets2, keys3, items2));
  ASSERT_OK_AND_ASSIGN(auto array5, MapArray::FromArrays(offsets2, keys2, items3));
  ASSERT_OK_AND_ASSIGN(auto array6, MapArray::FromArrays(offsets3, keys3, items3));

  ASSERT_TRUE(array1->Equals(array2));
  ASSERT_TRUE(array1->RangeEquals(array2, 0, 5, 0));

  ASSERT_FALSE(array1->Equals(array3));  // different offsets
  ASSERT_FALSE(array1->RangeEquals(array3, 0, 5, 0));
  ASSERT_TRUE(array1->RangeEquals(array3, 0, 2, 0));
  ASSERT_FALSE(array1->RangeEquals(array3, 2, 5, 2));

  ASSERT_FALSE(array1->Equals(array4));  // different keys
  ASSERT_FALSE(array1->RangeEquals(array4, 0, 5, 0));
  ASSERT_TRUE(array1->RangeEquals(array4, 0, 2, 0));
  ASSERT_FALSE(array1->RangeEquals(array4, 2, 5, 2));

  ASSERT_FALSE(array1->Equals(array5));  // different items
  ASSERT_FALSE(array1->RangeEquals(array5, 0, 5, 0));
  ASSERT_TRUE(array1->RangeEquals(array5, 0, 2, 0));
  ASSERT_FALSE(array1->RangeEquals(array5, 2, 5, 2));

  ASSERT_FALSE(array1->Equals(array6));  // different everything
  ASSERT_FALSE(array1->RangeEquals(array6, 0, 5, 0));
  ASSERT_TRUE(array1->RangeEquals(array6, 0, 2, 0));
  ASSERT_FALSE(array1->RangeEquals(array6, 2, 5, 2));

  // Map array equality should be indifferent to field names
  ASSERT_OK_AND_ASSIGN(auto other_map_type,
                       MapType::Make(field("some_entries",
                                           struct_({field("some_key", utf8(), false),
                                                    field("some_value", int16())}),
                                           false)));
  ASSERT_OK_AND_ASSIGN(auto array7,
                       MapArray::FromArrays(other_map_type, offsets2, keys2, items2));
  ASSERT_TRUE(array1->Equals(array7));
  ASSERT_TRUE(array1->RangeEquals(array7, 0, 5, 0));
}

namespace {

template <typename TYPE>
Status BuildListOfStructPairs(TYPE& builder, std::shared_ptr<Array>* out) {
  auto struct_builder = internal::checked_cast<StructBuilder*>(builder.value_builder());
  auto field0_builder =
      internal::checked_cast<Int16Builder*>(struct_builder->field_builder(0));
  auto field1_builder =
      internal::checked_cast<Int16Builder*>(struct_builder->field_builder(1));

  RETURN_NOT_OK(builder.Append());
  RETURN_NOT_OK(field0_builder->AppendValues({0, 1}));
  RETURN_NOT_OK(field1_builder->AppendValues({1, -1}, {1, 0}));
  RETURN_NOT_OK(struct_builder->AppendValues(2, NULLPTR));
  RETURN_NOT_OK(builder.AppendNull());
  RETURN_NOT_OK(builder.Append());
  RETURN_NOT_OK(field0_builder->Append(2));
  RETURN_NOT_OK(field1_builder->Append(3));
  RETURN_NOT_OK(struct_builder->Append());
  RETURN_NOT_OK(builder.Append());
  RETURN_NOT_OK(builder.Append());
  RETURN_NOT_OK(field0_builder->AppendValues({3, 4}));
  RETURN_NOT_OK(field1_builder->AppendValues({4, 5}));
  RETURN_NOT_OK(struct_builder->AppendValues(2, NULLPTR));
  RETURN_NOT_OK(builder.Finish(out));
  RETURN_NOT_OK((*out)->Validate());

  return Status::OK();
}

}  // namespace

TEST_F(TestMapArray, ValueBuilder) {
  auto key_builder = std::make_shared<Int16Builder>();
  auto item_builder = std::make_shared<Int16Builder>();
  MapBuilder map_builder(default_memory_pool(), key_builder, item_builder);

  // Build Map array using key/item builder
  std::shared_ptr<Array> expected;
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(key_builder->AppendValues({0, 1}));
  ASSERT_OK(item_builder->AppendValues({1, -1}, {1, 0}));
  ASSERT_OK(map_builder.AppendNull());
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(key_builder->Append(2));
  ASSERT_OK(item_builder->Append(3));
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(map_builder.Append());
  ASSERT_OK(key_builder->AppendValues({3, 4}));
  ASSERT_OK(item_builder->AppendValues({4, 5}));
  ASSERT_OK(map_builder.Finish(&expected));
  ASSERT_OK(expected->Validate());

  map_builder.Reset();

  // Build Map array like an Array of Structs using value builder
  std::shared_ptr<Array> actual_map;
  ASSERT_OK(BuildListOfStructPairs(map_builder, &actual_map));
  ASSERT_ARRAYS_EQUAL(*actual_map, *expected);

  map_builder.Reset();

  // Build a ListArray of Structs, and compare MapArray to the List
  auto map_type = internal::checked_pointer_cast<MapType>(map_builder.type());
  auto struct_type = map_type->value_type();
  std::vector<std::shared_ptr<ArrayBuilder>> child_builders{key_builder, item_builder};
  auto struct_builder =
      std::make_shared<StructBuilder>(struct_type, default_memory_pool(), child_builders);
  ListBuilder list_builder(default_memory_pool(), struct_builder, map_type);

  std::shared_ptr<Array> actual_list;
  ASSERT_OK(BuildListOfStructPairs(list_builder, &actual_list));

  MapArray* map_ptr = internal::checked_cast<MapArray*>(actual_map.get());
  auto list_type = std::make_shared<ListType>(map_type->field(0));
  ListArray map_as_list(list_type, map_ptr->length(), map_ptr->data()->buffers[1],
                        map_ptr->values(), actual_map->data()->buffers[0],
                        map_ptr->null_count());

  ASSERT_ARRAYS_EQUAL(*actual_list, map_as_list);
}

// ----------------------------------------------------------------------
// FixedSizeList tests

class TestFixedSizeListArray : public ::testing::Test {
 public:
  void SetUp() {
    value_type_ = int32();
    type_ = fixed_size_list(value_type_, list_size());

    std::unique_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_.reset(checked_cast<FixedSizeListBuilder*>(tmp.release()));
  }

  void Done() {
    std::shared_ptr<Array> out;
    FinishAndCheckPadding(builder_.get(), &out);
    result_ = std::dynamic_pointer_cast<FixedSizeListArray>(out);
  }

 protected:
  static constexpr int32_t list_size() { return 2; }

  MemoryPool* pool_ = default_memory_pool();
  std::shared_ptr<DataType> type_, value_type_;

  std::shared_ptr<FixedSizeListBuilder> builder_;
  std::shared_ptr<FixedSizeListArray> result_;
};

TEST_F(TestFixedSizeListArray, Equality) {
  Int32Builder* vb = checked_cast<Int32Builder*>(builder_->value_builder());

  std::shared_ptr<Array> array, equal_array, unequal_array;
  std::vector<int32_t> equal_values = {1, 2, 3, 4, 5, 2, 2, 2, 5, 6};
  std::vector<int32_t> unequal_values = {1, 2, 2, 2, 3, 4, 5, 2};

  // setup two equal arrays
  ASSERT_OK(builder_->AppendValues(equal_values.size() / list_size()));
  ASSERT_OK(vb->AppendValues(equal_values.data(), equal_values.size()));
  ASSERT_OK(builder_->Finish(&array));

  ASSERT_OK(builder_->AppendValues(equal_values.size() / list_size()));
  ASSERT_OK(vb->AppendValues(equal_values.data(), equal_values.size()));

  ASSERT_OK(builder_->Finish(&equal_array));

  // now an unequal one
  ASSERT_OK(builder_->AppendValues(unequal_values.size() / list_size()));
  ASSERT_OK(vb->AppendValues(unequal_values.data(), unequal_values.size()));
  ASSERT_OK(builder_->Finish(&unequal_array));

  // Test array equality
  AssertArraysEqual(*array, *array);
  AssertArraysEqual(*array, *equal_array);
  EXPECT_FALSE(equal_array->Equals(unequal_array));
  EXPECT_FALSE(unequal_array->Equals(equal_array));

  // Test range equality
  EXPECT_TRUE(array->RangeEquals(0, 1, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(0, 2, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(1, 2, 1, unequal_array));
  EXPECT_TRUE(array->RangeEquals(1, 3, 2, unequal_array));
}

TEST_F(TestFixedSizeListArray, TestAppendNull) {
  ASSERT_OK(builder_->AppendNull());
  ASSERT_OK(builder_->AppendNull());

  Done();

  ASSERT_OK(result_->ValidateFull());
  ASSERT_TRUE(result_->IsNull(0));
  ASSERT_TRUE(result_->IsNull(1));

  ASSERT_EQ(0, result_->value_offset(0));
  ASSERT_EQ(list_size(), result_->value_offset(1));

  auto values = result_->values();
  ASSERT_EQ(list_size() * 2, values->length());
}

TEST_F(TestFixedSizeListArray, TestAppendNulls) {
  ASSERT_OK(builder_->AppendNulls(3));

  Done();

  ASSERT_OK(result_->ValidateFull());
  ASSERT_EQ(result_->length(), 3);
  ASSERT_EQ(result_->null_count(), 3);
  ASSERT_TRUE(result_->IsNull(0));
  ASSERT_TRUE(result_->IsNull(1));
  ASSERT_TRUE(result_->IsNull(2));

  ASSERT_EQ(0, result_->value_offset(0));
  ASSERT_EQ(list_size(), result_->value_offset(1));
  ASSERT_EQ(list_size() * 2, result_->value_offset(2));

  auto values = result_->values();
  ASSERT_EQ(list_size() * 3, values->length());
}

void ValidateBasicFixedSizeListArray(const FixedSizeListArray* result,
                                     const std::vector<int32_t>& values,
                                     const std::vector<uint8_t>& is_valid) {
  ASSERT_OK(result->ValidateFull());
  ASSERT_EQ(1, result->null_count());
  ASSERT_LE(result->values()->null_count(), 2);

  ASSERT_EQ(3, result->length());
  for (int32_t i = 0; i < 3; ++i) {
    ASSERT_EQ(i * result->value_length(), result->value_offset(i));
  }

  for (int i = 0; i < result->length(); ++i) {
    ASSERT_EQ(is_valid[i] == 0, result->IsNull(i));
  }

  ASSERT_LE(result->length() * result->value_length(), result->values()->length());
  auto varr = std::dynamic_pointer_cast<Int32Array>(result->values());

  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid[i / result->value_length()] == 0) {
      continue;
    }
    ASSERT_EQ(values[i], varr->Value(i));
  }
}

TEST_F(TestFixedSizeListArray, TestBasics) {
  std::vector<int32_t> values = {0, 1, 2, 3, 4, 5};
  std::vector<uint8_t> is_valid = {1, 0, 1};

  Int32Builder* vb = checked_cast<Int32Builder*>(builder_->value_builder());

  int pos = 0;
  for (size_t i = 0; i < values.size() / list_size(); ++i) {
    if (is_valid[i] == 0) {
      ASSERT_OK(builder_->AppendNull());
      pos += list_size();
      continue;
    }
    ASSERT_OK(builder_->Append());
    for (int j = 0; j < list_size(); ++j) {
      ASSERT_OK(vb->Append(values[pos++]));
    }
  }

  Done();
  ValidateBasicFixedSizeListArray(result_.get(), values, is_valid);
}

TEST_F(TestFixedSizeListArray, BulkAppend) {
  std::vector<int32_t> values = {0, 1, 2, 3, 4, 5};
  std::vector<uint8_t> is_valid = {1, 0, 1};

  Int32Builder* vb = checked_cast<Int32Builder*>(builder_->value_builder());

  ASSERT_OK(builder_->AppendValues(values.size() / list_size(), is_valid.data()));
  for (int32_t value : values) {
    ASSERT_OK(vb->Append(value));
  }
  Done();
  ValidateBasicFixedSizeListArray(result_.get(), values, is_valid);
}

TEST_F(TestFixedSizeListArray, BulkAppendExcess) {
  std::vector<int32_t> values = {0, 1, 2, 3, 4, 5};
  std::vector<uint8_t> is_valid = {1, 0, 1};

  Int32Builder* vb = checked_cast<Int32Builder*>(builder_->value_builder());

  ASSERT_OK(builder_->AppendValues(values.size() / list_size(), is_valid.data()));
  for (int32_t value : values) {
    ASSERT_OK(vb->Append(value));
  }
  for (int32_t value : values) {
    ASSERT_OK(vb->Append(value));
  }

  Done();
  // We appended too many values to the child array, but that's OK
  ValidateBasicFixedSizeListArray(result_.get(), values, is_valid);
}

TEST_F(TestFixedSizeListArray, TestZeroLength) {
  // All buffers are null
  Done();
  ASSERT_OK(result_->ValidateFull());
}

TEST_F(TestFixedSizeListArray, TestBuilderPreserveFieldName) {
  auto list_type_with_name = fixed_size_list(field("counts", int32()), list_size());

  std::unique_ptr<ArrayBuilder> tmp;
  ASSERT_OK(MakeBuilder(pool_, list_type_with_name, &tmp));
  builder_.reset(checked_cast<FixedSizeListBuilder*>(tmp.release()));

  ASSERT_OK(builder_->AppendValues(4));

  std::shared_ptr<Array> list_array;
  ASSERT_OK(builder_->Finish(&list_array));

  const auto& type = checked_cast<FixedSizeListType&>(*list_array->type());
  ASSERT_EQ("counts", type.value_field()->name());
}

TEST_F(TestFixedSizeListArray, NegativeLength) {
  type_ = fixed_size_list(value_type_, -42);
  auto values = ArrayFromJSON(value_type_, "[]");
  result_ = std::make_shared<FixedSizeListArray>(type_, 0, values);
  ASSERT_RAISES(Invalid, result_->ValidateFull());
}

TEST_F(TestFixedSizeListArray, NotEnoughValues) {
  type_ = fixed_size_list(value_type_, 2);
  auto values = ArrayFromJSON(value_type_, "[]");
  result_ = std::make_shared<FixedSizeListArray>(type_, 1, values);
  ASSERT_RAISES(Invalid, result_->ValidateFull());

  // ARROW-13437: too many values is OK though
  values = ArrayFromJSON(value_type_, "[1, 2, 3, 4]");
  result_ = std::make_shared<FixedSizeListArray>(type_, 1, values);
  ASSERT_OK(result_->ValidateFull());
}

TEST_F(TestFixedSizeListArray, FlattenZeroLength) {
  Done();
  ASSERT_OK_AND_ASSIGN(auto flattened, result_->Flatten());
  ASSERT_OK(flattened->ValidateFull());
  ASSERT_EQ(0, flattened->length());
  AssertTypeEqual(*flattened->type(), *value_type_);
}

TEST_F(TestFixedSizeListArray, FlattenNulls) {
  ASSERT_OK(builder_->AppendNulls(2));
  Done();
  ASSERT_EQ(result_->data()->GetNullCount(), 2);
  ASSERT_OK_AND_ASSIGN(auto flattened, result_->Flatten());
  ASSERT_OK(flattened->ValidateFull());
  ASSERT_EQ(0, flattened->length());
  AssertTypeEqual(*flattened->type(), *value_type_);
}

TEST_F(TestFixedSizeListArray, Flatten) {
  std::vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6, 7};
  std::vector<uint8_t> is_valid = {1, 0, 1, 1};
  ASSERT_OK(builder_->AppendValues(4, is_valid.data()));
  auto* vb = checked_cast<Int32Builder*>(builder_->value_builder());
  ASSERT_OK(vb->AppendValues(values.data(), static_cast<int64_t>(values.size())));
  Done();

  {
    ASSERT_OK_AND_ASSIGN(auto flattened, result_->Flatten());
    ASSERT_OK(flattened->ValidateFull());
    ASSERT_EQ(6, flattened->length());
    AssertArraysEqual(*flattened, *ArrayFromJSON(value_type_, "[0, 1, 4, 5, 6, 7]"),
                      /*verbose=*/true);
  }

  {
    auto sliced = std::dynamic_pointer_cast<FixedSizeListArray>(result_->Slice(1, 2));
    ASSERT_OK_AND_ASSIGN(auto flattened, sliced->Flatten());
    ASSERT_OK(flattened->ValidateFull());
    ASSERT_EQ(2, flattened->length());
    AssertArraysEqual(*flattened, *ArrayFromJSON(value_type_, "[4, 5]"),
                      /*verbose=*/true);
  }
}

}  // namespace arrow
