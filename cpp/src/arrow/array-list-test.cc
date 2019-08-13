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

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

using ListTypes = ::testing::Types<ListType, LargeListType>;

// ----------------------------------------------------------------------
// List tests

template <typename T>
class TestListArray : public TestBuilder {
 public:
  using TypeClass = T;
  using offset_type = typename TypeClass::offset_type;
  using ArrayType = typename TypeTraits<TypeClass>::ArrayType;
  using BuilderType = typename TypeTraits<TypeClass>::BuilderType;
  using OffsetType = typename TypeTraits<TypeClass>::OffsetType;
  using OffsetArrayType = typename TypeTraits<TypeClass>::OffsetArrayType;
  using OffsetBuilderType = typename TypeTraits<TypeClass>::OffsetBuilderType;

  void SetUp() {
    TestBuilder::SetUp();

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
    ASSERT_OK(result->Validate());
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

  void TestFromArrays() {
    std::shared_ptr<Array> offsets1, offsets2, offsets3, offsets4, values;

    std::vector<bool> offsets_is_valid3 = {true, false, true, true};
    std::vector<bool> offsets_is_valid4 = {true, true, false, true};

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

    ArrayFromVector<Int8Type, int8_t>(values_is_valid, values_values, &values);

    auto list_type = std::make_shared<T>(int8());

    std::shared_ptr<Array> list1, list3, list4;
    ASSERT_OK(ArrayType::FromArrays(*offsets1, *values, pool_, &list1));
    ASSERT_OK(ArrayType::FromArrays(*offsets3, *values, pool_, &list3));
    ASSERT_OK(ArrayType::FromArrays(*offsets4, *values, pool_, &list4));
    ASSERT_OK(list1->Validate());
    ASSERT_OK(list3->Validate());
    ASSERT_OK(list4->Validate());

    ArrayType expected1(list_type, length, offsets1->data()->buffers[1], values,
                        offsets1->data()->buffers[0], 0);
    AssertArraysEqual(expected1, *list1);

    // Use null bitmap from offsets3, but clean offsets from non-null version
    ArrayType expected3(list_type, length, offsets1->data()->buffers[1], values,
                        offsets3->data()->buffers[0], 1);
    AssertArraysEqual(expected3, *list3);

    // Check that the last offset bit is zero
    ASSERT_FALSE(BitUtil::GetBit(list3->null_bitmap()->data(), length + 1));

    ArrayType expected4(list_type, length, offsets2->data()->buffers[1], values,
                        offsets4->data()->buffers[0], 1);
    AssertArraysEqual(expected4, *list4);

    // Test failure modes

    std::shared_ptr<Array> tmp;

    // Zero-length offsets
    ASSERT_RAISES(Invalid,
                  ArrayType::FromArrays(*offsets1->Slice(0, 0), *values, pool_, &tmp));

    // Offsets not the right type
    ASSERT_RAISES(TypeError, ArrayType::FromArrays(*values, *offsets1, pool_, &tmp));
  }

  void TestAppendNull() {
    ASSERT_OK(builder_->AppendNull());
    ASSERT_OK(builder_->AppendNull());

    Done();

    ASSERT_OK(result_->Validate());
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

    ASSERT_OK(result_->Validate());
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
    ASSERT_RAISES(Invalid, result_->Validate());
  }

  void TestZeroLength() {
    // All buffers are null
    Done();
    ASSERT_OK(result_->Validate());
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

 protected:
  std::shared_ptr<DataType> value_type_;

  std::shared_ptr<BuilderType> builder_;
  std::shared_ptr<ArrayType> result_;
};

TYPED_TEST_CASE(TestListArray, ListTypes);

TYPED_TEST(TestListArray, Basics) { this->TestBasics(); }

TYPED_TEST(TestListArray, Equality) { this->TestEquality(); }

TYPED_TEST(TestListArray, ValuesEquality) { this->TestValuesEquality(); }

TYPED_TEST(TestListArray, FromArrays) { this->TestFromArrays(); }

TYPED_TEST(TestListArray, AppendNull) { this->TestAppendNull(); }

TYPED_TEST(TestListArray, AppendNulls) { this->TestAppendNulls(); }

TYPED_TEST(TestListArray, BulkAppend) { this->TestBulkAppend(); }

TYPED_TEST(TestListArray, BulkAppendInvalid) { this->TestBulkAppendInvalid(); }

TYPED_TEST(TestListArray, ZeroLength) { this->TestZeroLength(); }

TYPED_TEST(TestListArray, BuilderPreserveFieldName) {
  this->TestBuilderPreserveFieldName();
}

// ----------------------------------------------------------------------
// Map tests

class TestMapArray : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();

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
  std::shared_ptr<DataType> value_type_, key_type_;

  std::shared_ptr<MapBuilder> builder_;
  std::shared_ptr<MapArray> result_;
};

TEST_F(TestMapArray, Equality) {
  auto& kb = checked_cast<StringBuilder&>(*builder_->key_builder());
  auto& ib = checked_cast<Int32Builder&>(*builder_->item_builder());

  std::shared_ptr<Array> array, equal_array, unequal_array;
  std::vector<int32_t> equal_offsets = {0, 1, 2, 5, 6, 7, 8, 10};
  std::vector<util::string_view> equal_keys = {"a", "a", "a", "b", "c",
                                               "a", "a", "a", "a", "b"};
  std::vector<int32_t> equal_values = {1, 2, 3, 4, 5, 2, 2, 2, 5, 6};
  std::vector<int32_t> unequal_offsets = {0, 1, 4, 7};
  std::vector<util::string_view> unequal_keys = {"a", "a", "b", "c", "a", "b", "c"};
  std::vector<int32_t> unequal_values = {1, 2, 2, 2, 3, 4, 5};

  // setup two equal arrays
  for (auto out : {&array, &equal_array}) {
    ASSERT_OK(builder_->AppendValues(equal_offsets.data(), equal_offsets.size()));
    for (auto&& key : equal_keys) {
      ASSERT_OK(kb.Append(key));
    }
    ASSERT_OK(ib.AppendValues(equal_values.data(), equal_values.size()));
    ASSERT_OK(builder_->Finish(out));
  }

  // now an unequal one
  ASSERT_OK(builder_->AppendValues(unequal_offsets.data(), unequal_offsets.size()));
  for (auto&& key : unequal_keys) {
    ASSERT_OK(kb.Append(key));
  }
  ASSERT_OK(ib.AppendValues(unequal_values.data(), unequal_values.size()));
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
  ASSERT_OK(actual->Validate());

  ASSERT_ARRAYS_EQUAL(*actual, expected);
}

TEST_F(TestMapArray, BuildingStringToInt) {
  auto type = map(utf8(), int32());

  std::vector<int32_t> offsets = {0, 2, 2, 3, 3};
  auto expected_keys = ArrayFromJSON(utf8(), R"(["joe", "mark", "cap"])");
  auto expected_values = ArrayFromJSON(int32(), "[0, null, 8]");
  std::shared_ptr<Buffer> expected_null_bitmap;
  ASSERT_OK(
      BitUtil::BytesToBits({1, 0, 1, 1}, default_memory_pool(), &expected_null_bitmap));
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
  ASSERT_OK(actual->Validate());

  ASSERT_ARRAYS_EQUAL(*actual, expected);
}

// ----------------------------------------------------------------------
// FixedSizeList tests

class TestFixedSizeListArray : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();

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
  std::shared_ptr<DataType> value_type_;

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

  ASSERT_OK(result_->Validate());
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

  ASSERT_OK(result_->Validate());
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
  ASSERT_OK(result->Validate());
  ASSERT_EQ(1, result->null_count());
  ASSERT_LE(result->values()->null_count(), 2);

  ASSERT_EQ(3, result->length());
  for (int32_t i = 0; i < 3; ++i) {
    ASSERT_EQ(i * result->value_length(), result->value_offset(i));
  }

  for (int i = 0; i < result->length(); ++i) {
    ASSERT_EQ(is_valid[i] == 0, result->IsNull(i));
  }

  ASSERT_EQ(result->length() * result->value_length(), result->values()->length());
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

TEST_F(TestFixedSizeListArray, BulkAppendInvalid) {
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
  ASSERT_RAISES(Invalid, result_->Validate());
}

TEST_F(TestFixedSizeListArray, TestZeroLength) {
  // All buffers are null
  Done();
  ASSERT_OK(result_->Validate());
}

TEST_F(TestFixedSizeListArray, TestBuilderPreserveFieleName) {
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

}  // namespace arrow
