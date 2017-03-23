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

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/status.h"
#include "arrow/test-common.h"
#include "arrow/test-util.h"
#include "arrow/type.h"

using std::shared_ptr;
using std::string;
using std::vector;

namespace arrow {

TEST(TestStructType, Basics) {
  TypePtr f0_type = TypePtr(new Int32Type());
  auto f0 = std::make_shared<Field>("f0", f0_type);

  TypePtr f1_type = TypePtr(new StringType());
  auto f1 = std::make_shared<Field>("f1", f1_type);

  TypePtr f2_type = TypePtr(new UInt8Type());
  auto f2 = std::make_shared<Field>("f2", f2_type);

  vector<shared_ptr<Field>> fields = {f0, f1, f2};

  StructType struct_type(fields);

  ASSERT_TRUE(struct_type.child(0)->Equals(f0));
  ASSERT_TRUE(struct_type.child(1)->Equals(f1));
  ASSERT_TRUE(struct_type.child(2)->Equals(f2));

  ASSERT_EQ(struct_type.ToString(), "struct<f0: int32, f1: string, f2: uint8>");

  // TODO(wesm): out of bounds for field(...)
}

void ValidateBasicStructArray(const StructArray* result,
    const vector<uint8_t>& struct_is_valid, const vector<char>& list_values,
    const vector<uint8_t>& list_is_valid, const vector<int>& list_lengths,
    const vector<int>& list_offsets, const vector<int32_t>& int_values) {
  ASSERT_EQ(4, result->length());
  ASSERT_OK(result->Validate());

  auto list_char_arr = static_cast<ListArray*>(result->field(0).get());
  auto char_arr = static_cast<Int8Array*>(list_char_arr->values().get());
  auto int32_arr = static_cast<Int32Array*>(result->field(1).get());

  ASSERT_EQ(0, result->null_count());
  ASSERT_EQ(1, list_char_arr->null_count());
  ASSERT_EQ(0, int32_arr->null_count());

  // List<char>
  ASSERT_EQ(4, list_char_arr->length());
  ASSERT_EQ(10, list_char_arr->values()->length());
  for (size_t i = 0; i < list_offsets.size(); ++i) {
    ASSERT_EQ(list_offsets[i], list_char_arr->raw_value_offsets()[i]);
  }
  for (size_t i = 0; i < list_values.size(); ++i) {
    ASSERT_EQ(list_values[i], char_arr->Value(i));
  }

  // Int32
  ASSERT_EQ(4, int32_arr->length());
  for (size_t i = 0; i < int_values.size(); ++i) {
    ASSERT_EQ(int_values[i], int32_arr->Value(i));
  }
}

// ----------------------------------------------------------------------------------
// Struct test
class TestStructBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();

    auto int32_type = TypePtr(new Int32Type());
    auto char_type = TypePtr(new Int8Type());
    auto list_type = TypePtr(new ListType(char_type));

    std::vector<TypePtr> types = {list_type, int32_type};
    std::vector<FieldPtr> fields;
    fields.push_back(FieldPtr(new Field("list", list_type)));
    fields.push_back(FieldPtr(new Field("int", int32_type)));

    type_ = TypePtr(new StructType(fields));
    value_fields_ = fields;

    std::shared_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));

    builder_ = std::dynamic_pointer_cast<StructBuilder>(tmp);
    ASSERT_EQ(2, static_cast<int>(builder_->field_builders().size()));
  }

  void Done() {
    std::shared_ptr<Array> out;
    ASSERT_OK(builder_->Finish(&out));
    result_ = std::dynamic_pointer_cast<StructArray>(out);
  }

 protected:
  std::vector<FieldPtr> value_fields_;
  TypePtr type_;

  std::shared_ptr<StructBuilder> builder_;
  std::shared_ptr<StructArray> result_;
};

TEST_F(TestStructBuilder, TestAppendNull) {
  ASSERT_OK(builder_->AppendNull());
  ASSERT_OK(builder_->AppendNull());
  ASSERT_EQ(2, static_cast<int>(builder_->field_builders().size()));

  ListBuilder* list_vb = static_cast<ListBuilder*>(builder_->field_builder(0).get());
  ASSERT_OK(list_vb->AppendNull());
  ASSERT_OK(list_vb->AppendNull());
  ASSERT_EQ(2, list_vb->length());

  Int32Builder* int_vb = static_cast<Int32Builder*>(builder_->field_builder(1).get());
  ASSERT_OK(int_vb->AppendNull());
  ASSERT_OK(int_vb->AppendNull());
  ASSERT_EQ(2, int_vb->length());

  Done();

  ASSERT_OK(result_->Validate());

  ASSERT_EQ(2, static_cast<int>(result_->fields().size()));
  ASSERT_EQ(2, result_->length());
  ASSERT_EQ(2, result_->field(0)->length());
  ASSERT_EQ(2, result_->field(1)->length());
  ASSERT_TRUE(result_->IsNull(0));
  ASSERT_TRUE(result_->IsNull(1));
  ASSERT_TRUE(result_->field(0)->IsNull(0));
  ASSERT_TRUE(result_->field(0)->IsNull(1));
  ASSERT_TRUE(result_->field(1)->IsNull(0));
  ASSERT_TRUE(result_->field(1)->IsNull(1));

  ASSERT_EQ(Type::LIST, result_->field(0)->type_enum());
  ASSERT_EQ(Type::INT32, result_->field(1)->type_enum());
}

TEST_F(TestStructBuilder, TestBasics) {
  vector<int32_t> int_values = {1, 2, 3, 4};
  vector<char> list_values = {'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'};
  vector<int> list_lengths = {3, 0, 3, 4};
  vector<int> list_offsets = {0, 3, 3, 6, 10};
  vector<uint8_t> list_is_valid = {1, 0, 1, 1};
  vector<uint8_t> struct_is_valid = {1, 1, 1, 1};

  ListBuilder* list_vb = static_cast<ListBuilder*>(builder_->field_builder(0).get());
  Int8Builder* char_vb = static_cast<Int8Builder*>(list_vb->value_builder().get());
  Int32Builder* int_vb = static_cast<Int32Builder*>(builder_->field_builder(1).get());
  ASSERT_EQ(2, static_cast<int>(builder_->field_builders().size()));

  EXPECT_OK(builder_->Resize(list_lengths.size()));
  EXPECT_OK(char_vb->Resize(list_values.size()));
  EXPECT_OK(int_vb->Resize(int_values.size()));

  int pos = 0;
  for (size_t i = 0; i < list_lengths.size(); ++i) {
    ASSERT_OK(list_vb->Append(list_is_valid[i] > 0));
    int_vb->UnsafeAppend(int_values[i]);
    for (int j = 0; j < list_lengths[i]; ++j) {
      char_vb->UnsafeAppend(list_values[pos++]);
    }
  }

  for (size_t i = 0; i < struct_is_valid.size(); ++i) {
    ASSERT_OK(builder_->Append(struct_is_valid[i] > 0));
  }

  Done();

  ValidateBasicStructArray(result_.get(), struct_is_valid, list_values, list_is_valid,
      list_lengths, list_offsets, int_values);
}

TEST_F(TestStructBuilder, BulkAppend) {
  vector<int32_t> int_values = {1, 2, 3, 4};
  vector<char> list_values = {'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'};
  vector<int> list_lengths = {3, 0, 3, 4};
  vector<int> list_offsets = {0, 3, 3, 6};
  vector<uint8_t> list_is_valid = {1, 0, 1, 1};
  vector<uint8_t> struct_is_valid = {1, 1, 1, 1};

  ListBuilder* list_vb = static_cast<ListBuilder*>(builder_->field_builder(0).get());
  Int8Builder* char_vb = static_cast<Int8Builder*>(list_vb->value_builder().get());
  Int32Builder* int_vb = static_cast<Int32Builder*>(builder_->field_builder(1).get());

  ASSERT_OK(builder_->Resize(list_lengths.size()));
  ASSERT_OK(char_vb->Resize(list_values.size()));
  ASSERT_OK(int_vb->Resize(int_values.size()));

  builder_->Append(struct_is_valid.size(), struct_is_valid.data());

  list_vb->Append(list_offsets.data(), list_offsets.size(), list_is_valid.data());
  for (int8_t value : list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : int_values) {
    int_vb->UnsafeAppend(value);
  }

  Done();
  ValidateBasicStructArray(result_.get(), struct_is_valid, list_values, list_is_valid,
      list_lengths, list_offsets, int_values);
}

TEST_F(TestStructBuilder, BulkAppendInvalid) {
  vector<int32_t> int_values = {1, 2, 3, 4};
  vector<char> list_values = {'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'};
  vector<int> list_lengths = {3, 0, 3, 4};
  vector<int> list_offsets = {0, 3, 3, 6};
  vector<uint8_t> list_is_valid = {1, 0, 1, 1};
  vector<uint8_t> struct_is_valid = {1, 0, 1, 1};  // should be 1, 1, 1, 1

  ListBuilder* list_vb = static_cast<ListBuilder*>(builder_->field_builder(0).get());
  Int8Builder* char_vb = static_cast<Int8Builder*>(list_vb->value_builder().get());
  Int32Builder* int_vb = static_cast<Int32Builder*>(builder_->field_builder(1).get());

  ASSERT_OK(builder_->Reserve(list_lengths.size()));
  ASSERT_OK(char_vb->Reserve(list_values.size()));
  ASSERT_OK(int_vb->Reserve(int_values.size()));

  builder_->Append(struct_is_valid.size(), struct_is_valid.data());

  list_vb->Append(list_offsets.data(), list_offsets.size(), list_is_valid.data());
  for (int8_t value : list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : int_values) {
    int_vb->UnsafeAppend(value);
  }

  Done();
  // Even null bitmap of the parent Struct is not valid, Validate() will ignore it.
  ASSERT_OK(result_->Validate());
}

TEST_F(TestStructBuilder, TestEquality) {
  std::shared_ptr<Array> array, equal_array;
  std::shared_ptr<Array> unequal_bitmap_array, unequal_offsets_array,
      unequal_values_array;

  vector<int32_t> int_values = {1, 2, 3, 4};
  vector<char> list_values = {'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'};
  vector<int> list_lengths = {3, 0, 3, 4};
  vector<int> list_offsets = {0, 3, 3, 6};
  vector<uint8_t> list_is_valid = {1, 0, 1, 1};
  vector<uint8_t> struct_is_valid = {1, 1, 1, 1};

  vector<int32_t> unequal_int_values = {4, 2, 3, 1};
  vector<char> unequal_list_values = {'j', 'o', 'e', 'b', 'o', 'b', 'l', 'u', 'c', 'y'};
  vector<int> unequal_list_offsets = {0, 3, 4, 6};
  vector<uint8_t> unequal_list_is_valid = {1, 1, 1, 1};
  vector<uint8_t> unequal_struct_is_valid = {1, 0, 0, 1};

  ListBuilder* list_vb = static_cast<ListBuilder*>(builder_->field_builder(0).get());
  Int8Builder* char_vb = static_cast<Int8Builder*>(list_vb->value_builder().get());
  Int32Builder* int_vb = static_cast<Int32Builder*>(builder_->field_builder(1).get());
  ASSERT_OK(builder_->Reserve(list_lengths.size()));
  ASSERT_OK(char_vb->Reserve(list_values.size()));
  ASSERT_OK(int_vb->Reserve(int_values.size()));

  // setup two equal arrays, one of which takes an unequal bitmap
  builder_->Append(struct_is_valid.size(), struct_is_valid.data());
  list_vb->Append(list_offsets.data(), list_offsets.size(), list_is_valid.data());
  for (int8_t value : list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : int_values) {
    int_vb->UnsafeAppend(value);
  }

  ASSERT_OK(builder_->Finish(&array));

  ASSERT_OK(builder_->Resize(list_lengths.size()));
  ASSERT_OK(char_vb->Resize(list_values.size()));
  ASSERT_OK(int_vb->Resize(int_values.size()));

  builder_->Append(struct_is_valid.size(), struct_is_valid.data());
  list_vb->Append(list_offsets.data(), list_offsets.size(), list_is_valid.data());
  for (int8_t value : list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : int_values) {
    int_vb->UnsafeAppend(value);
  }

  ASSERT_OK(builder_->Finish(&equal_array));

  ASSERT_OK(builder_->Resize(list_lengths.size()));
  ASSERT_OK(char_vb->Resize(list_values.size()));
  ASSERT_OK(int_vb->Resize(int_values.size()));

  // setup an unequal one with the unequal bitmap
  builder_->Append(unequal_struct_is_valid.size(), unequal_struct_is_valid.data());
  list_vb->Append(list_offsets.data(), list_offsets.size(), list_is_valid.data());
  for (int8_t value : list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : int_values) {
    int_vb->UnsafeAppend(value);
  }

  ASSERT_OK(builder_->Finish(&unequal_bitmap_array));

  ASSERT_OK(builder_->Resize(list_lengths.size()));
  ASSERT_OK(char_vb->Resize(list_values.size()));
  ASSERT_OK(int_vb->Resize(int_values.size()));

  // setup an unequal one with unequal offsets
  builder_->Append(struct_is_valid.size(), struct_is_valid.data());
  list_vb->Append(unequal_list_offsets.data(), unequal_list_offsets.size(),
      unequal_list_is_valid.data());
  for (int8_t value : list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : int_values) {
    int_vb->UnsafeAppend(value);
  }

  ASSERT_OK(builder_->Finish(&unequal_offsets_array));

  ASSERT_OK(builder_->Resize(list_lengths.size()));
  ASSERT_OK(char_vb->Resize(list_values.size()));
  ASSERT_OK(int_vb->Resize(int_values.size()));

  // setup anunequal one with unequal values
  builder_->Append(struct_is_valid.size(), struct_is_valid.data());
  list_vb->Append(list_offsets.data(), list_offsets.size(), list_is_valid.data());
  for (int8_t value : unequal_list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : unequal_int_values) {
    int_vb->UnsafeAppend(value);
  }

  ASSERT_OK(builder_->Finish(&unequal_values_array));

  // Test array equality
  EXPECT_TRUE(array->Equals(array));
  EXPECT_TRUE(array->Equals(equal_array));
  EXPECT_TRUE(equal_array->Equals(array));
  EXPECT_FALSE(equal_array->Equals(unequal_bitmap_array));
  EXPECT_FALSE(unequal_bitmap_array->Equals(equal_array));
  EXPECT_FALSE(unequal_bitmap_array->Equals(unequal_values_array));
  EXPECT_FALSE(unequal_values_array->Equals(unequal_bitmap_array));
  EXPECT_FALSE(unequal_bitmap_array->Equals(unequal_offsets_array));
  EXPECT_FALSE(unequal_offsets_array->Equals(unequal_bitmap_array));

  // Test range equality
  EXPECT_TRUE(array->RangeEquals(0, 4, 0, equal_array));
  EXPECT_TRUE(array->RangeEquals(3, 4, 3, unequal_bitmap_array));
  EXPECT_TRUE(array->RangeEquals(0, 1, 0, unequal_offsets_array));
  EXPECT_FALSE(array->RangeEquals(0, 2, 0, unequal_offsets_array));
  EXPECT_FALSE(array->RangeEquals(1, 2, 1, unequal_offsets_array));
  EXPECT_FALSE(array->RangeEquals(0, 1, 0, unequal_values_array));
  EXPECT_TRUE(array->RangeEquals(1, 3, 1, unequal_values_array));
  EXPECT_FALSE(array->RangeEquals(3, 4, 3, unequal_values_array));

  // ARROW-33 Slice / equality
  std::shared_ptr<Array> slice, slice2;

  slice = array->Slice(2);
  slice2 = array->Slice(2);
  ASSERT_EQ(array->length() - 2, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(2, slice->length(), 0, slice));

  slice = array->Slice(1, 2);
  slice2 = array->Slice(1, 2);
  ASSERT_EQ(2, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(1, 3, 0, slice));
}

TEST_F(TestStructBuilder, TestZeroLength) {
  // All buffers are null
  Done();
  ASSERT_OK(result_->Validate());
}

}  // namespace arrow
