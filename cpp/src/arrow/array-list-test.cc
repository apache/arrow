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
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/status.h"
#include "arrow/test-util.h"
#include "arrow/type.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace arrow {

TEST(TypesTest, TestListType) {
  std::shared_ptr<DataType> vt = std::make_shared<UInt8Type>();

  ListType list_type(vt);
  ASSERT_EQ(list_type.type, Type::LIST);

  ASSERT_EQ(list_type.name(), string("list"));
  ASSERT_EQ(list_type.ToString(), string("list<item: uint8>"));

  ASSERT_EQ(list_type.value_type()->type, vt->type);
  ASSERT_EQ(list_type.value_type()->type, vt->type);

  std::shared_ptr<DataType> st = std::make_shared<StringType>();
  std::shared_ptr<DataType> lt = std::make_shared<ListType>(st);
  ASSERT_EQ(lt->ToString(), string("list<item: string>"));

  ListType lt2(lt);
  ASSERT_EQ(lt2.ToString(), string("list<item: list<item: string>>"));
}

// ----------------------------------------------------------------------
// List tests

class TestListBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();

    value_type_ = TypePtr(new Int32Type());
    type_ = TypePtr(new ListType(value_type_));

    std::shared_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_ = std::dynamic_pointer_cast<ListBuilder>(tmp);
  }

  void Done() {
    std::shared_ptr<Array> out;
    EXPECT_OK(builder_->Finish(&out));
    result_ = std::dynamic_pointer_cast<ListArray>(out);
  }

 protected:
  TypePtr value_type_;
  TypePtr type_;

  shared_ptr<ListBuilder> builder_;
  shared_ptr<ListArray> result_;
};

TEST_F(TestListBuilder, Equality) {
  Int32Builder* vb = static_cast<Int32Builder*>(builder_->value_builder().get());

  std::shared_ptr<Array> array, equal_array, unequal_array;
  vector<int32_t> equal_offsets = {0, 1, 2, 5};
  vector<int32_t> equal_values = {1, 2, 3, 4, 5, 2, 2, 2};
  vector<int32_t> unequal_offsets = {0, 1, 4};
  vector<int32_t> unequal_values = {1, 2, 2, 2, 3, 4, 5};

  // setup two equal arrays
  ASSERT_OK(builder_->Append(equal_offsets.data(), equal_offsets.size()));
  ASSERT_OK(vb->Append(equal_values.data(), equal_values.size()));

  ASSERT_OK(builder_->Finish(&array));
  ASSERT_OK(builder_->Append(equal_offsets.data(), equal_offsets.size()));
  ASSERT_OK(vb->Append(equal_values.data(), equal_values.size()));

  ASSERT_OK(builder_->Finish(&equal_array));
  // now an unequal one
  ASSERT_OK(builder_->Append(unequal_offsets.data(), unequal_offsets.size()));
  ASSERT_OK(vb->Append(unequal_values.data(), unequal_values.size()));

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
  EXPECT_TRUE(array->RangeEquals(3, 4, 1, unequal_array));
}

TEST_F(TestListBuilder, TestResize) {}

TEST_F(TestListBuilder, TestAppendNull) {
  ASSERT_OK(builder_->AppendNull());
  ASSERT_OK(builder_->AppendNull());

  Done();

  ASSERT_OK(result_->Validate());
  ASSERT_TRUE(result_->IsNull(0));
  ASSERT_TRUE(result_->IsNull(1));

  ASSERT_EQ(0, result_->raw_offsets()[0]);
  ASSERT_EQ(0, result_->offset(1));
  ASSERT_EQ(0, result_->offset(2));

  Int32Array* values = static_cast<Int32Array*>(result_->values().get());
  ASSERT_EQ(0, values->length());
}

void ValidateBasicListArray(const ListArray* result, const vector<int32_t>& values,
    const vector<uint8_t>& is_valid) {
  ASSERT_OK(result->Validate());
  ASSERT_EQ(1, result->null_count());
  ASSERT_EQ(0, result->values()->null_count());

  ASSERT_EQ(3, result->length());
  vector<int32_t> ex_offsets = {0, 3, 3, 7};
  for (size_t i = 0; i < ex_offsets.size(); ++i) {
    ASSERT_EQ(ex_offsets[i], result->offset(i));
  }

  for (int i = 0; i < result->length(); ++i) {
    ASSERT_EQ(!static_cast<bool>(is_valid[i]), result->IsNull(i));
  }

  ASSERT_EQ(7, result->values()->length());
  Int32Array* varr = static_cast<Int32Array*>(result->values().get());

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(values[i], varr->Value(i));
  }
}

TEST_F(TestListBuilder, TestBasics) {
  vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6};
  vector<int> lengths = {3, 0, 4};
  vector<uint8_t> is_valid = {1, 0, 1};

  Int32Builder* vb = static_cast<Int32Builder*>(builder_->value_builder().get());

  ASSERT_OK(builder_->Reserve(lengths.size()));
  ASSERT_OK(vb->Reserve(values.size()));

  int pos = 0;
  for (size_t i = 0; i < lengths.size(); ++i) {
    ASSERT_OK(builder_->Append(is_valid[i] > 0));
    for (int j = 0; j < lengths[i]; ++j) {
      vb->Append(values[pos++]);
    }
  }

  Done();
  ValidateBasicListArray(result_.get(), values, is_valid);
}

TEST_F(TestListBuilder, BulkAppend) {
  vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6};
  vector<int> lengths = {3, 0, 4};
  vector<uint8_t> is_valid = {1, 0, 1};
  vector<int32_t> offsets = {0, 3, 3};

  Int32Builder* vb = static_cast<Int32Builder*>(builder_->value_builder().get());
  ASSERT_OK(vb->Reserve(values.size()));

  builder_->Append(offsets.data(), offsets.size(), is_valid.data());
  for (int32_t value : values) {
    vb->Append(value);
  }
  Done();
  ValidateBasicListArray(result_.get(), values, is_valid);
}

TEST_F(TestListBuilder, BulkAppendInvalid) {
  vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6};
  vector<int> lengths = {3, 0, 4};
  vector<uint8_t> is_null = {0, 1, 0};
  vector<uint8_t> is_valid = {1, 0, 1};
  vector<int32_t> offsets = {0, 2, 4};  // should be 0, 3, 3 given the is_null array

  Int32Builder* vb = static_cast<Int32Builder*>(builder_->value_builder().get());
  ASSERT_OK(vb->Reserve(values.size()));

  builder_->Append(offsets.data(), offsets.size(), is_valid.data());
  builder_->Append(offsets.data(), offsets.size(), is_valid.data());
  for (int32_t value : values) {
    vb->Append(value);
  }

  Done();
  ASSERT_RAISES(Invalid, result_->Validate());
}

TEST_F(TestListBuilder, TestZeroLength) {
  // All buffers are null
  Done();
  ASSERT_OK(result_->Validate());
}

}  // namespace arrow
