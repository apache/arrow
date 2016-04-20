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

#include "arrow/type.h"
#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/test-util.h"
#include "arrow/types/struct.h"
#include "arrow/types/construct.h"
#include "arrow/types/list.h"
#include "arrow/types/primitive.h"
#include "arrow/types/test-common.h"
#include "arrow/util/status.h"


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

// .............................................................................
// Struct test
class TestStructBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();

    auto value_type = TypePtr(new Int32Type());
    auto char_type = TypePtr(new Int8Type());
    auto list_type = TypePtr(new ListType(char_type));

    std::vector<TypePtr> types = {list_type, value_type};
    std::vector<FieldPtr> fields;
    fields.push_back(FieldPtr(new Field("list", list_type)));
    fields.push_back(FieldPtr(new Field("int", value_type)));

    type_ = TypePtr(new StructType(fields));
    value_fields_ = fields;

    std::shared_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeStructBuilder(pool_, type_, fields, &tmp));

    builder_ = std::dynamic_pointer_cast<StructBuilder>(tmp);
  }

  void Done() {
    result_ = std::dynamic_pointer_cast<StructArray>(builder_->Finish());
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
  ASSERT_EQ(2, builder_->value_builder().size());

  Done();

  ASSERT_EQ(2, result_->values().size());
  ASSERT_EQ(2, result_->length());
  ASSERT_TRUE(result_->IsNull(0));
  ASSERT_TRUE(result_->IsNull(1));


  auto list_char = static_cast<ListArray*>(result_->values(0).get());
  auto chars = static_cast<Int8Array*>(list_char->values().get());
  auto int32 = static_cast<Int32Array*>(result_->values(1).get());
  ASSERT_EQ(0, list_char->length());
  ASSERT_EQ(0, chars->length());
  ASSERT_EQ(0, int32->length());

  ASSERT_EQ(Type::LIST, list_char->type_enum());
  ASSERT_EQ(Type::INT8, list_char->values()->type_enum());
  ASSERT_EQ(Type::INT32, int32->type_enum());
}

TEST_F(TestStructBuilder, TestBasics) {
  vector<int32_t> int_values = {1, 2, 3, 4};
  vector<char> list_values = {'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'};
  vector<int> list_lengths = {3, 0, 3, 4};
  vector<int> list_offsets = {0, 3, 3, 6, 10};
  vector<uint8_t> list_is_not_null = {1, 0, 1, 1};
  vector<uint8_t> struct_is_not_null = {1, 1, 1, 1};

  ListBuilder* list_vb = static_cast<ListBuilder*>(
      builder_->value_builder().at(0).get());
  Int8Builder* char_vb = static_cast<Int8Builder*>(
      list_vb->value_builder().get());
  Int32Builder* int_vb = static_cast<Int32Builder*>(
      builder_->value_builder().at(1).get());
  ASSERT_EQ(2, builder_->value_builder().size());

  EXPECT_OK(builder_->Reserve(list_lengths.size()));
  EXPECT_OK(char_vb->Reserve(list_values.size()));
  EXPECT_OK(int_vb->Reserve(int_values.size()));

  int pos = 0;
  for (size_t i = 0; i < list_lengths.size(); ++i) {
    ASSERT_OK(list_vb->Append(list_is_not_null[i] > 0));
    int_vb->Append(int_values[i]);
    for (int j = 0; j < list_lengths[i]; ++j) {
     char_vb->Append(list_values[pos++]);
    }
  }

  for (size_t i = 0; i < struct_is_not_null.size(); ++i) {
    ASSERT_OK(builder_->Append(struct_is_not_null[i] > 0));
  }

  Done();

  ASSERT_EQ(4, result_->length());

  auto list_char = static_cast<ListArray*>(result_->values(0).get());
  auto chars = static_cast<Int8Array*>(list_char->values().get());
  auto int32 = static_cast<Int32Array*>(result_->values(1).get());

  ASSERT_EQ(0, result_->null_count());
  ASSERT_EQ(1, list_char->null_count());
  ASSERT_EQ(0, int32->null_count());


  for (int i = 0; i < result_->length(); ++i) {
    ASSERT_EQ(!static_cast<bool>(struct_is_not_null[i]), result_->IsNull(i));
    ASSERT_EQ(!static_cast<bool>(list_is_not_null[i]), list_char->IsNull(i));
  }

  // List<char>
  ASSERT_EQ(4, list_char->length());
  ASSERT_EQ(10, list_char->values()->length());
  for (size_t i = 0; i < list_offsets.size(); ++i) {
    ASSERT_EQ(list_offsets[i], list_char->offsets()[i]);
  }
  for (size_t i = 0; i < list_values.size(); ++i) {
    ASSERT_EQ(list_values[i], chars->Value(i));
  }

  // Int32
  ASSERT_EQ(4, int32->length());
  for (size_t i = 0; i < int_values.size(); ++i) {
    ASSERT_EQ(int_values[i], int32->Value(i));
  }
}
} // namespace arrow
