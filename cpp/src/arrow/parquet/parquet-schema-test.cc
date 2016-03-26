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

#include "gtest/gtest.h"

#include "arrow/parquet/schema.h"

#include "arrow/test-util.h"
#include "arrow/util/status.h"

namespace arrow {

namespace parquet {

using parquet_cpp::Repetition;
using parquet_cpp::schema::NodePtr;
using parquet_cpp::schema::PrimitiveNode;

TEST(TestNodeConversion, Primitive) {
  std::shared_ptr<Field> field;

  NodePtr node = PrimitiveNode::Make("boolean", Repetition::REQUIRED,
      parquet_cpp::Type::BOOLEAN);

  ASSERT_OK(NodeToField(node, &field));
  ASSERT_EQ(field->name, "boolean");
  ASSERT_TRUE(field->type->Equals(std::make_shared<BooleanType>()));
  ASSERT_FALSE(field->nullable);

  node = PrimitiveNode::Make("int32", Repetition::REQUIRED, parquet_cpp::Type::INT32);
  ASSERT_OK(NodeToField(node, &field));
  ASSERT_EQ(field->name, "int32");
  ASSERT_TRUE(field->type->Equals(std::make_shared<Int32Type>()));
  ASSERT_FALSE(field->nullable);

  node = PrimitiveNode::Make("int64", Repetition::REQUIRED, parquet_cpp::Type::INT64);
  ASSERT_OK(NodeToField(node, &field));
  ASSERT_EQ(field->name, "int64");
  ASSERT_TRUE(field->type->Equals(std::make_shared<Int64Type>()));
  ASSERT_FALSE(field->nullable);

  // case parquet_cpp::Type::INT96:
  // TODO: Implement!
  // node = PrimitiveNode::Make("int96", Repetition::REQUIRED, parquet_cpp::Type::INT96);
  // field = NodeToField(node);
  // TODO: Assertions

  // case parquet_cpp::Type::FLOAT:
  node = PrimitiveNode::Make("float", Repetition::REQUIRED, parquet_cpp::Type::FLOAT);
  ASSERT_OK(NodeToField(node, &field));
  ASSERT_EQ(field->name, "float");
  ASSERT_TRUE(field->type->Equals(std::make_shared<FloatType>()));
  ASSERT_FALSE(field->nullable);

  // case parquet_cpp::Type::DOUBLE:
  node = PrimitiveNode::Make("double", Repetition::REQUIRED, parquet_cpp::Type::DOUBLE);
  ASSERT_OK(NodeToField(node, &field));
  ASSERT_EQ(field->name, "double");
  ASSERT_TRUE(field->type->Equals(std::make_shared<DoubleType>()));
  ASSERT_FALSE(field->nullable);

  // TODO: Implement!
  // node = PrimitiveNode::Make("byte_array", Repetition::REQUIRED,
  //    parquet_cpp::Type::BYTE_ARRAY);
  // field = NodeToField(node);
  // TODO: Assertions

  // TODO: Implement!
  // node = PrimitiveNode::Make("fixed_len_byte_array", Repetition::REQUIRED,
  //    parquet_cpp::Type::FIXED_LEN_BYTE_ARRAY);
  // field = NodeToField(node);
  // TODO: Assertions
}

const auto UINT8 = std::make_shared<UInt8Type>();

TEST(TestNodeConversion, Int96Timestamp) {
}

TEST(TestNodeConversion, ByteArray) {
  std::shared_ptr<Field> field;

  NodePtr node = PrimitiveNode::Make("field0", Repetition::OPTIONAL,
      parquet_cpp::Type::BYTE_ARRAY);
  ASSERT_OK(NodeToField(node, &field));

  std::shared_ptr<DataType> ex_type = std::make_shared<ListType>(
    std::make_shared<Field>("", UINT8));

  ASSERT_EQ(field->name, "field0");
  ASSERT_TRUE(field->type->Equals(ex_type));
  ASSERT_TRUE(field->nullable);

  node = PrimitiveNode::Make("field1", Repetition::OPTIONAL,
      parquet_cpp::Type::BYTE_ARRAY,
      parquet_cpp::LogicalType::UTF8);
  ASSERT_OK(NodeToField(node, &field));
  ex_type = std::make_shared<StringType>();

  ASSERT_EQ(field->name, "field1");
  ASSERT_TRUE(field->type->Equals(ex_type));
  ASSERT_TRUE(field->nullable);
}

TEST(TestNodeConversion, FixedLenByteArray) {
}

TEST(TestNodeConversion, Logical) {
}

TEST(TestSchemaConversion, Basics) {
}

} // namespace parquet

} // namespace arrow
