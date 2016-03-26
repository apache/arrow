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
#include <vector>

#include "gtest/gtest.h"

#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/util/status.h"

#include "arrow/parquet/schema.h"

namespace arrow {

namespace parquet {

using parquet_cpp::Repetition;
using parquet_cpp::schema::NodePtr;
using parquet_cpp::schema::GroupNode;
using parquet_cpp::schema::PrimitiveNode;

const auto BOOL = std::make_shared<BooleanType>();
const auto UINT8 = std::make_shared<UInt8Type>();
const auto INT32 = std::make_shared<Int32Type>();
const auto INT64 = std::make_shared<Int64Type>();
const auto FLOAT = std::make_shared<FloatType>();
const auto DOUBLE = std::make_shared<DoubleType>();
const auto UTF8 = std::make_shared<StringType>();
const auto BINARY = std::make_shared<ListType>(
    std::make_shared<Field>("", UINT8));

class TestConvertParquetSchema : public ::testing::Test {
 public:
  virtual void SetUp() {}

  void CheckFlatSchema(const std::shared_ptr<Schema>& expected_schema) {
    ASSERT_EQ(expected_schema->num_fields(), result_schema_->num_fields());
    for (int i = 0; i < expected_schema->num_fields(); ++i) {
      auto lhs = result_schema_->field(i);
      auto rhs = expected_schema->field(i);
      EXPECT_TRUE(lhs->Equals(rhs))
        << i << " " << lhs->ToString() << " != " << rhs->ToString();
    }
  }

  Status ConvertSchema(const std::vector<NodePtr>& nodes) {
    NodePtr schema = GroupNode::Make("schema", Repetition::REPEATED, nodes);
    descr_.Init(schema);
    return FromParquetSchema(&descr_, &result_schema_);
  }

 protected:
  parquet_cpp::SchemaDescriptor descr_;
  std::shared_ptr<Schema> result_schema_;
};

TEST_F(TestConvertParquetSchema, ParquetFlatPrimitives) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  parquet_fields.push_back(
      PrimitiveNode::Make("boolean", Repetition::REQUIRED, parquet_cpp::Type::BOOLEAN));
  arrow_fields.push_back(std::make_shared<Field>("boolean", BOOL, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int32", Repetition::REQUIRED, parquet_cpp::Type::INT32));
  arrow_fields.push_back(std::make_shared<Field>("int32", INT32, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int64", Repetition::REQUIRED, parquet_cpp::Type::INT64));
  arrow_fields.push_back(std::make_shared<Field>("int64", INT64, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("float", Repetition::OPTIONAL, parquet_cpp::Type::FLOAT));
  arrow_fields.push_back(std::make_shared<Field>("float", FLOAT));

  parquet_fields.push_back(
      PrimitiveNode::Make("double", Repetition::OPTIONAL, parquet_cpp::Type::DOUBLE));
  arrow_fields.push_back(std::make_shared<Field>("double", DOUBLE));

  parquet_fields.push_back(
      PrimitiveNode::Make("binary", Repetition::OPTIONAL,
          parquet_cpp::Type::BYTE_ARRAY));
  arrow_fields.push_back(std::make_shared<Field>("binary", BINARY));

  parquet_fields.push_back(
      PrimitiveNode::Make("string", Repetition::OPTIONAL,
          parquet_cpp::Type::BYTE_ARRAY,
          parquet_cpp::LogicalType::UTF8));
  arrow_fields.push_back(std::make_shared<Field>("string", UTF8));

  parquet_fields.push_back(
      PrimitiveNode::Make("flba-binary", Repetition::OPTIONAL,
          parquet_cpp::Type::FIXED_LEN_BYTE_ARRAY,
          parquet_cpp::LogicalType::NONE, 12));
  arrow_fields.push_back(std::make_shared<Field>("flba-binary", BINARY));

  auto arrow_schema = std::make_shared<Schema>(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields));

  CheckFlatSchema(arrow_schema);
}

TEST_F(TestConvertParquetSchema, UnsupportedThings) {
  std::vector<NodePtr> unsupported_nodes;

  unsupported_nodes.push_back(
      PrimitiveNode::Make("int96", Repetition::REQUIRED, parquet_cpp::Type::INT96));

  unsupported_nodes.push_back(
      GroupNode::Make("repeated-group", Repetition::REPEATED, {}));

  unsupported_nodes.push_back(
      PrimitiveNode::Make("int32", Repetition::OPTIONAL,
          parquet_cpp::Type::INT32, parquet_cpp::LogicalType::DATE));

  unsupported_nodes.push_back(
      PrimitiveNode::Make("int64", Repetition::OPTIONAL,
          parquet_cpp::Type::INT64, parquet_cpp::LogicalType::TIMESTAMP_MILLIS));

  for (const NodePtr& node : unsupported_nodes) {
    ASSERT_RAISES(NotImplemented, ConvertSchema({node}));
  }
}

TEST(TestNodeConversion, DateAndTime) {
}

} // namespace parquet

} // namespace arrow
