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

#include "parquet/arrow/schema.h"
#include "parquet/schema.h"

#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"

using arrow::ArrayFromVector;
using arrow::Field;
using arrow::TimeUnit;

using ParquetType = parquet::Type;
using parquet::LogicalType;
using parquet::Repetition;
using parquet::schema::GroupNode;
using parquet::schema::NodePtr;
using parquet::schema::PrimitiveNode;

namespace parquet {

namespace arrow {

const auto BOOL = ::arrow::boolean();
const auto UINT8 = ::arrow::uint8();
const auto INT32 = ::arrow::int32();
const auto INT64 = ::arrow::int64();
const auto FLOAT = ::arrow::float32();
const auto DOUBLE = ::arrow::float64();
const auto UTF8 = ::arrow::utf8();
const auto TIMESTAMP_MS = ::arrow::timestamp(TimeUnit::MILLI);
const auto TIMESTAMP_US = ::arrow::timestamp(TimeUnit::MICRO);
const auto TIMESTAMP_NS = ::arrow::timestamp(TimeUnit::NANO);
const auto BINARY = ::arrow::binary();
const auto DECIMAL_8_4 = std::make_shared<::arrow::Decimal128Type>(8, 4);

class TestConvertParquetSchema : public ::testing::Test {
 public:
  virtual void SetUp() {}

  void CheckFlatSchema(const std::shared_ptr<::arrow::Schema>& expected_schema) {
    ASSERT_EQ(expected_schema->num_fields(), result_schema_->num_fields());
    for (int i = 0; i < expected_schema->num_fields(); ++i) {
      auto lhs = result_schema_->field(i);
      auto rhs = expected_schema->field(i);
      EXPECT_TRUE(lhs->Equals(rhs))
          << i << " " << lhs->ToString() << " != " << rhs->ToString();
    }
  }

  ::arrow::Status ConvertSchema(const std::vector<NodePtr>& nodes) {
    NodePtr schema = GroupNode::Make("schema", Repetition::REPEATED, nodes);
    descr_.Init(schema);
    return FromParquetSchema(&descr_, &result_schema_);
  }

  ::arrow::Status ConvertSchema(const std::vector<NodePtr>& nodes,
                                const std::vector<int>& column_indices) {
    NodePtr schema = GroupNode::Make("schema", Repetition::REPEATED, nodes);
    descr_.Init(schema);
    return FromParquetSchema(&descr_, column_indices, &result_schema_);
  }

  ::arrow::Status ConvertSchema(
      const std::vector<NodePtr>& nodes,
      const std::shared_ptr<const KeyValueMetadata>& key_value_metadata) {
    NodePtr schema = GroupNode::Make("schema", Repetition::REPEATED, nodes);
    descr_.Init(schema);
    return FromParquetSchema(&descr_, {}, key_value_metadata, &result_schema_);
  }

 protected:
  SchemaDescriptor descr_;
  std::shared_ptr<::arrow::Schema> result_schema_;
};

TEST_F(TestConvertParquetSchema, ParquetFlatPrimitives) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  parquet_fields.push_back(
      PrimitiveNode::Make("boolean", Repetition::REQUIRED, ParquetType::BOOLEAN));
  arrow_fields.push_back(std::make_shared<Field>("boolean", BOOL, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int32", Repetition::REQUIRED, ParquetType::INT32));
  arrow_fields.push_back(std::make_shared<Field>("int32", INT32, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int64", Repetition::REQUIRED, ParquetType::INT64));
  arrow_fields.push_back(std::make_shared<Field>("int64", INT64, false));

  parquet_fields.push_back(PrimitiveNode::Make("timestamp", Repetition::REQUIRED,
                                               ParquetType::INT64,
                                               LogicalType::TIMESTAMP_MILLIS));
  arrow_fields.push_back(std::make_shared<Field>("timestamp", TIMESTAMP_MS, false));

  parquet_fields.push_back(PrimitiveNode::Make("timestamp[us]", Repetition::REQUIRED,
                                               ParquetType::INT64,
                                               LogicalType::TIMESTAMP_MICROS));
  arrow_fields.push_back(std::make_shared<Field>("timestamp[us]", TIMESTAMP_US, false));

  parquet_fields.push_back(PrimitiveNode::Make("date", Repetition::REQUIRED,
                                               ParquetType::INT32, LogicalType::DATE));
  arrow_fields.push_back(std::make_shared<Field>("date", ::arrow::date32(), false));

  parquet_fields.push_back(PrimitiveNode::Make(
      "time32", Repetition::REQUIRED, ParquetType::INT32, LogicalType::TIME_MILLIS));
  arrow_fields.push_back(
      std::make_shared<Field>("time32", ::arrow::time32(TimeUnit::MILLI), false));

  parquet_fields.push_back(PrimitiveNode::Make(
      "time64", Repetition::REQUIRED, ParquetType::INT64, LogicalType::TIME_MICROS));
  arrow_fields.push_back(
      std::make_shared<Field>("time64", ::arrow::time64(TimeUnit::MICRO), false));

  parquet_fields.push_back(
      PrimitiveNode::Make("timestamp96", Repetition::REQUIRED, ParquetType::INT96));
  arrow_fields.push_back(std::make_shared<Field>("timestamp96", TIMESTAMP_NS, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("float", Repetition::OPTIONAL, ParquetType::FLOAT));
  arrow_fields.push_back(std::make_shared<Field>("float", FLOAT));

  parquet_fields.push_back(
      PrimitiveNode::Make("double", Repetition::OPTIONAL, ParquetType::DOUBLE));
  arrow_fields.push_back(std::make_shared<Field>("double", DOUBLE));

  parquet_fields.push_back(
      PrimitiveNode::Make("binary", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY));
  arrow_fields.push_back(std::make_shared<Field>("binary", BINARY));

  parquet_fields.push_back(PrimitiveNode::Make(
      "string", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, LogicalType::UTF8));
  arrow_fields.push_back(std::make_shared<Field>("string", UTF8));

  parquet_fields.push_back(PrimitiveNode::Make("flba-binary", Repetition::OPTIONAL,
                                               ParquetType::FIXED_LEN_BYTE_ARRAY,
                                               LogicalType::NONE, 12));
  arrow_fields.push_back(
      std::make_shared<Field>("flba-binary", ::arrow::fixed_size_binary(12)));

  auto arrow_schema = std::make_shared<::arrow::Schema>(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(arrow_schema));
}

TEST_F(TestConvertParquetSchema, DuplicateFieldNames) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  parquet_fields.push_back(
      PrimitiveNode::Make("xxx", Repetition::REQUIRED, ParquetType::BOOLEAN));
  auto arrow_field1 = std::make_shared<Field>("xxx", BOOL, false);

  parquet_fields.push_back(
      PrimitiveNode::Make("xxx", Repetition::REQUIRED, ParquetType::INT32));
  auto arrow_field2 = std::make_shared<Field>("xxx", INT32, false);

  ASSERT_OK(ConvertSchema(parquet_fields));
  arrow_fields = {arrow_field1, arrow_field2};
  ASSERT_NO_FATAL_FAILURE(
      CheckFlatSchema(std::make_shared<::arrow::Schema>(arrow_fields)));

  ASSERT_OK(ConvertSchema(parquet_fields, std::vector<int>({0, 1})));
  arrow_fields = {arrow_field1, arrow_field2};
  ASSERT_NO_FATAL_FAILURE(
      CheckFlatSchema(std::make_shared<::arrow::Schema>(arrow_fields)));

  ASSERT_OK(ConvertSchema(parquet_fields, std::vector<int>({1, 0})));
  arrow_fields = {arrow_field2, arrow_field1};
  ASSERT_NO_FATAL_FAILURE(
      CheckFlatSchema(std::make_shared<::arrow::Schema>(arrow_fields)));
}

TEST_F(TestConvertParquetSchema, ParquetKeyValueMetadata) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  parquet_fields.push_back(
      PrimitiveNode::Make("boolean", Repetition::REQUIRED, ParquetType::BOOLEAN));
  arrow_fields.push_back(std::make_shared<Field>("boolean", BOOL, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int32", Repetition::REQUIRED, ParquetType::INT32));
  arrow_fields.push_back(std::make_shared<Field>("int32", INT32, false));

  auto key_value_metadata = std::make_shared<KeyValueMetadata>();
  key_value_metadata->Append("foo", "bar");
  key_value_metadata->Append("biz", "baz");
  ASSERT_OK(ConvertSchema(parquet_fields, key_value_metadata));

  auto arrow_metadata = result_schema_->metadata();
  ASSERT_EQ("foo", arrow_metadata->key(0));
  ASSERT_EQ("bar", arrow_metadata->value(0));
  ASSERT_EQ("biz", arrow_metadata->key(1));
  ASSERT_EQ("baz", arrow_metadata->value(1));
}

TEST_F(TestConvertParquetSchema, ParquetEmptyKeyValueMetadata) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  parquet_fields.push_back(
      PrimitiveNode::Make("int32", Repetition::REQUIRED, ParquetType::INT32));
  arrow_fields.push_back(std::make_shared<Field>("int32", INT32, false));

  std::shared_ptr<KeyValueMetadata> key_value_metadata = nullptr;
  ASSERT_OK(ConvertSchema(parquet_fields, key_value_metadata));

  auto arrow_metadata = result_schema_->metadata();
  ASSERT_EQ(arrow_metadata, nullptr);
}

TEST_F(TestConvertParquetSchema, ParquetFlatDecimals) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  parquet_fields.push_back(PrimitiveNode::Make("flba-decimal", Repetition::OPTIONAL,
                                               ParquetType::FIXED_LEN_BYTE_ARRAY,
                                               LogicalType::DECIMAL, 4, 8, 4));
  arrow_fields.push_back(std::make_shared<Field>("flba-decimal", DECIMAL_8_4));

  parquet_fields.push_back(PrimitiveNode::Make("binary-decimal", Repetition::OPTIONAL,
                                               ParquetType::BYTE_ARRAY,
                                               LogicalType::DECIMAL, -1, 8, 4));
  arrow_fields.push_back(std::make_shared<Field>("binary-decimal", DECIMAL_8_4));

  parquet_fields.push_back(PrimitiveNode::Make("int32-decimal", Repetition::OPTIONAL,
                                               ParquetType::INT32, LogicalType::DECIMAL,
                                               -1, 8, 4));
  arrow_fields.push_back(std::make_shared<Field>("int32-decimal", DECIMAL_8_4));

  parquet_fields.push_back(PrimitiveNode::Make("int64-decimal", Repetition::OPTIONAL,
                                               ParquetType::INT64, LogicalType::DECIMAL,
                                               -1, 8, 4));
  arrow_fields.push_back(std::make_shared<Field>("int64-decimal", DECIMAL_8_4));

  auto arrow_schema = std::make_shared<::arrow::Schema>(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(arrow_schema));
}

TEST_F(TestConvertParquetSchema, ParquetLists) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  // LIST encoding example taken from parquet-format/LogicalTypes.md

  // // List<String> (list non-null, elements nullable)
  // required group my_list (LIST) {
  //   repeated group list {
  //     optional binary element (UTF8);
  //   }
  // }
  {
    auto element = PrimitiveNode::Make("string", Repetition::OPTIONAL,
                                       ParquetType::BYTE_ARRAY, LogicalType::UTF8);
    auto list = GroupNode::Make("list", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::REQUIRED, {list}, LogicalType::LIST));
    auto arrow_element = std::make_shared<Field>("string", UTF8, true);
    auto arrow_list = std::make_shared<::arrow::ListType>(arrow_element);
    arrow_fields.push_back(std::make_shared<Field>("my_list", arrow_list, false));
  }

  // // List<String> (list nullable, elements non-null)
  // optional group my_list (LIST) {
  //   repeated group list {
  //     required binary element (UTF8);
  //   }
  // }
  {
    auto element = PrimitiveNode::Make("string", Repetition::REQUIRED,
                                       ParquetType::BYTE_ARRAY, LogicalType::UTF8);
    auto list = GroupNode::Make("list", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {list}, LogicalType::LIST));
    auto arrow_element = std::make_shared<Field>("string", UTF8, false);
    auto arrow_list = std::make_shared<::arrow::ListType>(arrow_element);
    arrow_fields.push_back(std::make_shared<Field>("my_list", arrow_list, true));
  }

  // Element types can be nested structures. For example, a list of lists:
  //
  // // List<List<Integer>>
  // optional group array_of_arrays (LIST) {
  //   repeated group list {
  //     required group element (LIST) {
  //       repeated group list {
  //         required int32 element;
  //       }
  //     }
  //   }
  // }
  {
    auto inner_element =
        PrimitiveNode::Make("int32", Repetition::REQUIRED, ParquetType::INT32);
    auto inner_list = GroupNode::Make("list", Repetition::REPEATED, {inner_element});
    auto element =
        GroupNode::Make("element", Repetition::REQUIRED, {inner_list}, LogicalType::LIST);
    auto list = GroupNode::Make("list", Repetition::REPEATED, {element});
    parquet_fields.push_back(GroupNode::Make("array_of_arrays", Repetition::OPTIONAL,
                                             {list}, LogicalType::LIST));
    auto arrow_inner_element = std::make_shared<Field>("int32", INT32, false);
    auto arrow_inner_list = std::make_shared<::arrow::ListType>(arrow_inner_element);
    auto arrow_element = std::make_shared<Field>("element", arrow_inner_list, false);
    auto arrow_list = std::make_shared<::arrow::ListType>(arrow_element);
    arrow_fields.push_back(std::make_shared<Field>("array_of_arrays", arrow_list, true));
  }

  // // List<String> (list nullable, elements non-null)
  // optional group my_list (LIST) {
  //   repeated group element {
  //     required binary str (UTF8);
  //   };
  // }
  {
    auto element = PrimitiveNode::Make("str", Repetition::REQUIRED,
                                       ParquetType::BYTE_ARRAY, LogicalType::UTF8);
    auto list = GroupNode::Make("element", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {list}, LogicalType::LIST));
    auto arrow_element = std::make_shared<Field>("str", UTF8, false);
    auto arrow_list = std::make_shared<::arrow::ListType>(arrow_element);
    arrow_fields.push_back(std::make_shared<Field>("my_list", arrow_list, true));
  }

  // // List<Integer> (nullable list, non-null elements)
  // optional group my_list (LIST) {
  //   repeated int32 element;
  // }
  {
    auto element =
        PrimitiveNode::Make("element", Repetition::REPEATED, ParquetType::INT32);
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {element}, LogicalType::LIST));
    auto arrow_element = std::make_shared<Field>("element", INT32, false);
    auto arrow_list = std::make_shared<::arrow::ListType>(arrow_element);
    arrow_fields.push_back(std::make_shared<Field>("my_list", arrow_list, true));
  }

  // // List<Tuple<String, Integer>> (nullable list, non-null elements)
  // optional group my_list (LIST) {
  //   repeated group element {
  //     required binary str (UTF8);
  //     required int32 num;
  //   };
  // }
  {
    auto str_element = PrimitiveNode::Make("str", Repetition::REQUIRED,
                                           ParquetType::BYTE_ARRAY, LogicalType::UTF8);
    auto num_element =
        PrimitiveNode::Make("num", Repetition::REQUIRED, ParquetType::INT32);
    auto element =
        GroupNode::Make("element", Repetition::REPEATED, {str_element, num_element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {element}, LogicalType::LIST));
    auto arrow_str = std::make_shared<Field>("str", UTF8, false);
    auto arrow_num = std::make_shared<Field>("num", INT32, false);
    std::vector<std::shared_ptr<Field>> fields({arrow_str, arrow_num});
    auto arrow_struct = std::make_shared<::arrow::StructType>(fields);
    auto arrow_element = std::make_shared<Field>("element", arrow_struct, false);
    auto arrow_list = std::make_shared<::arrow::ListType>(arrow_element);
    arrow_fields.push_back(std::make_shared<Field>("my_list", arrow_list, true));
  }

  // // List<OneTuple<String>> (nullable list, non-null elements)
  // optional group my_list (LIST) {
  //   repeated group array {
  //     required binary str (UTF8);
  //   };
  // }
  // Special case: group is named array
  {
    auto element = PrimitiveNode::Make("str", Repetition::REQUIRED,
                                       ParquetType::BYTE_ARRAY, LogicalType::UTF8);
    auto array = GroupNode::Make("array", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {array}, LogicalType::LIST));
    auto arrow_str = std::make_shared<Field>("str", UTF8, false);
    std::vector<std::shared_ptr<Field>> fields({arrow_str});
    auto arrow_struct = std::make_shared<::arrow::StructType>(fields);
    auto arrow_element = std::make_shared<Field>("array", arrow_struct, false);
    auto arrow_list = std::make_shared<::arrow::ListType>(arrow_element);
    arrow_fields.push_back(std::make_shared<Field>("my_list", arrow_list, true));
  }

  // // List<OneTuple<String>> (nullable list, non-null elements)
  // optional group my_list (LIST) {
  //   repeated group my_list_tuple {
  //     required binary str (UTF8);
  //   };
  // }
  // Special case: group named ends in _tuple
  {
    auto element = PrimitiveNode::Make("str", Repetition::REQUIRED,
                                       ParquetType::BYTE_ARRAY, LogicalType::UTF8);
    auto array = GroupNode::Make("my_list_tuple", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {array}, LogicalType::LIST));
    auto arrow_str = std::make_shared<Field>("str", UTF8, false);
    std::vector<std::shared_ptr<Field>> fields({arrow_str});
    auto arrow_struct = std::make_shared<::arrow::StructType>(fields);
    auto arrow_element = std::make_shared<Field>("my_list_tuple", arrow_struct, false);
    auto arrow_list = std::make_shared<::arrow::ListType>(arrow_element);
    arrow_fields.push_back(std::make_shared<Field>("my_list", arrow_list, true));
  }

  // One-level encoding: Only allows required lists with required cells
  //   repeated value_type name
  {
    parquet_fields.push_back(
        PrimitiveNode::Make("name", Repetition::REPEATED, ParquetType::INT32));
    auto arrow_element = std::make_shared<Field>("name", INT32, false);
    auto arrow_list = std::make_shared<::arrow::ListType>(arrow_element);
    arrow_fields.push_back(std::make_shared<Field>("name", arrow_list, false));
  }

  auto arrow_schema = std::make_shared<::arrow::Schema>(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(arrow_schema));
}

TEST_F(TestConvertParquetSchema, UnsupportedThings) {
  std::vector<NodePtr> unsupported_nodes;

  for (const NodePtr& node : unsupported_nodes) {
    ASSERT_RAISES(NotImplemented, ConvertSchema({node}));
  }
}

TEST_F(TestConvertParquetSchema, ParquetNestedSchema) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  // required group group1 {
  //   required bool leaf1;
  //   required int32 leaf2;
  // }
  // required int64 leaf3;
  {
    parquet_fields.push_back(GroupNode::Make(
        "group1", Repetition::REQUIRED,
        {PrimitiveNode::Make("leaf1", Repetition::REQUIRED, ParquetType::BOOLEAN),
         PrimitiveNode::Make("leaf2", Repetition::REQUIRED, ParquetType::INT32)}));
    parquet_fields.push_back(
        PrimitiveNode::Make("leaf3", Repetition::REQUIRED, ParquetType::INT64));

    auto group1_fields = {std::make_shared<Field>("leaf1", BOOL, false),
                          std::make_shared<Field>("leaf2", INT32, false)};
    auto arrow_group1_type = std::make_shared<::arrow::StructType>(group1_fields);
    arrow_fields.push_back(std::make_shared<Field>("group1", arrow_group1_type, false));
    arrow_fields.push_back(std::make_shared<Field>("leaf3", INT64, false));
  }

  auto arrow_schema = std::make_shared<::arrow::Schema>(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(arrow_schema));
}

TEST_F(TestConvertParquetSchema, ParquetNestedSchemaPartial) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  // Full Parquet Schema:
  // required group group1 {
  //   required int64 leaf1;
  //   required int64 leaf2;
  // }
  // required group group2 {
  //   required int64 leaf3;
  //   required int64 leaf4;
  // }
  // required int64 leaf5;
  //
  // Expected partial arrow schema (columns 0, 3, 4):
  // required group group1 {
  //   required int64 leaf1;
  // }
  // required group group2 {
  //   required int64 leaf4;
  // }
  // required int64 leaf5;
  {
    parquet_fields.push_back(GroupNode::Make(
        "group1", Repetition::REQUIRED,
        {PrimitiveNode::Make("leaf1", Repetition::REQUIRED, ParquetType::INT64),
         PrimitiveNode::Make("leaf2", Repetition::REQUIRED, ParquetType::INT64)}));
    parquet_fields.push_back(GroupNode::Make(
        "group2", Repetition::REQUIRED,
        {PrimitiveNode::Make("leaf3", Repetition::REQUIRED, ParquetType::INT64),
         PrimitiveNode::Make("leaf4", Repetition::REQUIRED, ParquetType::INT64)}));
    parquet_fields.push_back(
        PrimitiveNode::Make("leaf5", Repetition::REQUIRED, ParquetType::INT64));

    auto group1_fields = {std::make_shared<Field>("leaf1", INT64, false)};
    auto arrow_group1_type = std::make_shared<::arrow::StructType>(group1_fields);
    auto group2_fields = {std::make_shared<Field>("leaf4", INT64, false)};
    auto arrow_group2_type = std::make_shared<::arrow::StructType>(group2_fields);

    arrow_fields.push_back(std::make_shared<Field>("group1", arrow_group1_type, false));
    arrow_fields.push_back(std::make_shared<Field>("group2", arrow_group2_type, false));
    arrow_fields.push_back(std::make_shared<Field>("leaf5", INT64, false));
  }

  auto arrow_schema = std::make_shared<::arrow::Schema>(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields, std::vector<int>{0, 3, 4}));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(arrow_schema));
}

TEST_F(TestConvertParquetSchema, ParquetNestedSchemaPartialOrdering) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  // Full Parquet Schema:
  // required group group1 {
  //   required int64 leaf1;
  //   required int64 leaf2;
  // }
  // required group group2 {
  //   required int64 leaf3;
  //   required int64 leaf4;
  // }
  // required int64 leaf5;
  //
  // Expected partial arrow schema (columns 3, 4, 0):
  // required group group2 {
  //   required int64 leaf4;
  // }
  // required int64 leaf5;
  // required group group1 {
  //   required int64 leaf1;
  // }
  {
    parquet_fields.push_back(GroupNode::Make(
        "group1", Repetition::REQUIRED,
        {PrimitiveNode::Make("leaf1", Repetition::REQUIRED, ParquetType::INT64),
         PrimitiveNode::Make("leaf2", Repetition::REQUIRED, ParquetType::INT64)}));
    parquet_fields.push_back(GroupNode::Make(
        "group2", Repetition::REQUIRED,
        {PrimitiveNode::Make("leaf3", Repetition::REQUIRED, ParquetType::INT64),
         PrimitiveNode::Make("leaf4", Repetition::REQUIRED, ParquetType::INT64)}));
    parquet_fields.push_back(
        PrimitiveNode::Make("leaf5", Repetition::REQUIRED, ParquetType::INT64));

    auto group1_fields = {std::make_shared<Field>("leaf1", INT64, false)};
    auto arrow_group1_type = std::make_shared<::arrow::StructType>(group1_fields);
    auto group2_fields = {std::make_shared<Field>("leaf4", INT64, false)};
    auto arrow_group2_type = std::make_shared<::arrow::StructType>(group2_fields);

    arrow_fields.push_back(std::make_shared<Field>("group2", arrow_group2_type, false));
    arrow_fields.push_back(std::make_shared<Field>("leaf5", INT64, false));
    arrow_fields.push_back(std::make_shared<Field>("group1", arrow_group1_type, false));
  }

  auto arrow_schema = std::make_shared<::arrow::Schema>(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields, std::vector<int>{3, 4, 0}));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(arrow_schema));
}
TEST_F(TestConvertParquetSchema, ParquetRepeatedNestedSchema) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;
  {
    //   optional int32 leaf1;
    //   repeated group outerGroup {
    //     optional int32 leaf2;
    //     repeated group innerGroup {
    //       optional int32 leaf3;
    //     }
    //   }
    parquet_fields.push_back(
        PrimitiveNode::Make("leaf1", Repetition::OPTIONAL, ParquetType::INT32));
    parquet_fields.push_back(GroupNode::Make(
        "outerGroup", Repetition::REPEATED,
        {PrimitiveNode::Make("leaf2", Repetition::OPTIONAL, ParquetType::INT32),
         GroupNode::Make(
             "innerGroup", Repetition::REPEATED,
             {PrimitiveNode::Make("leaf3", Repetition::OPTIONAL, ParquetType::INT32)})}));

    auto inner_group_fields = {std::make_shared<Field>("leaf3", INT32, true)};
    auto inner_group_type = std::make_shared<::arrow::StructType>(inner_group_fields);
    auto outer_group_fields = {
        std::make_shared<Field>("leaf2", INT32, true),
        std::make_shared<Field>(
            "innerGroup",
            ::arrow::list(std::make_shared<Field>("innerGroup", inner_group_type, false)),
            false)};
    auto outer_group_type = std::make_shared<::arrow::StructType>(outer_group_fields);

    arrow_fields.push_back(std::make_shared<Field>("leaf1", INT32, true));
    arrow_fields.push_back(std::make_shared<Field>(
        "outerGroup",
        ::arrow::list(std::make_shared<Field>("outerGroup", outer_group_type, false)),
        false));
  }
  auto arrow_schema = std::make_shared<::arrow::Schema>(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(arrow_schema));
}

class TestConvertArrowSchema : public ::testing::Test {
 public:
  virtual void SetUp() {}

  void CheckFlatSchema(const std::vector<NodePtr>& nodes) {
    NodePtr schema_node = GroupNode::Make("schema", Repetition::REPEATED, nodes);
    const GroupNode* expected_schema_node =
        static_cast<const GroupNode*>(schema_node.get());
    const GroupNode* result_schema_node = result_schema_->group_node();

    ASSERT_EQ(expected_schema_node->field_count(), result_schema_node->field_count());

    for (int i = 0; i < expected_schema_node->field_count(); i++) {
      auto lhs = result_schema_node->field(i);
      auto rhs = expected_schema_node->field(i);
      EXPECT_TRUE(lhs->Equals(rhs.get()));
    }
  }

  ::arrow::Status ConvertSchema(const std::vector<std::shared_ptr<Field>>& fields) {
    arrow_schema_ = std::make_shared<::arrow::Schema>(fields);
    std::shared_ptr<::parquet::WriterProperties> properties =
        ::parquet::default_writer_properties();
    return ToParquetSchema(arrow_schema_.get(), *properties.get(), &result_schema_);
  }

 protected:
  std::shared_ptr<::arrow::Schema> arrow_schema_;
  std::shared_ptr<SchemaDescriptor> result_schema_;
};

TEST_F(TestConvertArrowSchema, ParquetFlatPrimitives) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  parquet_fields.push_back(
      PrimitiveNode::Make("boolean", Repetition::REQUIRED, ParquetType::BOOLEAN));
  arrow_fields.push_back(std::make_shared<Field>("boolean", BOOL, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int32", Repetition::REQUIRED, ParquetType::INT32));
  arrow_fields.push_back(std::make_shared<Field>("int32", INT32, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int64", Repetition::REQUIRED, ParquetType::INT64));
  arrow_fields.push_back(std::make_shared<Field>("int64", INT64, false));

  parquet_fields.push_back(PrimitiveNode::Make("date", Repetition::REQUIRED,
                                               ParquetType::INT32, LogicalType::DATE));
  arrow_fields.push_back(std::make_shared<Field>("date", ::arrow::date32(), false));

  parquet_fields.push_back(PrimitiveNode::Make("date64", Repetition::REQUIRED,
                                               ParquetType::INT32, LogicalType::DATE));
  arrow_fields.push_back(std::make_shared<Field>("date64", ::arrow::date64(), false));

  parquet_fields.push_back(PrimitiveNode::Make("timestamp", Repetition::REQUIRED,
                                               ParquetType::INT64,
                                               LogicalType::TIMESTAMP_MILLIS));
  arrow_fields.push_back(std::make_shared<Field>("timestamp", TIMESTAMP_MS, false));

  parquet_fields.push_back(PrimitiveNode::Make("timestamp[us]", Repetition::REQUIRED,
                                               ParquetType::INT64,
                                               LogicalType::TIMESTAMP_MICROS));
  arrow_fields.push_back(std::make_shared<Field>("timestamp[us]", TIMESTAMP_US, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("float", Repetition::OPTIONAL, ParquetType::FLOAT));
  arrow_fields.push_back(std::make_shared<Field>("float", FLOAT));

  parquet_fields.push_back(
      PrimitiveNode::Make("double", Repetition::OPTIONAL, ParquetType::DOUBLE));
  arrow_fields.push_back(std::make_shared<Field>("double", DOUBLE));

  parquet_fields.push_back(PrimitiveNode::Make(
      "string", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, LogicalType::UTF8));
  arrow_fields.push_back(std::make_shared<Field>("string", UTF8));

  parquet_fields.push_back(PrimitiveNode::Make(
      "binary", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, LogicalType::NONE));
  arrow_fields.push_back(std::make_shared<Field>("binary", BINARY));

  ASSERT_OK(ConvertSchema(arrow_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(parquet_fields));
}

TEST_F(TestConvertArrowSchema, ParquetFlatPrimitivesAsDictionaries) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;
  std::shared_ptr<::arrow::Array> dict;

  parquet_fields.push_back(
      PrimitiveNode::Make("int32", Repetition::REQUIRED, ParquetType::INT32));
  ArrayFromVector<::arrow::Int32Type, int32_t>(std::vector<int32_t>(), &dict);
  arrow_fields.push_back(
      ::arrow::field("int32", ::arrow::dictionary(::arrow::int8(), dict), false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int64", Repetition::REQUIRED, ParquetType::INT64));
  ArrayFromVector<::arrow::Int64Type, int64_t>(std::vector<int64_t>(), &dict);
  arrow_fields.push_back(std::make_shared<Field>(
      "int64", ::arrow::dictionary(::arrow::int8(), dict), false));

  parquet_fields.push_back(PrimitiveNode::Make("date", Repetition::REQUIRED,
                                               ParquetType::INT32, LogicalType::DATE));
  ArrayFromVector<::arrow::Date32Type, int32_t>(std::vector<int32_t>(), &dict);
  arrow_fields.push_back(
      std::make_shared<Field>("date", ::arrow::dictionary(::arrow::int8(), dict), false));

  parquet_fields.push_back(PrimitiveNode::Make("date64", Repetition::REQUIRED,
                                               ParquetType::INT32, LogicalType::DATE));
  ArrayFromVector<::arrow::Date64Type, int64_t>(std::vector<int64_t>(), &dict);
  arrow_fields.push_back(std::make_shared<Field>(
      "date64", ::arrow::dictionary(::arrow::int8(), dict), false));

  parquet_fields.push_back(
      PrimitiveNode::Make("float", Repetition::OPTIONAL, ParquetType::FLOAT));
  ArrayFromVector<::arrow::FloatType, float>(std::vector<float>(), &dict);
  arrow_fields.push_back(
      std::make_shared<Field>("float", ::arrow::dictionary(::arrow::int8(), dict)));

  parquet_fields.push_back(
      PrimitiveNode::Make("double", Repetition::OPTIONAL, ParquetType::DOUBLE));
  ArrayFromVector<::arrow::DoubleType, double>(std::vector<double>(), &dict);
  arrow_fields.push_back(
      std::make_shared<Field>("double", ::arrow::dictionary(::arrow::int8(), dict)));

  parquet_fields.push_back(PrimitiveNode::Make(
      "string", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, LogicalType::UTF8));
  ::arrow::StringBuilder string_builder(::arrow::default_memory_pool());
  ASSERT_OK(string_builder.Finish(&dict));
  arrow_fields.push_back(
      std::make_shared<Field>("string", ::arrow::dictionary(::arrow::int8(), dict)));

  parquet_fields.push_back(PrimitiveNode::Make(
      "binary", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, LogicalType::NONE));
  ::arrow::BinaryBuilder binary_builder(::arrow::default_memory_pool());
  ASSERT_OK(binary_builder.Finish(&dict));
  arrow_fields.push_back(
      std::make_shared<Field>("binary", ::arrow::dictionary(::arrow::int8(), dict)));

  ASSERT_OK(ConvertSchema(arrow_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(parquet_fields));
}

TEST_F(TestConvertArrowSchema, ParquetLists) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  // parquet_arrow will always generate 3-level LIST encodings

  // // List<String> (list non-null, elements nullable)
  // required group my_list (LIST) {
  //   repeated group list {
  //     optional binary element (UTF8);
  //   }
  // }
  {
    auto element = PrimitiveNode::Make("string", Repetition::OPTIONAL,
                                       ParquetType::BYTE_ARRAY, LogicalType::UTF8);
    auto list = GroupNode::Make("list", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::REQUIRED, {list}, LogicalType::LIST));
    auto arrow_element = std::make_shared<Field>("string", UTF8, true);
    auto arrow_list = std::make_shared<::arrow::ListType>(arrow_element);
    arrow_fields.push_back(std::make_shared<Field>("my_list", arrow_list, false));
  }

  // // List<String> (list nullable, elements non-null)
  // optional group my_list (LIST) {
  //   repeated group list {
  //     required binary element (UTF8);
  //   }
  // }
  {
    auto element = PrimitiveNode::Make("string", Repetition::REQUIRED,
                                       ParquetType::BYTE_ARRAY, LogicalType::UTF8);
    auto list = GroupNode::Make("list", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {list}, LogicalType::LIST));
    auto arrow_element = std::make_shared<Field>("string", UTF8, false);
    auto arrow_list = std::make_shared<::arrow::ListType>(arrow_element);
    arrow_fields.push_back(std::make_shared<Field>("my_list", arrow_list, true));
  }

  ASSERT_OK(ConvertSchema(arrow_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(parquet_fields));
}

TEST_F(TestConvertArrowSchema, UnsupportedTypes) {
  std::vector<std::shared_ptr<Field>> unsupported_fields = {
      ::arrow::field("f0", ::arrow::time64(TimeUnit::NANO))};

  for (const auto& field : unsupported_fields) {
    ASSERT_RAISES(NotImplemented, ConvertSchema({field}));
  }
}

TEST_F(TestConvertArrowSchema, ParquetFlatDecimals) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  // TODO: Test Decimal Arrow -> Parquet conversion

  ASSERT_OK(ConvertSchema(arrow_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(parquet_fields));
}

TEST(InvalidSchema, ParquetNegativeDecimalScale) {
  const auto& type = ::arrow::decimal(23, -2);
  const auto& field = ::arrow::field("f0", type);
  const auto& arrow_schema = ::arrow::schema({field});
  std::shared_ptr<::parquet::WriterProperties> properties =
      ::parquet::default_writer_properties();
  std::shared_ptr<SchemaDescriptor> result_schema;

  ASSERT_RAISES(IOError,
                ToParquetSchema(arrow_schema.get(), *properties.get(), &result_schema));
}

}  // namespace arrow
}  // namespace parquet
