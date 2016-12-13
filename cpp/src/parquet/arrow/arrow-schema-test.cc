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

#include "arrow/api.h"
#include "arrow/test-util.h"

using arrow::Field;

using ParquetType = parquet::Type;
using parquet::LogicalType;
using parquet::Repetition;
using parquet::schema::NodePtr;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

namespace parquet {

namespace arrow {

const auto BOOL = std::make_shared<::arrow::BooleanType>();
const auto UINT8 = std::make_shared<::arrow::UInt8Type>();
const auto INT32 = std::make_shared<::arrow::Int32Type>();
const auto INT64 = std::make_shared<::arrow::Int64Type>();
const auto FLOAT = std::make_shared<::arrow::FloatType>();
const auto DOUBLE = std::make_shared<::arrow::DoubleType>();
const auto UTF8 = std::make_shared<::arrow::StringType>();
const auto TIMESTAMP_MS =
    std::make_shared<::arrow::TimestampType>(::arrow::TimestampType::Unit::MILLI);
// TODO: This requires parquet-cpp implementing the MICROS enum value
// const auto TIMESTAMP_US = std::make_shared<TimestampType>(TimestampType::Unit::MICRO);
const auto BINARY =
    std::make_shared<::arrow::ListType>(std::make_shared<Field>("", UINT8));
const auto DECIMAL_8_4 = std::make_shared<::arrow::DecimalType>(8, 4);

class TestConvertParquetSchema : public ::testing::Test {
 public:
  virtual void SetUp() {}

  void CheckFlatSchema(const std::shared_ptr<::arrow::Schema>& expected_schema) {
    ASSERT_EQ(expected_schema->num_fields(), result_schema_->num_fields());
    for (int i = 0; i < expected_schema->num_fields(); ++i) {
      auto lhs = result_schema_->field(i);
      auto rhs = expected_schema->field(i);
      EXPECT_TRUE(lhs->Equals(rhs)) << i << " " << lhs->ToString()
                                    << " != " << rhs->ToString();
    }
  }

  ::arrow::Status ConvertSchema(const std::vector<NodePtr>& nodes) {
    NodePtr schema = GroupNode::Make("schema", Repetition::REPEATED, nodes);
    descr_.Init(schema);
    return FromParquetSchema(&descr_, &result_schema_);
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
      ParquetType::INT64, LogicalType::TIMESTAMP_MILLIS));
  arrow_fields.push_back(std::make_shared<Field>("timestamp", TIMESTAMP_MS, false));

  // parquet_fields.push_back(PrimitiveNode::Make("timestamp", Repetition::REQUIRED,
  //     ParquetType::INT64, LogicalType::TIMESTAMP_MICROS));
  // arrow_fields.push_back(std::make_shared<Field>("timestamp", TIMESTAMP_US, false));

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
      ParquetType::FIXED_LEN_BYTE_ARRAY, LogicalType::NONE, 12));
  arrow_fields.push_back(std::make_shared<Field>("flba-binary", BINARY));

  auto arrow_schema = std::make_shared<::arrow::Schema>(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields));

  CheckFlatSchema(arrow_schema);
}

TEST_F(TestConvertParquetSchema, ParquetFlatDecimals) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  parquet_fields.push_back(PrimitiveNode::Make("flba-decimal", Repetition::OPTIONAL,
      ParquetType::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 4, 8, 4));
  arrow_fields.push_back(std::make_shared<Field>("flba-decimal", DECIMAL_8_4));

  parquet_fields.push_back(PrimitiveNode::Make("binary-decimal", Repetition::OPTIONAL,
      ParquetType::BYTE_ARRAY, LogicalType::DECIMAL, -1, 8, 4));
  arrow_fields.push_back(std::make_shared<Field>("binary-decimal", DECIMAL_8_4));

  parquet_fields.push_back(PrimitiveNode::Make("int32-decimal", Repetition::OPTIONAL,
      ParquetType::INT32, LogicalType::DECIMAL, -1, 8, 4));
  arrow_fields.push_back(std::make_shared<Field>("int32-decimal", DECIMAL_8_4));

  parquet_fields.push_back(PrimitiveNode::Make("int64-decimal", Repetition::OPTIONAL,
      ParquetType::INT64, LogicalType::DECIMAL, -1, 8, 4));
  arrow_fields.push_back(std::make_shared<Field>("int64-decimal", DECIMAL_8_4));

  auto arrow_schema = std::make_shared<::arrow::Schema>(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields));

  CheckFlatSchema(arrow_schema);
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
    auto element = PrimitiveNode::Make(
        "string", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, LogicalType::UTF8);
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
    auto element = PrimitiveNode::Make(
        "string", Repetition::REQUIRED, ParquetType::BYTE_ARRAY, LogicalType::UTF8);
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
    parquet_fields.push_back(GroupNode::Make(
        "array_of_arrays", Repetition::OPTIONAL, {list}, LogicalType::LIST));
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
    auto element = PrimitiveNode::Make(
        "str", Repetition::REQUIRED, ParquetType::BYTE_ARRAY, LogicalType::UTF8);
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
    auto str_element = PrimitiveNode::Make(
        "str", Repetition::REQUIRED, ParquetType::BYTE_ARRAY, LogicalType::UTF8);
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
    auto element = PrimitiveNode::Make(
        "str", Repetition::REQUIRED, ParquetType::BYTE_ARRAY, LogicalType::UTF8);
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
    auto element = PrimitiveNode::Make(
        "str", Repetition::REQUIRED, ParquetType::BYTE_ARRAY, LogicalType::UTF8);
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

  CheckFlatSchema(arrow_schema);
}

TEST_F(TestConvertParquetSchema, UnsupportedThings) {
  std::vector<NodePtr> unsupported_nodes;

  unsupported_nodes.push_back(
      PrimitiveNode::Make("int96", Repetition::REQUIRED, ParquetType::INT96));

  unsupported_nodes.push_back(PrimitiveNode::Make(
      "int32", Repetition::OPTIONAL, ParquetType::INT32, LogicalType::DATE));

  for (const NodePtr& node : unsupported_nodes) {
    ASSERT_RAISES(NotImplemented, ConvertSchema({node}));
  }
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

  parquet_fields.push_back(PrimitiveNode::Make("timestamp", Repetition::REQUIRED,
      ParquetType::INT64, LogicalType::TIMESTAMP_MILLIS));
  arrow_fields.push_back(std::make_shared<Field>("timestamp", TIMESTAMP_MS, false));

  // parquet_fields.push_back(PrimitiveNode::Make("timestamp", Repetition::REQUIRED,
  //     ParquetType::INT64, LogicalType::TIMESTAMP_MICROS));
  // arrow_fields.push_back(std::make_shared<Field>("timestamp", TIMESTAMP_US, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("float", Repetition::OPTIONAL, ParquetType::FLOAT));
  arrow_fields.push_back(std::make_shared<Field>("float", FLOAT));

  parquet_fields.push_back(
      PrimitiveNode::Make("double", Repetition::OPTIONAL, ParquetType::DOUBLE));
  arrow_fields.push_back(std::make_shared<Field>("double", DOUBLE));

  // TODO: String types need to be clarified a bit more in the Arrow spec
  parquet_fields.push_back(PrimitiveNode::Make(
      "string", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, LogicalType::UTF8));
  arrow_fields.push_back(std::make_shared<Field>("string", UTF8));

  ASSERT_OK(ConvertSchema(arrow_fields));

  CheckFlatSchema(parquet_fields);
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
    auto element = PrimitiveNode::Make(
        "string", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, LogicalType::UTF8);
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
    auto element = PrimitiveNode::Make(
        "string", Repetition::REQUIRED, ParquetType::BYTE_ARRAY, LogicalType::UTF8);
    auto list = GroupNode::Make("list", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {list}, LogicalType::LIST));
    auto arrow_element = std::make_shared<Field>("string", UTF8, false);
    auto arrow_list = std::make_shared<::arrow::ListType>(arrow_element);
    arrow_fields.push_back(std::make_shared<Field>("my_list", arrow_list, true));
  }

  ASSERT_OK(ConvertSchema(arrow_fields));

  CheckFlatSchema(parquet_fields);
}

TEST_F(TestConvertArrowSchema, ParquetFlatDecimals) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  // TODO: Test Decimal Arrow -> Parquet conversion

  ASSERT_OK(ConvertSchema(arrow_fields));

  CheckFlatSchema(parquet_fields);
}

TEST(TestNodeConversion, DateAndTime) {}

}  // namespace arrow

}  // namespace parquet
