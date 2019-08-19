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

#include "parquet/arrow/reader.h"
#include "parquet/arrow/reader_internal.h"
#include "parquet/arrow/schema.h"
#include "parquet/file_reader.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"

#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"

using arrow::ArrayFromVector;
using arrow::Field;
using arrow::TimeUnit;

using ParquetType = parquet::Type;
using parquet::ConvertedType;
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
      auto result_field = result_schema_->field(i);
      auto expected_field = expected_schema->field(i);
      EXPECT_TRUE(result_field->Equals(expected_field))
          << "Field " << i << "\n  result: " << result_field->ToString()
          << "\n  expected: " << expected_field->ToString();
    }
  }

  ::arrow::Status ConvertSchema(
      const std::vector<NodePtr>& nodes,
      const std::shared_ptr<const KeyValueMetadata>& key_value_metadata = nullptr) {
    NodePtr schema = GroupNode::Make("schema", Repetition::REPEATED, nodes);
    descr_.Init(schema);
    ArrowReaderProperties props;
    return FromParquetSchema(&descr_, props, key_value_metadata, &result_schema_);
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
  arrow_fields.push_back(::arrow::field("boolean", BOOL, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int32", Repetition::REQUIRED, ParquetType::INT32));
  arrow_fields.push_back(::arrow::field("int32", INT32, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int64", Repetition::REQUIRED, ParquetType::INT64));
  arrow_fields.push_back(::arrow::field("int64", INT64, false));

  parquet_fields.push_back(PrimitiveNode::Make("timestamp", Repetition::REQUIRED,
                                               ParquetType::INT64,
                                               ConvertedType::TIMESTAMP_MILLIS));
  arrow_fields.push_back(
      ::arrow::field("timestamp", ::arrow::timestamp(TimeUnit::MILLI), false));

  parquet_fields.push_back(PrimitiveNode::Make("timestamp[us]", Repetition::REQUIRED,
                                               ParquetType::INT64,
                                               ConvertedType::TIMESTAMP_MICROS));
  arrow_fields.push_back(
      ::arrow::field("timestamp[us]", ::arrow::timestamp(TimeUnit::MICRO), false));

  parquet_fields.push_back(PrimitiveNode::Make("date", Repetition::REQUIRED,
                                               ParquetType::INT32, ConvertedType::DATE));
  arrow_fields.push_back(::arrow::field("date", ::arrow::date32(), false));

  parquet_fields.push_back(PrimitiveNode::Make(
      "time32", Repetition::REQUIRED, ParquetType::INT32, ConvertedType::TIME_MILLIS));
  arrow_fields.push_back(
      ::arrow::field("time32", ::arrow::time32(TimeUnit::MILLI), false));

  parquet_fields.push_back(PrimitiveNode::Make(
      "time64", Repetition::REQUIRED, ParquetType::INT64, ConvertedType::TIME_MICROS));
  arrow_fields.push_back(
      ::arrow::field("time64", ::arrow::time64(TimeUnit::MICRO), false));

  parquet_fields.push_back(
      PrimitiveNode::Make("timestamp96", Repetition::REQUIRED, ParquetType::INT96));
  arrow_fields.push_back(::arrow::field("timestamp96", TIMESTAMP_NS, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("float", Repetition::OPTIONAL, ParquetType::FLOAT));
  arrow_fields.push_back(::arrow::field("float", FLOAT));

  parquet_fields.push_back(
      PrimitiveNode::Make("double", Repetition::OPTIONAL, ParquetType::DOUBLE));
  arrow_fields.push_back(::arrow::field("double", DOUBLE));

  parquet_fields.push_back(
      PrimitiveNode::Make("binary", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY));
  arrow_fields.push_back(::arrow::field("binary", BINARY));

  parquet_fields.push_back(PrimitiveNode::Make(
      "string", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, ConvertedType::UTF8));
  arrow_fields.push_back(::arrow::field("string", UTF8));

  parquet_fields.push_back(PrimitiveNode::Make("flba-binary", Repetition::OPTIONAL,
                                               ParquetType::FIXED_LEN_BYTE_ARRAY,
                                               ConvertedType::NONE, 12));
  arrow_fields.push_back(::arrow::field("flba-binary", ::arrow::fixed_size_binary(12)));

  auto arrow_schema = ::arrow::schema(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(arrow_schema));
}

TEST_F(TestConvertParquetSchema, ParquetAnnotatedFields) {
  struct FieldConstructionArguments {
    std::string name;
    std::shared_ptr<const LogicalType> logical_type;
    parquet::Type::type physical_type;
    int physical_length;
    std::shared_ptr<::arrow::DataType> datatype;
  };

  std::vector<FieldConstructionArguments> cases = {
      {"string", LogicalType::String(), ParquetType::BYTE_ARRAY, -1, ::arrow::utf8()},
      {"enum", LogicalType::Enum(), ParquetType::BYTE_ARRAY, -1, ::arrow::binary()},
      {"decimal(8, 2)", LogicalType::Decimal(8, 2), ParquetType::INT32, -1,
       ::arrow::decimal(8, 2)},
      {"decimal(16, 4)", LogicalType::Decimal(16, 4), ParquetType::INT64, -1,
       ::arrow::decimal(16, 4)},
      {"decimal(32, 8)", LogicalType::Decimal(32, 8), ParquetType::FIXED_LEN_BYTE_ARRAY,
       16, ::arrow::decimal(32, 8)},
      {"date", LogicalType::Date(), ParquetType::INT32, -1, ::arrow::date32()},
      {"time(ms)", LogicalType::Time(true, LogicalType::TimeUnit::MILLIS),
       ParquetType::INT32, -1, ::arrow::time32(::arrow::TimeUnit::MILLI)},
      {"time(us)", LogicalType::Time(true, LogicalType::TimeUnit::MICROS),
       ParquetType::INT64, -1, ::arrow::time64(::arrow::TimeUnit::MICRO)},
      {"time(ns)", LogicalType::Time(true, LogicalType::TimeUnit::NANOS),
       ParquetType::INT64, -1, ::arrow::time64(::arrow::TimeUnit::NANO)},
      {"time(ms)", LogicalType::Time(false, LogicalType::TimeUnit::MILLIS),
       ParquetType::INT32, -1, ::arrow::time32(::arrow::TimeUnit::MILLI)},
      {"time(us)", LogicalType::Time(false, LogicalType::TimeUnit::MICROS),
       ParquetType::INT64, -1, ::arrow::time64(::arrow::TimeUnit::MICRO)},
      {"time(ns)", LogicalType::Time(false, LogicalType::TimeUnit::NANOS),
       ParquetType::INT64, -1, ::arrow::time64(::arrow::TimeUnit::NANO)},
      {"timestamp(true, ms)", LogicalType::Timestamp(true, LogicalType::TimeUnit::MILLIS),
       ParquetType::INT64, -1, ::arrow::timestamp(::arrow::TimeUnit::MILLI, "UTC")},
      {"timestamp(true, us)", LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS),
       ParquetType::INT64, -1, ::arrow::timestamp(::arrow::TimeUnit::MICRO, "UTC")},
      {"timestamp(true, ns)", LogicalType::Timestamp(true, LogicalType::TimeUnit::NANOS),
       ParquetType::INT64, -1, ::arrow::timestamp(::arrow::TimeUnit::NANO, "UTC")},
      {"timestamp(false, ms)",
       LogicalType::Timestamp(false, LogicalType::TimeUnit::MILLIS), ParquetType::INT64,
       -1, ::arrow::timestamp(::arrow::TimeUnit::MILLI)},
      {"timestamp(false, us)",
       LogicalType::Timestamp(false, LogicalType::TimeUnit::MICROS), ParquetType::INT64,
       -1, ::arrow::timestamp(::arrow::TimeUnit::MICRO)},
      {"timestamp(false, ns)",
       LogicalType::Timestamp(false, LogicalType::TimeUnit::NANOS), ParquetType::INT64,
       -1, ::arrow::timestamp(::arrow::TimeUnit::NANO)},
      {"int(8, false)", LogicalType::Int(8, false), ParquetType::INT32, -1,
       ::arrow::uint8()},
      {"int(8, true)", LogicalType::Int(8, true), ParquetType::INT32, -1,
       ::arrow::int8()},
      {"int(16, false)", LogicalType::Int(16, false), ParquetType::INT32, -1,
       ::arrow::uint16()},
      {"int(16, true)", LogicalType::Int(16, true), ParquetType::INT32, -1,
       ::arrow::int16()},
      {"int(32, false)", LogicalType::Int(32, false), ParquetType::INT32, -1,
       ::arrow::uint32()},
      {"int(32, true)", LogicalType::Int(32, true), ParquetType::INT32, -1,
       ::arrow::int32()},
      {"int(64, false)", LogicalType::Int(64, false), ParquetType::INT64, -1,
       ::arrow::uint64()},
      {"int(64, true)", LogicalType::Int(64, true), ParquetType::INT64, -1,
       ::arrow::int64()},
      {"json", LogicalType::JSON(), ParquetType::BYTE_ARRAY, -1, ::arrow::binary()},
      {"bson", LogicalType::BSON(), ParquetType::BYTE_ARRAY, -1, ::arrow::binary()},
      {"interval", LogicalType::Interval(), ParquetType::FIXED_LEN_BYTE_ARRAY, 12,
       ::arrow::fixed_size_binary(12)},
      {"uuid", LogicalType::UUID(), ParquetType::FIXED_LEN_BYTE_ARRAY, 16,
       ::arrow::fixed_size_binary(16)},
      {"none", LogicalType::None(), ParquetType::BOOLEAN, -1, ::arrow::boolean()},
      {"none", LogicalType::None(), ParquetType::INT32, -1, ::arrow::int32()},
      {"none", LogicalType::None(), ParquetType::INT64, -1, ::arrow::int64()},
      {"none", LogicalType::None(), ParquetType::FLOAT, -1, ::arrow::float32()},
      {"none", LogicalType::None(), ParquetType::DOUBLE, -1, ::arrow::float64()},
      {"none", LogicalType::None(), ParquetType::BYTE_ARRAY, -1, ::arrow::binary()},
      {"none", LogicalType::None(), ParquetType::FIXED_LEN_BYTE_ARRAY, 64,
       ::arrow::fixed_size_binary(64)},
      {"null", LogicalType::Null(), ParquetType::BYTE_ARRAY, -1, ::arrow::null()},
  };

  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  for (const FieldConstructionArguments& c : cases) {
    parquet_fields.push_back(PrimitiveNode::Make(c.name, Repetition::OPTIONAL,
                                                 c.logical_type, c.physical_type,
                                                 c.physical_length));
    arrow_fields.push_back(::arrow::field(c.name, c.datatype));
  }

  ASSERT_OK(ConvertSchema(parquet_fields));
  auto arrow_schema = ::arrow::schema(arrow_fields);
  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(arrow_schema));
}

TEST_F(TestConvertParquetSchema, DuplicateFieldNames) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  parquet_fields.push_back(
      PrimitiveNode::Make("xxx", Repetition::REQUIRED, ParquetType::BOOLEAN));
  auto arrow_field1 = ::arrow::field("xxx", BOOL, false);

  parquet_fields.push_back(
      PrimitiveNode::Make("xxx", Repetition::REQUIRED, ParquetType::INT32));
  auto arrow_field2 = ::arrow::field("xxx", INT32, false);

  ASSERT_OK(ConvertSchema(parquet_fields));
  arrow_fields = {arrow_field1, arrow_field2};
  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(::arrow::schema(arrow_fields)));
}

TEST_F(TestConvertParquetSchema, ParquetKeyValueMetadata) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;

  parquet_fields.push_back(
      PrimitiveNode::Make("boolean", Repetition::REQUIRED, ParquetType::BOOLEAN));
  arrow_fields.push_back(::arrow::field("boolean", BOOL, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int32", Repetition::REQUIRED, ParquetType::INT32));
  arrow_fields.push_back(::arrow::field("int32", INT32, false));

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
  arrow_fields.push_back(::arrow::field("int32", INT32, false));

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
                                               ConvertedType::DECIMAL, 4, 8, 4));
  arrow_fields.push_back(::arrow::field("flba-decimal", DECIMAL_8_4));

  parquet_fields.push_back(PrimitiveNode::Make("binary-decimal", Repetition::OPTIONAL,
                                               ParquetType::BYTE_ARRAY,
                                               ConvertedType::DECIMAL, -1, 8, 4));
  arrow_fields.push_back(::arrow::field("binary-decimal", DECIMAL_8_4));

  parquet_fields.push_back(PrimitiveNode::Make("int32-decimal", Repetition::OPTIONAL,
                                               ParquetType::INT32, ConvertedType::DECIMAL,
                                               -1, 8, 4));
  arrow_fields.push_back(::arrow::field("int32-decimal", DECIMAL_8_4));

  parquet_fields.push_back(PrimitiveNode::Make("int64-decimal", Repetition::OPTIONAL,
                                               ParquetType::INT64, ConvertedType::DECIMAL,
                                               -1, 8, 4));
  arrow_fields.push_back(::arrow::field("int64-decimal", DECIMAL_8_4));

  auto arrow_schema = ::arrow::schema(arrow_fields);
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
                                       ParquetType::BYTE_ARRAY, ConvertedType::UTF8);
    auto list = GroupNode::Make("list", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::REQUIRED, {list}, ConvertedType::LIST));
    auto arrow_element = ::arrow::field("string", UTF8, true);
    auto arrow_list = ::arrow::list(arrow_element);
    arrow_fields.push_back(::arrow::field("my_list", arrow_list, false));
  }

  // // List<String> (list nullable, elements non-null)
  // optional group my_list (LIST) {
  //   repeated group list {
  //     required binary element (UTF8);
  //   }
  // }
  {
    auto element = PrimitiveNode::Make("string", Repetition::REQUIRED,
                                       ParquetType::BYTE_ARRAY, ConvertedType::UTF8);
    auto list = GroupNode::Make("list", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {list}, ConvertedType::LIST));
    auto arrow_element = ::arrow::field("string", UTF8, false);
    auto arrow_list = ::arrow::list(arrow_element);
    arrow_fields.push_back(::arrow::field("my_list", arrow_list, true));
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
    auto element = GroupNode::Make("element", Repetition::REQUIRED, {inner_list},
                                   ConvertedType::LIST);
    auto list = GroupNode::Make("list", Repetition::REPEATED, {element});
    parquet_fields.push_back(GroupNode::Make("array_of_arrays", Repetition::OPTIONAL,
                                             {list}, ConvertedType::LIST));
    auto arrow_inner_element = ::arrow::field("int32", INT32, false);
    auto arrow_inner_list = ::arrow::list(arrow_inner_element);
    auto arrow_element = ::arrow::field("element", arrow_inner_list, false);
    auto arrow_list = ::arrow::list(arrow_element);
    arrow_fields.push_back(::arrow::field("array_of_arrays", arrow_list, true));
  }

  // // List<String> (list nullable, elements non-null)
  // optional group my_list (LIST) {
  //   repeated group element {
  //     required binary str (UTF8);
  //   };
  // }
  {
    auto element = PrimitiveNode::Make("str", Repetition::REQUIRED,
                                       ParquetType::BYTE_ARRAY, ConvertedType::UTF8);
    auto list = GroupNode::Make("element", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {list}, ConvertedType::LIST));
    auto arrow_element = ::arrow::field("str", UTF8, false);
    auto arrow_list = ::arrow::list(arrow_element);
    arrow_fields.push_back(::arrow::field("my_list", arrow_list, true));
  }

  // // List<Integer> (nullable list, non-null elements)
  // optional group my_list (LIST) {
  //   repeated int32 element;
  // }
  {
    auto element =
        PrimitiveNode::Make("element", Repetition::REPEATED, ParquetType::INT32);
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {element}, ConvertedType::LIST));
    auto arrow_element = ::arrow::field("element", INT32, false);
    auto arrow_list = ::arrow::list(arrow_element);
    arrow_fields.push_back(::arrow::field("my_list", arrow_list, true));
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
                                           ParquetType::BYTE_ARRAY, ConvertedType::UTF8);
    auto num_element =
        PrimitiveNode::Make("num", Repetition::REQUIRED, ParquetType::INT32);
    auto element =
        GroupNode::Make("element", Repetition::REPEATED, {str_element, num_element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {element}, ConvertedType::LIST));
    auto arrow_str = ::arrow::field("str", UTF8, false);
    auto arrow_num = ::arrow::field("num", INT32, false);
    std::vector<std::shared_ptr<Field>> fields({arrow_str, arrow_num});
    auto arrow_struct = ::arrow::struct_(fields);
    auto arrow_element = ::arrow::field("element", arrow_struct, false);
    auto arrow_list = ::arrow::list(arrow_element);
    arrow_fields.push_back(::arrow::field("my_list", arrow_list, true));
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
                                       ParquetType::BYTE_ARRAY, ConvertedType::UTF8);
    auto array = GroupNode::Make("array", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {array}, ConvertedType::LIST));
    auto arrow_str = ::arrow::field("str", UTF8, false);
    std::vector<std::shared_ptr<Field>> fields({arrow_str});
    auto arrow_struct = ::arrow::struct_(fields);
    auto arrow_element = ::arrow::field("array", arrow_struct, false);
    auto arrow_list = ::arrow::list(arrow_element);
    arrow_fields.push_back(::arrow::field("my_list", arrow_list, true));
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
                                       ParquetType::BYTE_ARRAY, ConvertedType::UTF8);
    auto array = GroupNode::Make("my_list_tuple", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {array}, ConvertedType::LIST));
    auto arrow_str = ::arrow::field("str", UTF8, false);
    std::vector<std::shared_ptr<Field>> fields({arrow_str});
    auto arrow_struct = ::arrow::struct_(fields);
    auto arrow_element = ::arrow::field("my_list_tuple", arrow_struct, false);
    auto arrow_list = ::arrow::list(arrow_element);
    arrow_fields.push_back(::arrow::field("my_list", arrow_list, true));
  }

  // One-level encoding: Only allows required lists with required cells
  //   repeated value_type name
  {
    parquet_fields.push_back(
        PrimitiveNode::Make("name", Repetition::REPEATED, ParquetType::INT32));
    auto arrow_element = ::arrow::field("name", INT32, false);
    auto arrow_list = ::arrow::list(arrow_element);
    arrow_fields.push_back(::arrow::field("name", arrow_list, false));
  }

  auto arrow_schema = ::arrow::schema(arrow_fields);
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

    auto group1_fields = {::arrow::field("leaf1", BOOL, false),
                          ::arrow::field("leaf2", INT32, false)};
    auto arrow_group1_type = ::arrow::struct_(group1_fields);
    arrow_fields.push_back(::arrow::field("group1", arrow_group1_type, false));
    arrow_fields.push_back(::arrow::field("leaf3", INT64, false));
  }

  auto arrow_schema = ::arrow::schema(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(arrow_schema));
}

TEST_F(TestConvertParquetSchema, ParquetNestedSchema2) {
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

    auto group1_fields = {::arrow::field("leaf1", INT64, false),
                          ::arrow::field("leaf2", INT64, false)};
    auto arrow_group1_type = ::arrow::struct_(group1_fields);
    auto group2_fields = {::arrow::field("leaf3", INT64, false),
                          ::arrow::field("leaf4", INT64, false)};
    auto arrow_group2_type = ::arrow::struct_(group2_fields);
    arrow_fields.push_back(::arrow::field("group1", arrow_group1_type, false));
    arrow_fields.push_back(::arrow::field("group2", arrow_group2_type, false));
    arrow_fields.push_back(::arrow::field("leaf5", INT64, false));
  }

  auto arrow_schema = ::arrow::schema(arrow_fields);
  ASSERT_OK(ConvertSchema(parquet_fields));
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

    auto inner_group_fields = {::arrow::field("leaf3", INT32, true)};
    auto inner_group_type = ::arrow::struct_(inner_group_fields);
    auto outer_group_fields = {
        ::arrow::field("leaf2", INT32, true),
        ::arrow::field(
            "innerGroup",
            ::arrow::list(::arrow::field("innerGroup", inner_group_type, false)), false)};
    auto outer_group_type = ::arrow::struct_(outer_group_fields);

    arrow_fields.push_back(::arrow::field("leaf1", INT32, true));
    arrow_fields.push_back(::arrow::field(
        "outerGroup",
        ::arrow::list(::arrow::field("outerGroup", outer_group_type, false)), false));
  }
  auto arrow_schema = ::arrow::schema(arrow_fields);
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
    arrow_schema_ = ::arrow::schema(fields);
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
  arrow_fields.push_back(::arrow::field("boolean", BOOL, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int32", Repetition::REQUIRED, ParquetType::INT32));
  arrow_fields.push_back(::arrow::field("int32", INT32, false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int64", Repetition::REQUIRED, ParquetType::INT64));
  arrow_fields.push_back(::arrow::field("int64", INT64, false));

  parquet_fields.push_back(PrimitiveNode::Make("date", Repetition::REQUIRED,
                                               ParquetType::INT32, ConvertedType::DATE));
  arrow_fields.push_back(::arrow::field("date", ::arrow::date32(), false));

  parquet_fields.push_back(PrimitiveNode::Make("date64", Repetition::REQUIRED,
                                               ParquetType::INT32, ConvertedType::DATE));
  arrow_fields.push_back(::arrow::field("date64", ::arrow::date64(), false));

  parquet_fields.push_back(PrimitiveNode::Make("timestamp", Repetition::REQUIRED,
                                               ParquetType::INT64,
                                               ConvertedType::TIMESTAMP_MILLIS));
  arrow_fields.push_back(
      ::arrow::field("timestamp", ::arrow::timestamp(TimeUnit::MILLI, "UTC"), false));

  parquet_fields.push_back(PrimitiveNode::Make("timestamp[us]", Repetition::REQUIRED,
                                               ParquetType::INT64,
                                               ConvertedType::TIMESTAMP_MICROS));
  arrow_fields.push_back(
      ::arrow::field("timestamp[us]", ::arrow::timestamp(TimeUnit::MICRO, "UTC"), false));

  parquet_fields.push_back(
      PrimitiveNode::Make("float", Repetition::OPTIONAL, ParquetType::FLOAT));
  arrow_fields.push_back(::arrow::field("float", FLOAT));

  parquet_fields.push_back(
      PrimitiveNode::Make("double", Repetition::OPTIONAL, ParquetType::DOUBLE));
  arrow_fields.push_back(::arrow::field("double", DOUBLE));

  parquet_fields.push_back(PrimitiveNode::Make(
      "string", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, ConvertedType::UTF8));
  arrow_fields.push_back(::arrow::field("string", UTF8));

  parquet_fields.push_back(PrimitiveNode::Make(
      "binary", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, ConvertedType::NONE));
  arrow_fields.push_back(::arrow::field("binary", BINARY));

  ASSERT_OK(ConvertSchema(arrow_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(parquet_fields));
}

TEST_F(TestConvertArrowSchema, ArrowFields) {
  struct FieldConstructionArguments {
    std::string name;
    std::shared_ptr<::arrow::DataType> datatype;
    std::shared_ptr<const LogicalType> logical_type;
    parquet::Type::type physical_type;
    int physical_length;
  };

  std::vector<FieldConstructionArguments> cases = {
      {"boolean", ::arrow::boolean(), LogicalType::None(), ParquetType::BOOLEAN, -1},
      {"binary", ::arrow::binary(), LogicalType::None(), ParquetType::BYTE_ARRAY, -1},
      {"fixed_size_binary", ::arrow::fixed_size_binary(64), LogicalType::None(),
       ParquetType::FIXED_LEN_BYTE_ARRAY, 64},
      {"uint8", ::arrow::uint8(), LogicalType::Int(8, false), ParquetType::INT32, -1},
      {"int8", ::arrow::int8(), LogicalType::Int(8, true), ParquetType::INT32, -1},
      {"uint16", ::arrow::uint16(), LogicalType::Int(16, false), ParquetType::INT32, -1},
      {"int16", ::arrow::int16(), LogicalType::Int(16, true), ParquetType::INT32, -1},
      {"uint32", ::arrow::uint32(), LogicalType::None(), ParquetType::INT64,
       -1},  // Parquet 1.0
      {"int32", ::arrow::int32(), LogicalType::None(), ParquetType::INT32, -1},
      {"uint64", ::arrow::uint64(), LogicalType::Int(64, false), ParquetType::INT64, -1},
      {"int64", ::arrow::int64(), LogicalType::None(), ParquetType::INT64, -1},
      {"float32", ::arrow::float32(), LogicalType::None(), ParquetType::FLOAT, -1},
      {"float64", ::arrow::float64(), LogicalType::None(), ParquetType::DOUBLE, -1},
      {"utf8", ::arrow::utf8(), LogicalType::String(), ParquetType::BYTE_ARRAY, -1},
      {"decimal(1, 0)", ::arrow::decimal(1, 0), LogicalType::Decimal(1, 0),
       ParquetType::FIXED_LEN_BYTE_ARRAY, 1},
      {"decimal(8, 2)", ::arrow::decimal(8, 2), LogicalType::Decimal(8, 2),
       ParquetType::FIXED_LEN_BYTE_ARRAY, 4},
      {"decimal(16, 4)", ::arrow::decimal(16, 4), LogicalType::Decimal(16, 4),
       ParquetType::FIXED_LEN_BYTE_ARRAY, 7},
      {"decimal(32, 8)", ::arrow::decimal(32, 8), LogicalType::Decimal(32, 8),
       ParquetType::FIXED_LEN_BYTE_ARRAY, 14},
      {"time32", ::arrow::time32(::arrow::TimeUnit::MILLI),
       LogicalType::Time(true, LogicalType::TimeUnit::MILLIS), ParquetType::INT32, -1},
      {"time64(microsecond)", ::arrow::time64(::arrow::TimeUnit::MICRO),
       LogicalType::Time(true, LogicalType::TimeUnit::MICROS), ParquetType::INT64, -1},
      {"time64(nanosecond)", ::arrow::time64(::arrow::TimeUnit::NANO),
       LogicalType::Time(true, LogicalType::TimeUnit::NANOS), ParquetType::INT64, -1},
      {"timestamp(millisecond)", ::arrow::timestamp(::arrow::TimeUnit::MILLI),
       LogicalType::Timestamp(false, LogicalType::TimeUnit::MILLIS,
                              /*is_from_converted_type=*/false,
                              /*force_set_converted_type=*/true),
       ParquetType::INT64, -1},
      {"timestamp(microsecond)", ::arrow::timestamp(::arrow::TimeUnit::MICRO),
       LogicalType::Timestamp(false, LogicalType::TimeUnit::MICROS,
                              /*is_from_converted_type=*/false,
                              /*force_set_converted_type=*/true),
       ParquetType::INT64, -1},
      // Parquet v1, values converted to microseconds
      {"timestamp(nanosecond)", ::arrow::timestamp(::arrow::TimeUnit::NANO),
       LogicalType::Timestamp(false, LogicalType::TimeUnit::MICROS,
                              /*is_from_converted_type=*/false,
                              /*force_set_converted_type=*/true),
       ParquetType::INT64, -1},
      {"timestamp(millisecond, UTC)", ::arrow::timestamp(::arrow::TimeUnit::MILLI, "UTC"),
       LogicalType::Timestamp(true, LogicalType::TimeUnit::MILLIS), ParquetType::INT64,
       -1},
      {"timestamp(microsecond, UTC)", ::arrow::timestamp(::arrow::TimeUnit::MICRO, "UTC"),
       LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS), ParquetType::INT64,
       -1},
      {"timestamp(nanosecond, UTC)", ::arrow::timestamp(::arrow::TimeUnit::NANO, "UTC"),
       LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS), ParquetType::INT64,
       -1},
      {"timestamp(millisecond, CET)", ::arrow::timestamp(::arrow::TimeUnit::MILLI, "CET"),
       LogicalType::Timestamp(true, LogicalType::TimeUnit::MILLIS), ParquetType::INT64,
       -1},
      {"timestamp(microsecond, CET)", ::arrow::timestamp(::arrow::TimeUnit::MICRO, "CET"),
       LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS), ParquetType::INT64,
       -1},
      {"timestamp(nanosecond, CET)", ::arrow::timestamp(::arrow::TimeUnit::NANO, "CET"),
       LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS), ParquetType::INT64,
       -1},
      {"null", ::arrow::null(), LogicalType::Null(), ParquetType::INT32, -1}};

  std::vector<std::shared_ptr<Field>> arrow_fields;
  std::vector<NodePtr> parquet_fields;

  for (const FieldConstructionArguments& c : cases) {
    arrow_fields.push_back(::arrow::field(c.name, c.datatype, false));
    parquet_fields.push_back(PrimitiveNode::Make(c.name, Repetition::REQUIRED,
                                                 c.logical_type, c.physical_type,
                                                 c.physical_length));
  }

  ASSERT_OK(ConvertSchema(arrow_fields));
  CheckFlatSchema(parquet_fields);
  // ASSERT_NO_FATAL_FAILURE();
}

TEST_F(TestConvertArrowSchema, ArrowNonconvertibleFields) {
  struct FieldConstructionArguments {
    std::string name;
    std::shared_ptr<::arrow::DataType> datatype;
  };

  std::vector<FieldConstructionArguments> cases = {
      {"float16", ::arrow::float16()},
  };

  for (const FieldConstructionArguments& c : cases) {
    auto field = ::arrow::field(c.name, c.datatype);
    ASSERT_RAISES(NotImplemented, ConvertSchema({field}));
  }
}

TEST_F(TestConvertArrowSchema, ParquetFlatPrimitivesAsDictionaries) {
  std::vector<NodePtr> parquet_fields;
  std::vector<std::shared_ptr<Field>> arrow_fields;
  std::shared_ptr<::arrow::Array> dict;

  parquet_fields.push_back(
      PrimitiveNode::Make("int32", Repetition::REQUIRED, ParquetType::INT32));
  arrow_fields.push_back(::arrow::field(
      "int32", ::arrow::dictionary(::arrow::int8(), ::arrow::int32()), false));

  parquet_fields.push_back(
      PrimitiveNode::Make("int64", Repetition::REQUIRED, ParquetType::INT64));
  arrow_fields.push_back(::arrow::field(
      "int64", ::arrow::dictionary(::arrow::int8(), ::arrow::int64()), false));

  parquet_fields.push_back(PrimitiveNode::Make("date", Repetition::REQUIRED,
                                               ParquetType::INT32, ConvertedType::DATE));
  arrow_fields.push_back(::arrow::field(
      "date", ::arrow::dictionary(::arrow::int8(), ::arrow::date32()), false));

  parquet_fields.push_back(PrimitiveNode::Make("date64", Repetition::REQUIRED,
                                               ParquetType::INT32, ConvertedType::DATE));
  arrow_fields.push_back(::arrow::field(
      "date64", ::arrow::dictionary(::arrow::int8(), ::arrow::date64()), false));

  parquet_fields.push_back(
      PrimitiveNode::Make("float", Repetition::OPTIONAL, ParquetType::FLOAT));
  arrow_fields.push_back(
      ::arrow::field("float", ::arrow::dictionary(::arrow::int8(), ::arrow::float32())));

  parquet_fields.push_back(
      PrimitiveNode::Make("double", Repetition::OPTIONAL, ParquetType::DOUBLE));
  arrow_fields.push_back(
      ::arrow::field("double", ::arrow::dictionary(::arrow::int8(), ::arrow::float64())));

  parquet_fields.push_back(PrimitiveNode::Make(
      "string", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, ConvertedType::UTF8));
  arrow_fields.push_back(
      ::arrow::field("string", ::arrow::dictionary(::arrow::int8(), ::arrow::utf8())));

  parquet_fields.push_back(PrimitiveNode::Make(
      "binary", Repetition::OPTIONAL, ParquetType::BYTE_ARRAY, ConvertedType::NONE));
  arrow_fields.push_back(
      ::arrow::field("binary", ::arrow::dictionary(::arrow::int8(), ::arrow::binary())));

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
                                       ParquetType::BYTE_ARRAY, ConvertedType::UTF8);
    auto list = GroupNode::Make("list", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::REQUIRED, {list}, ConvertedType::LIST));
    auto arrow_element = ::arrow::field("string", UTF8, true);
    auto arrow_list = ::arrow::list(arrow_element);
    arrow_fields.push_back(::arrow::field("my_list", arrow_list, false));
  }

  // // List<String> (list nullable, elements non-null)
  // optional group my_list (LIST) {
  //   repeated group list {
  //     required binary element (UTF8);
  //   }
  // }
  {
    auto element = PrimitiveNode::Make("string", Repetition::REQUIRED,
                                       ParquetType::BYTE_ARRAY, ConvertedType::UTF8);
    auto list = GroupNode::Make("list", Repetition::REPEATED, {element});
    parquet_fields.push_back(
        GroupNode::Make("my_list", Repetition::OPTIONAL, {list}, ConvertedType::LIST));
    auto arrow_element = ::arrow::field("string", UTF8, false);
    auto arrow_list = ::arrow::list(arrow_element);
    arrow_fields.push_back(::arrow::field("my_list", arrow_list, true));
  }

  ASSERT_OK(ConvertSchema(arrow_fields));

  ASSERT_NO_FATAL_FAILURE(CheckFlatSchema(parquet_fields));
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

TEST(TestFromParquetSchema, CorruptMetadata) {
  // PARQUET-1565: ensure that an IOError is returned when the parquet file contains
  // corrupted metadata.
  auto path = test::get_data_file("PARQUET-1481.parquet", /*is_good=*/false);

  std::unique_ptr<parquet::ParquetFileReader> reader =
      parquet::ParquetFileReader::OpenFile(path);
  const auto parquet_schema = reader->metadata()->schema();
  std::shared_ptr<::arrow::Schema> arrow_schema;
  ArrowReaderProperties props;
  ASSERT_RAISES(IOError, FromParquetSchema(parquet_schema, props, &arrow_schema));
}

}  // namespace arrow
}  // namespace parquet
