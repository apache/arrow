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

#include <gtest/gtest.h>

#include <string>

#include "parquet/exception.h"
#include "parquet/test_util.h"
#include "parquet/variant.h"

#include <arrow/filesystem/localfs.h>
#include <arrow/testing/gtest_util.h>

namespace parquet::variant {

TEST(ParquetVariant, MetadataBase) {
  std::string dir_string(parquet::test::get_variant_dir());
  auto file_system = std::make_shared<::arrow::fs::LocalFileSystem>();
  std::vector<std::string> primitive_metadatas = {
      // "primitive_null.metadata",
      "primitive_boolean_true.metadata", "primitive_boolean_false.metadata",
      "primitive_date.metadata",         "primitive_decimal4.metadata",
      "primitive_decimal8.metadata",     "primitive_decimal16.metadata",
      "primitive_float.metadata",        "primitive_double.metadata",
      "primitive_int8.metadata",         "primitive_int16.metadata",
      "primitive_int32.metadata",        "primitive_int64.metadata",
      "primitive_binary.metadata",       "primitive_string.metadata",
  };
  for (auto& test_file : primitive_metadatas) {
    ARROW_SCOPED_TRACE("Testing file: " + test_file);
    std::string path = dir_string + "/" + test_file;
    ASSERT_OK_AND_ASSIGN(auto file, file_system->OpenInputFile(path));
    ASSERT_OK_AND_ASSIGN(auto file_size, file->GetSize());
    ASSERT_OK_AND_ASSIGN(auto buf, file->Read(file_size));

    VariantMetadata metadata(std::string_view{*buf});
    EXPECT_EQ(1, metadata.version());
    EXPECT_THROW(metadata.getMetadataKey(0), ParquetException);
  }

  {
    std::string object_metadata = {"object_primitive.metadata"};
    ARROW_SCOPED_TRACE("Testing file: " + object_metadata);
    std::string path = dir_string + "/" + object_metadata;
    ASSERT_OK_AND_ASSIGN(auto file, file_system->OpenInputFile(path));
    ASSERT_OK_AND_ASSIGN(auto file_size, file->GetSize());
    ASSERT_OK_AND_ASSIGN(auto buf, file->Read(file_size));

    VariantMetadata metadata(std::string_view{*buf});
    EXPECT_EQ("int_field", metadata.getMetadataKey(0));
    EXPECT_EQ("double_field", metadata.getMetadataKey(1));
    EXPECT_EQ("boolean_true_field", metadata.getMetadataKey(2));
    EXPECT_EQ("boolean_false_field", metadata.getMetadataKey(3));
    EXPECT_EQ("string_field", metadata.getMetadataKey(4));
    EXPECT_EQ("null_field", metadata.getMetadataKey(5));
    EXPECT_EQ("timestamp_field", metadata.getMetadataKey(6));
  }
}

std::string metadata_test_file_name(std::string_view test_name) {
  return std::string(test_name) + ".metadata";
}

std::string value_test_file_name(std::string_view test_name) {
  return std::string(test_name) + ".value";
}

// 修改后的辅助函数，用于从文件加载 VariantValue，并保持 Buffer 生命周期
VariantValue LoadVariantValue(const std::string& test_name,
                              std::shared_ptr<::arrow::Buffer>* metadata_buf_out,
                              std::shared_ptr<::arrow::Buffer>* value_buf_out) {
  std::string dir_string(parquet::test::get_variant_dir());
  auto file_system = std::make_shared<::arrow::fs::LocalFileSystem>();

  std::string metadata_path = dir_string + "/" + metadata_test_file_name(test_name);
  EXPECT_OK_AND_ASSIGN(auto metadata_file, file_system->OpenInputFile(metadata_path));
  EXPECT_OK_AND_ASSIGN(auto metadata_size, metadata_file->GetSize());
  EXPECT_OK_AND_ASSIGN(auto metadata_buf, metadata_file->Read(metadata_size));
  *metadata_buf_out = metadata_buf;

  std::string value_path = dir_string + "/" + value_test_file_name(test_name);
  EXPECT_OK_AND_ASSIGN(auto value_file, file_system->OpenInputFile(value_path));
  EXPECT_OK_AND_ASSIGN(auto value_size, value_file->GetSize());
  EXPECT_OK_AND_ASSIGN(auto value_buf, value_file->Read(value_size));
  *value_buf_out = value_buf;

  VariantMetadata metadata(std::string_view{**metadata_buf_out});
  return VariantValue{metadata, std::string_view{**value_buf_out}};
}

TEST(ParquetVariant, BooleanValue) {
  // test true
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_boolean_true", &metadata_buf, &value_buf);
    std::cout << variant.typeDebugString() << '\n';
    EXPECT_EQ(VariantType::BOOLEAN, variant.getType());
    EXPECT_EQ("BOOLEAN", variant.typeDebugString());
    EXPECT_EQ(true, variant.getBool());
  }

  // test false
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_boolean_false", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::BOOLEAN, variant.getType());
    EXPECT_EQ(false, variant.getBool());
  }

  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int32", &metadata_buf, &value_buf);
    EXPECT_THROW(variant.getBool(), ParquetException);
  }
}

TEST(ParquetVariant, NumericValues) {
  // 测试 Int8 值
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int8", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::BYTE, variant.getType());
    EXPECT_EQ("BYTE", variant.typeDebugString());
    EXPECT_EQ(42, variant.getInt8());
  }

  // 测试 Int16 值
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int16", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::SHORT, variant.getType());
    EXPECT_EQ("SHORT", variant.typeDebugString());
    EXPECT_EQ(12345, variant.getInt16());
  }

  // 测试 Int32 值
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int32", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::INT, variant.getType());
    EXPECT_EQ("INT", variant.typeDebugString());
    EXPECT_EQ(1234567890, variant.getInt32());
  }

  // 测试 Int64 值
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int64", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::LONG, variant.getType());
    EXPECT_EQ("LONG", variant.typeDebugString());
    EXPECT_EQ(1234567890123456789LL, variant.getInt64());
  }

  // 测试 Float 值
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_float", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::FLOAT, variant.getType());
    EXPECT_EQ("FLOAT", variant.typeDebugString());
    EXPECT_FLOAT_EQ(3.14159f, variant.getFloat());
  }

  // 测试 Double 值
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_double", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::DOUBLE, variant.getType());
    EXPECT_EQ("DOUBLE", variant.typeDebugString());
    EXPECT_DOUBLE_EQ(2.71828182845904523536, variant.getDouble());
  }

  // 测试类型不匹配的异常
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int32", &metadata_buf, &value_buf);
    EXPECT_THROW(variant.getInt64(), ParquetException);
    EXPECT_THROW(variant.getFloat(), ParquetException);
    EXPECT_THROW(variant.getDouble(), ParquetException);
  }
}

TEST(ParquetVariant, StringValues) {
  // 测试普通字符串
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_string", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::STRING, variant.getType());
    EXPECT_EQ("STRING", variant.typeDebugString());
    EXPECT_EQ("Hello, World!", variant.getString());
  }

  // 测试短字符串（使用 ShortString 优化）
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("short_string", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::STRING, variant.getType());
    EXPECT_EQ(VariantBasicType::ShortString, variant.getBasicType());
    EXPECT_EQ("Short", variant.getString());
  }

  // 测试二进制数据
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_binary", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::BINARY, variant.getType());
    EXPECT_EQ("BINARY", variant.typeDebugString());
    auto binary_data = variant.getBinary();
    EXPECT_EQ(10, binary_data.size());
  }

  // 测试类型不匹配的异常
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int32", &metadata_buf, &value_buf);
    EXPECT_THROW(variant.getString(), ParquetException);
    EXPECT_THROW(variant.getBinary(), ParquetException);
  }
}

TEST(ParquetVariant, NullValue) {
  std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
  auto variant = LoadVariantValue("primitive_null", &metadata_buf, &value_buf);
  EXPECT_EQ(VariantType::VARIANT_NULL, variant.getType());
  EXPECT_EQ("NULL", variant.typeDebugString());
}

TEST(ParquetVariant, ObjectValues) {
  std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
  auto variant = LoadVariantValue("object_primitive", &metadata_buf, &value_buf);
  EXPECT_EQ(VariantType::OBJECT, variant.getType());
  EXPECT_EQ("OBJECT", variant.typeDebugString());

  // 获取对象信息
  auto obj_info = variant.getObjectInfo();
  EXPECT_EQ(7, obj_info.num_elements);

  // 通过键获取值
  auto int_field = variant.getObjectValueByKey("int_field");
  ASSERT_TRUE(int_field.has_value());
  EXPECT_EQ(VariantType::INT, int_field->getType());
  EXPECT_EQ(42, int_field->getInt32());

  auto double_field = variant.getObjectValueByKey("double_field");
  ASSERT_TRUE(double_field.has_value());
  EXPECT_EQ(VariantType::DOUBLE, double_field->getType());
  EXPECT_DOUBLE_EQ(3.14159, double_field->getDouble());

  auto boolean_true_field = variant.getObjectValueByKey("boolean_true_field");
  ASSERT_TRUE(boolean_true_field.has_value());
  EXPECT_EQ(VariantType::BOOLEAN, boolean_true_field->getType());
  EXPECT_TRUE(boolean_true_field->getBool());

  auto boolean_false_field = variant.getObjectValueByKey("boolean_false_field");
  ASSERT_TRUE(boolean_false_field.has_value());
  EXPECT_EQ(VariantType::BOOLEAN, boolean_false_field->getType());
  EXPECT_FALSE(boolean_false_field->getBool());

  auto string_field = variant.getObjectValueByKey("string_field");
  ASSERT_TRUE(string_field.has_value());
  EXPECT_EQ(VariantType::STRING, string_field->getType());
  EXPECT_EQ("Hello, World!", string_field->getString());

  auto null_field = variant.getObjectValueByKey("null_field");
  ASSERT_TRUE(null_field.has_value());
  EXPECT_EQ(VariantType::VARIANT_NULL, null_field->getType());

  // 测试不存在的键
  auto non_existent = variant.getObjectValueByKey("non_existent");
  EXPECT_FALSE(non_existent.has_value());

  // 通过字段ID获取值
  std::string_view key;
  auto field_by_id = variant.getObjectFieldByFieldId(0, &key);
  ASSERT_TRUE(field_by_id.has_value());
  EXPECT_EQ("int_field", key);
  EXPECT_EQ(VariantType::INT, field_by_id->getType());
  EXPECT_EQ(42, field_by_id->getInt32());
}

TEST(ParquetVariant, ArrayValues) {
  std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
  auto variant = LoadVariantValue("array_primitive", &metadata_buf, &value_buf);
  EXPECT_EQ(VariantType::ARRAY, variant.getType());
  EXPECT_EQ("ARRAY", variant.typeDebugString());

  // 获取数组信息
  auto array_info = variant.getArrayInfo();
  EXPECT_EQ(5, array_info.num_elements);

  // 通过索引获取值
  auto element0 = variant.getArrayValueByIndex(0);
  EXPECT_EQ(VariantType::INT, element0.getType());
  EXPECT_EQ(1, element0.getInt32());

  auto element1 = variant.getArrayValueByIndex(1);
  EXPECT_EQ(VariantType::INT, element1.getType());
  EXPECT_EQ(2, element1.getInt32());

  auto element2 = variant.getArrayValueByIndex(2);
  EXPECT_EQ(VariantType::INT, element2.getType());
  EXPECT_EQ(3, element2.getInt32());

  auto element3 = variant.getArrayValueByIndex(3);
  EXPECT_EQ(VariantType::INT, element3.getType());
  EXPECT_EQ(4, element3.getInt32());

  auto element4 = variant.getArrayValueByIndex(4);
  EXPECT_EQ(VariantType::INT, element4.getType());
  EXPECT_EQ(5, element4.getInt32());

  // 测试越界索引
  EXPECT_THROW(variant.getArrayValueByIndex(5), ParquetException);
  EXPECT_THROW(variant.getArrayValueByIndex(100), ParquetException);
}

TEST(ParquetVariant, DecimalValues) {
  // 测试 Decimal4 值
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_decimal4", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::DECIMAL4, variant.getType());
    EXPECT_EQ("DECIMAL4", variant.typeDebugString());
    auto decimal = variant.getDecimal4();
    EXPECT_EQ(2, decimal.scale);
    EXPECT_EQ("123.45", decimal.value.ToString(decimal.scale));
  }

  // 测试 Decimal8 值
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_decimal8", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::DECIMAL8, variant.getType());
    EXPECT_EQ("DECIMAL8", variant.typeDebugString());
    auto decimal = variant.getDecimal8();
    EXPECT_EQ(4, decimal.scale);
    EXPECT_EQ("12345.6789", decimal.value.ToString(decimal.scale));
  }

  // 测试 Decimal16 值
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_decimal16", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::DECIMAL16, variant.getType());
    EXPECT_EQ("DECIMAL16", variant.typeDebugString());
    auto decimal = variant.getDecimal16();
    EXPECT_EQ(8, decimal.scale);
    EXPECT_EQ("123456789.12345678", decimal.value.ToString(decimal.scale));
  }
}

TEST(ParquetVariant, DateTimeValues) {
  // 测试日期值
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_date", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::DATE, variant.getType());
    EXPECT_EQ("DATE", variant.typeDebugString());
    // 日期值表示为自 Unix 纪元以来的天数
    EXPECT_EQ(18262, variant.getInt32());  // 2020-01-01
  }

  // 测试时间值
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_time", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::TIME, variant.getType());
    EXPECT_EQ("TIME", variant.typeDebugString());
    // 时间值表示为自午夜以来的微秒数
    EXPECT_EQ(43200000000, variant.timeNTZ());  // 12:00:00
  }

  // 测试带时区的时间戳
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_timestamp_tz", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::TIMESTAMP_TZ, variant.getType());
    EXPECT_EQ("TIMESTAMP_TZ", variant.typeDebugString());
    // 时间戳值表示为自 Unix 纪元以来的微秒数
    EXPECT_EQ(1577836800000000, variant.getTimestamp());  // 2020-01-01 00:00:00 UTC
  }

  // 测试不带时区的时间戳
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_timestamp_ntz", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::TIMESTAMP_NTZ, variant.getType());
    EXPECT_EQ("TIMESTAMP_NTZ", variant.typeDebugString());
    // 时间戳值表示为自 Unix 纪元以来的微秒数
    EXPECT_EQ(1577836800000000, variant.getTimestampNTZ());  // 2020-01-01 00:00:00
  }
}

// TEST(ParquetVariant, UuidValue) {
//   std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
//   auto variant = LoadVariantValue("primitive_uuid", &metadata_buf, &value_buf);
//   EXPECT_EQ(VariantType::UUID, variant.getType());
//   EXPECT_EQ("UUID", variant.typeDebugString());
//
//   // UUID 是 16 字节的二进制数据
//   const uint8_t* uuid = variant.getUuid();
//   ASSERT_NE(nullptr, uuid);
//
//   // 检查 UUID 的格式（这里只是示例，实际值可能不同）
//   std::string uuid_str;
//   for (int i = 0; i < 16; i++) {
//     char hex[3];
//     snprintf(hex, sizeof(hex), "%02x", uuid[i]);
//     uuid_str += hex;
//     if (i == 3 || i == 5 || i == 7 || i == 9) {
//       uuid_str += "-";
//     }
//   }
//
//   EXPECT_EQ(36, uuid_str.length()); // 标准 UUID 字符串长度
// }

TEST(ParquetVariant, NestedStructures) {
  // 测试嵌套对象
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("object_nested", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::OBJECT, variant.getType());

    auto nested_obj = variant.getObjectValueByKey("nested_object");
    ASSERT_TRUE(nested_obj.has_value());
    EXPECT_EQ(VariantType::OBJECT, nested_obj->getType());

    auto nested_field = nested_obj->getObjectValueByKey("nested_field");
    ASSERT_TRUE(nested_field.has_value());
    EXPECT_EQ(VariantType::STRING, nested_field->getType());
    EXPECT_EQ("Nested value", nested_field->getString());
  }

  // 测试嵌套数组
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("array_nested", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::ARRAY, variant.getType());

    auto nested_array = variant.getArrayValueByIndex(0);
    EXPECT_EQ(VariantType::ARRAY, nested_array.getType());

    auto array_info = nested_array.getArrayInfo();
    EXPECT_EQ(3, array_info.num_elements);

    auto element0 = nested_array.getArrayValueByIndex(0);
    EXPECT_EQ(VariantType::INT, element0.getType());
    EXPECT_EQ(1, element0.getInt32());

    auto element1 = nested_array.getArrayValueByIndex(1);
    EXPECT_EQ(VariantType::INT, element1.getType());
    EXPECT_EQ(2, element1.getInt32());

    auto element2 = nested_array.getArrayValueByIndex(2);
    EXPECT_EQ(VariantType::INT, element2.getType());
    EXPECT_EQ(3, element2.getInt32());
  }

  // 测试对象中的数组
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("object_with_array", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::OBJECT, variant.getType());

    auto array_field = variant.getObjectValueByKey("array_field");
    ASSERT_TRUE(array_field.has_value());
    EXPECT_EQ(VariantType::ARRAY, array_field->getType());

    auto array_info = array_field->getArrayInfo();
    EXPECT_EQ(3, array_info.num_elements);

    auto element0 = array_field->getArrayValueByIndex(0);
    EXPECT_EQ(VariantType::INT, element0.getType());
    EXPECT_EQ(1, element0.getInt32());
  }

  // 测试数组中的对象
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("array_with_objects", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::ARRAY, variant.getType());

    auto object_element = variant.getArrayValueByIndex(0);
    EXPECT_EQ(VariantType::OBJECT, object_element.getType());

    auto field = object_element.getObjectValueByKey("field");
    ASSERT_TRUE(field.has_value());
    EXPECT_EQ(VariantType::STRING, field->getType());
    EXPECT_EQ("Value", field->getString());
  }
}

}  // namespace parquet::variant