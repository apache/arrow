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
#include <arrow/util/base64.h>

namespace parquet::variant {

std::string metadata_test_file_name(std::string_view test_name) {
  return std::string(test_name) + ".metadata";
}

std::string value_test_file_name(std::string_view test_name) {
  return std::string(test_name) + ".value";
}

std::shared_ptr<::arrow::Buffer> readFromFile(::arrow::fs::FileSystem& fs,
                                              const std::string& path) {
  ASSIGN_OR_ABORT(auto file, fs.OpenInputFile(path));
  ASSIGN_OR_ABORT(auto file_size, file->GetSize());
  ASSIGN_OR_ABORT(auto buf, file->Read(file_size));
  return buf;
}

TEST(ParquetVariant, MetadataBase) {
  std::string dir_string(parquet::test::get_variant_dir());
  auto file_system = std::make_shared<::arrow::fs::LocalFileSystem>();
  std::vector<std::string> primitive_metadatas = {
      // FIXME(mwish): null metadata is corrupt, see
      // https://github.com/apache/parquet-testing/issues/81
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
    auto buf = readFromFile(*file_system, path);

    VariantMetadata metadata(std::string_view{*buf});
    EXPECT_EQ(1, metadata.version());
    EXPECT_THROW(metadata.getMetadataKey(0), ParquetException);
  }
  {
    std::string object_metadata = "object_primitive.metadata";
    ARROW_SCOPED_TRACE("Testing file: " + object_metadata);
    std::string path = dir_string + "/" + object_metadata;
    auto buf = readFromFile(*file_system, path);

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

VariantValue LoadVariantValue(const std::string& test_name,
                              std::shared_ptr<::arrow::Buffer>* metadata_buf_out,
                              std::shared_ptr<::arrow::Buffer>* value_buf_out) {
  std::string dir_string(parquet::test::get_variant_dir());
  // TODO(mwish): Share in a base class?
  auto file_system = std::make_shared<::arrow::fs::LocalFileSystem>();

  std::string metadata_path = dir_string + "/" + metadata_test_file_name(test_name);
  *metadata_buf_out = readFromFile(*file_system, metadata_path);

  std::string value_path = dir_string + "/" + value_test_file_name(test_name);
  *value_buf_out = readFromFile(*file_system, value_path);

  std::string_view value{**value_buf_out};

  VariantMetadata metadata(std::string_view{**metadata_buf_out});
  return VariantValue{metadata, value};
}

TEST(ParquetVariant, BooleanValue) {
  // test true
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_boolean_true", &metadata_buf, &value_buf);
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
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int8", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::INT8, variant.getType());
    EXPECT_EQ("INT8", variant.typeDebugString());
    EXPECT_EQ(42, variant.getInt8());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int16", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::INT16, variant.getType());
    EXPECT_EQ("INT16", variant.typeDebugString());
    EXPECT_EQ(1234, variant.getInt16());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int32", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::INT32, variant.getType());
    EXPECT_EQ("INT32", variant.typeDebugString());
    EXPECT_EQ(123456, variant.getInt32());
  }
  {
    // FIXME(mwish): https://github.com/apache/parquet-testing/issues/82
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int64", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::INT32, variant.getType());
    EXPECT_EQ("INT32", variant.typeDebugString());
    EXPECT_EQ(12345678, variant.getInt32());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_float", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::FLOAT, variant.getType());
    EXPECT_EQ("FLOAT", variant.typeDebugString());
    EXPECT_FLOAT_EQ(1234567940.0f, variant.getFloat());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_double", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::DOUBLE, variant.getType());
    EXPECT_EQ("DOUBLE", variant.typeDebugString());
    EXPECT_DOUBLE_EQ(1234567890.1234, variant.getDouble());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int32", &metadata_buf, &value_buf);
    EXPECT_THROW(variant.getInt64(), ParquetException);
    EXPECT_THROW(variant.getFloat(), ParquetException);
    EXPECT_THROW(variant.getDouble(), ParquetException);
  }
}

TEST(ParquetVariant, StringValues) {
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_string", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::STRING, variant.getType());
    EXPECT_EQ("STRING", variant.typeDebugString());
    std::string expected =
        R"(This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!)";
    EXPECT_EQ(expected, variant.getString());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("short_string", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::STRING, variant.getType());
    EXPECT_EQ(VariantBasicType::ShortString, variant.getBasicType());
    std::string expected = R"(Less than 64 bytes (❤️ with utf8))";
    EXPECT_EQ(expected, variant.getString());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_binary", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::BINARY, variant.getType());
    EXPECT_EQ("BINARY", variant.typeDebugString());
    auto binary_data = variant.getBinary();
    std::string expected = ::arrow::util::base64_decode("AxM33q2+78r+");
    EXPECT_EQ(expected, binary_data);
  }

  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int32", &metadata_buf, &value_buf);
    EXPECT_THROW(variant.getString(), ParquetException);
    EXPECT_THROW(variant.getBinary(), ParquetException);
  }
}

TEST(ParquetVariant, ObjectValues) {
  std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
  auto variant = LoadVariantValue("object_primitive", &metadata_buf, &value_buf);
  EXPECT_EQ(VariantType::OBJECT, variant.getType());
  EXPECT_EQ("OBJECT", variant.typeDebugString());

  auto obj_info = variant.getObjectInfo();
  EXPECT_EQ(7, obj_info.num_elements);
  auto handle_int_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::INT8, value->getType());
    EXPECT_EQ(1, value->getInt8());
  };
  auto handle_double_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::DECIMAL4, value->getType());
    auto decimal_value = value->getDecimal4();
    EXPECT_EQ("1.23456789", decimal_value.value.ToString(decimal_value.scale));
  };
  auto handle_boolean_true_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::BOOLEAN, value->getType());
    EXPECT_TRUE(value->getBool());
  };
  auto handle_boolean_false_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::BOOLEAN, value->getType());
    EXPECT_FALSE(value->getBool());
  };
  auto handle_string_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::STRING, value->getType());
    EXPECT_EQ("Apache Parquet", value->getString());
  };
  auto handle_null_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::VARIANT_NULL, value->getType());
  };
  auto handle_timestamp_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::STRING, value->getType());
    EXPECT_EQ("2025-04-16T12:34:56.78", value->getString());
  };

  std::map<std::string, std::function<void(const std::optional<VariantValue>& value)>>
      key_handler = {{"int_field", handle_int_field},
                     {"double_field", handle_double_field},
                     {"boolean_true_field", handle_boolean_true_field},
                     {"boolean_false_field", handle_boolean_false_field},
                     {"string_field", handle_string_field},
                     {"null_field", handle_null_field},
                     {"timestamp_field", handle_timestamp_field}};
  // Test getObjectValueByKey with existing keys
  for (auto& [key, handler] : key_handler) {
    auto value = variant.getObjectValueByKey(key);
    handler(value);
  }
  // Test non-existing key
  {
    auto ne = variant.getObjectValueByKey("non_exists");
    EXPECT_FALSE(ne.has_value());
  }
  // Test get by index
  for (uint32_t i = 0; i < obj_info.num_elements; ++i) {
    auto value = variant.getObjectFieldByFieldId(i);
    auto key = variant.metadata.getMetadataKey(i);
    auto iter = key_handler.find(std::string(key));
    ASSERT_TRUE(iter != key_handler.end());
    auto handler = iter->second;
    handler(value);
  }
  EXPECT_FALSE(variant.getObjectFieldByFieldId(100).has_value());
}

TEST(ParquetVariant, NestedObjectValues) {
  std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
  auto variant = LoadVariantValue("object_nested", &metadata_buf, &value_buf);
  EXPECT_EQ(VariantType::OBJECT, variant.getType());
  EXPECT_EQ("OBJECT", variant.typeDebugString());
  auto info = variant.getObjectInfo();
  EXPECT_EQ(3, info.num_elements);

  // Trying to get the exists key
  auto id = variant.getObjectValueByKey("id", info);
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(VariantType::INT8, id->getType());
  EXPECT_EQ(1, id->getInt8());

  auto observation = variant.getObjectValueByKey("observation", info);
  ASSERT_TRUE(observation.has_value());
  EXPECT_EQ(VariantType::OBJECT, observation->getType());

  auto species = variant.getObjectValueByKey("species", info);
  ASSERT_TRUE(species.has_value());
  EXPECT_EQ(VariantType::OBJECT, species->getType());
  auto species_info = species->getObjectInfo();
  EXPECT_EQ(2, species_info.num_elements);

  // Inner object works well
  {
    auto species_object_info = species->getObjectInfo();
    EXPECT_EQ(2, species_object_info.num_elements);
    auto name = species->getObjectValueByKey("name");
    ASSERT_TRUE(name.has_value());
    EXPECT_EQ(VariantType::STRING, name->getType());
    EXPECT_EQ("lava monster", name->getString());

    auto population = species->getObjectValueByKey("population");
    ASSERT_TRUE(population.has_value());
    EXPECT_EQ(VariantType::INT16, population->getType());
    EXPECT_EQ(6789, population->getInt16());
  }

  // Get inner key outside will fail
  {
    std::vector<std::string_view> observation_keys = {"location", "time", "value"};
    for (auto& key : observation_keys) {
      // Only observation would get it successfully.
      auto inner_value = observation->getObjectValueByKey(key);
      ASSERT_TRUE(inner_value.has_value());

      inner_value = variant.getObjectValueByKey(key);
      ASSERT_FALSE(inner_value.has_value());

      inner_value = species->getObjectValueByKey(key);
      ASSERT_FALSE(inner_value.has_value());
    }
  }
  // Get outside keys in inner object
  {
    auto inner_value = observation->getObjectValueByKey("id");
    EXPECT_FALSE(inner_value.has_value());

    inner_value = species->getObjectValueByKey("id");
    EXPECT_FALSE(inner_value.has_value());
  }
}

TEST(ParquetVariant, DecimalValues) {
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_decimal4", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::DECIMAL4, variant.getType());
    EXPECT_EQ("DECIMAL4", variant.typeDebugString());
    auto decimal = variant.getDecimal4();
    EXPECT_EQ(2, decimal.scale);
    EXPECT_EQ("12.34", decimal.value.ToString(decimal.scale));
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_decimal8", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::DECIMAL8, variant.getType());
    EXPECT_EQ("DECIMAL8", variant.typeDebugString());
    auto decimal = variant.getDecimal8();
    EXPECT_EQ(2, decimal.scale);
    EXPECT_EQ("12345678.90", decimal.value.ToString(decimal.scale));
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_decimal16", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::DECIMAL16, variant.getType());
    EXPECT_EQ("DECIMAL16", variant.typeDebugString());
    auto decimal = variant.getDecimal16();
    EXPECT_EQ(2, decimal.scale);
    EXPECT_EQ("12345678912345678.90", decimal.value.ToString(decimal.scale));
  }
}

TEST(ParquetVariant, DateTimeValues) {
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_date", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::DATE, variant.getType());
    EXPECT_EQ("DATE", variant.typeDebugString());
    // 2025-04-16
    EXPECT_EQ(20194, variant.getDate());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_timestamp", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::TIMESTAMP_TZ, variant.getType());
    EXPECT_EQ("TIMESTAMP_TZ", variant.typeDebugString());
    EXPECT_EQ(1744821296780000, variant.getTimestamp());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_timestampntz", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::TIMESTAMP_NTZ, variant.getType());
    EXPECT_EQ("TIMESTAMP_NTZ", variant.typeDebugString());
    EXPECT_EQ(1744806896780000, variant.getTimestampNTZ());
  }
}

TEST(ParquetVariant, ArrayValues) {
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("array_primitive", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::ARRAY, variant.getType());
    EXPECT_EQ("ARRAY", variant.typeDebugString());

    auto array_info = variant.getArrayInfo();
    EXPECT_EQ(4, array_info.num_elements);

    auto element0 = variant.getArrayValueByIndex(0);
    EXPECT_EQ(VariantType::INT8, element0.getType());
    EXPECT_EQ(2, element0.getInt8());

    auto element1 = variant.getArrayValueByIndex(1);
    EXPECT_EQ(VariantType::INT8, element1.getType());
    EXPECT_EQ(1, element1.getInt8());

    auto element2 = variant.getArrayValueByIndex(2);
    EXPECT_EQ(VariantType::INT8, element2.getType());
    EXPECT_EQ(5, element2.getInt8());

    auto element3 = variant.getArrayValueByIndex(3);
    EXPECT_EQ(VariantType::INT8, element3.getType());
    EXPECT_EQ(9, element3.getInt8());

    EXPECT_THROW(variant.getArrayValueByIndex(4), ParquetException);
    EXPECT_THROW(variant.getArrayValueByIndex(100), ParquetException);
    EXPECT_THROW(variant.getObjectInfo(), ParquetException);
  }
  {
    // array_empty
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("array_empty", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::ARRAY, variant.getType());
    EXPECT_EQ("ARRAY", variant.typeDebugString());
    auto array_info = variant.getArrayInfo();
    EXPECT_EQ(0, array_info.num_elements);

    EXPECT_THROW(variant.getArrayValueByIndex(0), ParquetException);
    EXPECT_THROW(variant.getObjectInfo(), ParquetException);
  }
}

TEST(ParquetVariant, ArrayValuesNested) {
  std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
  auto variant = LoadVariantValue("array_nested", &metadata_buf, &value_buf);
  EXPECT_EQ(VariantType::ARRAY, variant.getType());
  EXPECT_EQ("ARRAY", variant.typeDebugString());
  auto object_info = variant.getArrayInfo();
  EXPECT_EQ(3, object_info.num_elements);
  {
    auto first_element = variant.getArrayValueByIndex(0);
    EXPECT_EQ(VariantType::OBJECT, first_element.getType());
    auto first_element_info = first_element.getObjectInfo();
    EXPECT_EQ(2, first_element_info.num_elements);
    auto id = first_element.getObjectValueByKey("id");
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(VariantType::INT8, id->getType());
    EXPECT_EQ(1, id->getInt8());
  }
  {
    auto second_element = variant.getArrayValueByIndex(1);
    EXPECT_EQ(VariantType::VARIANT_NULL, second_element.getType());
  }
  {
    auto third_element = variant.getArrayValueByIndex(2);
    EXPECT_EQ(VariantType::OBJECT, third_element.getType());
    auto third_element_info = third_element.getObjectInfo();
    EXPECT_EQ(3, third_element_info.num_elements);
    auto id = third_element.getObjectValueByKey("id");
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(VariantType::INT8, id->getType());
    EXPECT_EQ(2, id->getInt8());
  }
}

}  // namespace parquet::variant
