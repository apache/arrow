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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

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

uint8_t primitiveHeader(VariantPrimitiveType primitive) {
  return (static_cast<uint8_t>(primitive) << 2);
}

// TODO(mwish): Extract this to primitive metadata test
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
    EXPECT_THROW(metadata.GetMetadataKey(0), ParquetException);
  }
  {
    std::string object_metadata = "object_primitive.metadata";
    ARROW_SCOPED_TRACE("Testing file: " + object_metadata);
    std::string path = dir_string + "/" + object_metadata;
    auto buf = readFromFile(*file_system, path);

    VariantMetadata metadata(std::string_view{*buf});
    EXPECT_EQ("int_field", metadata.GetMetadataKey(0));
    EXPECT_EQ("double_field", metadata.GetMetadataKey(1));
    EXPECT_EQ("boolean_true_field", metadata.GetMetadataKey(2));
    EXPECT_EQ("boolean_false_field", metadata.GetMetadataKey(3));
    EXPECT_EQ("string_field", metadata.GetMetadataKey(4));
    EXPECT_EQ("null_field", metadata.GetMetadataKey(5));
    EXPECT_EQ("timestamp_field", metadata.GetMetadataKey(6));
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
  std::string_view metadata{**metadata_buf_out};
  return VariantValue{metadata, value};
}

TEST(ParquetVariant, NullValue) {
  std::string_view empty_metadata(VariantMetadata::kEmptyMetadataChars, 3);
  const uint8_t null_chars[] = {primitiveHeader(VariantPrimitiveType::NullType)};
  VariantValue variant{empty_metadata,
                       std::string_view{reinterpret_cast<const char*>(null_chars), 1}};
  EXPECT_EQ(VariantType::Null, variant.getType());
  EXPECT_EQ("Null", variant.typeDebugString());
}

TEST(ParquetVariant, BooleanValue) {
  // test true
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_boolean_true", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Boolean, variant.getType());
    EXPECT_EQ("Boolean", variant.typeDebugString());
    EXPECT_EQ(true, variant.getBool());
  }
  // test false
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_boolean_false", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Boolean, variant.getType());
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
    EXPECT_EQ(VariantType::Int8, variant.getType());
    EXPECT_EQ("Int8", variant.typeDebugString());
    EXPECT_EQ(42, variant.getInt8());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int16", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Int16, variant.getType());
    EXPECT_EQ("Int16", variant.typeDebugString());
    EXPECT_EQ(1234, variant.getInt16());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int32", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Int32, variant.getType());
    EXPECT_EQ("Int32", variant.typeDebugString());
    EXPECT_EQ(123456, variant.getInt32());
  }
  {
    // FIXME(mwish): https://github.com/apache/parquet-testing/issues/82
    //  The primitive_int64 is a int32 value, but the metadata is int64.
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int64", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Int32, variant.getType());
    EXPECT_EQ("Int32", variant.typeDebugString());
    EXPECT_EQ(12345678, variant.getInt32());
  }
  {
    // Test handwritten int64
    const uint8_t int64_chars[] = {primitiveHeader(VariantPrimitiveType::Int64),
                                   0xB1,
                                   0x1C,
                                   0x6C,
                                   0xB1,
                                   0xF4,
                                   0x10,
                                   0x22,
                                   0x11};
    std::string_view metadata(VariantMetadata::kEmptyMetadataChars, 3);
    std::string_view value{reinterpret_cast<const char*>(int64_chars),
                           sizeof(int64_chars)};
    VariantValue variant{metadata, value};
    EXPECT_EQ(VariantType::Int64, variant.getType());
    EXPECT_EQ(1234567890987654321L, variant.getInt64());
  }
  {
    // Test handwritten int64 negative
    const uint8_t int64_chars[] = {primitiveHeader(VariantPrimitiveType::Int64),
                                   0xFF,
                                   0xFF,
                                   0xFF,
                                   0xFF,
                                   0xFF,
                                   0xFF,
                                   0xFF,
                                   0xFF};
    std::string_view metadata(VariantMetadata::kEmptyMetadataChars, 3);
    std::string_view value{reinterpret_cast<const char*>(int64_chars),
                           sizeof(int64_chars)};
    VariantValue variant{metadata, value};
    EXPECT_EQ(VariantType::Int64, variant.getType());
    EXPECT_EQ(-1L, variant.getInt64());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_float", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Float, variant.getType());
    EXPECT_EQ("Float", variant.typeDebugString());
    EXPECT_FLOAT_EQ(1234567940.0f, variant.getFloat());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_double", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Double, variant.getType());
    EXPECT_EQ("Double", variant.typeDebugString());
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
    EXPECT_EQ(VariantType::String, variant.getType());
    EXPECT_EQ("String", variant.typeDebugString());
    std::string expected =
        R"(This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!)";
    EXPECT_EQ(expected, variant.getString());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("short_string", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::String, variant.getType());
    EXPECT_EQ(VariantBasicType::ShortString, variant.getBasicType());
    std::string expected = R"(Less than 64 bytes (❤️ with utf8))";
    EXPECT_EQ(expected, variant.getString());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_binary", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Binary, variant.getType());
    EXPECT_EQ("Binary", variant.typeDebugString());
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
  EXPECT_EQ(VariantType::Object, variant.getType());
  EXPECT_EQ("Object", variant.typeDebugString());

  EXPECT_EQ(7, variant.num_elements());
  auto handle_int_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::Int8, value->getType());
    EXPECT_EQ(1, value->getInt8());
  };
  auto handle_double_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::Decimal4, value->getType());
    auto decimal_value = value->getDecimal4();
    EXPECT_EQ("1.23456789", decimal_value.value.ToString(decimal_value.scale));
  };
  auto handle_boolean_true_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::Boolean, value->getType());
    EXPECT_TRUE(value->getBool());
  };
  auto handle_boolean_false_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::Boolean, value->getType());
    EXPECT_FALSE(value->getBool());
  };
  auto handle_string_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::String, value->getType());
    EXPECT_EQ("Apache Parquet", value->getString());
  };
  auto handle_null_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::Null, value->getType());
  };
  auto handle_timestamp_field = [](const std::optional<VariantValue>& value) {
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(VariantType::String, value->getType());
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
  {
    ARROW_SCOPED_TRACE("Test getObjectValueByKey with existing keys");
    for (auto& [key, handler] : key_handler) {
      auto value = variant.getObjectValueByKey(key);
      handler(value);
    }
  }
  // Test non-existing key
  {
    auto ne = variant.getObjectValueByKey("non_exists");
    EXPECT_FALSE(ne.has_value());
  }
  // Test get by index
  {
    ARROW_SCOPED_TRACE("Test getObjectFieldByFieldId with existing indexes");
    for (uint32_t i = 0; i < variant.num_elements(); ++i) {
      auto value = variant.getObjectFieldByFieldId(i);
      auto key = variant.metadata().GetMetadataKey(i);
      auto iter = key_handler.find(std::string(key));
      ASSERT_TRUE(iter != key_handler.end());
      auto handler = iter->second;
      handler(value);
    }
  }
  EXPECT_FALSE(variant.getObjectFieldByFieldId(100).has_value());
}

TEST(ParquetVariant, NestedObjectValues) {
  std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
  auto variant = LoadVariantValue("object_nested", &metadata_buf, &value_buf);
  EXPECT_EQ(VariantType::Object, variant.getType());
  EXPECT_EQ("Object", variant.typeDebugString());
  EXPECT_EQ(3, variant.num_elements());

  // Trying to get the exists key
  auto id = variant.getObjectValueByKey("id");
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(VariantType::Int8, id->getType());
  EXPECT_EQ(1, id->getInt8());

  auto observation = variant.getObjectValueByKey("observation");
  ASSERT_TRUE(observation.has_value());
  EXPECT_EQ(VariantType::Object, observation->getType());

  auto species = variant.getObjectValueByKey("species");
  ASSERT_TRUE(species.has_value());
  EXPECT_EQ(VariantType::Object, species->getType());

  // Inner object works well
  {
    EXPECT_EQ(2, species->num_elements());
    auto name = species->getObjectValueByKey("name");
    ASSERT_TRUE(name.has_value());
    EXPECT_EQ(VariantType::String, name->getType());
    EXPECT_EQ("lava monster", name->getString());

    auto population = species->getObjectValueByKey("population");
    ASSERT_TRUE(population.has_value());
    EXPECT_EQ(VariantType::Int16, population->getType());
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
    EXPECT_EQ(VariantType::Decimal4, variant.getType());
    EXPECT_EQ("Decimal4", variant.typeDebugString());
    auto decimal = variant.getDecimal4();
    EXPECT_EQ(2, decimal.scale);
    EXPECT_EQ("12.34", decimal.value.ToString(decimal.scale));
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_decimal8", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Decimal8, variant.getType());
    EXPECT_EQ("Decimal8", variant.typeDebugString());
    auto decimal = variant.getDecimal8();
    EXPECT_EQ(2, decimal.scale);
    EXPECT_EQ("12345678.90", decimal.value.ToString(decimal.scale));
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_decimal16", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Decimal16, variant.getType());
    EXPECT_EQ("Decimal16", variant.typeDebugString());
    auto decimal = variant.getDecimal16();
    EXPECT_EQ(2, decimal.scale);
    EXPECT_EQ("12345678912345678.90", decimal.value.ToString(decimal.scale));
  }
}

TEST(ParquetVariant, Uuid) {
  std::string_view empty_metadata(VariantMetadata::kEmptyMetadataChars, 3);
  const uint8_t uuid_chars[] = {primitiveHeader(VariantPrimitiveType::Uuid),
                                0x00,
                                0x11,
                                0x22,
                                0x33,
                                0x44,
                                0x55,
                                0x66,
                                0x77,
                                0x88,
                                0x99,
                                0xAA,
                                0xBB,
                                0xCC,
                                0xDD,
                                0xEE,
                                0xFF};
  std::string_view value(reinterpret_cast<const char*>(uuid_chars), sizeof(uuid_chars));
  VariantValue variant(empty_metadata, value);
  ASSERT_EQ(VariantType::Uuid, variant.getType());
  auto uuid_val = variant.getUuid();
  boost::uuids::uuid uuid{};
  for (size_t i = 0; i < uuid.size(); ++i) {
    uuid.data[i] = uuid_val[i];
  }
  EXPECT_EQ("00112233-4455-6677-8899-aabbccddeeff", to_string(uuid));
}

TEST(ParquetVariant, DateTimeValues) {
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_date", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Date, variant.getType());
    EXPECT_EQ("Date", variant.typeDebugString());
    // 2025-04-16
    EXPECT_EQ(20194, variant.getDate());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_timestamp", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::TimestampMicrosTz, variant.getType());
    EXPECT_EQ("TimestampMicrosTz", variant.typeDebugString());
    EXPECT_EQ(1744821296780000, variant.getTimestampMicros());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_timestampntz", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::TimestampMicrosNtz, variant.getType());
    EXPECT_EQ("TimestampMicrosNtz", variant.typeDebugString());
    EXPECT_EQ(1744806896780000, variant.getTimestampMicrosNtz());
  }
  {
    // Timestamp Nanos tz negative
    std::string_view empty_metadata(VariantMetadata::kEmptyMetadataChars, 3);
    const uint8_t timestamp_nanos_ntz_chars[] = {
        primitiveHeader(VariantPrimitiveType::TimestampNanosTz),
        0xFF,
        0xFF,
        0xFF,
        0xFF,
        0xFF,
        0xFF,
        0xFF,
        0xFF};
    std::string_view value{reinterpret_cast<const char*>(timestamp_nanos_ntz_chars),
                           sizeof(timestamp_nanos_ntz_chars)};
    VariantValue variant{empty_metadata, value};
    EXPECT_EQ(VariantType::TimestampNanosTz, variant.getType());
    EXPECT_EQ(-1L, variant.getTimestampNanosTz());
  }
  {
    // Timestamp Nanos tz negative
    std::string_view empty_metadata(VariantMetadata::kEmptyMetadataChars, 3);
    const uint8_t timestamp_nanos_ntz_chars[] = {
        primitiveHeader(VariantPrimitiveType::TimestampNanosTz),
        0x15,
        0xC9,
        0xBB,
        0x86,
        0xB4,
        0x0C,
        0x37,
        0x18};
    std::string_view value{reinterpret_cast<const char*>(timestamp_nanos_ntz_chars),
                           sizeof(timestamp_nanos_ntz_chars)};
    VariantValue variant{empty_metadata, value};
    EXPECT_EQ(VariantType::TimestampNanosTz, variant.getType());
    EXPECT_EQ(1744877350123456789L, variant.getTimestampNanosTz());
  }
  {
    // Timestamp Nanos Ntz
    std::string_view empty_metadata(VariantMetadata::kEmptyMetadataChars, 3);
    const uint8_t timestamp_nanos_ntz_chars[] = {
        primitiveHeader(VariantPrimitiveType::TimestampNanosNtz),
        0x15,
        0xC9,
        0xBB,
        0x86,
        0xB4,
        0x0C,
        0x37,
        0x18};
    std::string_view value{reinterpret_cast<const char*>(timestamp_nanos_ntz_chars),
                           sizeof(timestamp_nanos_ntz_chars)};
    VariantValue variant{empty_metadata, value};
    EXPECT_EQ(VariantType::TimestampNanosNtz, variant.getType());
    EXPECT_EQ(1744877350123456789L, variant.getTimestampNanosNtz());
  }
}

TEST(ParquetVariant, ArrayValues) {
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("array_primitive", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Array, variant.getType());
    EXPECT_EQ("Array", variant.typeDebugString());

    EXPECT_EQ(4, variant.num_elements());

    auto element0 = variant.getArrayValueByIndex(0);
    EXPECT_EQ(VariantType::Int8, element0.getType());
    EXPECT_EQ(2, element0.getInt8());

    auto element1 = variant.getArrayValueByIndex(1);
    EXPECT_EQ(VariantType::Int8, element1.getType());
    EXPECT_EQ(1, element1.getInt8());

    auto element2 = variant.getArrayValueByIndex(2);
    EXPECT_EQ(VariantType::Int8, element2.getType());
    EXPECT_EQ(5, element2.getInt8());

    auto element3 = variant.getArrayValueByIndex(3);
    EXPECT_EQ(VariantType::Int8, element3.getType());
    EXPECT_EQ(9, element3.getInt8());

    EXPECT_THROW(variant.getArrayValueByIndex(4), ParquetException);
    EXPECT_THROW(variant.getArrayValueByIndex(100), ParquetException);
    EXPECT_THROW(variant.getObjectValueByKey("10"), ParquetException);
    EXPECT_THROW(variant.getObjectFieldByFieldId(10), ParquetException);
  }
  {
    // array_empty
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("array_empty", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::Array, variant.getType());
    EXPECT_EQ("Array", variant.typeDebugString());
    EXPECT_EQ(0, variant.num_elements());

    EXPECT_THROW(variant.getArrayValueByIndex(0), ParquetException);
    EXPECT_THROW(variant.getObjectValueByKey("key"), ParquetException);
  }
}

TEST(ParquetVariant, ArrayValuesNested) {
  std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
  auto variant = LoadVariantValue("array_nested", &metadata_buf, &value_buf);
  EXPECT_EQ(VariantType::Array, variant.getType());
  EXPECT_EQ("Array", variant.typeDebugString());
  EXPECT_EQ(3, variant.num_elements());
  {
    auto first_element = variant.getArrayValueByIndex(0);
    EXPECT_EQ(VariantType::Object, first_element.getType());
    EXPECT_EQ(2, first_element.num_elements());
    auto id = first_element.getObjectValueByKey("id");
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(VariantType::Int8, id->getType());
    EXPECT_EQ(1, id->getInt8());
  }
  {
    auto second_element = variant.getArrayValueByIndex(1);
    EXPECT_EQ(VariantType::Null, second_element.getType());
  }
  {
    auto third_element = variant.getArrayValueByIndex(2);
    EXPECT_EQ(VariantType::Object, third_element.getType());
    EXPECT_EQ(3, third_element.num_elements());
    auto id = third_element.getObjectValueByKey("id");
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(VariantType::Int8, id->getType());
    EXPECT_EQ(2, id->getInt8());
  }
}

}  // namespace parquet::variant
