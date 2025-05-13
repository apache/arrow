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
    auto buf = readFromFile(*file_system, object_metadata);

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
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int8", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::BYTE, variant.getType());
    EXPECT_EQ("BYTE", variant.typeDebugString());
    EXPECT_EQ(42, variant.getInt8());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int16", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::SHORT, variant.getType());
    EXPECT_EQ("SHORT", variant.typeDebugString());
    EXPECT_EQ(1234, variant.getInt16());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int32", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::INT, variant.getType());
    EXPECT_EQ("INT", variant.typeDebugString());
    EXPECT_EQ(123456, variant.getInt32());
  }
  {
    // FIXME(mwish): https://github.com/apache/parquet-testing/issues/82
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_int64", &metadata_buf, &value_buf);
    //   EXPECT_EQ(VariantType::LONG, variant.getType());
    //   EXPECT_EQ("LONG", variant.typeDebugString());
    EXPECT_EQ(12345678, variant.getInt32());
  }
  {
    std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
    auto variant = LoadVariantValue("primitive_float", &metadata_buf, &value_buf);
    EXPECT_EQ(VariantType::FLOAT, variant.getType());
    EXPECT_EQ("FLOAT", variant.typeDebugString());
    EXPECT_FLOAT_EQ(1234567940.0, variant.getFloat());
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

TEST(ParquetVariant, NullValue) {
  // https://github.com/apache/parquet-testing/issues/81
  /*
  std::shared_ptr<::arrow::Buffer> metadata_buf, value_buf;
  auto variant = LoadVariantValue("primitive_null", &metadata_buf, &value_buf);
  EXPECT_EQ(VariantType::VARIANT_NULL, variant.getType());
  EXPECT_EQ("NULL", variant.typeDebugString());
  */
}

}  // namespace parquet::variant