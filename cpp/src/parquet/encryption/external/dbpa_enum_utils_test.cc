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

#include <dbpa_interface.h>
#include "arrow/util/type_fwd.h"
#include "parquet/encryption/external/dbpa_enum_utils.h"
#include "parquet/platform.h"
#include "parquet/types.h"

#include <magic_enum/magic_enum.hpp>

using magic_enum::enum_count;

namespace parquet::encryption::external {

class DBPAUtilsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Calculate enum sizes once during test initialization

    // We use "Magic Enum" to check the sizes of the enums.
    // https://github.com/Neargye/magic_enum
    // (the additional library is needed as reflection for enums is not available in C++)
    parquet_type_enum_size_ = magic_enum::enum_count<parquet::Type::type>();
    arrow_compression_enum_size_ = magic_enum::enum_count<::arrow::Compression::type>();
  }

  // Enum sizes calculated during test initialization
  std::size_t parquet_type_enum_size_;
  std::size_t arrow_compression_enum_size_;
};

TEST_F(DBPAUtilsTest, ParquetTypeToExternal) {
  // Test all valid parquet types
  EXPECT_EQ(DBPAEnumUtils::ParquetTypeToDBPA(parquet::Type::BOOLEAN),
            dbps::external::Type::BOOLEAN);
  EXPECT_EQ(DBPAEnumUtils::ParquetTypeToDBPA(parquet::Type::INT32),
            dbps::external::Type::INT32);
  EXPECT_EQ(DBPAEnumUtils::ParquetTypeToDBPA(parquet::Type::INT64),
            dbps::external::Type::INT64);
  EXPECT_EQ(DBPAEnumUtils::ParquetTypeToDBPA(parquet::Type::INT96),
            dbps::external::Type::INT96);
  EXPECT_EQ(DBPAEnumUtils::ParquetTypeToDBPA(parquet::Type::FLOAT),
            dbps::external::Type::FLOAT);
  EXPECT_EQ(DBPAEnumUtils::ParquetTypeToDBPA(parquet::Type::DOUBLE),
            dbps::external::Type::DOUBLE);
  EXPECT_EQ(DBPAEnumUtils::ParquetTypeToDBPA(parquet::Type::BYTE_ARRAY),
            dbps::external::Type::BYTE_ARRAY);
  EXPECT_EQ(DBPAEnumUtils::ParquetTypeToDBPA(parquet::Type::FIXED_LEN_BYTE_ARRAY),
            dbps::external::Type::FIXED_LEN_BYTE_ARRAY);
}

TEST_F(DBPAUtilsTest, ArrowCompressionToExternal) {
  // Test all valid arrow compression types that have mappings
  EXPECT_EQ(DBPAEnumUtils::ArrowCompressionToDBPA(::arrow::Compression::UNCOMPRESSED),
            dbps::external::CompressionCodec::UNCOMPRESSED);
  EXPECT_EQ(DBPAEnumUtils::ArrowCompressionToDBPA(::arrow::Compression::SNAPPY),
            dbps::external::CompressionCodec::SNAPPY);
  EXPECT_EQ(DBPAEnumUtils::ArrowCompressionToDBPA(::arrow::Compression::GZIP),
            dbps::external::CompressionCodec::GZIP);
  EXPECT_EQ(DBPAEnumUtils::ArrowCompressionToDBPA(::arrow::Compression::LZO),
            dbps::external::CompressionCodec::LZO);
  EXPECT_EQ(DBPAEnumUtils::ArrowCompressionToDBPA(::arrow::Compression::BROTLI),
            dbps::external::CompressionCodec::BROTLI);
  EXPECT_EQ(DBPAEnumUtils::ArrowCompressionToDBPA(::arrow::Compression::LZ4),
            dbps::external::CompressionCodec::LZ4);
  EXPECT_EQ(DBPAEnumUtils::ArrowCompressionToDBPA(::arrow::Compression::ZSTD),
            dbps::external::CompressionCodec::ZSTD);
  EXPECT_EQ(DBPAEnumUtils::ArrowCompressionToDBPA(::arrow::Compression::LZ4_FRAME),
            dbps::external::CompressionCodec::LZ4_FRAME);
  EXPECT_EQ(DBPAEnumUtils::ArrowCompressionToDBPA(::arrow::Compression::BZ2),
            dbps::external::CompressionCodec::BZ2);
  EXPECT_EQ(DBPAEnumUtils::ArrowCompressionToDBPA(::arrow::Compression::LZ4_HADOOP),
            dbps::external::CompressionCodec::LZ4_HADOOP);
}

TEST_F(DBPAUtilsTest, AllValidTypeMappings) {
  // Test that all valid parquet types can be converted to external types
  // Using the actual enum values from parquet::Type (excluding UNDEFINED)
  std::vector<parquet::Type::type> valid_parquet_types = {
      parquet::Type::BOOLEAN,    parquet::Type::INT32,
      parquet::Type::INT64,      parquet::Type::INT96,
      parquet::Type::FLOAT,      parquet::Type::DOUBLE,
      parquet::Type::BYTE_ARRAY, parquet::Type::FIXED_LEN_BYTE_ARRAY,
      parquet::Type::UNDEFINED};

  // Ensure that the map is complete.
  ASSERT_EQ(valid_parquet_types.size(), parquet_type_enum_size_);

  for (auto parquet_type : valid_parquet_types) {
    EXPECT_NO_THROW(DBPAEnumUtils::ParquetTypeToDBPA(parquet_type));
  }
}

TEST_F(DBPAUtilsTest, AllValidCompressionMappings) {
  // Test that all valid arrow compression types that have mappings work
  // Using the actual enum values from arrow::Compression that are supported
  std::vector<::arrow::Compression::type> valid_arrow_compressions = {
      ::arrow::Compression::UNCOMPRESSED, ::arrow::Compression::SNAPPY,
      ::arrow::Compression::GZIP,         ::arrow::Compression::LZO,
      ::arrow::Compression::BROTLI,       ::arrow::Compression::LZ4,
      ::arrow::Compression::ZSTD,         ::arrow::Compression::LZ4_FRAME,
      ::arrow::Compression::BZ2,          ::arrow::Compression::LZ4_HADOOP};

  ASSERT_EQ(valid_arrow_compressions.size(), arrow_compression_enum_size_);

  for (auto arrow_compression : valid_arrow_compressions) {
    EXPECT_NO_THROW(DBPAEnumUtils::ArrowCompressionToDBPA(arrow_compression));
  }
}

TEST_F(DBPAUtilsTest, MapSizeAssertions) {
  // Test the actual map sizes by accessing the enums and the public static maps directly
  // This provides a direct way to verify map completeness

  // Parquet::Type::type assertions
  EXPECT_EQ(parquet_type_enum_size_, 9)
      << "Expected 9 parquet type mappings (excluding UNDEFINED)";

  EXPECT_EQ(parquet_type_enum_size_, DBPAEnumUtils::parquet_to_external_type_map.size())
      << "Expected 9 parquet type mappings (excluding UNDEFINED)";

  EXPECT_EQ(DBPAEnumUtils::parquet_to_external_type_map.size(), 9)
      << "Expected 9 parquet type mappings (excluding UNDEFINED)";

  // Arrow::Compression::type assertions
  EXPECT_EQ(DBPAEnumUtils::arrow_to_external_compression_map.size(), 10)
      << "Expected 10 arrow compression mappings";

  EXPECT_EQ(arrow_compression_enum_size_, 10) << "Expected 10 arrow compression mappings";

  EXPECT_EQ(arrow_compression_enum_size_,
            DBPAEnumUtils::arrow_to_external_compression_map.size())
      << "Expected 10 arrow compression mappings";
}
}  // namespace parquet::encryption::external
