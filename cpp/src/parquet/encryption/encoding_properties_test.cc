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

#include <map>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "parquet/column_page.h"
#include "parquet/encryption/encoding_properties.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace parquet::encryption::test {

using ::parquet::ColumnDescriptor;
using ::parquet::Encoding;
using ::parquet::PageType;
using ::parquet::Type;

static std::shared_ptr<::parquet::SchemaDescriptor> MakeSingleInt32Schema(
    const std::string& col_name = "col") {
  using ::parquet::schema::GroupNode;
  using ::parquet::schema::NodePtr;
  using ::parquet::schema::NodeVector;
  using ::parquet::schema::PrimitiveNode;

  NodeVector fields;
  fields.push_back(
      PrimitiveNode::Make(col_name, ::parquet::Repetition::REQUIRED, Type::INT32));
  NodePtr schema = GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, fields);

  auto descr = std::make_shared<::parquet::SchemaDescriptor>();
  descr->Init(schema);
  return descr;
}

TEST(EncodingPropertiesTest, BuilderRequiresPageType) {
  auto builder = EncodingProperties::Builder();
  builder.ColumnPath("a");
  builder.PhysicalType(Type::INT32);
  builder.CompressionCodec(::arrow::Compression::SNAPPY);
  EXPECT_THROW(builder.Build(), std::invalid_argument);
}

TEST(EncodingPropertiesTest, DictionaryPageSettersAndToMap) {
  auto props = EncodingProperties::Builder()
                   .PageType(PageType::DICTIONARY_PAGE)
                   .PageEncoding(Encoding::PLAIN)
                   .Build();

  props->set_column_path("schema.col");
  props->set_physical_type(Type::DOUBLE, std::nullopt);
  props->set_compression_codec(::arrow::Compression::ZSTD);

  EXPECT_NO_THROW(props->validate());

  auto m = props->ToPropertiesMap();
  ASSERT_EQ(m.at("column_path"), std::string("schema.col"));
  ASSERT_EQ(m.at("physical_type"), std::string("DOUBLE"));
  ASSERT_EQ(m.at("compression_codec"), std::string("ZSTD"));
  ASSERT_EQ(m.at("page_type"), std::string("DICTIONARY_PAGE"));
  ASSERT_EQ(m.at("page_encoding"), std::string("PLAIN"));
}

TEST(EncodingPropertiesTest, BuilderDataPageV1ValidationSuccessAndMap) {
  auto props = EncodingProperties::Builder()
                   .ColumnPath("col")
                   .PhysicalType(Type::INT32)
                   .CompressionCodec(::arrow::Compression::SNAPPY)
                   .PageType(PageType::DATA_PAGE)
                   .PageEncoding(Encoding::PLAIN)
                   .DataPageNumValues(123)
                   .PageV1DefinitionLevelEncoding(Encoding::RLE)
                   .PageV1RepetitionLevelEncoding(Encoding::RLE)
                   .DataPageMaxDefinitionLevel(1)
                   .DataPageMaxRepetitionLevel(0)
                   .Build();

  EXPECT_NO_THROW(props->validate());

  auto m = props->ToPropertiesMap();
  ASSERT_EQ(m.at("column_path"), std::string("col"));
  ASSERT_EQ(m.at("physical_type"), std::string("INT32"));
  ASSERT_EQ(m.at("compression_codec"), std::string("SNAPPY"));
  ASSERT_EQ(m.at("page_type"), std::string("DATA_PAGE_V1"));
  ASSERT_EQ(m.at("page_encoding"), std::string("PLAIN"));
  ASSERT_EQ(m.at("data_page_num_values"), std::to_string(123));
  ASSERT_EQ(m.at("data_page_max_definition_level"), std::to_string(1));
  ASSERT_EQ(m.at("data_page_max_repetition_level"), std::to_string(0));
  ASSERT_EQ(m.at("page_v1_definition_level_encoding"), std::string("RLE"));
  ASSERT_EQ(m.at("page_v1_repetition_level_encoding"), std::string("RLE"));
}

TEST(EncodingPropertiesTest, BuilderDataPageV2ValidationSuccessAndMap) {
  auto props = EncodingProperties::Builder()
                   .ColumnPath("col")
                   .PhysicalType(Type::BYTE_ARRAY)
                   .CompressionCodec(::arrow::Compression::ZSTD)
                   .PageType(PageType::DATA_PAGE_V2)
                   .PageEncoding(Encoding::DELTA_LENGTH_BYTE_ARRAY)
                   .DataPageNumValues(42)
                   .PageV2DefinitionLevelsByteLength(8)
                   .PageV2RepetitionLevelsByteLength(4)
                   .PageV2NumNulls(5)
                   .PageV2IsCompressed(true)
                   .DataPageMaxDefinitionLevel(1)
                   .DataPageMaxRepetitionLevel(0)
                   .Build();

  EXPECT_NO_THROW(props->validate());
  auto m = props->ToPropertiesMap();
  ASSERT_EQ(m.at("column_path"), std::string("col"));
  ASSERT_EQ(m.at("physical_type"), std::string("BYTE_ARRAY"));
  ASSERT_EQ(m.at("compression_codec"), std::string("ZSTD"));
  ASSERT_EQ(m.at("page_type"), std::string("DATA_PAGE_V2"));
  ASSERT_EQ(m.at("page_encoding"), std::string("DELTA_LENGTH_BYTE_ARRAY"));
  ASSERT_EQ(m.at("data_page_num_values"), std::to_string(42));
  ASSERT_EQ(m.at("data_page_max_definition_level"), std::to_string(1));
  ASSERT_EQ(m.at("data_page_max_repetition_level"), std::to_string(0));
  ASSERT_EQ(m.at("page_v2_definition_levels_byte_length"), std::to_string(8));
  ASSERT_EQ(m.at("page_v2_repetition_levels_byte_length"), std::to_string(4));
  ASSERT_EQ(m.at("page_v2_num_nulls"), std::to_string(5));
  ASSERT_EQ(m.at("page_v2_is_compressed"), "true");
}

TEST(EncodingPropertiesTest, BuilderDataPageV2MissingFieldsValidationFails) {
  auto props = EncodingProperties::Builder()
                   .ColumnPath("col")
                   .PhysicalType(Type::INT32)
                   .CompressionCodec(::arrow::Compression::GZIP)
                   .PageType(PageType::DATA_PAGE_V2)
                   .PageEncoding(Encoding::DELTA_BINARY_PACKED)
                   .DataPageNumValues(10)
                   .PageV2DefinitionLevelsByteLength(4)
                   .PageV2RepetitionLevelsByteLength(4)
                   // Intentionally omit PageV2NumNulls and PageV2IsCompressed
                   .Build();

  EXPECT_THROW(props->validate(), std::invalid_argument);
}

TEST(EncodingPropertiesTest, FixedLengthBytesWrongUsageThrows) {
  auto props = EncodingProperties::Builder()
                   .ColumnPath("col")
                   .PhysicalType(Type::INT32)
                   .CompressionCodec(::arrow::Compression::SNAPPY)
                   .PageType(PageType::DATA_PAGE)
                   .PageEncoding(Encoding::PLAIN)
                   .DataPageNumValues(5)
                   .PageV1DefinitionLevelEncoding(Encoding::RLE)
                   .PageV1RepetitionLevelEncoding(Encoding::RLE)
                   .Build();

  // Set a fixed length while physical type is not FIXED_LEN_BYTE_ARRAY
  props->set_physical_type(Type::INT32, std::optional<std::int64_t>(16));
  EXPECT_THROW(props->validate(), std::invalid_argument);
}

TEST(EncodingPropertiesTest, BuilderOptionalFixedLengthBytesAndToMap) {
  auto props = EncodingProperties::Builder()
                   .PageType(PageType::DICTIONARY_PAGE)
                   .PageEncoding(Encoding::RLE_DICTIONARY)
                   .Build();
  props->set_column_path("fixed_col");
  props->set_physical_type(Type::FIXED_LEN_BYTE_ARRAY, std::optional<std::int64_t>(16));
  props->set_compression_codec(::arrow::Compression::LZ4);

  EXPECT_NO_THROW(props->validate());
  auto m = props->ToPropertiesMap();
  ASSERT_EQ(m.at("fixed_length_bytes"), std::to_string(16));
}

TEST(EncodingPropertiesTest, MissingPageEncodingThrows) {
  auto props = EncodingProperties::Builder().PageType(PageType::DICTIONARY_PAGE).Build();
  props->set_column_path("schema.col");

  EXPECT_THROW(props->validate(), std::invalid_argument);
}

TEST(EncodingPropertiesTest, MakeFromMetadataDataPageV1) {
  auto schema = MakeSingleInt32Schema();
  const ColumnDescriptor* descr = schema->Column(0);

  // WriterProperties with explicit compression for path "col"
  auto path = parquet::schema::ColumnPath::FromDotString("col");
  parquet::WriterProperties::Builder wp_builder;
  wp_builder.compression(path, ::arrow::Compression::SNAPPY);
  auto writer_props = wp_builder.build();

  // Build a V1 data page
  auto buffer = ::parquet::AllocateBuffer();
  parquet::DataPageV1 page(buffer, /*num_values=*/7, Encoding::PLAIN, Encoding::RLE,
                           Encoding::RLE, /*uncompressed_size=*/0);

  auto props = EncodingProperties::MakeFromMetadata(descr, writer_props.get(), page);
  // Should be valid and have all keys
  EXPECT_NO_THROW(props->validate());
  auto m = props->ToPropertiesMap();
  ASSERT_EQ(m.at("column_path"), std::string("col"));
  ASSERT_EQ(m.at("physical_type"), std::string("INT32"));
  ASSERT_EQ(m.at("compression_codec"), std::string("SNAPPY"));
  ASSERT_EQ(m.at("page_type"), std::string("DATA_PAGE_V1"));
  ASSERT_EQ(m.at("page_encoding"), std::string("PLAIN"));
  ASSERT_EQ(m.at("data_page_num_values"), std::to_string(7));
  ASSERT_EQ(m.at("page_v1_definition_level_encoding"), std::string("RLE"));
  ASSERT_EQ(m.at("page_v1_repetition_level_encoding"), std::string("RLE"));
  ASSERT_EQ(m.at("data_page_max_definition_level"), std::to_string(0));
  ASSERT_EQ(m.at("data_page_max_repetition_level"), std::to_string(0));
}

TEST(EncodingPropertiesTest, MakeFromMetadataDictionaryPage) {
  auto schema = MakeSingleInt32Schema();
  const ColumnDescriptor* descr = schema->Column(0);

  auto path = parquet::schema::ColumnPath::FromDotString("col");
  parquet::WriterProperties::Builder wp_builder;
  wp_builder.compression(path, ::arrow::Compression::GZIP);
  auto writer_props = wp_builder.build();

  auto buffer = ::parquet::AllocateBuffer();
  parquet::DictionaryPage page(buffer, /*num_values=*/4, Encoding::RLE_DICTIONARY);

  auto props = EncodingProperties::MakeFromMetadata(descr, writer_props.get(), page);
  EXPECT_NO_THROW(props->validate());
  auto m = props->ToPropertiesMap();
  ASSERT_EQ(m.at("column_path"), std::string("col"));
  ASSERT_EQ(m.at("physical_type"), std::string("INT32"));
  ASSERT_EQ(m.at("compression_codec"), std::string("GZIP"));
  ASSERT_EQ(m.at("page_type"), std::string("DICTIONARY_PAGE"));
  ASSERT_EQ(m.at("page_encoding"), std::string("RLE_DICTIONARY"));
}

TEST(EncodingPropertiesTest, MakeFromMetadataDataPageV2ValidationAndMap) {
  auto schema = MakeSingleInt32Schema();
  const ColumnDescriptor* descr = schema->Column(0);

  auto path = parquet::schema::ColumnPath::FromDotString("col");
  parquet::WriterProperties::Builder wp_builder;
  wp_builder.compression(path, ::arrow::Compression::ZSTD);
  auto writer_props = wp_builder.build();

  auto buffer = ::parquet::AllocateBuffer();
  parquet::DataPageV2 page(buffer, /*num_values=*/5, /*num_nulls=*/2, /*num_rows=*/5,
                           Encoding::DELTA_BYTE_ARRAY,
                           /*definition_levels_byte_length=*/3,
                           /*repetition_levels_byte_length=*/2,
                           /*uncompressed_size=*/0, /*is_compressed=*/true);

  auto props = EncodingProperties::MakeFromMetadata(descr, writer_props.get(), page);
  EXPECT_NO_THROW(props->validate());

  auto m = props->ToPropertiesMap();
  ASSERT_EQ(m.at("column_path"), std::string("col"));
  ASSERT_EQ(m.at("physical_type"), std::string("INT32"));
  ASSERT_EQ(m.at("compression_codec"), std::string("ZSTD"));
  ASSERT_EQ(m.at("page_type"), std::string("DATA_PAGE_V2"));
  ASSERT_EQ(m.at("page_encoding"), std::string("DELTA_BYTE_ARRAY"));
  ASSERT_EQ(m.at("data_page_num_values"), std::to_string(5));
  ASSERT_EQ(m.at("data_page_max_definition_level"), std::to_string(0));
  ASSERT_EQ(m.at("data_page_max_repetition_level"), std::to_string(0));
  ASSERT_EQ(m.at("page_v2_definition_levels_byte_length"), std::to_string(3));
  ASSERT_EQ(m.at("page_v2_repetition_levels_byte_length"), std::to_string(2));
  ASSERT_EQ(m.at("page_v2_num_nulls"), std::to_string(2));
  ASSERT_EQ(m.at("page_v2_is_compressed"), "true");
}

TEST(EncodingPropertiesTest, MakeFromMetadataUnknownPageTypeThrows) {
  auto schema = MakeSingleInt32Schema();
  const ColumnDescriptor* descr = schema->Column(0);

  auto path = parquet::schema::ColumnPath::FromDotString("col");
  parquet::WriterProperties::Builder wp_builder;
  wp_builder.compression(path, ::arrow::Compression::SNAPPY);
  auto writer_props = wp_builder.build();

  auto buffer = ::parquet::AllocateBuffer();
  parquet::Page index_page(buffer, PageType::INDEX_PAGE);

  EXPECT_THROW(
      EncodingProperties::MakeFromMetadata(descr, writer_props.get(), index_page),
      std::invalid_argument);
}

TEST(EncodingPropertiesTest, MakeFromMetadataFixedLenByteArrayPropagatesLength) {
  using ::parquet::schema::GroupNode;
  using ::parquet::schema::NodePtr;
  using ::parquet::schema::NodeVector;
  using ::parquet::schema::PrimitiveNode;

  // Build a schema with a FIXED_LEN_BYTE_ARRAY(16) column named "col"
  NodeVector fields;
  fields.push_back(PrimitiveNode::Make("col", ::parquet::Repetition::REQUIRED,
                                       Type::FIXED_LEN_BYTE_ARRAY,
                                       ::parquet::ConvertedType::NONE,
                                       /*type_length=*/16));
  NodePtr schema = GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, fields);
  auto descr = std::make_shared<::parquet::SchemaDescriptor>();
  descr->Init(schema);

  const ColumnDescriptor* col_descr = descr->Column(0);

  // WriterProperties for path "col"
  auto path = parquet::schema::ColumnPath::FromDotString("col");
  parquet::WriterProperties::Builder wp_builder;
  wp_builder.compression(path, ::arrow::Compression::SNAPPY);
  auto writer_props = wp_builder.build();

  // Use a DATA_PAGE_V1 with minimal valid settings
  auto buffer = ::parquet::AllocateBuffer();
  parquet::DataPageV1 page(buffer, /*num_values=*/3, Encoding::PLAIN, Encoding::RLE,
                           Encoding::RLE, /*uncompressed_size=*/0);

  auto props = EncodingProperties::MakeFromMetadata(col_descr, writer_props.get(), page);
  EXPECT_NO_THROW(props->validate());
  auto m = props->ToPropertiesMap();
  ASSERT_EQ(m.at("column_path"), std::string("col"));
  ASSERT_EQ(m.at("physical_type"), std::string("FIXED_LEN_BYTE_ARRAY"));
  ASSERT_EQ(m.at("compression_codec"), std::string("SNAPPY"));
  ASSERT_EQ(m.at("page_type"), std::string("DATA_PAGE_V1"));
  ASSERT_EQ(m.at("page_encoding"), std::string("PLAIN"));
  ASSERT_EQ(m.at("fixed_length_bytes"), std::to_string(16));
}

TEST(EncodingPropertiesTest, DataPageV1MissingNumValuesThrows) {
  auto props = EncodingProperties::Builder()
                   .ColumnPath("col")
                   .PhysicalType(Type::DOUBLE)
                   .CompressionCodec(::arrow::Compression::SNAPPY)
                   .PageType(PageType::DATA_PAGE)
                   .PageEncoding(Encoding::PLAIN)
                   .PageV1DefinitionLevelEncoding(Encoding::RLE)
                   .PageV1RepetitionLevelEncoding(Encoding::RLE)
                   .Build();

  EXPECT_THROW(props->validate(), std::invalid_argument);
}

TEST(EncodingPropertiesTest, SequentialFailuresForDataPageV1RequiredFields) {
  // Start with minimal setup for a DATA_PAGE and add required fields step by step
  auto builder = EncodingProperties::Builder()
                     .ColumnPath("col")
                     .PhysicalType(Type::INT32)
                     .CompressionCodec(::arrow::Compression::SNAPPY)
                     .PageType(PageType::DATA_PAGE)
                     .PageEncoding(Encoding::PLAIN);

  auto props = builder.Build();
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing num_values

  builder.DataPageNumValues(10);
  props = builder.Build();
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing max def level

  builder.DataPageMaxDefinitionLevel(0);
  props = builder.Build();
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing max rep level

  builder.DataPageMaxRepetitionLevel(0);
  props = builder.Build();
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing V1 deflvl encoding

  builder.PageV1DefinitionLevelEncoding(Encoding::RLE);
  props = builder.Build();
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing V1 replvl encoding

  builder.PageV1RepetitionLevelEncoding(Encoding::RLE);
  props = builder.Build();
  EXPECT_NO_THROW(props->validate());
}

TEST(EncodingPropertiesTest, SequentialFailuresForDataPageV2RequiredFields) {
  // Start with minimal setup for a DATA_PAGE_V2 and add required fields step by step
  auto builder = EncodingProperties::Builder()
                     .ColumnPath("col")
                     .PhysicalType(Type::INT32)
                     .CompressionCodec(::arrow::Compression::ZSTD)
                     .PageType(PageType::DATA_PAGE_V2)
                     .PageEncoding(Encoding::DELTA_BINARY_PACKED);

  auto props = builder.Build();
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing num_values

  builder.DataPageNumValues(5);
  props = builder.Build();
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing max def level

  builder.DataPageMaxDefinitionLevel(0);
  props = builder.Build();
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing max rep level

  builder.DataPageMaxRepetitionLevel(0);
  props = builder.Build();
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing v2 num_nulls

  builder.PageV2NumNulls(2);
  props = builder.Build();
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing v2 deflvl bytes

  builder.PageV2DefinitionLevelsByteLength(3);
  props = builder.Build();
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing v2 replvl bytes

  builder.PageV2RepetitionLevelsByteLength(2);
  props = builder.Build();
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing v2 is_compressed

  builder.PageV2IsCompressed(true);
  props = builder.Build();
  EXPECT_NO_THROW(props->validate());
}

TEST(EncodingPropertiesTest, SequentialFailuresForDictionaryPageRequiredFields) {
  // Dictionary pages require: column path, physical type, compression codec, page type,
  // and page encoding. We'll add them incrementally.
  auto builder = EncodingProperties::Builder();

  // Missing everything => Build should throw because page type is required at build time
  EXPECT_THROW(builder.Build(), std::invalid_argument);

  builder.PageType(PageType::DICTIONARY_PAGE);
  auto props = builder.Build();
  // Now validation fails due to missing encoding and column-level properties
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing encoding

  builder.PageEncoding(Encoding::RLE_DICTIONARY);
  props = builder.Build();
  // Still missing column/path
  EXPECT_THROW(props->validate(), std::invalid_argument);  // missing column path

  props->set_column_path("schema.col");
  // Dictionary page doesn't require physical type or compression to validate
  EXPECT_NO_THROW(props->validate());
}

}  // namespace parquet::encryption::test
