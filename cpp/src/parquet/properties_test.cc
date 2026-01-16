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

#include "arrow/buffer.h"
#include "arrow/io/memory.h"

#include "parquet/file_reader.h"
#include "parquet/properties.h"

namespace parquet {

using schema::ColumnPath;

namespace test {

TEST(TestReaderProperties, Basics) {
  ReaderProperties props;

  ASSERT_EQ(props.buffer_size(), kDefaultBufferSize);
  ASSERT_EQ(props.footer_read_size(), kDefaultFooterReadSize);
  ASSERT_FALSE(props.is_buffered_stream_enabled());
  ASSERT_FALSE(props.page_checksum_verification());
}

TEST(TestWriterProperties, Basics) {
  std::shared_ptr<WriterProperties> props = WriterProperties::Builder().build();

  ASSERT_EQ(kDefaultDataPageSize, props->data_pagesize());
  ASSERT_EQ(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT, props->dictionary_pagesize_limit());
  ASSERT_EQ(ParquetVersion::PARQUET_2_6, props->version());
  ASSERT_EQ(ParquetDataPageVersion::V1, props->data_page_version());
  ASSERT_FALSE(props->page_checksum_enabled());
}

TEST(TestWriterProperties, DefaultCompression) {
  std::shared_ptr<WriterProperties> props = WriterProperties::Builder().build();

  ASSERT_EQ(props->compression(ColumnPath::FromDotString("any")),
            Compression::UNCOMPRESSED);
  ASSERT_EQ(props->compression_level(ColumnPath::FromDotString("any")),
            ::arrow::util::kUseDefaultCompressionLevel);
}

TEST(TestWriterProperties, AdvancedHandling) {
  WriterProperties::Builder builder;
  builder.compression("gzip", Compression::GZIP);
  builder.compression("zstd", Compression::ZSTD);
  builder.compression(Compression::SNAPPY);
  builder.encoding(Encoding::DELTA_BINARY_PACKED);
  builder.encoding("delta-length", Encoding::DELTA_LENGTH_BYTE_ARRAY);
  builder.data_page_version(ParquetDataPageVersion::V2);
  std::shared_ptr<WriterProperties> props = builder.build();

  ASSERT_EQ(Compression::GZIP, props->compression(ColumnPath::FromDotString("gzip")));
  ASSERT_EQ(Compression::ZSTD, props->compression(ColumnPath::FromDotString("zstd")));
  ASSERT_EQ(Compression::SNAPPY,
            props->compression(ColumnPath::FromDotString("delta-length")));
  ASSERT_EQ(Encoding::DELTA_BINARY_PACKED,
            props->encoding(ColumnPath::FromDotString("gzip")));
  ASSERT_EQ(Encoding::DELTA_LENGTH_BYTE_ARRAY,
            props->encoding(ColumnPath::FromDotString("delta-length")));
  ASSERT_EQ(ParquetDataPageVersion::V2, props->data_page_version());
}

TEST(TestWriterProperties, SetCodecOptions) {
  constexpr int ZSTD_c_windowLog = 101;

  WriterProperties::Builder builder;
  builder.compression("gzip", Compression::GZIP);
  auto zstd_codec_options = std::make_shared<::arrow::util::ZstdCodecOptions>();
  zstd_codec_options->compression_context_params = {{ZSTD_c_windowLog, 23}};
  builder.codec_options("zstd", zstd_codec_options);
  builder.compression("brotli", Compression::BROTLI);
  auto gzip_codec_options = std::make_shared<::arrow::util::GZipCodecOptions>();
  gzip_codec_options->compression_level = 5;
  gzip_codec_options->window_bits = 12;
  builder.codec_options("gzip", gzip_codec_options);
  auto codec_options = std::make_shared<CodecOptions>();
  builder.codec_options(codec_options);
  auto brotli_codec_options = std::make_shared<::arrow::util::BrotliCodecOptions>();
  brotli_codec_options->compression_level = 11;
  brotli_codec_options->window_bits = 20;
  builder.codec_options("brotli", brotli_codec_options);
  std::shared_ptr<WriterProperties> props = builder.build();

  ASSERT_EQ(5,
            props->codec_options(ColumnPath::FromDotString("gzip"))->compression_level);
  ASSERT_EQ(12, std::dynamic_pointer_cast<::arrow::util::GZipCodecOptions>(
                    props->codec_options(ColumnPath::FromDotString("gzip")))
                    ->window_bits);
  ASSERT_EQ(Codec::UseDefaultCompressionLevel(),
            props->codec_options(ColumnPath::FromDotString("zstd"))->compression_level);
  ASSERT_EQ(11,
            props->codec_options(ColumnPath::FromDotString("brotli"))->compression_level);
  ASSERT_EQ(20, std::dynamic_pointer_cast<::arrow::util::BrotliCodecOptions>(
                    props->codec_options(ColumnPath::FromDotString("brotli")))
                    ->window_bits);
}

TEST(TestWriterProperties, ContentDefinedChunkingSettings) {
  WriterProperties::Builder builder;
  std::shared_ptr<WriterProperties> props = builder.build();

  ASSERT_FALSE(props->content_defined_chunking_enabled());
  auto cdc_options = props->content_defined_chunking_options();
  ASSERT_EQ(cdc_options.min_chunk_size, 256 * 1024);
  ASSERT_EQ(cdc_options.max_chunk_size, 1024 * 1024);
  ASSERT_EQ(cdc_options.norm_level, 0);

  builder.enable_content_defined_chunking();
  builder.content_defined_chunking_options(CdcOptions{512 * 1024, 2048 * 1024, 1});
  props = builder.build();
  ASSERT_TRUE(props->content_defined_chunking_enabled());
  cdc_options = props->content_defined_chunking_options();
  ASSERT_EQ(cdc_options.min_chunk_size, 512 * 1024);
  ASSERT_EQ(cdc_options.max_chunk_size, 2048 * 1024);
  ASSERT_EQ(cdc_options.norm_level, 1);
}

TEST(TestReaderProperties, GetStreamInsufficientData) {
  // ARROW-6058
  std::string data = "shorter than expected";
  auto buf = std::make_shared<Buffer>(data);
  auto reader = std::make_shared<::arrow::io::BufferReader>(buf);

  ReaderProperties props;
  try {
    ARROW_UNUSED(props.GetStream(reader, 12, 15));
    FAIL() << "No exception raised";
  } catch (const ParquetException& e) {
    std::string ex_what =
        ("Tried reading 15 bytes starting at position 12"
         " from file but only got 9");
    ASSERT_EQ(ex_what, e.what());
  }
}

struct WriterPropertiesTestCase {
  WriterPropertiesTestCase(std::shared_ptr<WriterProperties> props, std::string label)
      : properties(std::move(props)), label(std::move(label)) {}

  std::shared_ptr<WriterProperties> properties;
  std::string label;
};

void PrintTo(const WriterPropertiesTestCase& p, std::ostream* os) { *os << p.label; }

class WriterPropertiesTest : public testing::TestWithParam<WriterPropertiesTestCase> {};

TEST_P(WriterPropertiesTest, RoundTripThroughBuilder) {
  const std::shared_ptr<WriterProperties>& properties = GetParam().properties;
  const std::vector<std::shared_ptr<ColumnPath>> columns{
      ColumnPath::FromDotString("a"),
      ColumnPath::FromDotString("b"),
  };

  const auto round_tripped = WriterProperties::Builder(*properties).build();

  ASSERT_EQ(round_tripped->content_defined_chunking_enabled(),
            properties->content_defined_chunking_enabled());
  ASSERT_EQ(round_tripped->created_by(), properties->created_by());
  ASSERT_EQ(round_tripped->data_pagesize(), properties->data_pagesize());
  ASSERT_EQ(round_tripped->data_page_version(), properties->data_page_version());
  ASSERT_EQ(round_tripped->dictionary_index_encoding(),
            properties->dictionary_index_encoding());
  ASSERT_EQ(round_tripped->dictionary_pagesize_limit(),
            properties->dictionary_pagesize_limit());
  ASSERT_EQ(round_tripped->file_encryption_properties(),
            properties->file_encryption_properties());
  ASSERT_EQ(round_tripped->max_rows_per_page(), properties->max_rows_per_page());
  ASSERT_EQ(round_tripped->max_row_group_length(), properties->max_row_group_length());
  ASSERT_EQ(round_tripped->memory_pool(), properties->memory_pool());
  ASSERT_EQ(round_tripped->page_checksum_enabled(), properties->page_checksum_enabled());
  ASSERT_EQ(round_tripped->size_statistics_level(), properties->size_statistics_level());
  ASSERT_EQ(round_tripped->sorting_columns(), properties->sorting_columns());
  ASSERT_EQ(round_tripped->store_decimal_as_integer(),
            properties->store_decimal_as_integer());
  ASSERT_EQ(round_tripped->write_batch_size(), properties->write_batch_size());
  ASSERT_EQ(round_tripped->version(), properties->version());

  const auto cdc_options = properties->content_defined_chunking_options();
  const auto round_tripped_cdc_options =
      round_tripped->content_defined_chunking_options();
  ASSERT_EQ(round_tripped_cdc_options.min_chunk_size, cdc_options.min_chunk_size);
  ASSERT_EQ(round_tripped_cdc_options.max_chunk_size, cdc_options.max_chunk_size);
  ASSERT_EQ(round_tripped_cdc_options.norm_level, cdc_options.norm_level);

  for (const auto& column : columns) {
    const auto& column_properties = properties->column_properties(column);
    const auto& round_tripped_col = round_tripped->column_properties(column);

    ASSERT_EQ(round_tripped_col.compression(), column_properties.compression());
    ASSERT_EQ(round_tripped_col.compression_level(),
              column_properties.compression_level());
    ASSERT_EQ(round_tripped_col.dictionary_enabled(),
              column_properties.dictionary_enabled());
    ASSERT_EQ(round_tripped_col.encoding(), column_properties.encoding());
    ASSERT_EQ(round_tripped_col.max_statistics_size(),
              column_properties.max_statistics_size());
    ASSERT_EQ(round_tripped_col.page_index_enabled(),
              column_properties.page_index_enabled());
    ASSERT_EQ(round_tripped_col.statistics_enabled(),
              column_properties.statistics_enabled());
  }
}

std::vector<WriterPropertiesTestCase> writer_properties_test_cases() {
  std::vector<WriterPropertiesTestCase> test_cases;

  test_cases.emplace_back(default_writer_properties(), "default_properties");

  {
    WriterProperties::Builder builder;
    const auto column_a = ColumnPath::FromDotString("a");

    builder.created_by("parquet-cpp-properties-test");
    builder.encoding(Encoding::BYTE_STREAM_SPLIT);
    builder.compression(Compression::ZSTD);
    builder.compression_level(2);
    builder.disable_dictionary();
    builder.disable_statistics();
    builder.enable_content_defined_chunking();
    builder.dictionary_pagesize_limit(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT - 1);
    builder.write_batch_size(DEFAULT_WRITE_BATCH_SIZE - 1);
    builder.max_row_group_length(DEFAULT_MAX_ROW_GROUP_LENGTH - 1);
    builder.data_pagesize(kDefaultDataPageSize - 1);
    builder.max_rows_per_page(kDefaultMaxRowsPerPage - 1);
    builder.data_page_version(ParquetDataPageVersion::V2);
    builder.version(ParquetVersion::type::PARQUET_2_4);
    builder.enable_store_decimal_as_integer();
    builder.disable_write_page_index();
    builder.set_size_statistics_level(SizeStatisticsLevel::ColumnChunk);
    builder.set_sorting_columns(
        std::vector<SortingColumn>{SortingColumn{1, true, false}});

    test_cases.emplace_back(builder.build(), "override_defaults");
  }

  const auto column_a = ColumnPath::FromDotString("a");
  {
    WriterProperties::Builder builder;
    builder.disable_dictionary();
    builder.enable_dictionary(column_a);
    test_cases.emplace_back(builder.build(), "dictionary_column_override");
  }
  {
    WriterProperties::Builder builder;
    builder.disable_statistics();
    builder.enable_statistics(column_a);
    test_cases.emplace_back(builder.build(), "statistics_column_override");
  }
  {
    WriterProperties::Builder builder;
    builder.compression(Compression::SNAPPY);
    builder.compression(column_a, Compression::UNCOMPRESSED);
    builder.compression_level(column_a, 2);
    test_cases.emplace_back(builder.build(), "compression_column_override");
  }
  {
    WriterProperties::Builder builder;
    builder.encoding(Encoding::UNDEFINED);
    builder.encoding(column_a, Encoding::BYTE_STREAM_SPLIT);
    test_cases.emplace_back(builder.build(), "encoding_column_override");
  }
  {
    WriterProperties::Builder builder;
    builder.disable_write_page_index();
    builder.enable_write_page_index(column_a);
    test_cases.emplace_back(builder.build(), "page_index_column_override");
  }

  return test_cases;
}

INSTANTIATE_TEST_SUITE_P(WriterPropertiesTest, WriterPropertiesTest,
                         ::testing::ValuesIn(writer_properties_test_cases()));

}  // namespace test
}  // namespace parquet
