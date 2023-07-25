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
#include "arrow/io/buffered.h"
#include "arrow/io/memory.h"

#include "parquet/file_reader.h"
#include "parquet/properties.h"

namespace parquet {

using schema::ColumnPath;

namespace test {

TEST(TestReaderProperties, Basics) {
  ReaderProperties props;

  ASSERT_EQ(props.buffer_size(), kDefaultBufferSize);
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
  WriterProperties::Builder builder;
  builder.compression("gzip", Compression::GZIP);
  builder.compression("zstd", Compression::ZSTD);
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

TEST(TestReaderProperties, ColumnChunkSpecificBufferSize) {
  ReaderProperties props;
  props.set_buffer_size(8);
  ASSERT_EQ(props.column_reader_properties(), nullptr);

  props.set_buffer_size(16, /*row_group_index=*/0, /*column_index=*/1);
  props.set_buffer_size(32, /*row_group_index=*/2, /*column_index=*/0);
  props.set_buffer_size(64, /*row_group_index=*/2, /*column_index=*/1);
  ASSERT_NE(props.column_reader_properties(), nullptr);
  ASSERT_EQ(props.column_reader_properties()->size(), 2);

  ASSERT_EQ(props.buffer_size(), 8);
  ASSERT_EQ(props.buffer_size(/*row_group_index=*/0, /*column_index=*/0), 8);
  ASSERT_EQ(props.buffer_size(/*row_group_index=*/0, /*column_index=*/1), 16);
  ASSERT_EQ(props.buffer_size(/*row_group_index=*/1, /*column_index=*/0), 8);
  ASSERT_EQ(props.buffer_size(/*row_group_index=*/2, /*column_index=*/0), 32);
  ASSERT_EQ(props.buffer_size(/*row_group_index=*/2, /*column_index=*/1), 64);

  props.set_column_reader_properties(nullptr);
  ASSERT_EQ(props.buffer_size(/*row_group_index=*/0, /*column_index=*/0), 8);
  ASSERT_EQ(props.buffer_size(/*row_group_index=*/0, /*column_index=*/1), 8);
  ASSERT_EQ(props.buffer_size(/*row_group_index=*/1, /*column_index=*/0), 8);
  ASSERT_EQ(props.buffer_size(/*row_group_index=*/2, /*column_index=*/0), 8);
  ASSERT_EQ(props.buffer_size(/*row_group_index=*/2, /*column_index=*/1), 8);
}

TEST(TestReaderProperties, GetStreamCustomizedBufferSize) {
  std::string data = "shorter than expected";
  auto buf = std::make_shared<Buffer>(data);
  auto reader = std::make_shared<::arrow::io::BufferReader>(buf);

  ReaderProperties props;
  props.enable_buffered_stream();
  props.set_buffer_size(8);

  try {
    // Set buffer_size as 0 will not use buffered stream, although buffered stream
    // is enabled above.
    ARROW_UNUSED(props.GetStream(reader, /*start=*/12, /*num_bytes=*/15,
                                 /*custom_buffer_size=*/std::make_optional<int64_t>(0)));
    FAIL() << "No exception raised";
  } catch (const ParquetException& e) {
    std::string ex_what =
        ("Tried reading 15 bytes starting at position 12"
         " from file but only got 9");
    ASSERT_EQ(ex_what, e.what());
  }

  // Honor the customized buffer_size 16 over the default 8 set in props.
  std::shared_ptr<ArrowInputStream> input_stream =
      props.GetStream(reader, /*start=*/0, /*num_bytes=*/16,
                      /*custom_buffer_size=*/std::make_optional<int64_t>(16));
  ::arrow::io::BufferedInputStream* buffered_input_stream =
      static_cast<::arrow::io::BufferedInputStream*>(input_stream.get());
  ASSERT_EQ(buffered_input_stream->buffer_size(), 16);
}

}  // namespace test
}  // namespace parquet
