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
  ASSERT_FALSE(props.is_buffered_stream_enabled());
  ASSERT_FALSE(props.page_checksum_verification());
}

TEST(TestWriterProperties, Basics) {
  std::shared_ptr<WriterProperties> props = WriterProperties::Builder().build();

  ASSERT_EQ(kDefaultDataPageSize, props->data_pagesize());
  ASSERT_EQ(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT, props->dictionary_pagesize_limit());
  ASSERT_EQ(ParquetVersion::PARQUET_2_4, props->version());
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

}  // namespace test
}  // namespace parquet
