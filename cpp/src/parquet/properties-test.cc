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

#include "parquet/file/reader.h"
#include "parquet/properties.h"

namespace parquet {

using schema::ColumnPath;

namespace test {

TEST(TestReaderProperties, Basics) {
  ReaderProperties props;

  ASSERT_EQ(DEFAULT_BUFFER_SIZE, props.buffer_size());
  ASSERT_EQ(DEFAULT_USE_BUFFERED_STREAM, props.is_buffered_stream_enabled());
}

TEST(TestWriterProperties, Basics) {
  std::shared_ptr<WriterProperties> props = WriterProperties::Builder().build();

  ASSERT_EQ(DEFAULT_PAGE_SIZE, props->data_pagesize());
  ASSERT_EQ(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT, props->dictionary_pagesize_limit());
  ASSERT_EQ(DEFAULT_WRITER_VERSION, props->version());
}

TEST(TestWriterProperties, AdvancedHandling) {
  WriterProperties::Builder builder;
  builder.compression("gzip", Compression::GZIP);
  builder.compression("zstd", Compression::ZSTD);
  builder.compression(Compression::SNAPPY);
  builder.encoding(Encoding::DELTA_BINARY_PACKED);
  builder.encoding("delta-length", Encoding::DELTA_LENGTH_BYTE_ARRAY);
  std::shared_ptr<WriterProperties> props = builder.build();

  ASSERT_EQ(Compression::GZIP, props->compression(ColumnPath::FromDotString("gzip")));
  ASSERT_EQ(Compression::ZSTD, props->compression(ColumnPath::FromDotString("zstd")));
  ASSERT_EQ(Compression::SNAPPY,
            props->compression(ColumnPath::FromDotString("delta-length")));
  ASSERT_EQ(Encoding::DELTA_BINARY_PACKED,
            props->encoding(ColumnPath::FromDotString("gzip")));
  ASSERT_EQ(Encoding::DELTA_LENGTH_BYTE_ARRAY,
            props->encoding(ColumnPath::FromDotString("delta-length")));
}

}  // namespace test
}  // namespace parquet
