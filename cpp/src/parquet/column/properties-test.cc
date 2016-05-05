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

#include "parquet/column/properties.h"
#include "parquet/file/reader.h"

namespace parquet {

namespace test {

TEST(TestReaderProperties, Basics) {
  ReaderProperties props;

  ASSERT_EQ(DEFAULT_BUFFER_SIZE, props.buffer_size());
  ASSERT_EQ(DEFAULT_USE_BUFFERED_STREAM, props.is_buffered_stream_enabled());
}

TEST(TestWriterProperties, Basics) {
  WriterProperties props;

  ASSERT_EQ(DEFAULT_PAGE_SIZE, props.data_pagesize());
  ASSERT_EQ(DEFAULT_DICTIONARY_PAGE_SIZE, props.dictionary_pagesize());
  ASSERT_EQ(DEFAULT_IS_DICTIONARY_ENABLED, props.is_dictionary_enabled());
}

}  // namespace test
}  // namespace parquet
