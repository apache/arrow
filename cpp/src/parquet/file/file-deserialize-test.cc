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

#include <algorithm>
#include <cstdlib>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>

#include "parquet/column/page.h"
#include "parquet/column/test-util.h"

#include "parquet/file/reader-internal.h"
#include "parquet/thrift/parquet_types.h"
#include "parquet/thrift/util.h"
#include "parquet/types.h"
#include "parquet/util/input.h"

namespace parquet_cpp {

class TestSerializedPage : public ::testing::Test {
 public:
  void InitSerializedPageReader(const uint8_t* buffer, size_t header_size,
      Compression::type codec) {
    std::unique_ptr<InputStream> stream;
    stream.reset(new InMemoryInputStream(buffer, header_size));
    page_reader_.reset(new SerializedPageReader(std::move(stream), codec));
  }

 protected:
  std::unique_ptr<SerializedPageReader> page_reader_;
};

TEST_F(TestSerializedPage, TestLargePageHeaders) {
  parquet::PageHeader in_page_header;
  parquet::DataPageHeader data_page_header;
  parquet::PageHeader out_page_header;
  parquet::Statistics stats;
  int expected_header_size = 512 * 1024; //512 KB
  int stats_size = 256 * 1024; // 256 KB
  std::string serialized_buffer;
  int num_values = 4141;

  InitStats(stats_size, stats);
  InitDataPage(stats, data_page_header, num_values);
  InitPageHeader(data_page_header, in_page_header);

  // Serialize the Page header
  ASSERT_NO_THROW(serialized_buffer = SerializeThriftMsg(&in_page_header,
      expected_header_size));
  // check header size is between 256 KB to 16 MB
  ASSERT_LE(stats_size, serialized_buffer.length());
  ASSERT_GE(DEFAULT_MAX_PAGE_HEADER_SIZE, serialized_buffer.length());

  InitSerializedPageReader(reinterpret_cast<const uint8_t*>(serialized_buffer.c_str()),
      serialized_buffer.length(), Compression::UNCOMPRESSED);

  std::shared_ptr<Page> current_page = page_reader_->NextPage();
  ASSERT_EQ(PageType::DATA_PAGE, current_page->type());
  const DataPage* page = static_cast<const DataPage*>(current_page.get());
  ASSERT_EQ(num_values, page->num_values());
}

TEST_F(TestSerializedPage, TestFailLargePageHeaders) {
  parquet::PageHeader in_page_header;
  parquet::DataPageHeader data_page_header;
  parquet::PageHeader out_page_header;
  parquet::Statistics stats;
  int expected_header_size = 512 * 1024; // 512 KB
  int stats_size = 256 * 1024; // 256 KB
  int max_header_size = 128 * 1024; // 128 KB
  int num_values = 4141;
  std::string serialized_buffer;

  InitStats(stats_size, stats);
  InitDataPage(stats, data_page_header, num_values);
  InitPageHeader(data_page_header, in_page_header);

  // Serialize the Page header
  ASSERT_NO_THROW(serialized_buffer = SerializeThriftMsg(&in_page_header,
      expected_header_size));
  // check header size is between 256 KB to 16 MB
  ASSERT_LE(stats_size, serialized_buffer.length());
  ASSERT_GE(DEFAULT_MAX_PAGE_HEADER_SIZE, serialized_buffer.length());

  InitSerializedPageReader(reinterpret_cast<const uint8_t*>(serialized_buffer.c_str()),
      serialized_buffer.length(), Compression::UNCOMPRESSED);

  // Set the max page header size to 128 KB, which is less than the current header size
  page_reader_->set_max_page_header_size(max_header_size);

  ASSERT_THROW(page_reader_->NextPage(), ParquetException);
}
} // namespace parquet_cpp
