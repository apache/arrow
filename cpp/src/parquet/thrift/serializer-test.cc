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

#include <cstdint>
#include <exception>
#include <string>

#include "parquet/column/test-util.h"
#include "parquet/thrift/parquet_types.h"
#include "parquet/thrift/util.h"

using std::string;

namespace parquet_cpp {

class TestThrift : public ::testing::Test {

};

TEST_F(TestThrift, TestSerializerDeserializer) {
  parquet::PageHeader in_page_header;
  parquet::DataPageHeader data_page_header;
  parquet::PageHeader out_page_header;
  parquet::Statistics stats;
  uint32_t max_header_len = 1024;
  uint32_t expected_header_size = 1024;
  uint32_t stats_size = 512;
  std::string serialized_buffer;
  int num_values = 4444;

  InitStats(stats_size, stats);
  InitDataPage(stats, data_page_header, num_values);
  InitPageHeader(data_page_header, in_page_header);

  // Serialize the Page header
  ASSERT_NO_THROW(serialized_buffer = SerializeThriftMsg(&in_page_header, expected_header_size));
  ASSERT_LE(stats_size, serialized_buffer.length());
  ASSERT_GE(max_header_len, serialized_buffer.length());

  uint32_t header_size = 1024;
  // Deserialize the serialized page buffer
  ASSERT_NO_THROW(DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(serialized_buffer.c_str()),
      &header_size, &out_page_header));
  ASSERT_LE(stats_size, header_size);
  ASSERT_GE(max_header_len, header_size);

  ASSERT_EQ(parquet::Encoding::PLAIN, out_page_header.data_page_header.encoding);
  ASSERT_EQ(parquet::Encoding::RLE, out_page_header.data_page_header.definition_level_encoding);
  ASSERT_EQ(parquet::Encoding::RLE, out_page_header.data_page_header.repetition_level_encoding);
  for(int i = 0; i < stats_size; i++){
    EXPECT_EQ(i % 255, (reinterpret_cast<const uint8_t*>
        (out_page_header.data_page_header.statistics.max.c_str()))[i]);
  }
  ASSERT_EQ(parquet::PageType::DATA_PAGE, out_page_header.type);
  ASSERT_EQ(num_values, out_page_header.data_page_header.num_values);

}

} // namespace parquet_cpp
