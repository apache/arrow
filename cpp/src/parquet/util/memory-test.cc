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

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "parquet/exception.h"
#include "parquet/util/memory.h"
#include "parquet/util/test-common.h"

using arrow::default_memory_pool;
using arrow::MemoryPool;

namespace parquet {

class TestBuffer : public ::testing::Test {};

TEST(TestBufferedInputStream, Basics) {
  int64_t source_size = 256;
  int64_t stream_offset = 10;
  int64_t stream_size = source_size - stream_offset;
  int64_t chunk_size = 50;
  std::shared_ptr<ResizableBuffer> buf =
      AllocateBuffer(default_memory_pool(), source_size);
  ASSERT_EQ(source_size, buf->size());
  for (int i = 0; i < source_size; i++) {
    buf->mutable_data()[i] = static_cast<uint8_t>(i);
  }

  auto wrapper =
      std::make_shared<ArrowInputFile>(std::make_shared<::arrow::io::BufferReader>(buf));

  std::unique_ptr<BufferedInputStream> stream(new BufferedInputStream(
      default_memory_pool(), chunk_size, wrapper.get(), stream_offset, stream_size));

  const uint8_t* output;
  int64_t bytes_read;

  // source is at offset 10
  output = stream->Peek(10, &bytes_read);
  ASSERT_EQ(10, bytes_read);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(10 + i, output[i]) << i;
  }
  output = stream->Read(10, &bytes_read);
  ASSERT_EQ(10, bytes_read);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(10 + i, output[i]) << i;
  }
  output = stream->Read(10, &bytes_read);
  ASSERT_EQ(10, bytes_read);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(20 + i, output[i]) << i;
  }
  stream->Advance(5);
  stream->Advance(5);
  // source is at offset 40
  // read across buffer boundary. buffer size is 50
  output = stream->Read(20, &bytes_read);
  ASSERT_EQ(20, bytes_read);
  for (int i = 0; i < 20; i++) {
    ASSERT_EQ(40 + i, output[i]) << i;
  }
  // read more than original chunk_size
  output = stream->Read(60, &bytes_read);
  ASSERT_EQ(60, bytes_read);
  for (int i = 0; i < 60; i++) {
    ASSERT_EQ(60 + i, output[i]) << i;
  }

  stream->Advance(120);
  // source is at offset 240
  // read outside of source boundary. source size is 256
  output = stream->Read(30, &bytes_read);
  ASSERT_EQ(16, bytes_read);
  for (int i = 0; i < 16; i++) {
    ASSERT_EQ(240 + i, output[i]) << i;
  }
}

TEST(TestArrowInputFile, ReadAt) {
  std::string data = "this is the data";
  auto data_buffer = reinterpret_cast<const uint8_t*>(data.c_str());

  auto file = std::make_shared<::arrow::io::BufferReader>(data_buffer, data.size());
  auto source = std::make_shared<ArrowInputFile>(file);

  ASSERT_EQ(0, source->Tell());

  uint8_t buffer[50];

  ASSERT_NO_THROW(source->ReadAt(0, 4, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, "this", 4));

  // Note: it's undefined (and possibly platform-dependent) whether ArrowInputFile
  // updates the file position after ReadAt().
}

TEST(TestArrowInputFile, Read) {
  std::string data = "this is the data";
  auto data_buffer = reinterpret_cast<const uint8_t*>(data.c_str());

  auto file = std::make_shared<::arrow::io::BufferReader>(data_buffer, data.size());
  auto source = std::make_shared<ArrowInputFile>(file);

  ASSERT_EQ(0, source->Tell());

  std::shared_ptr<Buffer> pq_buffer, expected_buffer;

  ASSERT_NO_THROW(pq_buffer = source->Read(4));
  expected_buffer = std::make_shared<Buffer>(data_buffer, 4);
  ASSERT_TRUE(expected_buffer->Equals(*pq_buffer.get()));

  ASSERT_NO_THROW(pq_buffer = source->Read(7));
  expected_buffer = std::make_shared<Buffer>(data_buffer + 4, 7);
  ASSERT_TRUE(expected_buffer->Equals(*pq_buffer.get()));

  ASSERT_EQ(11, source->Tell());

  ASSERT_NO_THROW(pq_buffer = source->Read(8));
  expected_buffer = std::make_shared<Buffer>(data_buffer + 11, 5);
  ASSERT_TRUE(expected_buffer->Equals(*pq_buffer.get()));

  ASSERT_EQ(16, source->Tell());
}

}  // namespace parquet
