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
#include "parquet/test-util.h"
#include "parquet/util/memory.h"

using arrow::default_memory_pool;
using arrow::MemoryPool;

namespace parquet {

class TestBufferedInputStream : public ::testing::Test {
 public:
  void SetUp() {
    // Create a buffer larger than source size, to check that the stream end is upholded.
    std::shared_ptr<ResizableBuffer> buf =
        AllocateBuffer(default_memory_pool(), source_size_ + 10);
    ASSERT_LT(source_size_, buf->size());
    for (int i = 0; i < source_size_; i++) {
      buf->mutable_data()[i] = static_cast<uint8_t>(i);
    }
    source_ = std::make_shared<ArrowInputFile>(
        std::make_shared<::arrow::io::BufferReader>(buf));
    stream_.reset(new BufferedInputStream(default_memory_pool(), chunk_size_,
                                          source_.get(), stream_offset_, stream_size_));
  }

 protected:
  int64_t source_size_ = 256;
  int64_t stream_offset_ = 10;
  int64_t stream_size_ = source_size_ - stream_offset_;
  int64_t chunk_size_ = 50;
  std::shared_ptr<RandomAccessSource> source_;
  std::unique_ptr<BufferedInputStream> stream_;
};

TEST_F(TestBufferedInputStream, Basics) {
  const uint8_t* output;
  int64_t bytes_read;

  // source is at offset 10
  output = stream_->Peek(10, &bytes_read);
  ASSERT_EQ(10, bytes_read);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(10 + i, output[i]) << i;
  }
  output = stream_->Read(10, &bytes_read);
  ASSERT_EQ(10, bytes_read);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(10 + i, output[i]) << i;
  }
  output = stream_->Read(10, &bytes_read);
  ASSERT_EQ(10, bytes_read);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(20 + i, output[i]) << i;
  }
  stream_->Advance(5);
  stream_->Advance(5);
  // source is at offset 40
  // read across buffer boundary. buffer size is 50
  output = stream_->Read(20, &bytes_read);
  ASSERT_EQ(20, bytes_read);
  for (int i = 0; i < 20; i++) {
    ASSERT_EQ(40 + i, output[i]) << i;
  }
  // read more than original chunk size
  output = stream_->Read(60, &bytes_read);
  ASSERT_EQ(60, bytes_read);
  for (int i = 0; i < 60; i++) {
    ASSERT_EQ(60 + i, output[i]) << i;
  }

  stream_->Advance(120);
  // source is at offset 240
  // read outside of source boundary. source size is 256
  output = stream_->Read(30, &bytes_read);
  ASSERT_EQ(16, bytes_read);
  for (int i = 0; i < 16; i++) {
    ASSERT_EQ(240 + i, output[i]) << i;
  }
  // Stream exhausted
  output = stream_->Read(1, &bytes_read);
  ASSERT_EQ(bytes_read, 0);
}

TEST_F(TestBufferedInputStream, LargeFirstPeek) {
  // Test a first peek larger than chunk size
  const uint8_t* output;
  int64_t bytes_read;
  int64_t n = 70;
  ASSERT_GT(n, chunk_size_);

  // source is at offset 10
  output = stream_->Peek(n, &bytes_read);
  ASSERT_EQ(n, bytes_read);
  for (int i = 0; i < n; i++) {
    ASSERT_EQ(10 + i, output[i]) << i;
  }
  output = stream_->Peek(n, &bytes_read);
  ASSERT_EQ(n, bytes_read);
  for (int i = 0; i < n; i++) {
    ASSERT_EQ(10 + i, output[i]) << i;
  }
  output = stream_->Read(n, &bytes_read);
  ASSERT_EQ(n, bytes_read);
  for (int i = 0; i < n; i++) {
    ASSERT_EQ(10 + i, output[i]) << i;
  }
  // source is at offset 10 + n
  output = stream_->Read(20, &bytes_read);
  ASSERT_EQ(20, bytes_read);
  for (int i = 0; i < 20; i++) {
    ASSERT_EQ(10 + n + i, output[i]) << i;
  }
}

TEST_F(TestBufferedInputStream, OneByteReads) {
  const uint8_t* output;
  int64_t bytes_read;

  for (int i = 0; i < stream_size_; ++i) {
    output = stream_->Read(1, &bytes_read);
    ASSERT_EQ(bytes_read, 1);
    ASSERT_EQ(10 + i, output[0]) << i;
  }
  // Stream exhausted
  output = stream_->Read(1, &bytes_read);
  ASSERT_EQ(bytes_read, 0);
}

TEST_F(TestBufferedInputStream, BufferExactlyExhausted) {
  // Test exhausting the buffer exactly then issuing further reads (PARQUET-1571).
  const uint8_t* output;
  int64_t bytes_read;

  // source is at offset 10
  int64_t n = 10;
  output = stream_->Read(n, &bytes_read);
  ASSERT_EQ(n, bytes_read);
  for (int i = 0; i < n; i++) {
    ASSERT_EQ(10 + i, output[i]) << i;
  }
  // source is at offset 20
  // Exhaust buffer exactly
  n = stream_->remaining_in_buffer();
  output = stream_->Read(n, &bytes_read);
  ASSERT_EQ(n, bytes_read);
  for (int i = 0; i < n; i++) {
    ASSERT_EQ(20 + i, output[i]) << i;
  }
  // source is at offset 20 + n
  // Read new buffer
  output = stream_->Read(10, &bytes_read);
  ASSERT_EQ(10, bytes_read);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(20 + n + i, output[i]) << i;
  }
  // source is at offset 30 + n
  output = stream_->Read(10, &bytes_read);
  ASSERT_EQ(10, bytes_read);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(30 + n + i, output[i]) << i;
  }
}

}  // namespace parquet
