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
#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "parquet/exception.h"
#include "parquet/util/buffer.h"
#include "parquet/util/input.h"
#include "parquet/util/mem-allocator.h"
#include "parquet/util/output.h"
#include "parquet/util/test-common.h"

namespace parquet {

TEST(TestBufferedInputStream, Basics) {
  int64_t source_size = 256;
  int64_t stream_offset = 10;
  int64_t stream_size = source_size - stream_offset;
  int64_t chunk_size = 50;
  auto buf = std::make_shared<OwnedMutableBuffer>(source_size);
  ASSERT_EQ(source_size, buf->size());
  for (int i = 0; i < source_size; i++) {
    buf->mutable_data()[i] = i;
  }

  std::unique_ptr<BufferReader> source(new BufferReader(buf));
  std::unique_ptr<MemoryAllocator> allocator(new TrackingAllocator());
  std::unique_ptr<BufferedInputStream> stream(new BufferedInputStream(
      allocator.get(), chunk_size, source.get(), stream_offset, stream_size));

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

TEST(TestInMemoryOutputStream, Basics) {
  std::unique_ptr<InMemoryOutputStream> stream(new InMemoryOutputStream(8));

  std::vector<uint8_t> data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

  stream->Write(&data[0], 4);
  ASSERT_EQ(4, stream->Tell());
  stream->Write(&data[4], data.size() - 4);

  std::shared_ptr<Buffer> buffer = stream->GetBuffer();

  Buffer data_buf(data.data(), data.size());

  ASSERT_TRUE(data_buf.Equals(*buffer));
}

TEST(TestBufferedReader, Basics) {
  std::vector<uint8_t> data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
  auto buffer = std::make_shared<Buffer>(data.data(), data.size());
  BufferReader reader(buffer);

  uint8_t out[4];
  ASSERT_EQ(4, reader.Read(4, out));
  ASSERT_EQ(4, reader.Tell());
  ASSERT_EQ(0, out[0]);
  ASSERT_EQ(1, out[1]);
  ASSERT_EQ(2, out[2]);
  ASSERT_EQ(3, out[3]);

  reader.Seek(8);
  ASSERT_EQ(8, reader.Tell());

  auto out_buffer = reader.Read(5);
  ASSERT_EQ(8, out_buffer->data()[0]);
  ASSERT_EQ(9, out_buffer->data()[1]);
  ASSERT_EQ(10, out_buffer->data()[2]);
  ASSERT_EQ(11, out_buffer->data()[3]);
  ASSERT_EQ(12, out_buffer->data()[4]);

  // Read past the end of the buffer
  ASSERT_EQ(13, reader.Tell());
  ASSERT_EQ(0, reader.Read(4, out));
  ASSERT_EQ(0, reader.Read(4)->size());

  reader.Close();
}

static bool file_exists(const std::string& path) {
  return std::ifstream(path.c_str()).good();
}

template <typename ReaderType>
class TestFileReaders : public ::testing::Test {
 public:
  void SetUp() {
    test_path_ = "parquet-input-output-test.txt";
    if (file_exists(test_path_)) { std::remove(test_path_.c_str()); }
    test_data_ = "testingdata";

    std::ofstream stream;
    stream.open(test_path_.c_str());
    stream << test_data_;
    filesize_ = test_data_.size();
  }

  void TearDown() { DeleteTestFile(); }

  void DeleteTestFile() {
    if (file_exists(test_path_)) { std::remove(test_path_.c_str()); }
  }

 protected:
  ReaderType source;
  std::string test_path_;
  std::string test_data_;
  int filesize_;
};

typedef ::testing::Types<LocalFileSource, MemoryMapSource> ReaderTypes;

TYPED_TEST_CASE(TestFileReaders, ReaderTypes);

TYPED_TEST(TestFileReaders, NonExistentFile) {
  ASSERT_THROW(this->source.Open("0xDEADBEEF.txt"), ParquetException);
}

TYPED_TEST(TestFileReaders, Read) {
  this->source.Open(this->test_path_);

  ASSERT_EQ(this->filesize_, this->source.Size());

  std::shared_ptr<Buffer> buffer = this->source.Read(4);
  ASSERT_EQ(4, buffer->size());
  ASSERT_EQ(0, memcmp(this->test_data_.c_str(), buffer->data(), 4));

  // Read past EOF
  buffer = this->source.Read(10);
  ASSERT_EQ(7, buffer->size());
  ASSERT_EQ(0, memcmp(this->test_data_.c_str() + 4, buffer->data(), 7));
}

TYPED_TEST(TestFileReaders, FileDisappeared) {
  this->source.Open(this->test_path_);
  this->source.Seek(4);
  this->DeleteTestFile();
  this->source.Close();
}

TYPED_TEST(TestFileReaders, BadSeek) {
  this->source.Open(this->test_path_);
  ASSERT_THROW(this->source.Seek(this->filesize_ + 1), ParquetException);
}

}  // namespace parquet
