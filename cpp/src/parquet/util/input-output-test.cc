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
#include "parquet/util/output.h"
#include "parquet/util/test-common.h"

namespace parquet_cpp {

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

static bool file_exists(const std::string& path) {
  return std::ifstream(path.c_str()).good();
}

template <typename ReaderType>
class TestFileReaders : public ::testing::Test {
 public:
  void SetUp() {
    test_path_ = "parquet-input-output-test.txt";
    if (file_exists(test_path_)) {
      std::remove(test_path_.c_str());
    }
    test_data_ = "testingdata";

    std::ofstream stream;
    stream.open(test_path_.c_str());
    stream << test_data_;
    filesize_ = test_data_.size();
  }

  void TearDown() {
    DeleteTestFile();
  }

  void DeleteTestFile() {
    if (file_exists(test_path_)) {
      std::remove(test_path_.c_str());
    }
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

} // namespace parquet_cpp
