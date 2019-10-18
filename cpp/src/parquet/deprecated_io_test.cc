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

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"

#include "parquet/deprecated_io.h"
#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/test_util.h"

using arrow::default_memory_pool;
using arrow::MemoryPool;

namespace parquet {

class MockRandomAccessSource : public RandomAccessSource {
 public:
  MockRandomAccessSource(const uint8_t* data, int64_t size)
      : data_(data), position_(0), size_(size) {}

  int64_t Size() const override { return size_; }

  int64_t Read(int64_t nbytes, uint8_t* out) override {
    ThrowIfClosed();
    int64_t bytes_to_read = std::min(nbytes, size_ - position_);
    if (bytes_to_read == 0) {
      return 0;
    }
    memcpy(out, data_ + position_, bytes_to_read);
    position_ += bytes_to_read;
    return bytes_to_read;
  }

  std::shared_ptr<Buffer> Read(int64_t nbytes) override {
    ThrowIfClosed();
    int64_t bytes_to_read = std::min(nbytes, size_ - position_);
    std::shared_ptr<ResizableBuffer> out =
        AllocateBuffer(::arrow::default_memory_pool(), bytes_to_read);
    Read(bytes_to_read, out->mutable_data());
    return std::move(out);
  }

  std::shared_ptr<Buffer> ReadAt(int64_t position, int64_t nbytes) override {
    ThrowIfClosed();
    position_ = position;
    return Read(nbytes);
  }

  int64_t ReadAt(int64_t position, int64_t nbytes, uint8_t* out) override {
    ThrowIfClosed();
    position_ = position;
    return Read(nbytes, out);
  }

  void Close() override { closed_ = true; }

  int64_t Tell() override {
    ThrowIfClosed();
    return position_;
  }

  bool closed() const { return closed_; }

 private:
  const uint8_t* data_;
  int64_t position_;
  int64_t size_;
  bool closed_ = false;

  void ThrowIfClosed() {
    if (closed_) {
      throw ParquetException("file is closed");
    }
  }
};

TEST(ParquetInputWrapper, BasicOperation) {
  std::string data = "some example data";

  auto source = std::unique_ptr<RandomAccessSource>(new MockRandomAccessSource(
      reinterpret_cast<const uint8_t*>(data.data()), static_cast<int64_t>(data.size())));
  ParquetInputWrapper wrapper(std::move(source));

  ASSERT_FALSE(wrapper.closed());

  int64_t position = -1;
  ASSERT_OK(wrapper.Tell(&position));
  ASSERT_EQ(0, position);

  // GetSize
  int64_t size = -1;
  ASSERT_OK(wrapper.GetSize(&size));
  ASSERT_EQ(size, static_cast<int64_t>(data.size()));

  // Read into memory
  uint8_t buf[4] = {0};
  int64_t bytes_read = -1;
  ASSERT_OK(wrapper.Read(4, &bytes_read, buf));
  ASSERT_EQ(4, bytes_read);
  ASSERT_EQ(0, memcmp(buf, data.data(), 4));

  ASSERT_OK(wrapper.Tell(&position));
  ASSERT_EQ(4, position);

  // Seek
  ASSERT_RAISES(NotImplemented, wrapper.Seek(5));

  // Read buffer
  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(wrapper.Read(7, &buffer));
  ASSERT_EQ(0, memcmp(buffer->data(), data.data() + 4, 7));

  // ReadAt
  ASSERT_OK(wrapper.ReadAt(13, 4, &buffer));
  ASSERT_EQ(4, buffer->size());
  ASSERT_EQ(0, memcmp(buffer->data(), data.data() + 13, 4));

  // Close
  ASSERT_OK(wrapper.Close());
  ASSERT_TRUE(wrapper.closed());
}

class MockOutputStream : public OutputStream {
 public:
  MockOutputStream() {}

  void Write(const uint8_t* data, int64_t length) override {
    ThrowIfClosed();
    size_ += length;
  }

  void Close() override { closed_ = true; }

  int64_t Tell() override {
    ThrowIfClosed();
    return size_;
  }

  bool closed() const { return closed_; }

 private:
  int64_t size_ = 0;
  bool closed_ = false;

  void ThrowIfClosed() {
    if (closed_) {
      throw ParquetException("file is closed");
    }
  }
};

TEST(ParquetOutputWrapper, BasicOperation) {
  auto stream = std::unique_ptr<OutputStream>(new MockOutputStream);
  ParquetOutputWrapper wrapper(std::move(stream));

  int64_t position = -1;
  ASSERT_OK(wrapper.Tell(&position));
  ASSERT_EQ(0, position);

  std::string data = "food";

  ASSERT_OK(wrapper.Write(reinterpret_cast<const uint8_t*>(data.data()), 4));
  ASSERT_OK(wrapper.Tell(&position));
  ASSERT_EQ(4, position);

  // Close
  ASSERT_OK(wrapper.Close());
  ASSERT_TRUE(wrapper.closed());

  // Test catch exceptions
  ASSERT_RAISES(IOError, wrapper.Tell(&position));
  ASSERT_RAISES(IOError, wrapper.Write(reinterpret_cast<const uint8_t*>(data.data()), 4));
}

TEST(ParquetOutputWrapper, DtorCloses) {
  MockOutputStream stream;
  { ParquetOutputWrapper wrapper(&stream); }
  ASSERT_TRUE(stream.closed());
}

}  // namespace parquet
