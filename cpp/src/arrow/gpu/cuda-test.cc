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
#include <limits>
#include <string>

#include "gtest/gtest.h"

#include "arrow/status.h"
#include "arrow/test-util.h"

#include "arrow/gpu/cuda_memory.h"

namespace arrow {
namespace gpu {

constexpr int kGpuNumber = 0;

class TestCudaBuffer : public ::testing::Test {};

TEST_F(TestCudaBuffer, Allocate) {
  const int64_t kSize = 100;
  std::shared_ptr<CudaBuffer> buffer;

  ASSERT_OK(AllocateCudaBuffer(kGpuNumber, kSize, &buffer));
  ASSERT_EQ(kSize, buffer->size());
}

void AssertCudaBufferEquals(const CudaBuffer& buffer, const uint8_t* host_data,
                            const int64_t nbytes) {
  std::shared_ptr<MutableBuffer> result;
  ASSERT_OK(AllocateBuffer(default_memory_pool(), nbytes, &result));
  ASSERT_OK(buffer.CopyToHost(0, buffer.size(), result->mutable_data()));
  ASSERT_EQ(0, std::memcmp(result->data(), host_data, nbytes));
}

TEST_F(TestCudaBuffer, CopyFromHost) {
  const int64_t kSize = 1000;
  std::shared_ptr<CudaBuffer> device_buffer;
  ASSERT_OK(AllocateCudaBuffer(kGpuNumber, kSize, &device_buffer));

  std::shared_ptr<PoolBuffer> host_buffer;
  ASSERT_OK(test::MakeRandomBytePoolBuffer(kSize, default_memory_pool(), &host_buffer));

  ASSERT_OK(device_buffer->CopyFromHost(0, host_buffer->data(), 500));
  ASSERT_OK(device_buffer->CopyFromHost(500, host_buffer->data() + 500, kSize - 500));

  AssertCudaBufferEquals(*device_buffer, host_buffer->data(), kSize);
}

class TestCudaBufferWriter : public ::testing::Test {
 public:
  void Allocate(const int64_t size) {
    ASSERT_OK(AllocateCudaBuffer(kGpuNumber, size, &device_buffer_));
    writer_.reset(new CudaBufferWriter(device_buffer_));
  }

  void TestWrites(const int64_t total_bytes, const int64_t chunksize,
                  const int64_t buffer_size = 0) {
    std::shared_ptr<PoolBuffer> buffer;
    ASSERT_OK(
        test::MakeRandomBytePoolBuffer(total_bytes, default_memory_pool(), &buffer));

    if (buffer_size > 0) {
      ASSERT_OK(writer_->SetBufferSize(buffer_size));
    }

    int64_t position = 0;
    ASSERT_OK(writer_->Tell(&position));
    ASSERT_EQ(0, position);

    const uint8_t* host_data = buffer->data();
    ASSERT_OK(writer_->Write(host_data, chunksize));
    ASSERT_OK(writer_->Tell(&position));
    ASSERT_EQ(chunksize, position);

    ASSERT_OK(writer_->Seek(0));
    ASSERT_OK(writer_->Tell(&position));
    ASSERT_EQ(0, position);

    while (position < total_bytes) {
      int64_t bytes_to_write = std::min(chunksize, total_bytes - position);
      ASSERT_OK(writer_->Write(host_data + position, bytes_to_write));
      position += bytes_to_write;
    }

    ASSERT_OK(writer_->Flush());

    AssertCudaBufferEquals(*device_buffer_, buffer->data(), total_bytes);
  }

 protected:
  std::shared_ptr<CudaBuffer> device_buffer_;
  std::unique_ptr<CudaBufferWriter> writer_;
};

TEST_F(TestCudaBufferWriter, UnbufferedWrites) {
  const int64_t kTotalSize = 1 << 16;
  Allocate(kTotalSize);
  TestWrites(kTotalSize, 1000);
}

TEST_F(TestCudaBufferWriter, BufferedWrites) {
  const int64_t kTotalSize = 1 << 16;
  Allocate(kTotalSize);
  TestWrites(kTotalSize, 1000, 1 << 12);
}

TEST_F(TestCudaBufferWriter, EdgeCases) {
  Allocate(1000);

  std::shared_ptr<PoolBuffer> buffer;
  ASSERT_OK(test::MakeRandomBytePoolBuffer(1000, default_memory_pool(), &buffer));
  const uint8_t* host_data = buffer->data();

  ASSERT_EQ(0, writer_->buffer_size());
  ASSERT_OK(writer_->SetBufferSize(100));
  ASSERT_EQ(100, writer_->buffer_size());

  // Write 0 bytes
  int64_t position = 0;
  ASSERT_OK(writer_->Write(host_data, 0));
  ASSERT_OK(writer_->Tell(&position));
  ASSERT_EQ(0, position);

  // Write some data, then change buffer size
  ASSERT_OK(writer_->Write(host_data, 10));
  ASSERT_OK(writer_->SetBufferSize(200));
  ASSERT_EQ(200, writer_->buffer_size());

  ASSERT_EQ(0, writer_->num_bytes_buffered());

  // Write more than buffer size
  ASSERT_OK(writer_->Write(host_data + 10, 300));
  ASSERT_EQ(0, writer_->num_bytes_buffered());

  // Write exactly buffer size
  ASSERT_OK(writer_->Write(host_data + 310, 200));
  ASSERT_EQ(0, writer_->num_bytes_buffered());

  // Write rest of bytes
  ASSERT_OK(writer_->Write(host_data + 510, 390));
  ASSERT_OK(writer_->Write(host_data + 900, 100));

  // Close flushes
  ASSERT_OK(writer_->Close());

  // Check that everything was written
  AssertCudaBufferEquals(*device_buffer_, host_data, 1000);
}

TEST(TestCudaBufferReader, Basics) {
  std::shared_ptr<CudaBuffer> device_buffer;

  const int64_t size = 1000;
  ASSERT_OK(AllocateCudaBuffer(kGpuNumber, size, &device_buffer));

  std::shared_ptr<PoolBuffer> buffer;
  ASSERT_OK(test::MakeRandomBytePoolBuffer(1000, default_memory_pool(), &buffer));
  const uint8_t* host_data = buffer->data();

  ASSERT_OK(device_buffer->CopyFromHost(0, host_data, 1000));

  CudaBufferReader reader(device_buffer);

  // Read to host memory
  uint8_t stack_buffer[100] = {0};
  int64_t bytes_read = 0;
  ASSERT_OK(reader.Seek(950));

  int64_t position = 0;
  ASSERT_OK(reader.Tell(&position));
  ASSERT_EQ(950, position);

  ASSERT_OK(reader.Read(100, &bytes_read, stack_buffer));
  ASSERT_EQ(50, bytes_read);
  ASSERT_EQ(0, std::memcmp(stack_buffer, host_data + 950, 50));
  ASSERT_OK(reader.Tell(&position));
  ASSERT_EQ(1000, position);

  ASSERT_OK(reader.Seek(925));
  std::shared_ptr<Buffer> tmp;
  ASSERT_OK(reader.Read(100, &tmp));
  ASSERT_EQ(75, tmp->size());
  ASSERT_OK(reader.Tell(&position));
  ASSERT_EQ(1000, position);

  ASSERT_OK(std::dynamic_pointer_cast<CudaBuffer>(tmp)->CopyToHost(0, 75, stack_buffer));
  ASSERT_EQ(0, std::memcmp(stack_buffer, host_data + 925, 75));
}

}  // namespace gpu
}  // namespace arrow
