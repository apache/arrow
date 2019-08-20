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

#include "arrow/ipc/api.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/test_common.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

#include "arrow/gpu/cuda_api.h"

namespace arrow {
namespace cuda {

constexpr int kGpuNumber = 0;

class TestCudaBufferBase : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK(CudaDeviceManager::GetInstance(&manager_));
    ASSERT_OK(manager_->GetContext(kGpuNumber, &context_));
  }

 protected:
  CudaDeviceManager* manager_;
  std::shared_ptr<CudaContext> context_;
};

class TestCudaBuffer : public TestCudaBufferBase {
 public:
  void SetUp() { TestCudaBufferBase::SetUp(); }
};

TEST_F(TestCudaBuffer, Allocate) {
  const int64_t kSize = 100;
  std::shared_ptr<CudaBuffer> buffer;
  ASSERT_OK(context_->Allocate(kSize, &buffer));
  ASSERT_EQ(kSize, buffer->size());
  ASSERT_EQ(kSize, context_->bytes_allocated());
}

void AssertCudaBufferEquals(const CudaBuffer& buffer, const uint8_t* host_data,
                            const int64_t nbytes) {
  std::shared_ptr<Buffer> result;
  ASSERT_OK(AllocateBuffer(default_memory_pool(), nbytes, &result));
  ASSERT_OK(buffer.CopyToHost(0, buffer.size(), result->mutable_data()));
  ASSERT_EQ(0, std::memcmp(result->data(), host_data, nbytes));
}

TEST_F(TestCudaBuffer, CopyFromHost) {
  const int64_t kSize = 1000;
  std::shared_ptr<CudaBuffer> device_buffer;
  ASSERT_OK(context_->Allocate(kSize, &device_buffer));

  std::shared_ptr<ResizableBuffer> host_buffer;
  ASSERT_OK(MakeRandomByteBuffer(kSize, default_memory_pool(), &host_buffer));

  ASSERT_OK(device_buffer->CopyFromHost(0, host_buffer->data(), 500));
  ASSERT_OK(device_buffer->CopyFromHost(500, host_buffer->data() + 500, kSize - 500));

  AssertCudaBufferEquals(*device_buffer, host_buffer->data(), kSize);
}

TEST_F(TestCudaBuffer, FromBuffer) {
  const int64_t kSize = 1000;
  // Initialize device buffer with random data
  std::shared_ptr<ResizableBuffer> host_buffer;
  std::shared_ptr<CudaBuffer> device_buffer;
  ASSERT_OK(context_->Allocate(kSize, &device_buffer));
  ASSERT_OK(MakeRandomByteBuffer(kSize, default_memory_pool(), &host_buffer));
  ASSERT_OK(device_buffer->CopyFromHost(0, host_buffer->data(), 1000));
  // Sanity check
  AssertCudaBufferEquals(*device_buffer, host_buffer->data(), kSize);

  // Get generic Buffer from device buffer
  std::shared_ptr<Buffer> buffer;
  std::shared_ptr<CudaBuffer> result;
  buffer = std::static_pointer_cast<Buffer>(device_buffer);
  ASSERT_OK(CudaBuffer::FromBuffer(buffer, &result));
  ASSERT_EQ(result->size(), kSize);
  ASSERT_EQ(result->is_mutable(), true);
  ASSERT_EQ(result->mutable_data(), buffer->mutable_data());
  AssertCudaBufferEquals(*result, host_buffer->data(), kSize);

  buffer = SliceBuffer(device_buffer, 0, kSize);
  ASSERT_OK(CudaBuffer::FromBuffer(buffer, &result));
  ASSERT_EQ(result->size(), kSize);
  ASSERT_EQ(result->is_mutable(), false);
  AssertCudaBufferEquals(*result, host_buffer->data(), kSize);

  buffer = SliceMutableBuffer(device_buffer, 0, kSize);
  ASSERT_OK(CudaBuffer::FromBuffer(buffer, &result));
  ASSERT_EQ(result->size(), kSize);
  ASSERT_EQ(result->is_mutable(), true);
  ASSERT_EQ(result->mutable_data(), buffer->mutable_data());
  AssertCudaBufferEquals(*result, host_buffer->data(), kSize);

  buffer = SliceMutableBuffer(device_buffer, 3, kSize - 10);
  buffer = SliceMutableBuffer(buffer, 8, kSize - 20);
  ASSERT_OK(CudaBuffer::FromBuffer(buffer, &result));
  ASSERT_EQ(result->size(), kSize - 20);
  ASSERT_EQ(result->is_mutable(), true);
  ASSERT_EQ(result->mutable_data(), buffer->mutable_data());
  AssertCudaBufferEquals(*result, host_buffer->data() + 11, kSize - 20);
}

// IPC only supported on Linux
#if defined(__linux)

TEST_F(TestCudaBuffer, DISABLED_ExportForIpc) {
  // For this test to work, a second process needs to be spawned
  const int64_t kSize = 1000;
  std::shared_ptr<CudaBuffer> device_buffer;
  ASSERT_OK(context_->Allocate(kSize, &device_buffer));

  std::shared_ptr<ResizableBuffer> host_buffer;
  ASSERT_OK(MakeRandomByteBuffer(kSize, default_memory_pool(), &host_buffer));
  ASSERT_OK(device_buffer->CopyFromHost(0, host_buffer->data(), kSize));

  // Export for IPC and serialize
  std::shared_ptr<CudaIpcMemHandle> ipc_handle;
  ASSERT_OK(device_buffer->ExportForIpc(&ipc_handle));

  std::shared_ptr<Buffer> serialized_handle;
  ASSERT_OK(ipc_handle->Serialize(default_memory_pool(), &serialized_handle));

  // Deserialize IPC handle and open
  std::shared_ptr<CudaIpcMemHandle> ipc_handle2;
  ASSERT_OK(CudaIpcMemHandle::FromBuffer(serialized_handle->data(), &ipc_handle2));

  std::shared_ptr<CudaBuffer> ipc_buffer;
  ASSERT_OK(context_->OpenIpcBuffer(*ipc_handle2, &ipc_buffer));

  ASSERT_EQ(kSize, ipc_buffer->size());

  std::shared_ptr<Buffer> ipc_data;
  ASSERT_OK(AllocateBuffer(default_memory_pool(), kSize, &ipc_data));
  ASSERT_OK(ipc_buffer->CopyToHost(0, kSize, ipc_data->mutable_data()));
  ASSERT_EQ(0, std::memcmp(ipc_buffer->data(), host_buffer->data(), kSize));
}

#endif

class TestCudaBufferWriter : public TestCudaBufferBase {
 public:
  void SetUp() { TestCudaBufferBase::SetUp(); }

  void Allocate(const int64_t size) {
    ASSERT_OK(context_->Allocate(size, &device_buffer_));
    writer_.reset(new CudaBufferWriter(device_buffer_));
  }

  void TestWrites(const int64_t total_bytes, const int64_t chunksize,
                  const int64_t buffer_size = 0) {
    std::shared_ptr<ResizableBuffer> buffer;
    ASSERT_OK(MakeRandomByteBuffer(total_bytes, default_memory_pool(), &buffer));

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

  std::shared_ptr<ResizableBuffer> buffer;
  ASSERT_OK(MakeRandomByteBuffer(1000, default_memory_pool(), &buffer));
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

class TestCudaBufferReader : public TestCudaBufferBase {
 public:
  void SetUp() { TestCudaBufferBase::SetUp(); }
};

TEST_F(TestCudaBufferReader, Basics) {
  std::shared_ptr<CudaBuffer> device_buffer;

  const int64_t size = 1000;
  ASSERT_OK(context_->Allocate(size, &device_buffer));

  std::shared_ptr<ResizableBuffer> buffer;
  ASSERT_OK(MakeRandomByteBuffer(1000, default_memory_pool(), &buffer));
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

class TestCudaArrowIpc : public TestCudaBufferBase {
 public:
  void SetUp() {
    TestCudaBufferBase::SetUp();
    pool_ = default_memory_pool();
  }

 protected:
  MemoryPool* pool_;
};

TEST_F(TestCudaArrowIpc, BasicWriteRead) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(ipc::test::MakeIntRecordBatch(&batch));

  std::shared_ptr<CudaBuffer> device_serialized;
  ASSERT_OK(SerializeRecordBatch(*batch, context_.get(), &device_serialized));

  // Test that ReadRecordBatch works properly
  std::shared_ptr<RecordBatch> device_batch;
  ASSERT_OK(ReadRecordBatch(batch->schema(), device_serialized, default_memory_pool(),
                            &device_batch));

  // Copy data from device, read batch, and compare
  std::shared_ptr<Buffer> host_buffer;
  int64_t size = device_serialized->size();
  ASSERT_OK(AllocateBuffer(pool_, size, &host_buffer));
  ASSERT_OK(device_serialized->CopyToHost(0, size, host_buffer->mutable_data()));

  std::shared_ptr<RecordBatch> cpu_batch;
  io::BufferReader cpu_reader(host_buffer);
  ipc::DictionaryMemo unused_memo;
  ASSERT_OK(ipc::ReadRecordBatch(batch->schema(), &unused_memo, &cpu_reader, &cpu_batch));

  CompareBatch(*batch, *cpu_batch);
}

class TestCudaContext : public TestCudaBufferBase {
 public:
  void SetUp() { TestCudaBufferBase::SetUp(); }
};

TEST_F(TestCudaContext, GetDeviceAddress) {
  const int64_t kSize = 100;
  std::shared_ptr<CudaBuffer> buffer;
  uint8_t* devptr = NULL;
  ASSERT_OK(context_->Allocate(kSize, &buffer));
  ASSERT_OK(context_->GetDeviceAddress(buffer.get()->mutable_data(), &devptr));
  ASSERT_EQ(buffer.get()->mutable_data(), devptr);
}

}  // namespace cuda
}  // namespace arrow
