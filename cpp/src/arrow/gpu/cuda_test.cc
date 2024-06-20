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

#include <cuda.h>

#include "gtest/gtest.h"

#include "arrow/c/bridge.h"
#include "arrow/c/util_internal.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/api.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/test_common.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

#include "arrow/gpu/cuda_api.h"
#include "arrow/gpu/cuda_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::ArrayExportGuard;
using internal::ArrayStreamExportGuard;
using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::SchemaExportGuard;

namespace cuda {

using internal::ContextSaver;
using internal::StatusFromCuda;

#define ASSERT_CUDA_OK(expr) ASSERT_OK(::arrow::cuda::internal::StatusFromCuda((expr)))

constexpr int kGpuNumber = 0;
// Needs a second GPU installed
constexpr int kOtherGpuNumber = 1;

template <typename Expected>
void AssertCudaBufferEquals(const CudaBuffer& buffer, Expected&& expected) {
  ASSERT_OK_AND_ASSIGN(auto result, AllocateBuffer(buffer.size()));
  ASSERT_OK(buffer.CopyToHost(0, buffer.size(), result->mutable_data()));
  AssertBufferEqual(*result, expected);
}

template <typename Expected>
void AssertCudaBufferEquals(const Buffer& buffer, Expected&& expected) {
  ASSERT_TRUE(IsCudaDevice(*buffer.device()));
  AssertCudaBufferEquals(checked_cast<const CudaBuffer&>(buffer),
                         std::forward<Expected>(expected));
}

class TestCudaBase : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK_AND_ASSIGN(manager_, CudaDeviceManager::Instance());
    ASSERT_OK_AND_ASSIGN(device_, manager_->GetDevice(kGpuNumber));
    //     ASSERT_OK(device_->GetContext(kGpuNumber, &context_));
    ASSERT_OK_AND_ASSIGN(context_, device_->GetContext());
    ASSERT_OK_AND_ASSIGN(mm_, AsCudaMemoryManager(device_->default_memory_manager()));
    cpu_device_ = CPUDevice::Instance();
    cpu_mm_ = cpu_device_->default_memory_manager();
  }

  void TearDown() {
    for (auto cu_context : non_primary_contexts_) {
      ASSERT_CUDA_OK(cuCtxDestroy(cu_context));
    }
  }

  Result<CUcontext> NonPrimaryRawContext() {
    CUcontext ctx;
    RETURN_NOT_OK(StatusFromCuda(cuCtxCreate(&ctx, /*flags=*/0, device_->handle())));
    non_primary_contexts_.push_back(ctx);
    return ctx;
  }

  Result<std::shared_ptr<CudaContext>> NonPrimaryContext() {
    ARROW_ASSIGN_OR_RAISE(auto cuctx, NonPrimaryRawContext());
    return device_->GetSharedContext(cuctx);
  }

  // Returns nullptr if kOtherGpuNumber does not correspond to an installed GPU
  Result<std::shared_ptr<CudaDevice>> OtherGpuDevice() {
    auto maybe_device = CudaDevice::Make(kOtherGpuNumber);
    if (maybe_device.status().IsInvalid()) {
      return nullptr;
    }
    return maybe_device;
  }

 protected:
  CudaDeviceManager* manager_;
  std::shared_ptr<CudaDevice> device_;
  std::shared_ptr<CudaMemoryManager> mm_;
  std::shared_ptr<CudaContext> context_;
  std::shared_ptr<Device> cpu_device_;
  std::shared_ptr<MemoryManager> cpu_mm_;
  std::vector<CUcontext> non_primary_contexts_;
};

// ------------------------------------------------------------------------
// Test CudaDevice

class TestCudaDevice : public TestCudaBase {
 public:
  void SetUp() { TestCudaBase::SetUp(); }
};

TEST_F(TestCudaDevice, Basics) {
  ASSERT_FALSE(device_->is_cpu());
  ASSERT_TRUE(IsCudaDevice(*device_));
  ASSERT_EQ(device_->device_number(), kGpuNumber);
  ASSERT_GE(device_->total_memory(), 1 << 20);
  ASSERT_NE(device_->device_name(), "");
  ASSERT_NE(device_->ToString(), "");

  ASSERT_OK_AND_ASSIGN(auto other_device, CudaDevice::Make(kGpuNumber));
  ASSERT_FALSE(other_device->is_cpu());
  ASSERT_TRUE(IsCudaDevice(*other_device));
  ASSERT_EQ(other_device->device_number(), kGpuNumber);
  ASSERT_EQ(other_device->total_memory(), device_->total_memory());
  ASSERT_EQ(other_device->handle(), device_->handle());
  ASSERT_EQ(other_device->device_name(), device_->device_name());
  ASSERT_EQ(*other_device, *device_);

  ASSERT_FALSE(IsCudaDevice(*cpu_device_));

  // Try another device if possible
  ASSERT_OK_AND_ASSIGN(other_device, OtherGpuDevice());
  if (other_device != nullptr) {
    ASSERT_FALSE(other_device->is_cpu());
    ASSERT_EQ(other_device->device_number(), kOtherGpuNumber);
    ASSERT_NE(*other_device, *device_);
    ASSERT_NE(other_device->handle(), device_->handle());
    ASSERT_NE(other_device->ToString(), device_->ToString());
  }

  ASSERT_RAISES(Invalid, CudaDevice::Make(-1));
  ASSERT_RAISES(Invalid, CudaDevice::Make(99));
}

TEST_F(TestCudaDevice, Copy) {
  auto cpu_buffer = Buffer::FromString("some data");

  // CPU -> device
  ASSERT_OK_AND_ASSIGN(auto other_buffer, Buffer::Copy(cpu_buffer, mm_));
  ASSERT_EQ(other_buffer->device(), device_);
  AssertCudaBufferEquals(*other_buffer, "some data");

  // Copy non-owned
  ASSERT_OK_AND_ASSIGN(other_buffer, Buffer::CopyNonOwned(*cpu_buffer, mm_));
  ASSERT_EQ(other_buffer->device(), device_);
  AssertCudaBufferEquals(*other_buffer, "some data");

  // device -> CPU
  ASSERT_OK_AND_ASSIGN(cpu_buffer, Buffer::Copy(other_buffer, cpu_mm_));
  ASSERT_TRUE(cpu_buffer->device()->is_cpu());
  AssertBufferEqual(*cpu_buffer, "some data");

  // Copy non-owned
  ASSERT_OK_AND_ASSIGN(cpu_buffer, Buffer::CopyNonOwned(*other_buffer, cpu_mm_));
  ASSERT_TRUE(cpu_buffer->device()->is_cpu());
  AssertBufferEqual(*cpu_buffer, "some data");

  // device -> device
  auto old_address = other_buffer->address();
  ASSERT_OK_AND_ASSIGN(other_buffer, Buffer::Copy(other_buffer, mm_));
  ASSERT_EQ(other_buffer->device(), device_);
  ASSERT_NE(other_buffer->address(), old_address);
  AssertCudaBufferEquals(*other_buffer, "some data");

  // Copy non-owned
  old_address = other_buffer->address();
  ASSERT_OK_AND_ASSIGN(other_buffer, Buffer::CopyNonOwned(*other_buffer, mm_));
  ASSERT_EQ(other_buffer->device(), device_);
  ASSERT_NE(other_buffer->address(), old_address);
  AssertCudaBufferEquals(*other_buffer, "some data");

  // device (other context) -> device
  ASSERT_OK_AND_ASSIGN(auto other_context, NonPrimaryContext());
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<CudaBuffer> cuda_buffer,
                       other_context->Allocate(9));
  ASSERT_OK(cuda_buffer->CopyFromHost(0, "some data", 9));
  ASSERT_OK_AND_ASSIGN(other_buffer, Buffer::Copy(cuda_buffer, mm_));
  ASSERT_EQ(other_buffer->device(), device_);
  AssertCudaBufferEquals(*other_buffer, "some data");
  auto other_handle = cuda_buffer->context()->handle();
  ASSERT_OK_AND_ASSIGN(cuda_buffer, CudaBuffer::FromBuffer(other_buffer));
  ASSERT_NE(cuda_buffer->context()->handle(), other_handle);

  // device -> other device
  ASSERT_OK_AND_ASSIGN(auto other_device, OtherGpuDevice());
  if (other_device != nullptr) {
    ASSERT_OK_AND_ASSIGN(
        other_buffer, Buffer::Copy(cuda_buffer, other_device->default_memory_manager()));
    ASSERT_EQ(other_buffer->device(), other_device);
    AssertCudaBufferEquals(*other_buffer, "some data");
  }
}

TEST_F(TestCudaDevice, CreateSyncEvent) {
  ASSERT_OK_AND_ASSIGN(auto ev, mm_->MakeDeviceSyncEvent());
  ASSERT_TRUE(ev);
  auto cuda_ev = checked_pointer_cast<CudaDevice::SyncEvent>(ev);
  ASSERT_CUDA_OK(cuEventQuery(*cuda_ev));
}

TEST_F(TestCudaDevice, WrapDeviceSyncEvent) {
  // need a context to call cuEventCreate
  ContextSaver set_temporary(reinterpret_cast<CUcontext>(context_.get()->handle()));

  CUevent event;
  ASSERT_CUDA_OK(cuEventCreate(&event, CU_EVENT_DEFAULT));
  ASSERT_CUDA_OK(cuEventQuery(event));

  {
    // wrap event with no-op destructor
    ASSERT_OK_AND_ASSIGN(auto ev, mm_->WrapDeviceSyncEvent(&event, [](void*) {}));
    ASSERT_TRUE(ev);
    // verify it's the same event we passed in
    ASSERT_EQ(ev->get_raw(), &event);
    auto cuda_ev = checked_pointer_cast<CudaDevice::SyncEvent>(ev);
    ASSERT_CUDA_OK(cuEventQuery(*cuda_ev));
  }

  // verify that the event is still valid on the device when the shared_ptr
  // goes away since we didn't give it ownership.
  ASSERT_CUDA_OK(cuEventQuery(event));
  ASSERT_CUDA_OK(cuEventDestroy(event));
}

TEST_F(TestCudaDevice, DefaultStream) {
  ASSERT_OK_AND_ASSIGN(auto stream, device_->MakeStream());
  ASSERT_OK_AND_ASSIGN(auto ev, mm_->MakeDeviceSyncEvent());

  ASSERT_OK(ev->Record(*stream));
  ASSERT_OK(stream->WaitEvent(*ev));
  ASSERT_OK(ev->Wait());
  ASSERT_OK(stream->Synchronize());
}

TEST_F(TestCudaDevice, ExplicitStream) {
  // need a context to call cuEventCreate
  ContextSaver set_temporary(reinterpret_cast<CUcontext>(context_.get()->handle()));

  CUstream cu_stream = CU_STREAM_PER_THREAD;
  {
    ASSERT_OK_AND_ASSIGN(auto stream, device_->WrapStream(&cu_stream, nullptr));
    ASSERT_OK_AND_ASSIGN(auto ev, mm_->MakeDeviceSyncEvent());

    ASSERT_OK(ev->Record(*stream));
    ASSERT_OK(stream->WaitEvent(*ev));
    ASSERT_OK(ev->Wait());
    ASSERT_OK(stream->Synchronize());
  }
}

// ------------------------------------------------------------------------
// Test CudaContext

class TestCudaContext : public TestCudaBase {
 public:
  void SetUp() { TestCudaBase::SetUp(); }
};

TEST_F(TestCudaContext, Basics) { ASSERT_EQ(*context_->device(), *device_); }

TEST_F(TestCudaContext, NonPrimaryContext) {
  ASSERT_OK_AND_ASSIGN(auto other_context, NonPrimaryContext());
  ASSERT_EQ(*other_context->device(), *device_);
  ASSERT_NE(other_context->handle(), context_->handle());
}

TEST_F(TestCudaContext, GetDeviceAddress) {
  const int64_t kSize = 100;
  ASSERT_OK_AND_ASSIGN(auto buffer, context_->Allocate(kSize));
  // GetDeviceAddress() is idempotent on device addresses
  ASSERT_OK_AND_ASSIGN(auto devptr, context_->GetDeviceAddress(buffer->address()));
  ASSERT_EQ(devptr, buffer->address());
}

// ------------------------------------------------------------------------
// Test CudaBuffer

class TestCudaBuffer : public TestCudaBase {
 public:
  void SetUp() { TestCudaBase::SetUp(); }
};

TEST_F(TestCudaBuffer, Allocate) {
  const int64_t kSize = 100;
  std::shared_ptr<CudaBuffer> buffer;
  ASSERT_OK_AND_ASSIGN(buffer, context_->Allocate(kSize));
  ASSERT_EQ(buffer->device(), context_->device());
  ASSERT_EQ(kSize, buffer->size());
  ASSERT_EQ(kSize, context_->bytes_allocated());
  ASSERT_FALSE(buffer->is_cpu());
}

TEST_F(TestCudaBuffer, CopyFromHost) {
  const int64_t kSize = 1000;
  std::shared_ptr<CudaBuffer> device_buffer;
  ASSERT_OK_AND_ASSIGN(device_buffer, context_->Allocate(kSize));

  std::shared_ptr<ResizableBuffer> host_buffer;
  ASSERT_OK(MakeRandomByteBuffer(kSize, default_memory_pool(), &host_buffer));

  ASSERT_OK(device_buffer->CopyFromHost(0, host_buffer->data(), 500));
  ASSERT_OK(device_buffer->CopyFromHost(500, host_buffer->data() + 500, kSize - 500));

  AssertCudaBufferEquals(*device_buffer, *host_buffer);
}

TEST_F(TestCudaBuffer, FromBuffer) {
  const int64_t kSize = 1000;
  // Initialize device buffer with random data
  std::shared_ptr<ResizableBuffer> host_buffer;
  std::shared_ptr<CudaBuffer> device_buffer;
  ASSERT_OK_AND_ASSIGN(device_buffer, context_->Allocate(kSize));
  ASSERT_OK(MakeRandomByteBuffer(kSize, default_memory_pool(), &host_buffer));
  ASSERT_OK(device_buffer->CopyFromHost(0, host_buffer->data(), 1000));
  // Sanity check
  AssertCudaBufferEquals(*device_buffer, *host_buffer);

  // Get generic Buffer from device buffer
  std::shared_ptr<Buffer> buffer;
  std::shared_ptr<CudaBuffer> result;
  buffer = std::static_pointer_cast<Buffer>(device_buffer);
  ASSERT_OK_AND_ASSIGN(result, CudaBuffer::FromBuffer(buffer));
  ASSERT_EQ(result->size(), kSize);
  ASSERT_EQ(result->is_mutable(), true);
  ASSERT_EQ(result->address(), buffer->address());
  AssertCudaBufferEquals(*result, *host_buffer);

  buffer = SliceBuffer(device_buffer, 0, kSize);
  ASSERT_OK_AND_ASSIGN(result, CudaBuffer::FromBuffer(buffer));
  ASSERT_EQ(result->size(), kSize);
  ASSERT_EQ(result->is_mutable(), false);
  ASSERT_EQ(result->address(), buffer->address());
  AssertCudaBufferEquals(*result, *host_buffer);

  buffer = SliceMutableBuffer(device_buffer, 0, kSize);
  ASSERT_OK_AND_ASSIGN(result, CudaBuffer::FromBuffer(buffer));
  ASSERT_EQ(result->size(), kSize);
  ASSERT_EQ(result->is_mutable(), true);
  ASSERT_EQ(result->address(), buffer->address());
  AssertCudaBufferEquals(*result, *host_buffer);

  buffer = SliceMutableBuffer(device_buffer, 3, kSize - 10);
  buffer = SliceMutableBuffer(buffer, 8, kSize - 20);
  ASSERT_OK_AND_ASSIGN(result, CudaBuffer::FromBuffer(buffer));
  ASSERT_EQ(result->size(), kSize - 20);
  ASSERT_EQ(result->is_mutable(), true);
  ASSERT_EQ(result->address(), buffer->address());
  AssertCudaBufferEquals(*result, *SliceBuffer(host_buffer, 11, kSize - 20));
}

// IPC only supported on Linux
#if defined(__linux)

TEST_F(TestCudaBuffer, DISABLED_ExportForIpc) {
  // For this test to work, a second process needs to be spawned
  const int64_t kSize = 1000;
  std::shared_ptr<CudaBuffer> device_buffer;
  ASSERT_OK_AND_ASSIGN(device_buffer, context_->Allocate(kSize));

  std::shared_ptr<ResizableBuffer> host_buffer;
  ASSERT_OK(MakeRandomByteBuffer(kSize, default_memory_pool(), &host_buffer));
  ASSERT_OK(device_buffer->CopyFromHost(0, host_buffer->data(), kSize));

  // Export for IPC and serialize
  std::shared_ptr<CudaIpcMemHandle> ipc_handle;
  ASSERT_OK_AND_ASSIGN(ipc_handle, device_buffer->ExportForIpc());

  std::shared_ptr<Buffer> serialized_handle;
  ASSERT_OK_AND_ASSIGN(serialized_handle, ipc_handle->Serialize());

  // Deserialize IPC handle and open
  std::shared_ptr<CudaIpcMemHandle> ipc_handle2;
  ASSERT_OK_AND_ASSIGN(ipc_handle2,
                       CudaIpcMemHandle::FromBuffer(serialized_handle->data()));

  std::shared_ptr<CudaBuffer> ipc_buffer;
  ASSERT_OK_AND_ASSIGN(ipc_buffer, context_->OpenIpcBuffer(*ipc_handle2));

  ASSERT_EQ(kSize, ipc_buffer->size());

  ASSERT_OK_AND_ASSIGN(auto ipc_data, AllocateBuffer(kSize));
  ASSERT_OK(ipc_buffer->CopyToHost(0, kSize, ipc_data->mutable_data()));
  ASSERT_EQ(0, std::memcmp(ipc_buffer->data(), host_buffer->data(), kSize));
}

#endif

// ------------------------------------------------------------------------
// Test CudaHostBuffer

class TestCudaHostBuffer : public TestCudaBase {
 public:
};

TEST_F(TestCudaHostBuffer, AllocateGlobal) {
  // Allocation using the global AllocateCudaHostBuffer() function
  std::shared_ptr<CudaHostBuffer> host_buffer;
  ASSERT_OK_AND_ASSIGN(host_buffer, AllocateCudaHostBuffer(kGpuNumber, 1024));

  ASSERT_TRUE(host_buffer->is_cpu());
  ASSERT_EQ(host_buffer->memory_manager(), cpu_mm_);
  ASSERT_EQ(host_buffer->device_type(), DeviceAllocationType::kCUDA_HOST);

  ASSERT_OK_AND_ASSIGN(auto device_address, host_buffer->GetDeviceAddress(context_));
  ASSERT_NE(device_address, 0);
  ASSERT_OK_AND_ASSIGN(auto host_address, GetHostAddress(device_address));
  ASSERT_EQ(host_address, host_buffer->data());
}

TEST_F(TestCudaHostBuffer, ViewOnDevice) {
  ASSERT_OK_AND_ASSIGN(auto host_buffer, device_->AllocateHostBuffer(1024));

  ASSERT_TRUE(host_buffer->is_cpu());
  ASSERT_EQ(host_buffer->memory_manager(), cpu_mm_);
  ASSERT_EQ(host_buffer->device_type(), DeviceAllocationType::kCUDA_HOST);

  // Try to view the host buffer on the device.  This should correspond to
  // GetDeviceAddress() in the previous test.
  ASSERT_OK_AND_ASSIGN(auto device_buffer, Buffer::View(host_buffer, mm_));
  ASSERT_FALSE(device_buffer->is_cpu());
  ASSERT_EQ(device_buffer->memory_manager(), mm_);
  ASSERT_NE(device_buffer->address(), 0);
  ASSERT_EQ(device_buffer->size(), host_buffer->size());
  ASSERT_EQ(device_buffer->parent(), host_buffer);
  ASSERT_EQ(device_buffer->device_type(), DeviceAllocationType::kCUDA);

  // View back the device buffer on the CPU.  This should roundtrip.
  ASSERT_OK_AND_ASSIGN(auto buffer, Buffer::View(device_buffer, cpu_mm_));
  ASSERT_TRUE(buffer->is_cpu());
  ASSERT_EQ(buffer->memory_manager(), cpu_mm_);
  ASSERT_EQ(buffer->address(), host_buffer->address());
  ASSERT_EQ(buffer->size(), host_buffer->size());
  ASSERT_EQ(buffer->parent(), device_buffer);
  ASSERT_EQ(buffer->device_type(), DeviceAllocationType::kCUDA_HOST);
}

// ------------------------------------------------------------------------
// Test CudaBufferWriter

class TestCudaBufferWriter : public TestCudaBase {
 public:
  void SetUp() { TestCudaBase::SetUp(); }

  void Allocate(const int64_t size) {
    ASSERT_OK_AND_ASSIGN(device_buffer_, context_->Allocate(size));
    writer_.reset(new CudaBufferWriter(device_buffer_));
  }

  void TestWrites(const int64_t total_bytes, const int64_t chunksize,
                  const int64_t buffer_size = 0) {
    std::shared_ptr<ResizableBuffer> buffer;
    ASSERT_OK(MakeRandomByteBuffer(total_bytes, default_memory_pool(), &buffer));

    if (buffer_size > 0) {
      ASSERT_OK(writer_->SetBufferSize(buffer_size));
    }

    ASSERT_OK_AND_EQ(0, writer_->Tell());

    const uint8_t* host_data = buffer->data();
    ASSERT_OK(writer_->Write(host_data, chunksize));
    ASSERT_OK_AND_EQ(chunksize, writer_->Tell());

    ASSERT_OK(writer_->Seek(0));
    ASSERT_OK_AND_EQ(0, writer_->Tell());

    int64_t position = 0;
    while (position < total_bytes) {
      int64_t bytes_to_write = std::min(chunksize, total_bytes - position);
      ASSERT_OK(writer_->Write(host_data + position, bytes_to_write));
      position += bytes_to_write;
    }

    ASSERT_OK(writer_->Flush());

    AssertCudaBufferEquals(*device_buffer_, *buffer);
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
  ASSERT_OK(writer_->Write(host_data, 0));
  ASSERT_OK_AND_EQ(0, writer_->Tell());

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
  AssertCudaBufferEquals(*device_buffer_, Buffer(host_data, 1000));
}

// ------------------------------------------------------------------------
// Test CudaBufferReader

class TestCudaBufferReader : public TestCudaBase {
 public:
  void SetUp() { TestCudaBase::SetUp(); }
};

TEST_F(TestCudaBufferReader, Basics) {
  std::shared_ptr<CudaBuffer> device_buffer;

  const int64_t size = 1000;
  ASSERT_OK_AND_ASSIGN(device_buffer, context_->Allocate(size));

  std::shared_ptr<ResizableBuffer> buffer;
  ASSERT_OK(MakeRandomByteBuffer(1000, default_memory_pool(), &buffer));
  const uint8_t* host_data = buffer->data();

  ASSERT_OK(device_buffer->CopyFromHost(0, host_data, 1000));

  CudaBufferReader reader(device_buffer);

  uint8_t stack_buffer[100] = {0};
  ASSERT_OK(reader.Seek(950));

  ASSERT_OK_AND_EQ(950, reader.Tell());

  // Read() to host memory
  ASSERT_OK_AND_EQ(50, reader.Read(100, stack_buffer));
  ASSERT_EQ(0, std::memcmp(stack_buffer, host_data + 950, 50));
  ASSERT_OK_AND_EQ(1000, reader.Tell());

  // ReadAt() to host memory
  ASSERT_OK_AND_EQ(45, reader.ReadAt(123, 45, stack_buffer));
  ASSERT_EQ(0, std::memcmp(stack_buffer, host_data + 123, 45));
  ASSERT_OK_AND_EQ(1000, reader.Tell());

  // Read() to device buffer
  ASSERT_OK(reader.Seek(925));
  ASSERT_OK_AND_ASSIGN(auto tmp, reader.Read(100));
  ASSERT_EQ(75, tmp->size());
  ASSERT_FALSE(tmp->is_cpu());
  ASSERT_EQ(*tmp->device(), *device_);
  ASSERT_OK_AND_EQ(1000, reader.Tell());

  ASSERT_OK(std::dynamic_pointer_cast<CudaBuffer>(tmp)->CopyToHost(0, tmp->size(),
                                                                   stack_buffer));
  ASSERT_EQ(0, std::memcmp(stack_buffer, host_data + 925, tmp->size()));

  // ReadAt() to device buffer
  ASSERT_OK(reader.Seek(42));
  ASSERT_OK_AND_ASSIGN(tmp, reader.ReadAt(980, 30));
  ASSERT_EQ(20, tmp->size());
  ASSERT_FALSE(tmp->is_cpu());
  ASSERT_EQ(*tmp->device(), *device_);
  ASSERT_OK_AND_EQ(42, reader.Tell());

  ASSERT_OK(std::dynamic_pointer_cast<CudaBuffer>(tmp)->CopyToHost(0, tmp->size(),
                                                                   stack_buffer));
  ASSERT_EQ(0, std::memcmp(stack_buffer, host_data + 980, tmp->size()));
}

TEST_F(TestCudaBufferReader, WillNeed) {
  std::shared_ptr<CudaBuffer> device_buffer;

  const int64_t size = 1000;
  ASSERT_OK_AND_ASSIGN(device_buffer, context_->Allocate(size));

  CudaBufferReader reader(device_buffer);

  ASSERT_OK(reader.WillNeed({{0, size}}));
}

// ------------------------------------------------------------------------
// Test Cuda IPC

class TestCudaArrowIpc : public TestCudaBase {
 public:
  void SetUp() {
    TestCudaBase::SetUp();
    pool_ = default_memory_pool();
  }

 protected:
  MemoryPool* pool_;
};

TEST_F(TestCudaArrowIpc, BasicWriteRead) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(ipc::test::MakeIntRecordBatch(&batch));

  std::shared_ptr<CudaBuffer> device_serialized;
  ASSERT_OK_AND_ASSIGN(device_serialized, SerializeRecordBatch(*batch, context_.get()));

  // Test that ReadRecordBatch works properly
  ipc::DictionaryMemo unused_memo;
  std::shared_ptr<RecordBatch> device_batch;
  ASSERT_OK_AND_ASSIGN(device_batch,
                       ReadRecordBatch(batch->schema(), &unused_memo, device_serialized));

  ASSERT_OK(device_batch->Validate());

  // Copy data from device, read batch, and compare
  int64_t size = device_serialized->size();
  ASSERT_OK_AND_ASSIGN(auto host_buffer, AllocateBuffer(size, pool_));
  ASSERT_OK(device_serialized->CopyToHost(0, size, host_buffer->mutable_data()));

  std::shared_ptr<RecordBatch> cpu_batch;
  io::BufferReader cpu_reader(std::move(host_buffer));
  ASSERT_OK_AND_ASSIGN(
      cpu_batch, ipc::ReadRecordBatch(batch->schema(), &unused_memo,
                                      ipc::IpcReadOptions::Defaults(), &cpu_reader));

  CompareBatch(*batch, *cpu_batch);
}

TEST_F(TestCudaArrowIpc, DictionaryWriteRead) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(ipc::test::MakeDictionary(&batch));

  ipc::DictionaryMemo dictionary_memo;
  ASSERT_OK(ipc::internal::CollectDictionaries(*batch, &dictionary_memo));

  std::shared_ptr<CudaBuffer> device_serialized;
  ASSERT_OK_AND_ASSIGN(device_serialized, SerializeRecordBatch(*batch, context_.get()));

  // Test that ReadRecordBatch works properly
  std::shared_ptr<RecordBatch> device_batch;
  ASSERT_OK_AND_ASSIGN(device_batch, ReadRecordBatch(batch->schema(), &dictionary_memo,
                                                     device_serialized));

  // Copy data from device, read batch, and compare
  int64_t size = device_serialized->size();
  ASSERT_OK_AND_ASSIGN(auto host_buffer, AllocateBuffer(size, pool_));
  ASSERT_OK(device_serialized->CopyToHost(0, size, host_buffer->mutable_data()));

  std::shared_ptr<RecordBatch> cpu_batch;
  io::BufferReader cpu_reader(std::move(host_buffer));
  ASSERT_OK_AND_ASSIGN(
      cpu_batch, ipc::ReadRecordBatch(batch->schema(), &dictionary_memo,
                                      ipc::IpcReadOptions::Defaults(), &cpu_reader));

  CompareBatch(*batch, *cpu_batch);
}

// ------------------------------------------------------------------------
// Test C Device Interface export/import with CUDA
// (equivalent tests for non-CUDA live in bridge_test.cc)

class TestCudaDeviceArrayRoundtrip : public ::testing::Test {
 public:
  using ArrayFactory = std::function<Result<std::shared_ptr<Array>>()>;

  static ArrayFactory JSONArrayFactory(std::shared_ptr<DataType> type, const char* json) {
    return [=]() { return ArrayFromJSON(type, json); };
  }

  template <typename ArrayFactory>
  void TestWithArrayFactory(ArrayFactory&& factory) {
    TestWithArrayFactory(factory, factory);
  }

  template <typename ArrayFactory, typename ExpectedArrayFactory>
  void TestWithArrayFactory(ArrayFactory&& factory,
                            ExpectedArrayFactory&& factory_expected) {
    ASSERT_OK_AND_ASSIGN(auto manager, cuda::CudaDeviceManager::Instance());
    ASSERT_OK_AND_ASSIGN(auto device, manager->GetDevice(0));
    auto mm = device->default_memory_manager();

    std::shared_ptr<Array> array;
    std::shared_ptr<Array> device_array;
    ASSERT_OK_AND_ASSIGN(array, factory());
    ASSERT_OK_AND_ASSIGN(device_array, array->CopyTo(mm));

    struct ArrowDeviceArray c_array {};
    struct ArrowSchema c_schema {};
    ArrayExportGuard array_guard(&c_array.array);
    SchemaExportGuard schema_guard(&c_schema);

    ASSERT_OK(ExportType(*device_array->type(), &c_schema));
    std::shared_ptr<Device::SyncEvent> sync{nullptr};
    ASSERT_OK(ExportDeviceArray(*device_array, sync, &c_array));

    std::shared_ptr<Array> device_array_roundtripped;
    ASSERT_OK_AND_ASSIGN(device_array_roundtripped,
                         ImportDeviceArray(&c_array, &c_schema));
    ASSERT_TRUE(ArrowSchemaIsReleased(&c_schema));
    ASSERT_TRUE(ArrowArrayIsReleased(&c_array.array));

    // Check value of imported array (copy to CPU to assert equality)
    std::shared_ptr<Array> array_roundtripped;
    ASSERT_OK_AND_ASSIGN(array_roundtripped,
                         device_array_roundtripped->CopyTo(default_cpu_memory_manager()));
    ASSERT_OK(array_roundtripped->ValidateFull());
    {
      std::shared_ptr<Array> expected;
      ASSERT_OK_AND_ASSIGN(expected, factory_expected());
      AssertTypeEqual(*expected->type(), *array_roundtripped->type());
      AssertArraysEqual(*expected, *array_roundtripped, true);
    }

    // Re-export and re-import, now both at once
    ASSERT_OK(ExportDeviceArray(*device_array, sync, &c_array, &c_schema));
    device_array_roundtripped.reset();
    ASSERT_OK_AND_ASSIGN(device_array_roundtripped,
                         ImportDeviceArray(&c_array, &c_schema));
    ASSERT_TRUE(ArrowSchemaIsReleased(&c_schema));
    ASSERT_TRUE(ArrowArrayIsReleased(&c_array.array));

    // Check value of imported array (copy to CPU to assert equality)
    array_roundtripped.reset();
    ASSERT_OK_AND_ASSIGN(array_roundtripped,
                         device_array_roundtripped->CopyTo(default_cpu_memory_manager()));
    ASSERT_OK(array_roundtripped->ValidateFull());
    {
      std::shared_ptr<Array> expected;
      ASSERT_OK_AND_ASSIGN(expected, factory_expected());
      AssertTypeEqual(*expected->type(), *array_roundtripped->type());
      AssertArraysEqual(*expected, *array_roundtripped, true);
    }
  }

  void TestWithJSON(std::shared_ptr<DataType> type, const char* json) {
    TestWithArrayFactory(JSONArrayFactory(type, json));
  }
};

TEST_F(TestCudaDeviceArrayRoundtrip, Primitive) { TestWithJSON(int32(), "[4, 5, null]"); }

TEST_F(TestCudaDeviceArrayRoundtrip, Struct) {
  auto type = struct_({field("ints", int16()), field("strs", utf8())});

  TestWithJSON(type, "[]");
  TestWithJSON(type, R"([[4, "foo"], [5, "bar"]])");
  TestWithJSON(type, R"([[4, null], null, [5, "foo"]])");
}

TEST_F(TestCudaDeviceArrayRoundtrip, Dictionary) {
  auto factory = []() {
    auto values = ArrayFromJSON(utf8(), R"(["foo", "bar", "quux"])");
    auto indices = ArrayFromJSON(uint16(), "[0, 2, 1, null, 1]");
    return DictionaryArray::FromArrays(dictionary(indices->type(), values->type()),
                                       indices, values);
  };
  TestWithArrayFactory(factory);
}

}  // namespace cuda
}  // namespace arrow
