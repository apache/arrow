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
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/device.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

static const char kMyDeviceTypeName[] = "arrowtest::MyDevice";
static const DeviceAllocationType kMyDeviceType = DeviceAllocationType::kEXT_DEV;

static const int kMyDeviceAllowCopy = 1;
static const int kMyDeviceAllowView = 2;
static const int kMyDeviceDisallowCopyView = 3;

class MyDevice : public Device {
 public:
  explicit MyDevice(int value) : Device(), value_(value) {}

  const char* type_name() const override { return kMyDeviceTypeName; }

  std::string ToString() const override {
    switch (value_) {
      case kMyDeviceAllowCopy:
        return "MyDevice[noview]";
      case kMyDeviceAllowView:
        return "MyDevice[nocopy]";
      default:
        return "MyDevice[nocopy][noview]";
    }
  }

  bool Equals(const Device& other) const override {
    if (other.type_name() != kMyDeviceTypeName) {
      return false;
    }
    return checked_cast<const MyDevice&>(other).value_ == value_;
  }

  DeviceAllocationType device_type() const override { return kMyDeviceType; }

  std::shared_ptr<MemoryManager> default_memory_manager() override;

  int value() const { return value_; }

  bool allow_copy() const { return value_ == kMyDeviceAllowCopy; }

  bool allow_view() const { return value_ == kMyDeviceAllowView; }

 protected:
  int value_;
};

class MyMemoryManager : public MemoryManager {
 public:
  explicit MyMemoryManager(std::shared_ptr<Device> device) : MemoryManager(device) {}

  bool allow_copy() const {
    return checked_cast<const MyDevice&>(*device()).allow_copy();
  }

  bool allow_view() const {
    return checked_cast<const MyDevice&>(*device()).allow_view();
  }

  Result<std::shared_ptr<io::RandomAccessFile>> GetBufferReader(
      std::shared_ptr<Buffer> buf) override {
    return Status::NotImplemented("");
  }

  Result<std::unique_ptr<Buffer>> AllocateBuffer(int64_t size) override {
    return Status::NotImplemented("");
  }

  Result<std::shared_ptr<io::OutputStream>> GetBufferWriter(
      std::shared_ptr<Buffer> buf) override {
    return Status::NotImplemented("");
  }

 protected:
  Result<std::shared_ptr<Buffer>> CopyBufferFrom(
      const std::shared_ptr<Buffer>& buf,
      const std::shared_ptr<MemoryManager>& from) override;
  Result<std::shared_ptr<Buffer>> CopyBufferTo(
      const std::shared_ptr<Buffer>& buf,
      const std::shared_ptr<MemoryManager>& to) override;
  Result<std::unique_ptr<Buffer>> CopyNonOwnedFrom(
      const Buffer& buf, const std::shared_ptr<MemoryManager>& from) override;
  Result<std::unique_ptr<Buffer>> CopyNonOwnedTo(
      const Buffer& buf, const std::shared_ptr<MemoryManager>& to) override;
  Result<std::shared_ptr<Buffer>> ViewBufferFrom(
      const std::shared_ptr<Buffer>& buf,
      const std::shared_ptr<MemoryManager>& from) override;
  Result<std::shared_ptr<Buffer>> ViewBufferTo(
      const std::shared_ptr<Buffer>& buf,
      const std::shared_ptr<MemoryManager>& to) override;
};

class MyBuffer : public Buffer {
 public:
  MyBuffer(std::shared_ptr<MemoryManager> mm, const std::shared_ptr<Buffer>& parent)
      : Buffer(parent->data(), parent->size()) {
    parent_ = parent;
    SetMemoryManager(mm);
  }
};

std::shared_ptr<MemoryManager> MyDevice::default_memory_manager() {
  return std::make_shared<MyMemoryManager>(shared_from_this());
}

Result<std::shared_ptr<Buffer>> MyMemoryManager::CopyBufferFrom(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& from) {
  return CopyNonOwnedFrom(*buf, from);
}

Result<std::shared_ptr<Buffer>> MyMemoryManager::CopyBufferTo(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& to) {
  return CopyNonOwnedTo(*buf, to);
}

Result<std::unique_ptr<Buffer>> MyMemoryManager::CopyNonOwnedFrom(
    const Buffer& buf, const std::shared_ptr<MemoryManager>& from) {
  if (!allow_copy()) {
    return nullptr;
  }
  if (from->is_cpu()) {
    // CPU to MyDevice:
    // 1. CPU to CPU
    ARROW_ASSIGN_OR_RAISE(auto dest,
                          MemoryManager::CopyNonOwned(buf, default_cpu_memory_manager()));
    // 2. Wrap CPU buffer result
    return std::make_unique<MyBuffer>(shared_from_this(), std::move(dest));
  }
  return nullptr;
}

Result<std::unique_ptr<Buffer>> MyMemoryManager::CopyNonOwnedTo(
    const Buffer& buf, const std::shared_ptr<MemoryManager>& to) {
  if (!allow_copy()) {
    return nullptr;
  }
  if (to->is_cpu() && buf.parent()) {
    // MyDevice to CPU
    return MemoryManager::CopyNonOwned(*buf.parent(), to);
  }
  return nullptr;
}

Result<std::shared_ptr<Buffer>> MyMemoryManager::ViewBufferFrom(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& from) {
  if (!allow_view()) {
    return nullptr;
  }
  if (from->is_cpu()) {
    // CPU on MyDevice: wrap CPU buffer
    return std::make_shared<MyBuffer>(shared_from_this(), buf);
  }
  return nullptr;
}

Result<std::shared_ptr<Buffer>> MyMemoryManager::ViewBufferTo(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& to) {
  if (!allow_view()) {
    return nullptr;
  }
  if (to->is_cpu() && buf->parent()) {
    // MyDevice on CPU: unwrap buffer
    return buf->parent();
  }
  return nullptr;
}

// Like AssertBufferEqual, but doesn't call Buffer::data()
void AssertMyBufferEqual(const Buffer& buffer, std::string_view expected) {
  ASSERT_EQ(std::string_view(buffer), expected);
}

void AssertIsCPUBuffer(const Buffer& buf) {
  ASSERT_TRUE(buf.is_cpu());
  ASSERT_EQ(*buf.device(), *CPUDevice::Instance());
}

class TestDevice : public ::testing::Test {
 public:
  void SetUp() {
    cpu_device_ = CPUDevice::Instance();
    my_copy_device_ = std::make_shared<MyDevice>(kMyDeviceAllowCopy);
    my_view_device_ = std::make_shared<MyDevice>(kMyDeviceAllowView);
    my_other_device_ = std::make_shared<MyDevice>(kMyDeviceDisallowCopyView);

    cpu_mm_ = cpu_device_->default_memory_manager();
    my_copy_mm_ = my_copy_device_->default_memory_manager();
    my_view_mm_ = my_view_device_->default_memory_manager();
    my_other_mm_ = my_other_device_->default_memory_manager();

    cpu_src_ = Buffer::FromString("some data");
    my_copy_src_ = std::make_shared<MyBuffer>(my_copy_mm_, cpu_src_);
    my_view_src_ = std::make_shared<MyBuffer>(my_view_mm_, cpu_src_);
    my_other_src_ = std::make_shared<MyBuffer>(my_other_mm_, cpu_src_);
  }

 protected:
  std::shared_ptr<Device> cpu_device_, my_copy_device_, my_view_device_, my_other_device_;
  std::shared_ptr<MemoryManager> cpu_mm_, my_copy_mm_, my_view_mm_, my_other_mm_;
  std::shared_ptr<Buffer> cpu_src_, my_copy_src_, my_view_src_, my_other_src_;
};

TEST_F(TestDevice, Basics) {
  ASSERT_TRUE(cpu_device_->is_cpu());

  ASSERT_EQ(*cpu_device_, *cpu_device_);
  ASSERT_EQ(*my_copy_device_, *my_copy_device_);
  ASSERT_NE(*cpu_device_, *my_copy_device_);
  ASSERT_NE(*my_copy_device_, *cpu_device_);

  ASSERT_TRUE(cpu_mm_->is_cpu());
  ASSERT_FALSE(my_copy_mm_->is_cpu());
  ASSERT_FALSE(my_other_mm_->is_cpu());
}

TEST_F(TestDevice, Copy) {
  // CPU-to-CPU
  ASSERT_OK_AND_ASSIGN(auto buffer, MemoryManager::CopyBuffer(cpu_src_, cpu_mm_));
  ASSERT_EQ(buffer->device(), cpu_device_);
  ASSERT_TRUE(buffer->is_cpu());
  ASSERT_NE(buffer->address(), cpu_src_->address());
  ASSERT_EQ(buffer->device_type(), DeviceAllocationType::kCPU);
  ASSERT_NE(buffer->data(), nullptr);
  AssertBufferEqual(*buffer, "some data");

  ASSERT_OK_AND_ASSIGN(buffer, MemoryManager::CopyNonOwned(*cpu_src_, cpu_mm_));
  ASSERT_EQ(buffer->device(), cpu_device_);
  ASSERT_TRUE(buffer->is_cpu());
  ASSERT_NE(buffer->address(), cpu_src_->address());
  ASSERT_EQ(buffer->device_type(), DeviceAllocationType::kCPU);
  ASSERT_NE(buffer->data(), nullptr);
  AssertBufferEqual(*buffer, "some data");

  // CPU-to-device
  ASSERT_OK_AND_ASSIGN(buffer, MemoryManager::CopyBuffer(cpu_src_, my_copy_mm_));
  ASSERT_EQ(buffer->device(), my_copy_device_);
  ASSERT_FALSE(buffer->is_cpu());
  ASSERT_NE(buffer->address(), cpu_src_->address());
  ASSERT_EQ(buffer->device_type(), kMyDeviceType);
#ifdef NDEBUG
  ASSERT_EQ(buffer->data(), nullptr);
#endif
  AssertMyBufferEqual(*buffer, "some data");

  ASSERT_OK_AND_ASSIGN(buffer, MemoryManager::CopyNonOwned(*cpu_src_, my_copy_mm_));
  ASSERT_EQ(buffer->device(), my_copy_device_);
  ASSERT_FALSE(buffer->is_cpu());
  ASSERT_NE(buffer->address(), cpu_src_->address());
  ASSERT_EQ(buffer->device_type(), kMyDeviceType);
#ifdef NDEBUG
  ASSERT_EQ(buffer->data(), nullptr);
#endif
  AssertMyBufferEqual(*buffer, "some data");

  // Device-to-CPU
  ASSERT_OK_AND_ASSIGN(buffer, MemoryManager::CopyBuffer(my_copy_src_, cpu_mm_));
  ASSERT_EQ(buffer->device(), cpu_device_);
  ASSERT_TRUE(buffer->is_cpu());
  ASSERT_NE(buffer->address(), my_copy_src_->address());
  ASSERT_EQ(buffer->device_type(), DeviceAllocationType::kCPU);
  ASSERT_NE(buffer->data(), nullptr);
  AssertBufferEqual(*buffer, "some data");

  ASSERT_OK_AND_ASSIGN(buffer, MemoryManager::CopyNonOwned(*my_copy_src_, cpu_mm_));
  ASSERT_EQ(buffer->device(), cpu_device_);
  ASSERT_TRUE(buffer->is_cpu());
  ASSERT_NE(buffer->address(), my_copy_src_->address());
  ASSERT_EQ(buffer->device_type(), DeviceAllocationType::kCPU);
  ASSERT_NE(buffer->data(), nullptr);
  AssertBufferEqual(*buffer, "some data");

  // Device-to-device with an intermediate CPU copy
  ASSERT_OK_AND_ASSIGN(buffer, MemoryManager::CopyBuffer(my_copy_src_, my_copy_mm_));
  ASSERT_EQ(buffer->device(), my_copy_device_);
  ASSERT_FALSE(buffer->is_cpu());
  ASSERT_NE(buffer->address(), my_copy_src_->address());
  ASSERT_EQ(buffer->device_type(), kMyDeviceType);
#ifdef NDEBUG
  ASSERT_EQ(buffer->data(), nullptr);
#endif
  AssertMyBufferEqual(*buffer, "some data");

  // Device-to-device with an intermediate view on CPU, then a copy from CPU to device
  ASSERT_OK_AND_ASSIGN(buffer, MemoryManager::CopyBuffer(my_view_src_, my_copy_mm_));
  ASSERT_EQ(buffer->device(), my_copy_device_);
  ASSERT_FALSE(buffer->is_cpu());
  ASSERT_NE(buffer->address(), my_copy_src_->address());
  ASSERT_EQ(buffer->device_type(), kMyDeviceType);
#ifdef NDEBUG
  ASSERT_EQ(buffer->data(), nullptr);
#endif
  AssertMyBufferEqual(*buffer, "some data");

  ASSERT_RAISES(NotImplemented, MemoryManager::CopyBuffer(cpu_src_, my_other_mm_));
  ASSERT_RAISES(NotImplemented, MemoryManager::CopyBuffer(my_other_src_, cpu_mm_));
}

TEST_F(TestDevice, View) {
  // CPU-on-CPU
  ASSERT_OK_AND_ASSIGN(auto buffer, MemoryManager::ViewBuffer(cpu_src_, cpu_mm_));
  ASSERT_EQ(buffer->device(), cpu_device_);
  ASSERT_TRUE(buffer->is_cpu());
  ASSERT_EQ(buffer->address(), cpu_src_->address());
  ASSERT_EQ(buffer->device_type(), DeviceAllocationType::kCPU);
  ASSERT_NE(buffer->data(), nullptr);
  AssertBufferEqual(*buffer, "some data");

  // CPU-on-device
  ASSERT_OK_AND_ASSIGN(buffer, MemoryManager::ViewBuffer(cpu_src_, my_view_mm_));
  ASSERT_EQ(buffer->device(), my_view_device_);
  ASSERT_FALSE(buffer->is_cpu());
  ASSERT_EQ(buffer->address(), cpu_src_->address());
  ASSERT_EQ(buffer->device_type(), kMyDeviceType);
#ifdef NDEBUG
  ASSERT_EQ(buffer->data(), nullptr);
#endif
  AssertMyBufferEqual(*buffer, "some data");

  // Device-on-CPU
  ASSERT_OK_AND_ASSIGN(buffer, MemoryManager::ViewBuffer(my_view_src_, cpu_mm_));
  ASSERT_EQ(buffer->device(), cpu_device_);
  ASSERT_TRUE(buffer->is_cpu());
  ASSERT_EQ(buffer->address(), my_copy_src_->address());
  ASSERT_EQ(buffer->device_type(), DeviceAllocationType::kCPU);
  ASSERT_NE(buffer->data(), nullptr);
  AssertBufferEqual(*buffer, "some data");

  ASSERT_RAISES(NotImplemented, MemoryManager::CopyBuffer(cpu_src_, my_other_mm_));
  ASSERT_RAISES(NotImplemented, MemoryManager::CopyBuffer(my_other_src_, cpu_mm_));
}

TEST(TestAllocate, Basics) {
  ASSERT_OK_AND_ASSIGN(auto new_buffer, AllocateBuffer(1024));
  auto mm = new_buffer->memory_manager();
  ASSERT_TRUE(mm->is_cpu());
  ASSERT_EQ(mm.get(), default_cpu_memory_manager().get());
  auto cpu_mm = checked_pointer_cast<CPUMemoryManager>(mm);
  ASSERT_EQ(cpu_mm->pool(), default_memory_pool());

  auto pool = std::make_shared<ProxyMemoryPool>(default_memory_pool());
  ASSERT_OK_AND_ASSIGN(new_buffer, AllocateBuffer(1024, pool.get()));
  mm = new_buffer->memory_manager();
  ASSERT_TRUE(mm->is_cpu());
  cpu_mm = checked_pointer_cast<CPUMemoryManager>(mm);
  ASSERT_EQ(cpu_mm->pool(), pool.get());
  new_buffer.reset();  // Destroy before pool
}

TEST(TestAllocate, Bitmap) {
  ASSERT_OK_AND_ASSIGN(auto new_buffer, AllocateBitmap(100));
  AssertIsCPUBuffer(*new_buffer);
  EXPECT_GE(new_buffer->size(), 13);
  EXPECT_EQ(new_buffer->capacity() % 8, 0);
}

TEST(TestAllocate, EmptyBitmap) {
  ASSERT_OK_AND_ASSIGN(auto new_buffer, AllocateEmptyBitmap(100));
  AssertIsCPUBuffer(*new_buffer);
  EXPECT_EQ(new_buffer->size(), 13);
  EXPECT_EQ(new_buffer->capacity() % 8, 0);
  EXPECT_TRUE(std::all_of(new_buffer->data(), new_buffer->data() + new_buffer->capacity(),
                          [](int8_t byte) { return byte == 0; }));
}

TEST(TestBuffer, FromStdString) {
  std::string val = "hello, world";

  Buffer buf(val);
  AssertIsCPUBuffer(buf);
  ASSERT_EQ(0, memcmp(buf.data(), val.c_str(), val.size()));
  ASSERT_EQ(static_cast<int64_t>(val.size()), buf.size());
}

TEST(TestBuffer, Alignment) {
  std::string val = "hello, world";

  constexpr int64_t kAlignmentTest = 1024;
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Buffer> buf,
                       AllocateBuffer(val.size(), kAlignmentTest));
  ASSERT_EQ(buf->address() % kAlignmentTest, 0);
}

TEST(TestBuffer, FromStdStringWithMemory) {
  std::string expected = "hello, world";
  std::shared_ptr<Buffer> buf;

  {
    std::string temp = "hello, world";
    buf = Buffer::FromString(temp);
    AssertIsCPUBuffer(*buf);
    ASSERT_EQ(0, memcmp(buf->data(), temp.c_str(), temp.size()));
    ASSERT_EQ(static_cast<int64_t>(temp.size()), buf->size());
  }

  // Now temp goes out of scope and we check if created buffer
  // is still valid to make sure it actually owns its space
  ASSERT_EQ(0, memcmp(buf->data(), expected.c_str(), expected.size()));
  ASSERT_EQ(static_cast<int64_t>(expected.size()), buf->size());
}

TEST(TestBuffer, EqualsWithSameContent) {
  MemoryPool* pool = default_memory_pool();
  const int32_t bufferSize = 128 * 1024;
  uint8_t* rawBuffer1;
  ASSERT_OK(pool->Allocate(bufferSize, &rawBuffer1));
  memset(rawBuffer1, 12, bufferSize);
  uint8_t* rawBuffer2;
  ASSERT_OK(pool->Allocate(bufferSize, &rawBuffer2));
  memset(rawBuffer2, 12, bufferSize);
  uint8_t* rawBuffer3;
  ASSERT_OK(pool->Allocate(bufferSize, &rawBuffer3));
  memset(rawBuffer3, 3, bufferSize);

  Buffer buffer1(rawBuffer1, bufferSize);
  Buffer buffer2(rawBuffer2, bufferSize);
  Buffer buffer3(rawBuffer3, bufferSize);
  ASSERT_TRUE(buffer1.Equals(buffer2));
  ASSERT_FALSE(buffer1.Equals(buffer3));

  pool->Free(rawBuffer1, bufferSize);
  pool->Free(rawBuffer2, bufferSize);
  pool->Free(rawBuffer3, bufferSize);
}

TEST(TestBuffer, EqualsWithSameBuffer) {
  MemoryPool* pool = default_memory_pool();
  const int32_t bufferSize = 128 * 1024;
  uint8_t* rawBuffer;
  ASSERT_OK(pool->Allocate(bufferSize, &rawBuffer));
  memset(rawBuffer, 111, bufferSize);

  Buffer buffer1(rawBuffer, bufferSize);
  Buffer buffer2(rawBuffer, bufferSize);
  ASSERT_TRUE(buffer1.Equals(buffer2));

  const int64_t nbytes = bufferSize / 2;
  Buffer buffer3(rawBuffer, nbytes);
  ASSERT_TRUE(buffer1.Equals(buffer3, nbytes));
  ASSERT_FALSE(buffer1.Equals(buffer3, nbytes + 1));

  pool->Free(rawBuffer, bufferSize);
}

TEST(TestBuffer, CopySlice) {
  std::string data_str = "some data to copy";

  auto data = reinterpret_cast<const uint8_t*>(data_str.c_str());

  Buffer buf(data, data_str.size());

  ASSERT_OK_AND_ASSIGN(auto out, buf.CopySlice(5, 4));
  AssertIsCPUBuffer(*out);

  Buffer expected(data + 5, 4);
  ASSERT_TRUE(out->Equals(expected));
  // assert the padding is zeroed
  std::vector<uint8_t> zeros(out->capacity() - out->size());
  ASSERT_EQ(0, memcmp(out->data() + out->size(), zeros.data(), zeros.size()));
}

TEST(TestBuffer, CopySliceEmpty) {
  auto buf = std::make_shared<Buffer>("");
  ASSERT_OK_AND_ASSIGN(auto out, buf->CopySlice(0, 0));
  AssertBufferEqual(*out, "");

  buf = std::make_shared<Buffer>("1234");
  ASSERT_OK_AND_ASSIGN(out, buf->CopySlice(0, 0));
  AssertBufferEqual(*out, "");
  ASSERT_OK_AND_ASSIGN(out, buf->CopySlice(4, 0));
  AssertBufferEqual(*out, "");
}

TEST(TestBuffer, ToHexString) {
  const std::string data_str = "\a0hex string\xa9";
  Buffer buf(data_str);
  ASSERT_EQ(buf.ToHexString(), std::string("073068657820737472696E67A9"));
}

TEST(TestBuffer, SliceBuffer) {
  std::string data_str = "some data to slice";
  auto data = reinterpret_cast<const uint8_t*>(data_str.c_str());

  auto buf = std::make_shared<Buffer>(data, data_str.size());

  std::shared_ptr<Buffer> out = SliceBuffer(buf, 5, 4);
  AssertIsCPUBuffer(*out);
  Buffer expected(data + 5, 4);
  ASSERT_TRUE(out->Equals(expected));

  ASSERT_EQ(2, buf.use_count());
}

TEST(TestBuffer, SliceBufferSafe) {
  std::string data_str = "some data to slice";
  auto data = reinterpret_cast<const uint8_t*>(data_str.c_str());

  auto buf = std::make_shared<Buffer>(data, data_str.size());

  ASSERT_OK_AND_ASSIGN(auto sliced, SliceBufferSafe(buf, 5, 4));
  AssertBufferEqual(*sliced, "data");
  ASSERT_OK_AND_ASSIGN(sliced, SliceBufferSafe(buf, 0, 4));
  AssertBufferEqual(*sliced, "some");
  ASSERT_OK_AND_ASSIGN(sliced, SliceBufferSafe(buf, 0, 0));
  AssertBufferEqual(*sliced, "");
  ASSERT_OK_AND_ASSIGN(sliced, SliceBufferSafe(buf, 4, 0));
  AssertBufferEqual(*sliced, "");
  ASSERT_OK_AND_ASSIGN(sliced, SliceBufferSafe(buf, buf->size(), 0));
  AssertBufferEqual(*sliced, "");

  ASSERT_RAISES(IndexError, SliceBufferSafe(buf, -1, 0));
  ASSERT_RAISES(IndexError, SliceBufferSafe(buf, 0, -1));
  ASSERT_RAISES(IndexError, SliceBufferSafe(buf, 0, buf->size() + 1));
  ASSERT_RAISES(IndexError, SliceBufferSafe(buf, 2, buf->size() - 1));
  ASSERT_RAISES(IndexError, SliceBufferSafe(buf, buf->size() + 1, 0));
  ASSERT_RAISES(IndexError,
                SliceBufferSafe(buf, 3, std::numeric_limits<int64_t>::max() - 2));

  ASSERT_OK_AND_ASSIGN(sliced, SliceBufferSafe(buf, 0));
  AssertBufferEqual(*sliced, "some data to slice");
  ASSERT_OK_AND_ASSIGN(sliced, SliceBufferSafe(buf, 5));
  AssertBufferEqual(*sliced, "data to slice");
  ASSERT_OK_AND_ASSIGN(sliced, SliceBufferSafe(buf, buf->size()));
  AssertBufferEqual(*sliced, "");

  ASSERT_RAISES(IndexError, SliceBufferSafe(buf, -1));
  ASSERT_RAISES(IndexError, SliceBufferSafe(buf, buf->size() + 1));
}

TEST(TestMutableBuffer, Wrap) {
  std::vector<int32_t> values = {1, 2, 3};

  auto buf = MutableBuffer::Wrap(values.data(), values.size());
  AssertIsCPUBuffer(*buf);
  reinterpret_cast<int32_t*>(buf->mutable_data())[1] = 4;

  ASSERT_EQ(4, values[1]);
}

TEST(TestBuffer, FromStringRvalue) {
  std::string expected = "input data";

  std::shared_ptr<Buffer> buffer;
  {
    std::string data_str = "input data";
    buffer = Buffer::FromString(std::move(data_str));
    AssertIsCPUBuffer(*buffer);
  }

  ASSERT_FALSE(buffer->is_mutable());

  ASSERT_EQ(0, memcmp(buffer->data(), expected.c_str(), expected.size()));
  ASSERT_EQ(static_cast<int64_t>(expected.size()), buffer->size());
}

TEST(TestBuffer, SliceMutableBuffer) {
  std::string data_str = "some data to slice";
  auto data = reinterpret_cast<const uint8_t*>(data_str.c_str());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buffer, AllocateBuffer(50));

  memcpy(buffer->mutable_data(), data, data_str.size());

  std::shared_ptr<Buffer> slice = SliceMutableBuffer(buffer, 5, 10);
  AssertIsCPUBuffer(*slice);
  ASSERT_TRUE(slice->is_mutable());
  ASSERT_EQ(10, slice->size());

  Buffer expected(data + 5, 10);
  ASSERT_TRUE(slice->Equals(expected));
}

TEST(TestBuffer, GetReader) {
  const std::string data_str = "some data to read";
  auto data = reinterpret_cast<const uint8_t*>(data_str.c_str());

  auto buf = std::make_shared<Buffer>(data, data_str.size());
  ASSERT_OK_AND_ASSIGN(auto reader, Buffer::GetReader(buf));
  ASSERT_OK_AND_EQ(static_cast<int64_t>(data_str.size()), reader->GetSize());
  ASSERT_OK_AND_ASSIGN(auto read_buf, reader->ReadAt(5, 4));
  AssertBufferEqual(*read_buf, "data");
}

TEST(TestBuffer, GetWriter) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buf, AllocateBuffer(9));
  ASSERT_OK_AND_ASSIGN(auto writer, Buffer::GetWriter(buf));
  ASSERT_OK(writer->Write(reinterpret_cast<const uint8_t*>("some data"), 9));
  AssertBufferEqual(*buf, "some data");

  // Non-mutable buffer
  buf = std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>("xxx"), 3);
  ASSERT_RAISES(Invalid, Buffer::GetWriter(buf));
}

template <typename AllocateFunction>
void TestZeroSizeAllocateBuffer(MemoryPool* pool, AllocateFunction&& allocate_func) {
  auto allocated_bytes = pool->bytes_allocated();
  {
    std::shared_ptr<Buffer> buffer, buffer2;

    ASSERT_OK(allocate_func(pool, 0, &buffer));
    AssertIsCPUBuffer(*buffer);
    ASSERT_EQ(buffer->size(), 0);
    // Even 0-sized buffers should not have a null data pointer
    auto data = buffer->data();
    ASSERT_NE(data, nullptr);
    ASSERT_EQ(buffer->mutable_data(), data);

    // As an optimization, another 0-size buffer should share the same memory "area"
    ASSERT_OK(allocate_func(pool, 0, &buffer2));
    AssertIsCPUBuffer(*buffer2);
    ASSERT_EQ(buffer2->size(), 0);
    ASSERT_EQ(buffer2->data(), data);

    ASSERT_GE(pool->bytes_allocated(), allocated_bytes);
  }
  ASSERT_EQ(pool->bytes_allocated(), allocated_bytes);
}

TEST(TestAllocateBuffer, ZeroSize) {
  MemoryPool* pool = default_memory_pool();
  auto allocate_func = [](MemoryPool* pool, int64_t size, std::shared_ptr<Buffer>* out) {
    return AllocateBuffer(size, pool).Value(out);
  };
  TestZeroSizeAllocateBuffer(pool, allocate_func);
}

TEST(TestAllocateResizableBuffer, ZeroSize) {
  MemoryPool* pool = default_memory_pool();
  auto allocate_func = [](MemoryPool* pool, int64_t size, std::shared_ptr<Buffer>* out) {
    ARROW_ASSIGN_OR_RAISE(auto resizable, AllocateResizableBuffer(size, pool));
    *out = std::move(resizable);
    return Status::OK();
  };
  TestZeroSizeAllocateBuffer(pool, allocate_func);
}

TEST(TestAllocateResizableBuffer, ZeroResize) {
  MemoryPool* pool = default_memory_pool();
  auto allocated_bytes = pool->bytes_allocated();
  {
    std::shared_ptr<ResizableBuffer> buffer;

    ASSERT_OK_AND_ASSIGN(buffer, AllocateResizableBuffer(1000, pool));
    ASSERT_EQ(buffer->size(), 1000);
    ASSERT_NE(buffer->data(), nullptr);
    ASSERT_EQ(buffer->mutable_data(), buffer->data());

    ASSERT_GE(pool->bytes_allocated(), allocated_bytes + 1000);

    ASSERT_OK(buffer->Resize(0));
    ASSERT_NE(buffer->data(), nullptr);
    ASSERT_EQ(buffer->mutable_data(), buffer->data());

    ASSERT_GE(pool->bytes_allocated(), allocated_bytes);
    ASSERT_LT(pool->bytes_allocated(), allocated_bytes + 1000);
  }
  ASSERT_EQ(pool->bytes_allocated(), allocated_bytes);
}

TEST(TestBufferBuilder, ResizeReserve) {
  const std::string data = "some data";
  auto data_ptr = data.c_str();

  BufferBuilder builder;

  ASSERT_OK(builder.Append(data_ptr, 9));
  ASSERT_EQ(9, builder.length());

  ASSERT_OK(builder.Resize(128));
  ASSERT_EQ(128, builder.capacity());
  ASSERT_EQ(9, builder.length());

  // Do not shrink to fit
  ASSERT_OK(builder.Resize(64, false));
  ASSERT_EQ(128, builder.capacity());
  ASSERT_EQ(9, builder.length());

  // Shrink to fit
  ASSERT_OK(builder.Resize(64));
  ASSERT_EQ(64, builder.capacity());
  ASSERT_EQ(9, builder.length());

  // Reserve elements
  ASSERT_OK(builder.Reserve(60));
  ASSERT_EQ(128, builder.capacity());
  ASSERT_EQ(9, builder.length());
}

TEST(TestBufferBuilder, Alignment) {
  const std::string data = "some data";
  auto data_ptr = data.c_str();

  constexpr int kTestAlignment = 512;
  BufferBuilder builder(default_memory_pool(), /*alignment=*/kTestAlignment);
#define TEST_ALIGNMENT() \
  ASSERT_EQ(reinterpret_cast<uintptr_t>(builder.data()) % kTestAlignment, 0)

  ASSERT_OK(builder.Append(data_ptr, 9));
  TEST_ALIGNMENT();

  ASSERT_OK(builder.Resize(128));
  ASSERT_EQ(128, builder.capacity());
  ASSERT_EQ(9, builder.length());
  TEST_ALIGNMENT();

  // Do not shrink to fit
  ASSERT_OK(builder.Resize(64, false));
  TEST_ALIGNMENT();

  // Shrink to fit
  ASSERT_OK(builder.Resize(64));
  TEST_ALIGNMENT();

  // Reserve elements
  ASSERT_OK(builder.Reserve(60));
  TEST_ALIGNMENT();
#undef TEST_ALIGNMENT
}

TEST(TestBufferBuilder, Finish) {
  const std::string data = "some data";
  auto data_ptr = data.c_str();

  for (const bool shrink_to_fit : {true, false}) {
    ARROW_SCOPED_TRACE("shrink_to_fit = ", shrink_to_fit);
    BufferBuilder builder;
    ASSERT_OK(builder.Append(data_ptr, 9));
    ASSERT_OK(builder.Append(data_ptr, 9));
    ASSERT_EQ(18, builder.length());
    ASSERT_EQ(64, builder.capacity());

    ASSERT_OK_AND_ASSIGN(auto buf, builder.Finish(shrink_to_fit));
    ASSERT_EQ(buf->size(), 18);
    ASSERT_EQ(buf->capacity(), 64);
  }
  for (const bool shrink_to_fit : {true, false}) {
    ARROW_SCOPED_TRACE("shrink_to_fit = ", shrink_to_fit);
    BufferBuilder builder;
    ASSERT_OK(builder.Reserve(1024));
    builder.UnsafeAppend(data_ptr, 9);
    builder.UnsafeAppend(data_ptr, 9);
    ASSERT_EQ(18, builder.length());
    ASSERT_EQ(builder.capacity(), 1024);

    ASSERT_OK_AND_ASSIGN(auto buf, builder.Finish(shrink_to_fit));
    ASSERT_EQ(buf->size(), 18);
    ASSERT_EQ(buf->capacity(), shrink_to_fit ? 64 : 1024);
  }
}

TEST(TestBufferBuilder, FinishEmpty) {
  for (const bool shrink_to_fit : {true, false}) {
    ARROW_SCOPED_TRACE("shrink_to_fit = ", shrink_to_fit);
    BufferBuilder builder;
    ASSERT_EQ(0, builder.length());
    ASSERT_EQ(0, builder.capacity());

    ASSERT_OK_AND_ASSIGN(auto buf, builder.Finish(shrink_to_fit));
    ASSERT_EQ(buf->size(), 0);
    ASSERT_EQ(buf->capacity(), 0);
  }
  for (const bool shrink_to_fit : {true, false}) {
    ARROW_SCOPED_TRACE("shrink_to_fit = ", shrink_to_fit);
    BufferBuilder builder;
    ASSERT_OK(builder.Reserve(1024));
    ASSERT_EQ(0, builder.length());
    ASSERT_EQ(1024, builder.capacity());

    ASSERT_OK_AND_ASSIGN(auto buf, builder.Finish(shrink_to_fit));
    ASSERT_EQ(buf->size(), 0);
    ASSERT_EQ(buf->capacity(), shrink_to_fit ? 0 : 1024);
  }
}

template <typename T>
class TypedTestBufferBuilder : public ::testing::Test {};

using BufferBuilderElements = ::testing::Types<int16_t, uint32_t, double>;

TYPED_TEST_SUITE(TypedTestBufferBuilder, BufferBuilderElements);

TYPED_TEST(TypedTestBufferBuilder, BasicTypedBufferBuilderUsage) {
  TypedBufferBuilder<TypeParam> builder;

  ASSERT_OK(builder.Append(static_cast<TypeParam>(0)));
  ASSERT_EQ(builder.length(), 1);
  ASSERT_EQ(builder.capacity(), 64 / sizeof(TypeParam));

  constexpr int nvalues = 4;
  TypeParam values[nvalues];
  for (int i = 0; i != nvalues; ++i) {
    values[i] = static_cast<TypeParam>(i);
  }
  ASSERT_OK(builder.Append(values, nvalues));
  ASSERT_EQ(builder.length(), nvalues + 1);

  std::shared_ptr<Buffer> built;
  ASSERT_OK(builder.Finish(&built));
  AssertIsCPUBuffer(*built);

  auto data = reinterpret_cast<const TypeParam*>(built->data());
  ASSERT_EQ(data[0], static_cast<TypeParam>(0));
  for (auto value : values) {
    ++data;
    ASSERT_EQ(*data, value);
  }
}

TYPED_TEST(TypedTestBufferBuilder, AppendCopies) {
  TypedBufferBuilder<TypeParam> builder;

  ASSERT_OK(builder.Append(13, static_cast<TypeParam>(1)));
  ASSERT_OK(builder.Append(17, static_cast<TypeParam>(0)));
  ASSERT_EQ(builder.length(), 13 + 17);

  std::shared_ptr<Buffer> built;
  ASSERT_OK(builder.Finish(&built));

  auto data = reinterpret_cast<const TypeParam*>(built->data());
  for (int i = 0; i != 13 + 17; ++i, ++data) {
    ASSERT_EQ(*data, static_cast<TypeParam>(i < 13)) << "index = " << i;
  }
}

TEST(TestBoolBufferBuilder, Basics) {
  TypedBufferBuilder<bool> builder;

  ASSERT_OK(builder.Append(false));
  ASSERT_EQ(builder.length(), 1);
  ASSERT_EQ(builder.capacity(), 64 * 8);

  constexpr int nvalues = 4;
  uint8_t values[nvalues];
  for (int i = 0; i != nvalues; ++i) {
    values[i] = static_cast<uint8_t>(i);
  }
  ASSERT_OK(builder.Append(values, nvalues));
  ASSERT_EQ(builder.length(), nvalues + 1);

  ASSERT_EQ(builder.false_count(), 2);

  std::shared_ptr<Buffer> built;
  ASSERT_OK(builder.Finish(&built));
  AssertIsCPUBuffer(*built);

  ASSERT_EQ(bit_util::GetBit(built->data(), 0), false);
  for (int i = 0; i != nvalues; ++i) {
    ASSERT_EQ(bit_util::GetBit(built->data(), i + 1), static_cast<bool>(values[i]));
  }

  ASSERT_EQ(built->size(), bit_util::BytesForBits(nvalues + 1));
}

TEST(TestBoolBufferBuilder, AppendCopies) {
  TypedBufferBuilder<bool> builder;

  ASSERT_OK(builder.Append(13, true));
  ASSERT_OK(builder.Append(17, false));
  ASSERT_EQ(builder.length(), 13 + 17);
  ASSERT_EQ(builder.capacity(), 64 * 8);
  ASSERT_EQ(builder.false_count(), 17);

  std::shared_ptr<Buffer> built;
  ASSERT_OK(builder.Finish(&built));
  AssertIsCPUBuffer(*built);

  for (int i = 0; i != 13 + 17; ++i) {
    EXPECT_EQ(bit_util::GetBit(built->data(), i), i < 13) << "index = " << i;
  }

  ASSERT_EQ(built->size(), bit_util::BytesForBits(13 + 17));
}

TEST(TestBoolBufferBuilder, Reserve) {
  TypedBufferBuilder<bool> builder;

  ASSERT_OK(builder.Reserve(13 + 17));
  builder.UnsafeAppend(13, true);
  builder.UnsafeAppend(17, false);
  ASSERT_EQ(builder.length(), 13 + 17);
  ASSERT_EQ(builder.capacity(), 64 * 8);
  ASSERT_EQ(builder.false_count(), 17);

  ASSERT_OK_AND_ASSIGN(auto built, builder.Finish());
  AssertIsCPUBuffer(*built);
  ASSERT_EQ(built->size(), bit_util::BytesForBits(13 + 17));
}

template <typename T>
class TypedTestBuffer : public ::testing::Test {};

using BufferPtrs =
    ::testing::Types<std::shared_ptr<ResizableBuffer>, std::unique_ptr<ResizableBuffer>>;

TYPED_TEST_SUITE(TypedTestBuffer, BufferPtrs);

TYPED_TEST(TypedTestBuffer, IsMutableFlag) {
  Buffer buf(nullptr, 0);

  ASSERT_FALSE(buf.is_mutable());

  MutableBuffer mbuf(nullptr, 0);
  ASSERT_TRUE(mbuf.is_mutable());
  AssertIsCPUBuffer(mbuf);

  TypeParam pool_buf;
  ASSERT_OK_AND_ASSIGN(pool_buf, AllocateResizableBuffer(0));
  ASSERT_TRUE(pool_buf->is_mutable());
  AssertIsCPUBuffer(*pool_buf);
}

TYPED_TEST(TypedTestBuffer, Resize) {
  TypeParam buf;
  ASSERT_OK_AND_ASSIGN(buf, AllocateResizableBuffer(0));
  AssertIsCPUBuffer(*buf);

  ASSERT_EQ(0, buf->size());
  ASSERT_OK(buf->Resize(100));
  ASSERT_EQ(100, buf->size());
  ASSERT_OK(buf->Resize(200));
  ASSERT_EQ(200, buf->size());

  // Make it smaller, too
  ASSERT_OK(buf->Resize(50, true));
  ASSERT_EQ(50, buf->size());
  // We have actually shrunken in size
  // The spec requires that capacity is a multiple of 64
  ASSERT_EQ(64, buf->capacity());

  // Resize to a larger capacity again to test shrink_to_fit = false
  ASSERT_OK(buf->Resize(100));
  ASSERT_EQ(128, buf->capacity());
  ASSERT_OK(buf->Resize(50, false));
  ASSERT_EQ(128, buf->capacity());
}

TYPED_TEST(TypedTestBuffer, TypedResize) {
  TypeParam buf;
  ASSERT_OK_AND_ASSIGN(buf, AllocateResizableBuffer(0));

  ASSERT_EQ(0, buf->size());
  ASSERT_OK(buf->template TypedResize<double>(100));
  ASSERT_EQ(800, buf->size());
  ASSERT_OK(buf->template TypedResize<double>(200));
  ASSERT_EQ(1600, buf->size());

  ASSERT_OK(buf->template TypedResize<double>(50, true));
  ASSERT_EQ(400, buf->size());
  ASSERT_EQ(448, buf->capacity());

  ASSERT_OK(buf->template TypedResize<double>(100));
  ASSERT_EQ(832, buf->capacity());
  ASSERT_OK(buf->template TypedResize<double>(50, false));
  ASSERT_EQ(832, buf->capacity());
}

TYPED_TEST(TypedTestBuffer, ResizeOOM) {
// This test doesn't play nice with AddressSanitizer
#ifndef ADDRESS_SANITIZER
  // realloc fails, even though there may be no explicit limit
  TypeParam buf;
  ASSERT_OK_AND_ASSIGN(buf, AllocateResizableBuffer(0));
  ASSERT_OK(buf->Resize(100));
  if (default_memory_pool()->backend_name() == "mimalloc") {
    GTEST_SKIP() << "Skip synthetic OOM for mimalloc to avoid allocator fatal path";
  }
  int64_t to_alloc = std::min<uint64_t>(std::numeric_limits<int64_t>::max(),
                                        std::numeric_limits<size_t>::max());
  // Clamp to a still-impossible size so the allocator raises OutOfMemory
  constexpr int64_t kHugeAlloc = static_cast<int64_t>(1) << 48;  // 256 TB
  to_alloc = std::min(to_alloc, kHugeAlloc);
  // subtract 63 to prevent overflow after the size is aligned
  to_alloc -= 63;
  ASSERT_RAISES(OutOfMemory, buf->Resize(to_alloc));
#endif
}

TEST(TestBufferConcatenation, EmptyBuffer) {
  // GH-36913: UB shouldn't be triggered by copying from a null pointer
  const std::string contents = "hello, world";
  auto buffer = std::make_shared<Buffer>(contents);
  auto empty_buffer = std::make_shared<Buffer>(/*data=*/nullptr, /*size=*/0);
  ASSERT_OK_AND_ASSIGN(auto result, ConcatenateBuffers({buffer, empty_buffer}));
  AssertMyBufferEqual(*result, contents);
}

TEST(TestDeviceRegistry, Basics) {
  // Test the error cases for the device registry

  // CPU is already registered
  ASSERT_RAISES(KeyError,
                RegisterDeviceMapper(DeviceAllocationType::kCPU, [](int64_t device_id) {
                  return default_cpu_memory_manager();
                }));

  // VPI is not registered
  ASSERT_RAISES(KeyError, GetDeviceMapper(DeviceAllocationType::kVPI));
}

}  // namespace arrow
