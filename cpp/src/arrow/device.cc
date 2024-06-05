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

#include "arrow/device.h"

#include <cstring>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/util/logging.h"

namespace arrow {

MemoryManager::~MemoryManager() {}

Result<std::shared_ptr<Device::SyncEvent>> MemoryManager::MakeDeviceSyncEvent() {
  return nullptr;
}

Result<std::shared_ptr<Device::SyncEvent>> MemoryManager::WrapDeviceSyncEvent(
    void* sync_event, Device::SyncEvent::release_fn_t release_sync_event) {
  return nullptr;
}

Device::~Device() {}

#define COPY_BUFFER_SUCCESS(maybe_buffer) \
  ((maybe_buffer).ok() && *(maybe_buffer) != nullptr)

#define COPY_BUFFER_RETURN(maybe_buffer, to)              \
  if (!maybe_buffer.ok()) {                               \
    return maybe_buffer;                                  \
  }                                                       \
  if (COPY_BUFFER_SUCCESS(maybe_buffer)) {                \
    DCHECK_EQ(*(**maybe_buffer).device(), *to->device()); \
    return maybe_buffer;                                  \
  }

Result<std::shared_ptr<Buffer>> MemoryManager::CopyBuffer(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& to) {
  const auto& from = buf->memory_manager();
  auto maybe_buffer = to->CopyBufferFrom(buf, from);
  COPY_BUFFER_RETURN(maybe_buffer, to);
  // `to` doesn't support copying from `from`, try the other way
  maybe_buffer = from->CopyBufferTo(buf, to);
  COPY_BUFFER_RETURN(maybe_buffer, to);
  if (!from->is_cpu() && !to->is_cpu()) {
    // Try an intermediate view on the CPU
    auto cpu_mm = default_cpu_memory_manager();
    maybe_buffer = from->ViewBufferTo(buf, cpu_mm);
    if (!COPY_BUFFER_SUCCESS(maybe_buffer)) {
      // View failed, try a copy instead
      // XXX should we have a MemoryManager::IsCopySupportedTo(MemoryManager)
      // to avoid copying to CPU if copy from CPU to dest is unsupported?
      maybe_buffer = from->CopyBufferTo(buf, cpu_mm);
    }
    if (COPY_BUFFER_SUCCESS(maybe_buffer)) {
      // Copy from source to CPU succeeded, now try to copy from CPU into dest
      maybe_buffer = to->CopyBufferFrom(*maybe_buffer, cpu_mm);
      if (COPY_BUFFER_SUCCESS(maybe_buffer)) {
        return maybe_buffer;
      }
    }
  }

  return Status::NotImplemented("Copying buffer from ", from->device()->ToString(),
                                " to ", to->device()->ToString(), " not supported");
}

Result<std::unique_ptr<Buffer>> MemoryManager::CopyNonOwned(
    const Buffer& buf, const std::shared_ptr<MemoryManager>& to) {
  const auto& from = buf.memory_manager();
  auto maybe_buffer = to->CopyNonOwnedFrom(buf, from);
  COPY_BUFFER_RETURN(maybe_buffer, to);
  // `to` doesn't support copying from `from`, try the other way
  maybe_buffer = from->CopyNonOwnedTo(buf, to);
  COPY_BUFFER_RETURN(maybe_buffer, to);

  return Status::NotImplemented("Copying buffer from ", from->device()->ToString(),
                                " to ", to->device()->ToString(), " not supported");
}

Result<std::shared_ptr<Buffer>> MemoryManager::ViewBuffer(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& to) {
  if (buf->memory_manager() == to) {
    return buf;
  }
  const auto& from = buf->memory_manager();
  auto maybe_buffer = to->ViewBufferFrom(buf, from);
  COPY_BUFFER_RETURN(maybe_buffer, to);
  // `to` doesn't support viewing from `from`, try the other way
  maybe_buffer = from->ViewBufferTo(buf, to);
  COPY_BUFFER_RETURN(maybe_buffer, to);

  return Status::NotImplemented("Viewing buffer from ", from->device()->ToString(),
                                " on ", to->device()->ToString(), " not supported");
}

Status MemoryManager::CopyBufferSliceToCPU(const std::shared_ptr<Buffer>& buf,
                                           int64_t offset, int64_t length,
                                           uint8_t* out_data) {
  if (ARROW_PREDICT_TRUE(buf->is_cpu())) {
    memcpy(out_data, buf->data() + offset, static_cast<size_t>(length));
    return Status::OK();
  }

  auto& from = buf->memory_manager();
  auto cpu_mm = default_cpu_memory_manager();
  // Try a view first
  auto maybe_buffer_result = from->ViewBufferTo(buf, cpu_mm);
  if (!COPY_BUFFER_SUCCESS(maybe_buffer_result)) {
    // View failed, try a copy instead
    maybe_buffer_result = from->CopyBufferTo(buf, cpu_mm);
  }
  ARROW_ASSIGN_OR_RAISE(auto maybe_buffer, std::move(maybe_buffer_result));
  if (maybe_buffer != nullptr) {
    memcpy(out_data, maybe_buffer->data() + offset, static_cast<size_t>(length));
    return Status::OK();
  }

  return Status::NotImplemented("Copying buffer slice from ", from->device()->ToString(),
                                " to CPU not supported");
}

#undef COPY_BUFFER_RETURN
#undef COPY_BUFFER_SUCCESS

Result<std::shared_ptr<Buffer>> MemoryManager::CopyBufferFrom(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& from) {
  return nullptr;
}

Result<std::shared_ptr<Buffer>> MemoryManager::CopyBufferTo(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& to) {
  return nullptr;
}

Result<std::unique_ptr<Buffer>> MemoryManager::CopyNonOwnedFrom(
    const Buffer& buf, const std::shared_ptr<MemoryManager>& from) {
  return nullptr;
}

Result<std::unique_ptr<Buffer>> MemoryManager::CopyNonOwnedTo(
    const Buffer& buf, const std::shared_ptr<MemoryManager>& to) {
  return nullptr;
}

Result<std::shared_ptr<Buffer>> MemoryManager::ViewBufferFrom(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& from) {
  return nullptr;
}

Result<std::shared_ptr<Buffer>> MemoryManager::ViewBufferTo(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& to) {
  return nullptr;
}

// ----------------------------------------------------------------------
// CPU backend implementation

namespace {
const char kCPUDeviceTypeName[] = "arrow::CPUDevice";
}

std::shared_ptr<MemoryManager> CPUMemoryManager::Make(
    const std::shared_ptr<Device>& device, MemoryPool* pool) {
  return std::shared_ptr<MemoryManager>(new CPUMemoryManager(device, pool));
}

Result<std::shared_ptr<io::RandomAccessFile>> CPUMemoryManager::GetBufferReader(
    std::shared_ptr<Buffer> buf) {
  return std::make_shared<io::BufferReader>(std::move(buf));
}

Result<std::shared_ptr<io::OutputStream>> CPUMemoryManager::GetBufferWriter(
    std::shared_ptr<Buffer> buf) {
  return std::make_shared<io::FixedSizeBufferWriter>(std::move(buf));
}

Result<std::unique_ptr<Buffer>> CPUMemoryManager::AllocateBuffer(int64_t size) {
  return ::arrow::AllocateBuffer(size, pool_);
}

Result<std::shared_ptr<Buffer>> CPUMemoryManager::CopyBufferFrom(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& from) {
  return CopyNonOwnedFrom(*buf, from);
}

Result<std::unique_ptr<Buffer>> CPUMemoryManager::CopyNonOwnedFrom(
    const Buffer& buf, const std::shared_ptr<MemoryManager>& from) {
  if (!from->is_cpu()) {
    return nullptr;
  }
  ARROW_ASSIGN_OR_RAISE(auto dest, ::arrow::AllocateBuffer(buf.size(), pool_));
  if (buf.size() > 0) {
    memcpy(dest->mutable_data(), buf.data(), static_cast<size_t>(buf.size()));
  }
  return dest;
}

Result<std::shared_ptr<Buffer>> CPUMemoryManager::ViewBufferFrom(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& from) {
  if (!from->is_cpu()) {
    return nullptr;
  }
  // in this case the memory manager we're coming from is visible on the CPU,
  // but uses an allocation type other than CPU. Since we know the data is visible
  // to the CPU a "View" of this should use the CPUMemoryManager as the listed memory
  // manager.
  if (buf->device_type() != DeviceAllocationType::kCPU) {
    return std::make_shared<Buffer>(buf->address(), buf->size(), shared_from_this(), buf);
  }
  return buf;
}

Result<std::shared_ptr<Buffer>> CPUMemoryManager::CopyBufferTo(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& to) {
  return CopyNonOwnedTo(*buf, to);
}

Result<std::unique_ptr<Buffer>> CPUMemoryManager::CopyNonOwnedTo(
    const Buffer& buf, const std::shared_ptr<MemoryManager>& to) {
  if (!to->is_cpu()) {
    return nullptr;
  }
  ARROW_ASSIGN_OR_RAISE(auto dest, ::arrow::AllocateBuffer(buf.size(), pool_));
  if (buf.size() > 0) {
    memcpy(dest->mutable_data(), buf.data(), static_cast<size_t>(buf.size()));
  }
  return dest;
}

Result<std::shared_ptr<Buffer>> CPUMemoryManager::ViewBufferTo(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& to) {
  if (!to->is_cpu()) {
    return nullptr;
  }
  // in this case the memory manager we're coming from is visible on the CPU,
  // but uses an allocation type other than CPU. Since we know the data is visible
  // to the CPU a "View" of this should use the CPUMemoryManager as the listed memory
  // manager.
  if (buf->device_type() != DeviceAllocationType::kCPU) {
    return std::make_shared<Buffer>(buf->address(), buf->size(), to, buf);
  }
  return buf;
}

std::shared_ptr<MemoryManager> default_cpu_memory_manager() {
  static auto instance =
      CPUMemoryManager::Make(CPUDevice::Instance(), default_memory_pool());
  return instance;
}

std::shared_ptr<Device> CPUDevice::Instance() {
  static auto instance = std::shared_ptr<Device>(new CPUDevice());
  return instance;
}

const char* CPUDevice::type_name() const { return kCPUDeviceTypeName; }

std::string CPUDevice::ToString() const { return "CPUDevice()"; }

bool CPUDevice::Equals(const Device& other) const {
  return other.type_name() == kCPUDeviceTypeName;
}

std::shared_ptr<MemoryManager> CPUDevice::memory_manager(MemoryPool* pool) {
  if (pool == default_memory_pool()) {
    return default_cpu_memory_manager();
  } else {
    return CPUMemoryManager::Make(Instance(), pool);
  }
}

std::shared_ptr<MemoryManager> CPUDevice::default_memory_manager() {
  return default_cpu_memory_manager();
}

namespace {

class DeviceMapperRegistryImpl {
 public:
  DeviceMapperRegistryImpl() {}

  Status RegisterDevice(DeviceAllocationType device_type, DeviceMapper memory_mapper) {
    std::lock_guard<std::mutex> lock(lock_);
    auto [_, inserted] = registry_.try_emplace(device_type, std::move(memory_mapper));
    if (!inserted) {
      return Status::KeyError("Device type ", static_cast<int>(device_type),
                              " is already registered");
    }
    return Status::OK();
  }

  Result<DeviceMapper> GetMapper(DeviceAllocationType device_type) {
    std::lock_guard<std::mutex> lock(lock_);
    auto it = registry_.find(device_type);
    if (it == registry_.end()) {
      return Status::KeyError("Device type ", static_cast<int>(device_type),
                              "is not registered");
    }
    return it->second;
  }

 private:
  std::mutex lock_;
  std::unordered_map<DeviceAllocationType, DeviceMapper> registry_;
};

Result<std::shared_ptr<MemoryManager>> DefaultCPUDeviceMapper(int64_t device_id) {
  return default_cpu_memory_manager();
}

static std::unique_ptr<DeviceMapperRegistryImpl> CreateDeviceRegistry() {
  auto registry = std::make_unique<DeviceMapperRegistryImpl>();

  // Always register the CPU device
  DCHECK_OK(registry->RegisterDevice(DeviceAllocationType::kCPU, DefaultCPUDeviceMapper));

  return registry;
}

DeviceMapperRegistryImpl* GetDeviceRegistry() {
  static auto g_registry = CreateDeviceRegistry();
  return g_registry.get();
}

}  // namespace

Status RegisterDeviceMapper(DeviceAllocationType device_type, DeviceMapper mapper) {
  auto registry = GetDeviceRegistry();
  return registry->RegisterDevice(device_type, std::move(mapper));
}

Result<DeviceMapper> GetDeviceMapper(DeviceAllocationType device_type) {
  auto registry = GetDeviceRegistry();
  return registry->GetMapper(device_type);
}

}  // namespace arrow
