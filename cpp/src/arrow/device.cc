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
#include <utility>

#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/result.h"
#include "arrow/util/logging.h"

namespace arrow {

MemoryManager::~MemoryManager() {}

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

Result<std::shared_ptr<Buffer>> CPUMemoryManager::AllocateBuffer(int64_t size) {
  return ::arrow::AllocateBuffer(size, pool_);
}

Result<std::shared_ptr<Buffer>> CPUMemoryManager::CopyBufferFrom(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& from) {
  if (!from->is_cpu()) {
    return nullptr;
  }
  ARROW_ASSIGN_OR_RAISE(auto dest, ::arrow::AllocateBuffer(buf->size(), pool_));
  if (buf->size() > 0) {
    memcpy(dest->mutable_data(), buf->data(), static_cast<size_t>(buf->size()));
  }
  return std::move(dest);
}

Result<std::shared_ptr<Buffer>> CPUMemoryManager::ViewBufferFrom(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& from) {
  if (!from->is_cpu()) {
    return nullptr;
  }
  return buf;
}

Result<std::shared_ptr<Buffer>> CPUMemoryManager::CopyBufferTo(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& to) {
  if (!to->is_cpu()) {
    return nullptr;
  }
  ARROW_ASSIGN_OR_RAISE(auto dest, ::arrow::AllocateBuffer(buf->size(), pool_));
  if (buf->size() > 0) {
    memcpy(dest->mutable_data(), buf->data(), static_cast<size_t>(buf->size()));
  }
  return std::move(dest);
}

Result<std::shared_ptr<Buffer>> CPUMemoryManager::ViewBufferTo(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& to) {
  if (!to->is_cpu()) {
    return nullptr;
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
  return CPUMemoryManager::Make(Instance(), pool);
}

std::shared_ptr<MemoryManager> CPUDevice::default_memory_manager() {
  return default_cpu_memory_manager();
}

}  // namespace arrow
