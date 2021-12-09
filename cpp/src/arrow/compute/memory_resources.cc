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

#include "arrow/compute/memory_resources.h"

#include <array>
#include <memory>
#include <mutex>
#include <random>

#include "arrow/compute/exec.h"
#include "arrow/util/logging.h"

#ifdef __APPLE__
#include <sys/sysctl.h>
#include <sys/types.h>
#endif

#ifdef __linux__
#include <sys/statvfs.h>
#include <sys/sysinfo.h>
#endif

// Windows APIs
#include "arrow/util/windows_compatibility.h"

namespace arrow {

namespace compute {

std::string MemoryLevelName(MemoryLevel memory_level) {
  static const char* MemoryLevelNames[] = {ARROW_STRINGIFY(MemoryLevel::kDiskLevel),
                                           ARROW_STRINGIFY(MemoryLevel::kCpuLevel),
                                           ARROW_STRINGIFY(MemoryLevel::kGpuLevel)};

  return MemoryLevelNames[static_cast<int>(memory_level)];
}

std::string MemoryResource::ToString() const { return MemoryLevelName(memory_level_); }

MemoryResources::~MemoryResources() {}

std::unique_ptr<MemoryResources> MemoryResources::Make() {
  return std::unique_ptr<MemoryResources>(new MemoryResources());
}

Status MemoryResources::AddMemoryResource(std::shared_ptr<MemoryResource> resource) {
  auto level = static_cast<size_t>(resource->memory_level());
  if (stats_[level] != nullptr) {
    return Status::KeyError("Already have a resource type registered with name: ",
                            resource->ToString());
  }
  stats_[level] = std::move(resource);
  return Status::OK();
}

size_t MemoryResources::size() const { return stats_.size(); }

Result<MemoryResource*> MemoryResources::memory_resource(MemoryLevel memory_level) const {
  auto level = static_cast<size_t>(memory_level);
  if (stats_[level] == nullptr) {
    return Status::KeyError("No memory resource registered with level: ",
                            MemoryLevelName(memory_level));
  }
  return stats_[level].get();
}

std::vector<MemoryResource*> MemoryResources::memory_resources() const {
  std::vector<MemoryResource*> arr;
  for (auto&& resource : stats_) {
    if (resource != nullptr) {
      arr.push_back(resource.get());
    }
  }
  return arr;
}

namespace {

size_t GetTotalMemorySize() {
#ifdef __APPLE__
  int mib[2];
  size_t physical_memory;
  size_t length;
  // Get the Physical memory size
  mib[0] = CTL_HW;
  mib[1] = HW_MEMSIZE;
  length = sizeof(size_t);
  sysctl(mib, 2, &physical_memory, &length, NULL, 0);
  return physical_memory;
#elif defined(_WIN32)
  MEMORYSTATUSEX status;
  status.dwLength = sizeof(status);
  GlobalMemoryStatusEx(&status);
  return status.ullTotalPhys;
#else  // Linux
  struct sysinfo si;
  sysinfo(&si);
  return (size_t)si.freeram;
#endif
}

struct CPUMemoryResource : public MemoryResource {
  explicit CPUMemoryResource(arrow::MemoryPool* pool, float memory_limit_threshold = 0.75)
      : MemoryResource(MemoryLevel::kCpuLevel), pool_(pool) {
    total_memory_size_ = GetTotalMemorySize();
    memory_limit_ =
        static_cast<int64_t>(std::round(memory_limit_threshold * total_memory_size_));
  }

  int64_t memory_used() override { return pool_->bytes_allocated(); }

  int64_t memory_limit() override { return memory_limit_; }

 private:
  arrow::MemoryPool* pool_;
  int64_t memory_limit_;
  int64_t total_memory_size_;
};

static std::unique_ptr<MemoryResources> CreateBuiltInMemoryResources(MemoryPool* pool) {
  auto resources = MemoryResources::Make();

  // CPU MemoryLevel
  auto cpu_level = std::make_shared<CPUMemoryResource>(pool);
  DCHECK_OK(resources->AddMemoryResource(std::move(cpu_level)));

  // Disk MemoryLevel ...

  return resources;
}

}  // namespace

MemoryResources* GetMemoryResources(MemoryPool* pool) {
  static auto resources = CreateBuiltInMemoryResources(pool);
  return resources.get();
}

}  // namespace compute
}  // namespace arrow
