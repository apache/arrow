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
#include "arrow/compute/exec.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/util/logging.h"

#include <array>
#include <memory>
#include <mutex>
#include <random>

#include <arrow/filesystem/filesystem.h>
#include <arrow/ipc/feather.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include "arrow/io/file.h"

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

class CPUDataHolder : public DataHolder {
 public:
  explicit CPUDataHolder(const std::shared_ptr<RecordBatch>& record_batch)
      : DataHolder(MemoryLevel::kCpuLevel), record_batch_(std::move(record_batch)) {}

  Result<ExecBatch> Get() override { return ExecBatch(*record_batch_); }

 private:
  std::shared_ptr<RecordBatch> record_batch_;
};

namespace {

std::string RandomString(std::size_t length) {
  const std::string characters =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  std::random_device random_device;
  std::mt19937 generator(random_device());
  std::uniform_int_distribution<> distribution(0, characters.size() - 1);
  std::string random_string;
  for (std::size_t i = 0; i < length; ++i) {
    random_string += characters[distribution(generator)];
  }
  return random_string;
}

}  // namespace

Status StoreRecordBatch(const std::shared_ptr<RecordBatch>& record_batch,
                        const std::shared_ptr<fs::FileSystem>& filesystem,
                        const std::string& file_path) {
  auto output = filesystem->OpenOutputStream(file_path).ValueOrDie();
  auto writer =
      arrow::ipc::MakeFileWriter(output.get(), record_batch->schema()).ValueOrDie();
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*record_batch));
  return writer->Close();
}
Result<std::shared_ptr<RecordBatch>> RecoverRecordBatch(
    const std::shared_ptr<fs::FileSystem>& filesystem, const std::string& file_path) {
  ARROW_ASSIGN_OR_RAISE(auto input, filesystem->OpenInputFile(file_path));
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::feather::Reader::Open(input));
  std::shared_ptr<Table> table;
  ARROW_RETURN_NOT_OK(reader->Read(&table));
  TableBatchReader batch_iter(*table);
  ARROW_ASSIGN_OR_RAISE(auto batch, batch_iter.Next());
  return batch;
}

class DiskDataHolder : public DataHolder {
 public:
  DiskDataHolder(const std::shared_ptr<RecordBatch>& record_batch,
                 MemoryPool* memory_pool)
      : DataHolder(MemoryLevel::kDiskLevel), memory_pool_(memory_pool) {
    std::string root_path;
    std::string file_name = "data-holder-temp-" + RandomString(64) + ".feather";

    filesystem_ =
        arrow::fs::FileSystemFromUri(cache_storage_root_path, &root_path).ValueOrDie();

    file_path_ = root_path + file_name;
    status_ = StoreRecordBatch(record_batch, filesystem_, file_path_);
  }

  Result<ExecBatch> Get() override {
    ARROW_RETURN_NOT_OK(status_);
    ARROW_ASSIGN_OR_RAISE(auto record_batch, RecoverRecordBatch(filesystem_, file_path_));
    return ExecBatch(*record_batch);
  }

 private:
  std::string file_path_;
  Status status_;
  MemoryPool* memory_pool_;
  std::shared_ptr<arrow::fs::FileSystem> filesystem_;
  const std::string cache_storage_root_path = "file:///tmp/";
};

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
#elif defined(_MSC_VER)
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
  CPUMemoryResource(arrow::MemoryPool* pool, float memory_limit_threshold = 0.75)
      : MemoryResource(MemoryLevel::kCpuLevel), pool_(pool) {
    total_memory_size_ = GetTotalMemorySize();
    memory_limit_ = memory_limit_threshold * total_memory_size_;
  }

  int64_t memory_used() override { return pool_->bytes_allocated(); }

  int64_t memory_limit() override { return memory_limit_; }

  Result<std::shared_ptr<DataHolder>> GetDataHolder(
      const std::shared_ptr<RecordBatch>& batch) override {
    auto data_holder = std::make_shared<CPUDataHolder>(batch);
    return data_holder;
  }

 private:
  arrow::MemoryPool* pool_;
  int64_t memory_limit_;
  int64_t total_memory_size_;
};

class DiskMemoryResource : public MemoryResource {
 public:
  DiskMemoryResource(arrow::MemoryPool* pool)
      : MemoryResource(MemoryLevel::kDiskLevel), pool_(pool) {
    memory_used_ = 0;
    memory_limit_ = std::numeric_limits<int64_t>::max();
  }

  int64_t memory_limit() override { return memory_limit_; }

  int64_t memory_used() override { return memory_used_; }

  Result<std::shared_ptr<DataHolder>> GetDataHolder(
      const std::shared_ptr<RecordBatch>& batch) override {
    auto data_holder = std::make_shared<DiskDataHolder>(batch, pool_);
    return data_holder;
  }

 private:
  int64_t memory_used_;
  int64_t memory_limit_;
  arrow::MemoryPool* pool_;
};

static std::unique_ptr<MemoryResources> CreateBuiltInMemoryResources(MemoryPool* pool) {
  auto resources = MemoryResources::Make();

  // CPU MemoryLevel
  auto cpu_level = std::make_shared<CPUMemoryResource>(pool);
  DCHECK_OK(resources->AddMemoryResource(std::move(cpu_level)));

  // Disk MemoryLevel
  auto disk_level = std::make_shared<DiskMemoryResource>(pool);
  DCHECK_OK(resources->AddMemoryResource(std::move(disk_level)));

  return resources;
}

}  // namespace

MemoryResources* GetMemoryResources(MemoryPool* pool) {
  static auto resources = CreateBuiltInMemoryResources(pool);
  return resources.get();
}

}  // namespace compute
}  // namespace arrow
