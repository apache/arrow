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

#pragma once

#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/util/macros.h"

#include <iterator>
#include <memory>
#include <string>

namespace arrow {

namespace compute {

struct ExecBatch;

enum class MemoryLevel : int { kGPULevel, kCPULevel, kDiskLevel };

class ARROW_EXPORT DataHolder {
 public:
  explicit DataHolder(MemoryLevel memory_level) : memory_level_(memory_level) {}

  MemoryLevel memory_level() const { return memory_level_; };

  virtual Result<ExecBatch> Get() = 0;

 private:
  MemoryLevel memory_level_;
};

class ARROW_EXPORT MemoryResource {
 public:
  MemoryResource(MemoryLevel memory_level) : memory_level_(memory_level) {}

  MemoryLevel memory_level() const { return memory_level_; }

  std::string ToString() const;

  virtual int64_t memory_limit() = 0;

  virtual int64_t memory_used() = 0;

  virtual Result<std::unique_ptr<DataHolder>> GetDataHolder(
      const std::shared_ptr<RecordBatch>& batch) = 0;

 private:
  MemoryLevel memory_level_;
};

class ARROW_EXPORT MemoryResources {
 public:
  ~MemoryResources();

  static std::unique_ptr<MemoryResources> Make();

  Status AddMemoryResource(std::unique_ptr<MemoryResource> resource);

  size_t size() const;

  Result<int64_t> memory_limit(MemoryLevel level) const;

  Result<int64_t> memory_used(MemoryLevel level) const;

  Result<MemoryResource*> memory_resource(MemoryLevel level) const;

 private:
  MemoryResources();

  class MemoryResourcesImpl;
  std::unique_ptr<MemoryResourcesImpl> impl_;
};

ARROW_EXPORT MemoryResources* GetMemoryResources(MemoryPool* pool);

}  // namespace compute
}  // namespace arrow