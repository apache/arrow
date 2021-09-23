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

#include <llvm/Support/MemoryBuffer.h>

#include "gandiva/cache.h"
#include "llvm/ExecutionEngine/ObjectCache.h"
#include "llvm/IR/Module.h"

namespace gandiva {
/// Class that enables the LLVM to use a custom rule to deal with the object code.
template <class CacheKey>
class GandivaObjectCache : public llvm::ObjectCache {
 public:
  GandivaObjectCache(
      std::shared_ptr<Cache<CacheKey, std::shared_ptr<llvm::MemoryBuffer>>>& cache,
      std::shared_ptr<CacheKey>& key) {
    cache_ = cache;
    cache_key_ = key;
    // Start measuring code gen time
    begin_time_ = std::chrono::high_resolution_clock::now();
  }

  ~GandivaObjectCache() {}

  void notifyObjectCompiled(const llvm::Module* M, llvm::MemoryBufferRef Obj) {
    // Stop measuring time and  calculate the elapsed time to compile the object code
    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time_)
            .count();

    std::unique_ptr<llvm::MemoryBuffer> obj_buffer =
        llvm::MemoryBuffer::getMemBufferCopy(Obj.getBuffer(), Obj.getBufferIdentifier());
    std::shared_ptr<llvm::MemoryBuffer> obj_code = std::move(obj_buffer);

    ValueCacheObject<std::shared_ptr<llvm::MemoryBuffer>> value_cache(
        obj_code, elapsed_time, obj_code->getBufferSize());

    cache_->PutObjectCode(*cache_key_.get(), value_cache);
  }

  std::unique_ptr<llvm::MemoryBuffer> getObject(const llvm::Module* M) {
    std::shared_ptr<llvm::MemoryBuffer> cached_obj =
        cache_->GetObjectCode(*cache_key_.get());
    auto null = std::nullptr_t();
    if (cached_obj != null) {
      std::unique_ptr<llvm::MemoryBuffer> cached_buffer = cached_obj->getMemBufferCopy(
          cached_obj->getBuffer(), cached_obj->getBufferIdentifier());
      return cached_buffer;
    }
    return null;
  }

 private:
  std::shared_ptr<CacheKey> cache_key_;
  std::shared_ptr<Cache<CacheKey, std::shared_ptr<llvm::MemoryBuffer>>> cache_;
  std::chrono::high_resolution_clock::time_point begin_time_;
};
}  // namespace gandiva
