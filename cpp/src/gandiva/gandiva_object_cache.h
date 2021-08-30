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
  };

  ~GandivaObjectCache() {}

  void notifyObjectCompiled(const llvm::Module* M, llvm::MemoryBufferRef Obj) {
    std::unique_ptr<llvm::MemoryBuffer> obj_buffer =
        llvm::MemoryBuffer::getMemBufferCopy(Obj.getBuffer(), Obj.getBufferIdentifier());
    std::shared_ptr<llvm::MemoryBuffer> obj_code = std::move(obj_buffer);
    cache_->PutObjectCode(*cache_key_.get(), obj_code, obj_code->getBufferSize());
  };

  std::unique_ptr<llvm::MemoryBuffer> getObject(const llvm::Module* M) {
    std::shared_ptr<llvm::MemoryBuffer> cached_obj =
        cache_->GetObjectCode(*cache_key_.get());
    if (cached_obj == nullptr) {
      return nullptr;
    }
    std::unique_ptr<llvm::MemoryBuffer> cached_buffer = cached_obj->getMemBufferCopy(
        cached_obj->getBuffer(), cached_obj->getBufferIdentifier());
    return cached_buffer;
  };

 private:
  std::shared_ptr<CacheKey> cache_key_;
  std::shared_ptr<Cache<CacheKey, std::shared_ptr<llvm::MemoryBuffer>>> cache_;
};
}  // namespace gandiva
