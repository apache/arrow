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

#ifdef GANDIVA_ENABLE_OBJECT_CODE_CACHE
#include "gandiva/gandiva_object_cache.h"

#include <utility>

namespace gandiva {

GandivaObjectCache::GandivaObjectCache(
    std::shared_ptr<Cache<ExpressionCacheKey, std::shared_ptr<llvm::MemoryBuffer>>>&
        cache,
    ExpressionCacheKey key)
    : cache_key_(std::move(key)) {
  cache_ = cache;
}

void GandivaObjectCache::notifyObjectCompiled(const llvm::Module* M,
                                              llvm::MemoryBufferRef Obj) {
  std::unique_ptr<llvm::MemoryBuffer> obj_buffer =
      llvm::MemoryBuffer::getMemBufferCopy(Obj.getBuffer(), Obj.getBufferIdentifier());
  std::shared_ptr<llvm::MemoryBuffer> obj_code = std::move(obj_buffer);

  cache_->PutObjectCode(cache_key_, obj_code);
}

std::unique_ptr<llvm::MemoryBuffer> GandivaObjectCache::getObject(const llvm::Module* M) {
  std::shared_ptr<llvm::MemoryBuffer> cached_obj = cache_->GetObjectCode(cache_key_);
  if (cached_obj != nullptr) {
    std::unique_ptr<llvm::MemoryBuffer> cached_buffer = cached_obj->getMemBufferCopy(
        cached_obj->getBuffer(), cached_obj->getBufferIdentifier());
    ARROW_LOG(INFO) << "[INFO][CACHE-LOG]: An object code was found on cache.";
    return cached_buffer;
  }
  ARROW_LOG(INFO) << "[INFO][CACHE-LOG]: No object code was found on cache.";
  return nullptr;
}

}  // namespace gandiva
#endif  // GANDIVA_ENABLE_OBJECT_CODE_CACHE
