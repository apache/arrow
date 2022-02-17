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

#ifdef GANDIVA_ENABLE_OBJECT_CODE_CACHE
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4244)
#pragma warning(disable : 4141)
#pragma warning(disable : 4146)
#pragma warning(disable : 4267)
#pragma warning(disable : 4624)
#endif

#include <llvm/ExecutionEngine/ObjectCache.h>
#include <llvm/Support/MemoryBuffer.h>

#include "gandiva/cache.h"
#include "gandiva/expression_cache_key.h"

namespace gandiva {
/// Class that enables the LLVM to use a custom rule to deal with the object code.
class GandivaObjectCache : public llvm::ObjectCache {
 public:
  explicit GandivaObjectCache(
      std::shared_ptr<Cache<ExpressionCacheKey, std::shared_ptr<llvm::MemoryBuffer>>>&
          cache,
      ExpressionCacheKey key);

  ~GandivaObjectCache() {}

  void notifyObjectCompiled(const llvm::Module* M, llvm::MemoryBufferRef Obj);

  std::unique_ptr<llvm::MemoryBuffer> getObject(const llvm::Module* M);

 private:
  ExpressionCacheKey cache_key_;
  std::shared_ptr<Cache<ExpressionCacheKey, std::shared_ptr<llvm::MemoryBuffer>>> cache_;
};
}  // namespace gandiva
#endif  // GANDIVA_ENABLE_OBJECT_CODE_CACHE
