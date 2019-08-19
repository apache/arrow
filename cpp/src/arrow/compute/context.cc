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

#include "arrow/compute/context.h"

#include <memory>

#include "arrow/buffer.h"
#include "arrow/util/cpu_info.h"

namespace arrow {
namespace compute {

FunctionContext::FunctionContext(MemoryPool* pool)
    : pool_(pool), cpu_info_(internal::CpuInfo::GetInstance()) {}

MemoryPool* FunctionContext::memory_pool() const { return pool_; }

Status FunctionContext::Allocate(const int64_t nbytes, std::shared_ptr<Buffer>* out) {
  return AllocateBuffer(pool_, nbytes, out);
}

void FunctionContext::SetStatus(const Status& status) {
  if (ARROW_PREDICT_FALSE(!status_.ok())) {
    return;
  }
  status_ = status;
}

/// \brief Clear any error status
void FunctionContext::ResetStatus() { status_ = Status::OK(); }

}  // namespace compute
}  // namespace arrow
