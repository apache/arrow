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

// Non-public header

#pragma once

#include <cassert>
#include <string>

#include <cuda.h>

#include "arrow/gpu/cuda_context.h"
#include "arrow/status.h"

namespace arrow {
namespace cuda {
namespace internal {

std::string CudaErrorDescription(CUresult err);

Status StatusFromCuda(CUresult res, const char* function_name = nullptr);

#define CU_RETURN_NOT_OK(FUNC_NAME, STMT)                               \
  do {                                                                  \
    CUresult __res = (STMT);                                            \
    if (__res != CUDA_SUCCESS) {                                        \
      return ::arrow::cuda::internal::StatusFromCuda(__res, FUNC_NAME); \
    }                                                                   \
  } while (0)

class ContextSaver {
 public:
  explicit ContextSaver(CUcontext new_context) { cuCtxPushCurrent(new_context); }
  explicit ContextSaver(const CudaContext& context)
      : ContextSaver(reinterpret_cast<CUcontext>(context.handle())) {}

  ~ContextSaver() {
    CUcontext unused;
    cuCtxPopCurrent(&unused);
  }
};

}  // namespace internal
}  // namespace cuda
}  // namespace arrow
