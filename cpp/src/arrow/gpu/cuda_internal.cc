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

#include "arrow/gpu/cuda_internal.h"

#include <sstream>
#include <string>

#include "arrow/util/logging.h"

namespace arrow {
namespace cuda {
namespace internal {

std::string CudaErrorDescription(CUresult err) {
  DCHECK_NE(err, CUDA_SUCCESS);
  std::stringstream ss;

  const char* name = nullptr;
  auto err_result = cuGetErrorName(err, &name);
  if (err_result == CUDA_SUCCESS) {
    DCHECK_NE(name, nullptr);
    ss << "[" << name << "] ";
  }

  const char* str = nullptr;
  err_result = cuGetErrorString(err, &str);
  if (err_result == CUDA_SUCCESS) {
    DCHECK_NE(str, nullptr);
    ss << str;
  } else {
    ss << "unknown error";
  }
  return ss.str();
}

Status StatusFromCuda(CUresult res, const char* function_name) {
  if (res == CUDA_SUCCESS) {
    return Status::OK();
  }
  std::stringstream ss;
  ss << "Cuda error " << res;
  if (function_name != nullptr) {
    ss << " in function '" << function_name << "'";
  }
  ss << ": " << CudaErrorDescription(res);
  return Status::IOError(ss.str());
}

}  // namespace internal
}  // namespace cuda
}  // namespace arrow
