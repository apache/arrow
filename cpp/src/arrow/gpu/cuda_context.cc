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

#include "arrow/gpu/cuda_context.h"

#include <cstdint>
#include <memory>
#include <string>

#include <cuda.h>

namespace arrow {
namespace gpu {

class CudaContext::CudaContextImpl {
 public:
  Status Init() {
    CUDADRV_RETURN_NOT_OK(cuInit(0));
    CUDADRV_RETURN_NOT_OK(cuDeviceGetCount(&num_devices_));

    // Create contexts
    device_contexts_.resize(num_devices_);
    for (int i = 0; i < num_devices_; ++i) {
      CUresult ret = cuCtxCreate(&device_contexts_[i], 0,
    }
  }

 private:
  int num_devices_;
  std::vector<CUcontext> device_contexts_;
};

}  // namespace gpu
}  // namespace arrow
