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

#ifndef ARROW_GPU_CUDA_COMMON_H
#define ARROW_GPU_CUDA_COMMON_H

#include <sstream>

#include <cuda.h>

namespace arrow {
namespace cuda {

#define CUDA_DCHECK(STMT) \
  do {                    \
    int ret = (STMT);     \
    DCHECK_EQ(0, ret);    \
    (void)ret;            \
  } while (0)

#define CU_RETURN_NOT_OK(STMT)                                                  \
  do {                                                                          \
    CUresult ret = (STMT);                                                      \
    if (ret != CUDA_SUCCESS) {                                                  \
      return Status::IOError("Cuda Driver API call in ", __FILE__, " at line ", \
                             __LINE__, " failed with code ", ret, ": ", #STMT); \
    }                                                                           \
  } while (0)

}  // namespace cuda
}  // namespace arrow

#endif  // ARROW_GPU_CUDA_COMMON_H
