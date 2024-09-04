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

#include <string>

#include "arrow/device_allocation_type_set.h"
#include "arrow/type_fwd.h"

namespace arrow {

const char* DeviceAllocationTypeToCStr(DeviceAllocationType type) {
  switch (type) {
    case DeviceAllocationType::kCPU:
      return "CPU";
    case DeviceAllocationType::kCUDA:
      return "CUDA";
    case DeviceAllocationType::kCUDA_HOST:
      return "CUDA_HOST";
    case DeviceAllocationType::kOPENCL:
      return "OPENCL";
    case DeviceAllocationType::kVULKAN:
      return "VULKAN";
    case DeviceAllocationType::kMETAL:
      return "METAL";
    case DeviceAllocationType::kVPI:
      return "VPI";
    case DeviceAllocationType::kROCM:
      return "ROCM";
    case DeviceAllocationType::kROCM_HOST:
      return "ROCM_HOST";
    case DeviceAllocationType::kEXT_DEV:
      return "EXT_DEV";
    case DeviceAllocationType::kCUDA_MANAGED:
      return "CUDA_MANAGED";
    case DeviceAllocationType::kONEAPI:
      return "ONEAPI";
    case DeviceAllocationType::kWEBGPU:
      return "WEBGPU";
    case DeviceAllocationType::kHEXAGON:
      return "HEXAGON";
  }
  return "<UNKNOWN>";
}

std::string DeviceAllocationTypeSet::ToString() const {
  std::string result = "{";
  for (int i = 1; i <= kDeviceAllocationTypeMax; i++) {
    if (device_type_bitset_.test(i)) {
      // Skip all the unused values in the enum.
      switch (i) {
        case 0:
        case 5:
        case 6:
          continue;
      }
      if (result.size() > 1) {
        result += ", ";
      }
      result += DeviceAllocationTypeToCStr(static_cast<DeviceAllocationType>(i));
    }
  }
  result += "}";
  return result;
}

}  // namespace arrow
