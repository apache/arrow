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

namespace arrow {

/// \brief EXPERIMENTAL: Device type enum which matches up with C Data Device types
enum class DeviceAllocationType : char {
  kCPU = 1,
  kCUDA = 2,
  kCUDA_HOST = 3,
  kOPENCL = 4,
  kVULKAN = 7,
  kMETAL = 8,
  kVPI = 9,
  kROCM = 10,
  kROCM_HOST = 11,
  kEXT_DEV = 12,
  kCUDA_MANAGED = 13,
  kONEAPI = 14,
  kWEBGPU = 15,
  kHEXAGON = 16,
};
constexpr int kDeviceAllocationTypeMax = 16;

inline const char* DeviceAllocationTypeToCStr(DeviceAllocationType type) {
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

}  // namespace arrow