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

#include <utility>
#include <vector>

#include "arrow/util/cpu_info.h"

namespace arrow {
namespace internal {

struct DispatchLevel {
  enum type { NONE = 0, SSE4_2, AVX2, AVX512, NEON, MAX };
};

template <typename FunctionType>
class DynamicDispatch {
  using FunctionIntance = std::pair<DispatchLevel::type, FunctionType>;

 public:
  // Provide all SIMD instances implemented
  virtual std::vector<FunctionIntance> Implementations() = 0;

  // Reslove the highest SIMD function of host CPU
  void Reslove() {
    FunctionIntance cur = {DispatchLevel::NONE, NULL};
    auto instances = Implementations();

    for (const auto& i : instances) {
      if (i.first >= cur.first && IsSupported(i.first)) {
        // Higher(or same) level than current
        cur = i;
      }
    }

    func = cur.second;
  }

  FunctionType func;

 private:
  bool IsSupported(DispatchLevel::type level) {
    auto cpu_info = arrow::internal::CpuInfo::GetInstance();

    switch (level) {
      case DispatchLevel::NONE:
        return true;
      case DispatchLevel::SSE4_2:
        return cpu_info->IsSupported(CpuInfo::SSE4_2);
      case DispatchLevel::AVX2:
        return cpu_info->IsSupported(CpuInfo::AVX2);
      case DispatchLevel::AVX512:
        return cpu_info->IsSupported(CpuInfo::AVX512);
      default:
        return false;
    }
  }
};

}  // namespace internal
}  // namespace arrow
