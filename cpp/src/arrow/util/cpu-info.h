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

#ifndef ARROW_UTIL_CPU_INFO_H
#define ARROW_UTIL_CPU_INFO_H

#include <cstdint>
#include <string>

#include "arrow/util/visibility.h"
#include <cpu_features/cpu_features_macros.h>
#if defined(CPU_FEATURES_ARCH_X86)
#include <cpu_features/cpuinfo_x86.h>
#endif

namespace arrow {
namespace internal {

/// CpuInfo is an interface to query for cpu information at runtime.  The caller can
/// query on hardware features support, i.e. is SSSE4. AVX2, etc.
/// It is cross platform as Google's cpu_features library is used under the hood
class ARROW_EXPORT CpuInfo {
 public:

  /// CpuInfo is defined as a singletion without rigorious thread safety checks,
  /// as CPUID is safe itself
  static CpuInfo& GetInstance();

  /// Returns the model name of the cpu (e.g. Intel i7-2600)
  std::string ModelName();

  /// Checks for the existence of SSE 4.2 on x*6-64 compatible processors
  bool CanUseSSE4_2() const;

  /// Checks for the existence of Suplemental SSE3 and halts if the feature is not available
  void CheckMinCpuAndHalt();
  
  /// Returns the initialized feature type appropriate for the current arhcitecture
#if defined(CPU_FEATURES_ARCH_X86)
  const cpu_features::X86Features 
#endif	  
	  Features() const;

private : 
  CpuInfo() = default;
  ~CpuInfo() = default;
  CpuInfo(const CpuInfo&) = delete;
  CpuInfo& operator=(const CpuInfo&) = delete;

private:
#if defined(CPU_FEATURES_ARCH_X86)
  static const cpu_features::X86Features features_;
#endif
};

}  // namespace internal
}  // namespace arrow

#endif  // ARROW_UTIL_CPU_INFO_H
