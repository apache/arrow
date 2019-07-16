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

#include "arrow/util/cpu-info.h"

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include "arrow/util/windows_compatibility.h"
#endif

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>

#include "arrow/util/logging.h"

namespace arrow {
namespace internal {
	
const cpu_features::X86Features CpuInfo::features_ = cpu_features::GetX86Info().features;

CpuInfo& CpuInfo::GetInstance() {
  // There is no need for heavyweight singleton,
  // as CPUID is faster and will always return the same value
  static CpuInfo instance;
  return instance;
}

const cpu_features::X86Features CpuInfo::Features() const { 
	return features_; 
}

void CpuInfo::CheckMinCpuAndHalt() {
  if (!features_.ssse3) {
    DCHECK(false) << "CPU does not support the Supplemental SSE3 instruction set";
  }
}

bool CpuInfo::CanUseSSE4_2() const {
#ifdef ARROW_USE_SIMD
  return features_.sse4_2;
#else
  return false;
#endif
}

std::string CpuInfo::ModelName() { 
  char brand_string[49];
  cpu_features::FillX86BrandString( brand_string );
  return brand_string;
}

}  // namespace internal
}  // namespace arrow
