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

#if defined(ARROW_HAVE_NEON)
#  define FIND_MIN_MAX_PLATFORM FindMinMaxNeon
#  define ARCH_PLATFORM xsimd::neon64
#elif defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_RUNTIME_SSE4_2)
#  define FIND_MIN_MAX_PLATFORM FindMinMaxSse42
#  define ARCH_PLATFORM xsimd::sse4_2
#endif

#if defined(FIND_MIN_MAX_PLATFORM)

#  include "parquet/level_comparison_simd_internal.h"
#  include "parquet/level_comparison_simd_kernel_internal.h"

namespace parquet::internal {

MinMax FIND_MIN_MAX_PLATFORM(const int16_t* levels, int64_t num_levels) {
  return FindMinMaxSimd<ARCH_PLATFORM>(levels, num_levels);
}

}  // namespace parquet::internal

#  undef ARCH_PLATFORM
#  undef FIND_MIN_MAX_PLATFORM
#endif  // FIND_MIN_MAX_PLATFORM
