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

// From Apache Impala (incubating) as of 2016-01-29. Pared down to a minimal
// set of functions needed for Apache Arrow / Apache parquet-cpp

#pragma once

#include <cstdint>
#include <string>

#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

/// CpuInfo is an interface to query for cpu information at runtime.  The caller can
/// ask for the sizes of the caches and what hardware features are supported.
/// On Linux, this information is pulled from a couple of sys files (/proc/cpuinfo and
/// /sys/devices)
class ARROW_EXPORT CpuInfo {
 public:
  static constexpr int64_t SSSE3 = (1 << 1);
  static constexpr int64_t SSE4_1 = (1 << 2);
  static constexpr int64_t SSE4_2 = (1 << 3);
  static constexpr int64_t POPCNT = (1 << 4);
  static constexpr int64_t ASIMD = (1 << 5);
  static constexpr int64_t AVX = (1 << 6);
  static constexpr int64_t AVX2 = (1 << 7);
  static constexpr int64_t AVX512F = (1 << 8);
  static constexpr int64_t AVX512CD = (1 << 9);
  static constexpr int64_t AVX512VL = (1 << 10);
  static constexpr int64_t AVX512DQ = (1 << 11);
  static constexpr int64_t AVX512BW = (1 << 12);
  static constexpr int64_t BMI1 = (1 << 13);
  static constexpr int64_t BMI2 = (1 << 14);

  /// Typical AVX512 subsets consists of AVX512F,AVX512BW,AVX512VL,AVX512CD,AVX512DQ
  static constexpr int64_t AVX512 = AVX512F | AVX512CD | AVX512VL | AVX512DQ | AVX512BW;

  /// Cache enums for L1 (data), L2 and L3
  enum CacheLevel {
    L1_CACHE = 0,
    L2_CACHE = 1,
    L3_CACHE = 2,
  };

  enum class Vendor : int { Unknown = 0, Intel, AMD };

  /// The SIMD level set by user
  enum UserSimdLevel {
    USER_SIMD_NONE = 0,
    USER_SIMD_SSE4_2,
    USER_SIMD_AVX,
    USER_SIMD_AVX2,
    USER_SIMD_AVX512,
    USER_SIMD_MAX,
  };

  static CpuInfo* GetInstance();

  /// Determine if the CPU meets the minimum CPU requirements and if not, issue an error
  /// and terminate.
  void VerifyCpuRequirements();

  /// Returns all the flags for this cpu
  int64_t hardware_flags();

  /// Returns whether or not the cpu supports all the flags
  bool IsSupported(int64_t flags) const { return (hardware_flags_ & flags) == flags; }

  /// \brief The processor supports SSE4.2 and the Arrow libraries are built
  /// with support for it
  bool CanUseSSE4_2() const;

  /// Toggle a hardware feature on and off.  It is not valid to turn on a feature
  /// that the underlying hardware cannot support. This is useful for testing.
  void EnableFeature(int64_t flag, bool enable);

  /// Returns the size of the cache in KB at this cache level
  int64_t CacheSize(CacheLevel level);

  /// Returns the number of cpu cycles per millisecond
  int64_t cycles_per_ms();

  /// Returns the number of cores (including hyper-threaded) on this machine.
  int num_cores();

  /// Returns the model name of the cpu (e.g. Intel i7-2600)
  std::string model_name();

  /// Returns the vendor of the cpu.
  Vendor vendor() const { return vendor_; }

  bool HasEfficientBmi2() const {
    // BMI2 (pext, pdep) is only efficient on Intel X86 processors.
    return vendor() == Vendor::Intel && IsSupported(BMI2);
  }

 private:
  CpuInfo();

  void Init();

  /// Inits CPU cache size variables with default values
  void SetDefaultCacheSize();

  /// Parse the SIMD level by ARROW_USER_SIMD_LEVEL env
  void ParseUserSimdLevel();

  int64_t hardware_flags_;
  int64_t original_hardware_flags_;
  int64_t cache_sizes_[L3_CACHE + 1];
  int64_t cycles_per_ms_;
  int num_cores_;
  std::string model_name_;
  Vendor vendor_;
};

}  // namespace internal
}  // namespace arrow
