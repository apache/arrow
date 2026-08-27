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

// From Apache Impala (incubating) as of 2016-01-29.

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>

#include <xsimd/xsimd.hpp>

#include "arrow/result.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/string.h"

#ifdef __linux__
#  include <fstream>
#endif

#ifdef __APPLE__
#  include <sys/sysctl.h>
#endif

#ifndef _MSC_VER
#  include <unistd.h>
#endif

#ifdef _WIN32
#  include <intrin.h>

#  include "arrow/util/windows_compatibility.h"
#endif

namespace arrow::internal {

namespace {

void OsRetrieveCpuInfo(int64_t* hardware_flags, CpuInfo::Vendor* vendor) {
  const auto cpu = xsimd::cpu_features();

  *hardware_flags |= cpu.popcnt() ? CpuInfo::POPCNT : 0;
  *hardware_flags |= cpu.bmi1() ? CpuInfo::BMI1 : 0;
  *hardware_flags |= cpu.bmi2() ? CpuInfo::BMI2 : 0;

  // SSE
  *hardware_flags |= cpu.ssse3() ? CpuInfo::SSSE3 : 0;
  *hardware_flags |= cpu.sse4_1() ? CpuInfo::SSE4_1 : 0;
  *hardware_flags |= cpu.sse4_2() ? CpuInfo::SSE4_2 : 0;
  // AVX
  *hardware_flags |= cpu.avx() ? CpuInfo::AVX : 0;
  *hardware_flags |= cpu.avx2() ? CpuInfo::AVX2 : 0;
  // AVX 512
  const bool avx512f = cpu.avx512f();
  *hardware_flags |= cpu.avx512f() ? CpuInfo::AVX512F : 0;
  *hardware_flags |= cpu.avx512cd() ? CpuInfo::AVX512CD : 0;
  *hardware_flags |= cpu.avx512dq() ? CpuInfo::AVX512DQ : 0;
  *hardware_flags |= cpu.avx512bw() ? CpuInfo::AVX512BW : 0;
  // TODO(xsimd): Missing in xsimd 14.2.0 but fixed afterwards.
  // Can be replaced with the following (no `if(avx512f)` required).
  // *hardware_flags |= cpu.avx512vl() ? CpuInfo::AVX512VL : 0;
  if (avx512f) {
    const auto cpu_x86 = xsimd::x86_cpu_features_backend_default();
    auto constexpr avx512vl = static_cast<xsimd::x86_cpuid_leaf7::ebx>(31);
    *hardware_flags |= cpu_x86.leaf7().all_bits_set<avx512vl>() ? CpuInfo::AVX512VL : 0;
  }

  // Neon
  *hardware_flags |= cpu.neon64() ? CpuInfo::ASIMD : 0;
  // SVE and length
  // Running SVE128 on a SVE256 machine is more tricky than the x86 equivalent of
  // running SSE code on an AVX machine and requires to explicitly change the
  // vector length using `prctl` (per thread setting).
  const bool sve = cpu.sve();
  const auto sve_size = cpu.sve_size_bytes();
  *hardware_flags |= sve ? CpuInfo::SVE : 0;
  *hardware_flags |= (sve && sve_size == 16) ? CpuInfo::SVE128 : 0;
  *hardware_flags |= (sve && sve_size == 32) ? CpuInfo::SVE256 : 0;
  *hardware_flags |= (sve && sve_size == 64) ? CpuInfo::SVE512 : 0;

  // x86 only
  switch (cpu.known_manufacturer()) {
    case (xsimd::x86_manufacturer::intel):
      *vendor = CpuInfo::Vendor::Intel;
      break;
    case (xsimd::x86_manufacturer::amd):
      *vendor = CpuInfo::Vendor::AMD;
      break;
    default: {
    }
  }
}

//============================== OS Dependent ==============================//

#if defined(_WIN32)
//------------------------------ WINDOWS ------------------------------//
template <std::size_t N>
void OsRetrieveCacheSize(std::array<int64_t, N>* cache_sizes) {
  static_assert(N >= 3);
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer = nullptr;
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer_position = nullptr;
  DWORD buffer_size = 0;
  size_t offset = 0;
  typedef BOOL(WINAPI * GetLogicalProcessorInformationFuncPointer)(void*, void*);
  GetLogicalProcessorInformationFuncPointer func_pointer =
      (GetLogicalProcessorInformationFuncPointer)GetProcAddress(
          GetModuleHandleW(L"kernel32"), "GetLogicalProcessorInformation");

  if (!func_pointer) {
    ARROW_LOG(WARNING) << "Failed to find procedure GetLogicalProcessorInformation";
    return;
  }

  // Get buffer size
  if (func_pointer(buffer, &buffer_size) && GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
    ARROW_LOG(WARNING) << "Failed to get size of processor information buffer";
    return;
  }

  buffer = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION)malloc(buffer_size);
  if (!buffer) {
    return;
  }

  if (!func_pointer(buffer, &buffer_size)) {
    ARROW_LOG(WARNING) << "Failed to get processor information";
    free(buffer);
    return;
  }

  buffer_position = buffer;
  while (offset + sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION) <= buffer_size) {
    if (RelationCache == buffer_position->Relationship) {
      PCACHE_DESCRIPTOR cache = &buffer_position->Cache;
      using level_t = decltype(cache->Level);
      if (cache->Level >= 1 && cache->Level <= static_cast<level_t>(N)) {
        const int64_t current = (*cache_sizes)[cache->Level - 1];
        (*cache_sizes)[cache->Level - 1] = std::max<int64_t>(current, cache->Size);
      }
    }
    offset += sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION);
    buffer_position++;
  }

  free(buffer);
}

#elif defined(__APPLE__)
//------------------------------ MACOS ------------------------------//
std::optional<int64_t> IntegerSysCtlByName(const char* name) {
  size_t len = sizeof(int64_t);
  int64_t data = 0;
  if (sysctlbyname(name, &data, &len, nullptr, 0) == 0) {
    return data;
  }
  // ENOENT is the official errno value for non-existing sysctl's,
  // but EINVAL and ENOTSUP have been seen in the wild.
  if (errno != ENOENT && errno != EINVAL && errno != ENOTSUP) {
    auto st = IOErrorFromErrno(errno, "sysctlbyname failed for '", name, "'");
    ARROW_LOG(WARNING) << st.ToString();
  }
  return std::nullopt;
}

template <std::size_t N>
void OsRetrieveCacheSize(std::array<int64_t, N>* cache_sizes) {
  static_assert(N >= 3);
  auto c = IntegerSysCtlByName("hw.l1dcachesize");
  if (c.has_value()) {
    (*cache_sizes)[0] = *c;
  }
  c = IntegerSysCtlByName("hw.l2cachesize");
  if (c.has_value()) {
    (*cache_sizes)[1] = *c;
  }
  c = IntegerSysCtlByName("hw.l3cachesize");
  if (c.has_value()) {
    (*cache_sizes)[2] = *c;
  }
}

#elif defined(__linux__)
//------------------------------ LINUX ------------------------------//

// Get cache size, return 0 on error
int64_t LinuxGetCacheSize(int level) {
  // get cache size by sysconf()
#  ifdef _SC_LEVEL1_DCACHE_SIZE
  constexpr auto kCacheSizeConf = std::array<int, 3>{
      _SC_LEVEL1_DCACHE_SIZE,
      _SC_LEVEL2_CACHE_SIZE,
      _SC_LEVEL3_CACHE_SIZE,
  };

  errno = 0;
  DCHECK(0 <= level && static_cast<std::size_t>(level) < kCacheSizeConf.size());
  const int64_t cache_size = sysconf(kCacheSizeConf[level]);
  if (errno == 0 && cache_size > 0) {
    return cache_size;
  }
#  endif

  // get cache size from sysfs if sysconf() fails or not supported
  constexpr auto kCacheSizeSysfs = std::array<const char*, 3>{
      "/sys/devices/system/cpu/cpu0/cache/index0/size",  // l1d (index1 is l1i)
      "/sys/devices/system/cpu/cpu0/cache/index2/size",  // l2
      "/sys/devices/system/cpu/cpu0/cache/index3/size",  // l3
  };

  DCHECK(0 <= level && static_cast<std::size_t>(level) < kCacheSizeSysfs.size());
  std::ifstream cacheinfo(kCacheSizeSysfs[level], std::ios::in);
  if (!cacheinfo) {
    return 0;
  }
  // cacheinfo is one line like: 65536, 64K, 1M, etc.
  uint64_t size = 0;
  char unit = '\0';
  cacheinfo >> size >> unit;
  if (unit == 'K') {
    size <<= 10;
  } else if (unit == 'M') {
    size <<= 20;
  } else if (unit == 'G') {
    size <<= 30;
  } else if (unit != '\0') {
    return 0;
  }
  return static_cast<int64_t>(size);
}

template <std::size_t N>
void OsRetrieveCacheSize(std::array<int64_t, N>* cache_sizes) {
  static_assert(N <= 3);
  for (int i = 0; i < static_cast<int>(N); ++i) {
    const int64_t cache_size = LinuxGetCacheSize(i);
    if (cache_size > 0) {
      (*cache_sizes)[i] = cache_size;
    }
  }
}

#else

template <std::size_t N>
void OsRetrieveCacheSize(std::array<int64_t, N>* /* cache_sizes */) {
  // NoOp, will be defaulted by CpuInfo::CacheSize
}

#endif  // WINDOWS, MACOS, LINUX

//============================== Arch Dependent ==============================//

#if XSIMD_TARGET_X86
//------------------------------ X86_64 ------------------------------//
bool ArchParseUserSimdLevel(const std::string& simd_level, int64_t* hardware_flags) {
  enum {
    USER_SIMD_NONE,
    USER_SIMD_SSE4_2,
    USER_SIMD_AVX,
    USER_SIMD_AVX2,
    USER_SIMD_AVX512,
    USER_SIMD_MAX,
  };

  int level = USER_SIMD_MAX;
  // Parse the level
  if (simd_level == "AVX512") {
    level = USER_SIMD_AVX512;
  } else if (simd_level == "AVX2") {
    level = USER_SIMD_AVX2;
  } else if (simd_level == "AVX") {
    level = USER_SIMD_AVX;
  } else if (simd_level == "SSE4_2") {
    level = USER_SIMD_SSE4_2;
  } else if (simd_level == "NONE") {
    level = USER_SIMD_NONE;
  } else {
    return false;
  }

  // Disable feature as the level
  if (level < USER_SIMD_AVX512) {
    *hardware_flags &= ~CpuInfo::AVX512;
  }
  if (level < USER_SIMD_AVX2) {
    *hardware_flags &= ~(CpuInfo::AVX2 | CpuInfo::BMI2);
  }
  if (level < USER_SIMD_AVX) {
    *hardware_flags &= ~CpuInfo::AVX;
  }
  if (level < USER_SIMD_SSE4_2) {
    *hardware_flags &= ~(CpuInfo::SSE4_2 | CpuInfo::BMI1);
  }
  return true;
}

void ArchVerifyCpuRequirements(const CpuInfo* ci) {
#  if defined(ARROW_HAVE_SSE4_2)
  if (!ci->IsDetected(CpuInfo::SSE4_2)) {
    DCHECK(false) << "CPU does not support the Supplemental SSE4_2 instruction set";
  }
#  endif
}

#elif XSIMD_TARGET_ARM64
//------------------------------ AARCH64 ------------------------------//
bool ArchParseUserSimdLevel(const std::string& simd_level, int64_t* hardware_flags) {
  enum {
    USER_SIMD_NONE,
    USER_SIMD_SVE,
    USER_SIMD_SVE128,
    USER_SIMD_SVE256,
    USER_SIMD_SVE512,
    USER_SIMD_MAX,
  };

  int level = USER_SIMD_MAX;
  if (simd_level == "SVE") {
    level = USER_SIMD_SVE;
  } else if (simd_level == "SVE128") {
    level = USER_SIMD_SVE128;
  } else if (simd_level == "SVE256") {
    level = USER_SIMD_SVE256;
  } else if (simd_level == "SVE512") {
    level = USER_SIMD_SVE512;
  } else if (simd_level == "NONE") {
    level = USER_SIMD_NONE;
  } else {
    return false;
  }

  if (level < USER_SIMD_SVE512) *hardware_flags &= ~CpuInfo::SVE512;
  if (level < USER_SIMD_SVE256) *hardware_flags &= ~CpuInfo::SVE256;
  if (level < USER_SIMD_SVE128) *hardware_flags &= ~CpuInfo::SVE128;
  if (level < USER_SIMD_SVE) *hardware_flags &= ~CpuInfo::SVE;
  return true;
}

void ArchVerifyCpuRequirements(const CpuInfo* ci) {
  if (!ci->IsDetected(CpuInfo::ASIMD)) {
    DCHECK(false) << "CPU does not support the Armv8 Neon instruction set";
  }
}

#else
//------------------------------ PPC, ... ------------------------------//
bool ArchParseUserSimdLevel(const std::string& simd_level, int64_t* hardware_flags) {
  return true;
}

void ArchVerifyCpuRequirements(const CpuInfo* ci) {}

#endif  // X86, ARM, PPC

}  // namespace

CpuInfo::CpuInfo() {
  OsRetrieveCacheSize(&cache_sizes_);
  OsRetrieveCpuInfo(&hardware_flags_, &vendor_);
  original_hardware_flags_ = hardware_flags_;
  num_cores_ = std::max(static_cast<int>(std::thread::hardware_concurrency()), 1);

  // parse user simd level
  auto maybe_env_var = GetEnvVar("ARROW_USER_SIMD_LEVEL");
  if (!maybe_env_var.ok()) {
    return;
  }
  std::string s = *std::move(maybe_env_var);
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  if (!ArchParseUserSimdLevel(s, &hardware_flags_)) {
    ARROW_LOG(WARNING) << "Invalid value for ARROW_USER_SIMD_LEVEL: " << s;
  }
}

const CpuInfo* CpuInfo::GetInstance() {
  static const CpuInfo cpu_info;
  return &cpu_info;
}

int64_t CpuInfo::CacheSize(CacheLevel level) const {
  constexpr auto kDefaultCacheSizes = std::array<int64_t, 3>{
      32 * 1024,    // Level 1: 32K
      256 * 1024,   // Level 2: 256K
      3072 * 1024,  // Level 3: 3M
  };
  static_assert(kDefaultCacheSizes.size() == kCacheLevels);

  static_assert(static_cast<int>(CacheLevel::L1) == 0);
  const int i = static_cast<int>(level);
  if (cache_sizes_[i] > 0) return cache_sizes_[i];
  if (i == 0) return kDefaultCacheSizes[0];
  // l3 may be not available, return maximum of l2 or default size
  return std::max(kDefaultCacheSizes[i], cache_sizes_[i - 1]);
}

bool CpuInfo::IsSupported(int64_t flags) const {
  return (hardware_flags_ & flags) == flags;
}

bool CpuInfo::IsDetected(int64_t flags) const {
  return (original_hardware_flags_ & flags) == flags;
}

void CpuInfo::VerifyCpuRequirements() const { return ArchVerifyCpuRequirements(this); }

void CpuInfo::EnableFeature(int64_t flag, bool enable) {
  if (!enable) {
    hardware_flags_ &= ~flag;
  } else {
    // Can't turn something on that can't be supported
    DCHECK_EQ((~original_hardware_flags_) & flag, 0);
    hardware_flags_ |= (flag & original_hardware_flags_);
  }
}

}  // namespace arrow::internal
