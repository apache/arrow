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

#include "arrow/util/cpu_info.h"

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

#include <stdlib.h>
#include <string.h>

#ifndef _MSC_VER
#include <unistd.h>
#endif

#ifdef _WIN32
#include <immintrin.h>
#include <intrin.h>
#include <array>
#include <bitset>

#include "arrow/util/windows_compatibility.h"
#endif

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>

#include "arrow/result.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/string.h"

namespace arrow {
namespace internal {

namespace {

using std::max;

constexpr int64_t kDefaultL1CacheSize = 32 * 1024;    // Level 1: 32k
constexpr int64_t kDefaultL2CacheSize = 256 * 1024;   // Level 2: 256k
constexpr int64_t kDefaultL3CacheSize = 3072 * 1024;  // Level 3: 3M

#if defined(__MINGW64_VERSION_MAJOR) && __MINGW64_VERSION_MAJOR < 5
void __cpuidex(int CPUInfo[4], int function_id, int subfunction_id) {
  __asm__ __volatile__("cpuid"
                       : "=a"(CPUInfo[0]), "=b"(CPUInfo[1]), "=c"(CPUInfo[2]),
                         "=d"(CPUInfo[3])
                       : "a"(function_id), "c"(subfunction_id));
}

int64_t _xgetbv(int xcr) {
  int out = 0;
  __asm__ __volatile__("xgetbv" : "=a"(out) : "c"(xcr) : "%edx");
  return out;
}
#endif

#ifdef __APPLE__
util::optional<int64_t> IntegerSysCtlByName(const char* name) {
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
  return util::nullopt;
}
#endif

#if defined(__GNUC__) && defined(__linux__) && defined(__aarch64__)
// There is no direct instruction to get cache size on Arm64 like '__cpuid' on x86;
// Get Arm64 cache size by reading '/sys/devices/system/cpu/cpu0/cache/index*/size';
// index* :
//   index0: L1 Dcache
//   index1: L1 Icache
//   index2: L2 cache
//   index3: L3 cache
const char* kL1CacheSizeFile = "/sys/devices/system/cpu/cpu0/cache/index0/size";
const char* kL2CacheSizeFile = "/sys/devices/system/cpu/cpu0/cache/index2/size";
const char* kL3CacheSizeFile = "/sys/devices/system/cpu/cpu0/cache/index3/size";

int64_t GetArm64CacheSize(const char* filename, int64_t default_size = -1) {
  char* content = nullptr;
  char* last_char = nullptr;
  size_t file_len = 0;

  // Read cache file to 'content' for getting cache size.
  FILE* cache_file = fopen(filename, "r");
  if (cache_file == nullptr) {
    return default_size;
  }
  int res = getline(&content, &file_len, cache_file);
  fclose(cache_file);
  if (res == -1) {
    return default_size;
  }
  std::unique_ptr<char, decltype(&free)> content_guard(content, &free);

  errno = 0;
  const auto cardinal_num = strtoull(content, &last_char, 0);
  if (errno != 0) {
    return default_size;
  }
  // kB, MB, or GB
  int64_t multip = 1;
  switch (*last_char) {
    case 'g':
    case 'G':
      multip *= 1024;
    case 'm':
    case 'M':
      multip *= 1024;
    case 'k':
    case 'K':
      multip *= 1024;
  }
  return cardinal_num * multip;
}
#endif

#if !defined(_WIN32) && !defined(__APPLE__)
struct {
  std::string name;
  int64_t flag;
} flag_mappings[] = {
#if (defined(__i386) || defined(_M_IX86) || defined(__x86_64__) || defined(_M_X64))
    {"ssse3", CpuInfo::SSSE3},       {"sse4_1", CpuInfo::SSE4_1},
    {"sse4_2", CpuInfo::SSE4_2},     {"popcnt", CpuInfo::POPCNT},
    {"avx", CpuInfo::AVX},           {"avx2", CpuInfo::AVX2},
    {"avx512f", CpuInfo::AVX512F},   {"avx512cd", CpuInfo::AVX512CD},
    {"avx512vl", CpuInfo::AVX512VL}, {"avx512dq", CpuInfo::AVX512DQ},
    {"avx512bw", CpuInfo::AVX512BW}, {"bmi1", CpuInfo::BMI1},
    {"bmi2", CpuInfo::BMI2},
#endif
#if defined(__aarch64__)
    {"asimd", CpuInfo::ASIMD},
#endif
};
const int64_t num_flags = sizeof(flag_mappings) / sizeof(flag_mappings[0]);

// Helper function to parse for hardware flags.
// values contains a list of space-separated flags.  check to see if the flags we
// care about are present.
// Returns a bitmap of flags.
int64_t ParseCPUFlags(const std::string& values) {
  int64_t flags = 0;
  for (int i = 0; i < num_flags; ++i) {
    if (values.find(flag_mappings[i].name) != std::string::npos) {
      flags |= flag_mappings[i].flag;
    }
  }
  return flags;
}
#endif

#ifdef _WIN32
bool RetrieveCacheSize(int64_t* cache_sizes) {
  if (!cache_sizes) {
    return false;
  }
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer = nullptr;
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer_position = nullptr;
  DWORD buffer_size = 0;
  size_t offset = 0;
  typedef BOOL(WINAPI * GetLogicalProcessorInformationFuncPointer)(void*, void*);
  GetLogicalProcessorInformationFuncPointer func_pointer =
      (GetLogicalProcessorInformationFuncPointer)GetProcAddress(
          GetModuleHandle("kernel32"), "GetLogicalProcessorInformation");

  if (!func_pointer) {
    return false;
  }

  // Get buffer size
  if (func_pointer(buffer, &buffer_size) && GetLastError() != ERROR_INSUFFICIENT_BUFFER)
    return false;

  buffer = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION)malloc(buffer_size);

  if (!buffer || !func_pointer(buffer, &buffer_size)) {
    return false;
  }

  buffer_position = buffer;
  while (offset + sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION) <= buffer_size) {
    if (RelationCache == buffer_position->Relationship) {
      PCACHE_DESCRIPTOR cache = &buffer_position->Cache;
      if (cache->Level >= 1 && cache->Level <= 3) {
        cache_sizes[cache->Level - 1] += cache->Size;
      }
    }
    offset += sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION);
    buffer_position++;
  }

  if (buffer) {
    free(buffer);
  }
  return true;
}

// Source: https://en.wikipedia.org/wiki/CPUID
bool RetrieveCPUInfo(int64_t* hardware_flags, std::string* model_name,
                     CpuInfo::Vendor* vendor) {
  if (!hardware_flags || !model_name || !vendor) {
    return false;
  }
  int register_EAX_id = 1;
  int highest_valid_id = 0;
  int highest_extended_valid_id = 0;
  std::bitset<32> features_ECX;
  std::array<int, 4> cpu_info;

  // Get highest valid id
  __cpuid(cpu_info.data(), 0);
  highest_valid_id = cpu_info[0];
  // HEX of "GenuineIntel": 47656E75 696E6549 6E74656C
  // HEX of "AuthenticAMD": 41757468 656E7469 63414D44
  if (cpu_info[1] == 0x756e6547 && cpu_info[2] == 0x49656e69 &&
      cpu_info[3] == 0x6c65746e) {
    *vendor = CpuInfo::Vendor::Intel;
  } else if (cpu_info[1] == 0x68747541 && cpu_info[2] == 0x69746e65 &&
             cpu_info[3] == 0x444d4163) {
    *vendor = CpuInfo::Vendor::AMD;
  }

  if (highest_valid_id <= register_EAX_id) return false;

  // EAX=1: Processor Info and Feature Bits
  __cpuidex(cpu_info.data(), register_EAX_id, 0);
  features_ECX = cpu_info[2];

  // Get highest extended id
  __cpuid(cpu_info.data(), 0x80000000);
  highest_extended_valid_id = cpu_info[0];

  // Retrieve CPU model name
  if (highest_extended_valid_id >= static_cast<int>(0x80000004)) {
    model_name->clear();
    for (int i = 0x80000002; i <= static_cast<int>(0x80000004); ++i) {
      __cpuidex(cpu_info.data(), i, 0);
      *model_name +=
          std::string(reinterpret_cast<char*>(cpu_info.data()), sizeof(cpu_info));
    }
  }

  bool zmm_enabled = false;
  if (features_ECX[27]) {  // OSXSAVE
    // Query if the OS supports saving ZMM registers when switching contexts
    int64_t xcr0 = _xgetbv(0);
    zmm_enabled = (xcr0 & 0xE0) == 0xE0;
  }

  if (features_ECX[9]) *hardware_flags |= CpuInfo::SSSE3;
  if (features_ECX[19]) *hardware_flags |= CpuInfo::SSE4_1;
  if (features_ECX[20]) *hardware_flags |= CpuInfo::SSE4_2;
  if (features_ECX[23]) *hardware_flags |= CpuInfo::POPCNT;
  if (features_ECX[23]) *hardware_flags |= CpuInfo::AVX;

  // cpuid with EAX=7, ECX=0: Extended Features
  register_EAX_id = 7;
  if (highest_valid_id > register_EAX_id) {
    __cpuidex(cpu_info.data(), register_EAX_id, 0);
    std::bitset<32> features_EBX = cpu_info[1];

    if (features_EBX[3]) *hardware_flags |= CpuInfo::BMI1;
    if (features_EBX[5]) *hardware_flags |= CpuInfo::AVX2;
    if (features_EBX[8]) *hardware_flags |= CpuInfo::BMI2;
    // ARROW-11427: only use AVX512 if enabled by the OS
    if (zmm_enabled) {
      if (features_EBX[16]) *hardware_flags |= CpuInfo::AVX512F;
      if (features_EBX[17]) *hardware_flags |= CpuInfo::AVX512DQ;
      if (features_EBX[28]) *hardware_flags |= CpuInfo::AVX512CD;
      if (features_EBX[30]) *hardware_flags |= CpuInfo::AVX512BW;
      if (features_EBX[31]) *hardware_flags |= CpuInfo::AVX512VL;
    }
  }

  return true;
}
#endif

}  // namespace

CpuInfo::CpuInfo()
    : hardware_flags_(0),
      num_cores_(1),
      model_name_("unknown"),
      vendor_(Vendor::Unknown) {}

std::unique_ptr<CpuInfo> g_cpu_info;
static std::once_flag cpuinfo_initialized;

CpuInfo* CpuInfo::GetInstance() {
  std::call_once(cpuinfo_initialized, []() {
    g_cpu_info.reset(new CpuInfo);
    g_cpu_info->Init();
  });
  return g_cpu_info.get();
}

void CpuInfo::Init() {
  std::string line;
  std::string name;
  std::string value;

  float max_mhz = 0;
  int num_cores = 0;

  memset(&cache_sizes_, 0, sizeof(cache_sizes_));

#ifdef _WIN32
  SYSTEM_INFO system_info;
  GetSystemInfo(&system_info);
  num_cores = system_info.dwNumberOfProcessors;

  LARGE_INTEGER performance_frequency;
  if (QueryPerformanceFrequency(&performance_frequency)) {
    max_mhz = static_cast<float>(performance_frequency.QuadPart);
  }
#elif defined(__APPLE__)
  // On macOS, get CPU information from system information base
  struct SysCtlCpuFeature {
    const char* name;
    int64_t flag;
  };
  std::vector<SysCtlCpuFeature> features = {
#if defined(__aarch64__)
    // ARM64 (note that this is exposed under Rosetta as well)
    {"hw.optional.neon", ASIMD},
#else
    // x86
    {"hw.optional.sse4_2", SSSE3 | SSE4_1 | SSE4_2 | POPCNT},
    {"hw.optional.avx1_0", AVX},
    {"hw.optional.avx2_0", AVX2},
    {"hw.optional.bmi1", BMI1},
    {"hw.optional.bmi2", BMI2},
    {"hw.optional.avx512f", AVX512F},
    {"hw.optional.avx512cd", AVX512CD},
    {"hw.optional.avx512dq", AVX512DQ},
    {"hw.optional.avx512bw", AVX512BW},
    {"hw.optional.avx512vl", AVX512VL},
#endif
  };
  for (const auto& feature : features) {
    auto v = IntegerSysCtlByName(feature.name);
    if (v.value_or(0)) {
      hardware_flags_ |= feature.flag;
    }
  }
#else
  // Read from /proc/cpuinfo
  std::ifstream cpuinfo("/proc/cpuinfo", std::ios::in);
  while (cpuinfo) {
    std::getline(cpuinfo, line);
    size_t colon = line.find(':');
    if (colon != std::string::npos) {
      name = TrimString(line.substr(0, colon - 1));
      value = TrimString(line.substr(colon + 1, std::string::npos));
      if (name.compare("flags") == 0 || name.compare("Features") == 0) {
        hardware_flags_ |= ParseCPUFlags(value);
      } else if (name.compare("cpu MHz") == 0) {
        // Every core will report a different speed.  We'll take the max, assuming
        // that when impala is running, the core will not be in a lower power state.
        // TODO: is there a more robust way to do this, such as
        // Window's QueryPerformanceFrequency()
        float mhz = static_cast<float>(atof(value.c_str()));
        max_mhz = max(mhz, max_mhz);
      } else if (name.compare("processor") == 0) {
        ++num_cores;
      } else if (name.compare("model name") == 0) {
        model_name_ = value;
      } else if (name.compare("vendor_id") == 0) {
        if (value.compare("GenuineIntel") == 0) {
          vendor_ = Vendor::Intel;
        } else if (value.compare("AuthenticAMD") == 0) {
          vendor_ = Vendor::AMD;
        }
      }
    }
  }
  if (cpuinfo.is_open()) cpuinfo.close();
#endif

#ifdef __APPLE__
  // On macOS, get cache size from system information base
  SetDefaultCacheSize();
  auto c = IntegerSysCtlByName("hw.l1dcachesize");
  if (c.has_value()) {
    cache_sizes_[0] = *c;
  }
  c = IntegerSysCtlByName("hw.l2cachesize");
  if (c.has_value()) {
    cache_sizes_[1] = *c;
  }
  c = IntegerSysCtlByName("hw.l3cachesize");
  if (c.has_value()) {
    cache_sizes_[2] = *c;
  }
#elif _WIN32
  if (!RetrieveCacheSize(cache_sizes_)) {
    SetDefaultCacheSize();
  }
  RetrieveCPUInfo(&hardware_flags_, &model_name_, &vendor_);
#else
  SetDefaultCacheSize();
#endif

  if (max_mhz != 0) {
    cycles_per_ms_ = static_cast<int64_t>(max_mhz);
#ifndef _WIN32
    cycles_per_ms_ *= 1000;
#endif
  } else {
    cycles_per_ms_ = 1000000;
  }
  original_hardware_flags_ = hardware_flags_;

  if (num_cores > 0) {
    num_cores_ = num_cores;
  } else {
    num_cores_ = 1;
  }

  // Parse the user simd level
  ParseUserSimdLevel();
}

void CpuInfo::VerifyCpuRequirements() {
#ifdef ARROW_HAVE_SSE4_2
  if (!IsSupported(CpuInfo::SSSE3)) {
    DCHECK(false) << "CPU does not support the Supplemental SSE3 instruction set";
  }
#endif
#if defined(ARROW_HAVE_NEON)
  if (!IsSupported(CpuInfo::ASIMD)) {
    DCHECK(false) << "CPU does not support the Armv8 Neon instruction set";
  }
#endif
}

bool CpuInfo::CanUseSSE4_2() const {
#if defined(ARROW_HAVE_SSE4_2)
  return IsSupported(CpuInfo::SSE4_2);
#else
  return false;
#endif
}

void CpuInfo::EnableFeature(int64_t flag, bool enable) {
  if (!enable) {
    hardware_flags_ &= ~flag;
  } else {
    // Can't turn something on that can't be supported
    DCHECK_NE(original_hardware_flags_ & flag, 0);
    hardware_flags_ |= flag;
  }
}

int64_t CpuInfo::hardware_flags() { return hardware_flags_; }

int64_t CpuInfo::CacheSize(CacheLevel level) { return cache_sizes_[level]; }

int64_t CpuInfo::cycles_per_ms() { return cycles_per_ms_; }

int CpuInfo::num_cores() { return num_cores_; }

std::string CpuInfo::model_name() { return model_name_; }

void CpuInfo::SetDefaultCacheSize() {
#if defined(_SC_LEVEL1_DCACHE_SIZE) && !defined(__aarch64__)
  // Call sysconf to query for the cache sizes
  cache_sizes_[0] = sysconf(_SC_LEVEL1_DCACHE_SIZE);
  cache_sizes_[1] = sysconf(_SC_LEVEL2_CACHE_SIZE);
  cache_sizes_[2] = sysconf(_SC_LEVEL3_CACHE_SIZE);
  ARROW_UNUSED(kDefaultL1CacheSize);
  ARROW_UNUSED(kDefaultL2CacheSize);
  ARROW_UNUSED(kDefaultL3CacheSize);
#elif defined(__GNUC__) && defined(__linux__) && defined(__aarch64__)
  cache_sizes_[0] = GetArm64CacheSize(kL1CacheSizeFile, kDefaultL1CacheSize);
  cache_sizes_[1] = GetArm64CacheSize(kL2CacheSizeFile, kDefaultL2CacheSize);
  cache_sizes_[2] = GetArm64CacheSize(kL3CacheSizeFile, kDefaultL3CacheSize);
#else
  // Provide reasonable default values if no info
  cache_sizes_[0] = kDefaultL1CacheSize;
  cache_sizes_[1] = kDefaultL2CacheSize;
  cache_sizes_[2] = kDefaultL3CacheSize;
#endif
}

void CpuInfo::ParseUserSimdLevel() {
  auto maybe_env_var = GetEnvVar("ARROW_USER_SIMD_LEVEL");
  if (!maybe_env_var.ok()) {
    // No user settings
    return;
  }
  std::string s = *std::move(maybe_env_var);
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::toupper(c); });

  int level = USER_SIMD_MAX;
  // Parse the level
  if (s == "AVX512") {
    level = USER_SIMD_AVX512;
  } else if (s == "AVX2") {
    level = USER_SIMD_AVX2;
  } else if (s == "AVX") {
    level = USER_SIMD_AVX;
  } else if (s == "SSE4_2") {
    level = USER_SIMD_SSE4_2;
  } else if (s == "NONE") {
    level = USER_SIMD_NONE;
  } else if (!s.empty()) {
    ARROW_LOG(WARNING) << "Invalid value for ARROW_USER_SIMD_LEVEL: " << s;
  }

  // Disable feature as the level
  if (level < USER_SIMD_AVX512) {  // Disable all AVX512 features
    EnableFeature(AVX512, false);
  }
  if (level < USER_SIMD_AVX2) {  // Disable all AVX2 features
    EnableFeature(AVX2 | BMI2, false);
  }
  if (level < USER_SIMD_AVX) {  // Disable all AVX features
    EnableFeature(AVX, false);
  }
  if (level < USER_SIMD_SSE4_2) {  // Disable all SSE4_2 features
    EnableFeature(SSE4_2 | BMI1, false);
  }
}

}  // namespace internal
}  // namespace arrow
