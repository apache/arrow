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

// From Apache Impala as of 2016-01-29. Pared down to a minimal set of
// functions needed for parquet-cpp

#include "parquet/util/cpu-info.h"

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

#include <mmintrin.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>

#include "parquet/exception.h"

using boost::algorithm::contains;
using boost::algorithm::trim;
using std::max;
using std::string;

namespace parquet_cpp {

bool CpuInfo::initialized_ = false;
int64_t CpuInfo::hardware_flags_ = 0;
int64_t CpuInfo::original_hardware_flags_;
int64_t CpuInfo::cache_sizes_[L3_CACHE + 1];
int64_t CpuInfo::cycles_per_ms_;
int CpuInfo::num_cores_ = 1;
string CpuInfo::model_name_ = "unknown"; // NOLINT

static struct {
  string name;
  int64_t flag;
} flag_mappings[] = {
  { "ssse3",  CpuInfo::SSSE3 },
  { "sse4_1", CpuInfo::SSE4_1 },
  { "sse4_2", CpuInfo::SSE4_2 },
  { "popcnt", CpuInfo::POPCNT },
};
static const int64_t num_flags = sizeof(flag_mappings) / sizeof(flag_mappings[0]);

// Helper function to parse for hardware flags.
// values contains a list of space-seperated flags.  check to see if the flags we
// care about are present.
// Returns a bitmap of flags.
int64_t ParseCPUFlags(const string& values) {
  int64_t flags = 0;
  for (int i = 0; i < num_flags; ++i) {
    if (contains(values, flag_mappings[i].name)) {
      flags |= flag_mappings[i].flag;
    }
  }
  return flags;
}

void CpuInfo::Init() {
  string line;
  string name;
  string value;

  float max_mhz = 0;
  int num_cores = 0;

  memset(&cache_sizes_, 0, sizeof(cache_sizes_));

  // Read from /proc/cpuinfo
  std::ifstream cpuinfo("/proc/cpuinfo", std::ios::in);
  while (cpuinfo) {
    getline(cpuinfo, line);
    size_t colon = line.find(':');
    if (colon != string::npos) {
      name = line.substr(0, colon - 1);
      value = line.substr(colon + 1, string::npos);
      trim(name);
      trim(value);
      if (name.compare("flags") == 0) {
        hardware_flags_ |= ParseCPUFlags(value);
      } else if (name.compare("cpu MHz") == 0) {
        // Every core will report a different speed.  We'll take the max, assuming
        // that when impala is running, the core will not be in a lower power state.
        // TODO: is there a more robust way to do this, such as
        // Window's QueryPerformanceFrequency()
        float mhz = atof(value.c_str());
        max_mhz = max(mhz, max_mhz);
      } else if (name.compare("processor") == 0) {
        ++num_cores;
      } else if (name.compare("model name") == 0) {
        model_name_ = value;
      }
    }
  }
  if (cpuinfo.is_open()) cpuinfo.close();

#ifdef __APPLE__
  // On Mac OS X use sysctl() to get the cache sizes
  size_t len = 0;
  sysctlbyname("hw.cachesize", NULL, &len, NULL, 0);
  uint64_t* data = static_cast<uint64_t*>(malloc(len));
  sysctlbyname("hw.cachesize", data, &len, NULL, 0);
  DCHECK(len / sizeof(uint64_t) >= 3);
  for (size_t i = 0; i < 3; ++i) {
    cache_sizes_[i] = data[i];
  }
#else
  // Call sysconf to query for the cache sizes
  cache_sizes_[0] = sysconf(_SC_LEVEL1_DCACHE_SIZE);
  cache_sizes_[1] = sysconf(_SC_LEVEL2_CACHE_SIZE);
  cache_sizes_[2] = sysconf(_SC_LEVEL3_CACHE_SIZE);
#endif

  if (max_mhz != 0) {
    cycles_per_ms_ = max_mhz * 1000;
  } else {
    cycles_per_ms_ = 1000000;
  }
  original_hardware_flags_ = hardware_flags_;

  if (num_cores > 0) {
    num_cores_ = num_cores;
  } else {
    num_cores_ = 1;
  }

  initialized_ = true;
}

void CpuInfo::VerifyCpuRequirements() {
  if (!CpuInfo::IsSupported(CpuInfo::SSSE3)) {
    throw ParquetException("CPU does not support the Supplemental SSE3 instruction set");
  }
}

void CpuInfo::EnableFeature(int64_t flag, bool enable) {
  DCHECK(initialized_);
  if (!enable) {
    hardware_flags_ &= ~flag;
  } else {
    // Can't turn something on that can't be supported
    DCHECK((original_hardware_flags_ & flag) != 0);
    hardware_flags_ |= flag;
  }
}

} // namespace parquet_cpp
