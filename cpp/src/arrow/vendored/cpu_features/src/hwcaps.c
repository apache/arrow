// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>
#include <string.h>

#include "cpu_features_macros.h"
#include "internal/filesystem.h"
#include "internal/hwcaps.h"
#include "internal/string_view.h"

#if defined(NDEBUG)
#define D(...)
#else
#include <stdio.h>
#define D(...)           \
  do {                   \
    printf(__VA_ARGS__); \
    fflush(stdout);      \
  } while (0)
#endif

////////////////////////////////////////////////////////////////////////////////
// Implementation of GetElfHwcapFromGetauxval
////////////////////////////////////////////////////////////////////////////////

#if defined(CPU_FEATURES_MOCK_GET_ELF_HWCAP_FROM_GETAUXVAL)
// Implementation will be provided by test/hwcaps_for_testing.cc.
#elif defined(HAVE_STRONG_GETAUXVAL)
#include <sys/auxv.h>
static unsigned long GetElfHwcapFromGetauxval(uint32_t hwcap_type) {
  return getauxval(hwcap_type);
}
#elif defined(HAVE_DLFCN_H)
// On Android we probe the system's C library for a 'getauxval' function and
// call it if it exits, or return 0 for failure. This function is available
// since API level 20.
//
// This code does *NOT* check for '__ANDROID_API__ >= 20' to support the edge
// case where some NDK developers use headers for a platform that is newer than
// the one really targetted by their application. This is typically done to use
// newer native APIs only when running on more recent Android versions, and
// requires careful symbol management.
//
// Note that getauxval() can't really be re-implemented here, because its
// implementation does not parse /proc/self/auxv. Instead it depends on values
// that are passed by the kernel at process-init time to the C runtime
// initialization layer.

#include <dlfcn.h>
#define AT_HWCAP 16
#define AT_HWCAP2 26
#define AT_PLATFORM 15
#define AT_BASE_PLATFORM 24

typedef unsigned long getauxval_func_t(unsigned long);

static uint32_t GetElfHwcapFromGetauxval(uint32_t hwcap_type) {
  uint32_t ret = 0;
  void* libc_handle = NULL;
  getauxval_func_t* func = NULL;

  dlerror();  // Cleaning error state before calling dlopen.
  libc_handle = dlopen("libc.so", RTLD_NOW);
  if (!libc_handle) {
    D("Could not dlopen() C library: %s\n", dlerror());
    return 0;
  }
  func = (getauxval_func_t*)dlsym(libc_handle, "getauxval");
  if (!func) {
    D("Could not find getauxval() in C library\n");
  } else {
    // Note: getauxval() returns 0 on failure. Doesn't touch errno.
    ret = (uint32_t)(*func)(hwcap_type);
  }
  dlclose(libc_handle);
  return ret;
}
#else
#error "This platform does not provide hardware capabilities."
#endif

// Implementation of GetHardwareCapabilities for OS that provide
// GetElfHwcapFromGetauxval().

// Fallback when getauxval is not available, retrieves hwcaps from
// "/proc/self/auxv".
static uint32_t GetElfHwcapFromProcSelfAuxv(uint32_t hwcap_type) {
  struct {
    uint32_t tag;
    uint32_t value;
  } entry;
  uint32_t result = 0;
  const char filepath[] = "/proc/self/auxv";
  const int fd = CpuFeatures_OpenFile(filepath);
  if (fd < 0) {
    D("Could not open %s\n", filepath);
    return 0;
  }
  for (;;) {
    const int ret = CpuFeatures_ReadFile(fd, (char*)&entry, sizeof entry);
    if (ret < 0) {
      D("Error while reading %s\n", filepath);
      break;
    }
    // Detect end of list.
    if (ret == 0 || (entry.tag == 0 && entry.value == 0)) {
      break;
    }
    if (entry.tag == hwcap_type) {
      result = entry.value;
      break;
    }
  }
  CpuFeatures_CloseFile(fd);
  return result;
}

// Retrieves hardware capabilities by first trying to call getauxval, if not
// available falls back to reading "/proc/self/auxv".
static unsigned long GetHardwareCapabilitiesFor(uint32_t type) {
  unsigned long hwcaps = GetElfHwcapFromGetauxval(type);
  if (!hwcaps) {
    D("Parsing /proc/self/auxv to extract ELF hwcaps!\n");
    hwcaps = GetElfHwcapFromProcSelfAuxv(type);
  }
  return hwcaps;
}

HardwareCapabilities CpuFeatures_GetHardwareCapabilities(void) {
  HardwareCapabilities capabilities;
  capabilities.hwcaps = GetHardwareCapabilitiesFor(AT_HWCAP);
  capabilities.hwcaps2 = GetHardwareCapabilitiesFor(AT_HWCAP2);
  return capabilities;
}

PlatformType kEmptyPlatformType;

PlatformType CpuFeatures_GetPlatformType(void) {
  PlatformType type = kEmptyPlatformType;
  char *platform = (char *)GetHardwareCapabilitiesFor(AT_PLATFORM);
  char *base_platform = (char *)GetHardwareCapabilitiesFor(AT_BASE_PLATFORM);

  if (platform != NULL)
    CpuFeatures_StringView_CopyString(str(platform), type.platform,
                                      sizeof(type.platform));
  if (base_platform != NULL)
    CpuFeatures_StringView_CopyString(str(base_platform), type.base_platform,
                                      sizeof(type.base_platform));
  return type;
}
