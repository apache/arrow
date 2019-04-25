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

#include "cpuinfo_aarch64.h"

#include "internal/filesystem.h"
#include "internal/hwcaps.h"
#include "internal/stack_line_reader.h"
#include "internal/string_view.h"
#include "internal/unix_features_aggregator.h"

#include <ctype.h>

DECLARE_SETTER(Aarch64Features, fp)
DECLARE_SETTER(Aarch64Features, asimd)
DECLARE_SETTER(Aarch64Features, aes)
DECLARE_SETTER(Aarch64Features, pmull)
DECLARE_SETTER(Aarch64Features, sha1)
DECLARE_SETTER(Aarch64Features, sha2)
DECLARE_SETTER(Aarch64Features, crc32)

static const CapabilityConfig kConfigs[] = {
    {{AARCH64_HWCAP_FP, 0}, "fp", &set_fp},           //
    {{AARCH64_HWCAP_ASIMD, 0}, "asimd", &set_asimd},  //
    {{AARCH64_HWCAP_AES, 0}, "aes", &set_aes},        //
    {{AARCH64_HWCAP_PMULL, 0}, "pmull", &set_pmull},  //
    {{AARCH64_HWCAP_SHA1, 0}, "sha1", &set_sha1},     //
    {{AARCH64_HWCAP_SHA2, 0}, "sha2", &set_sha2},     //
    {{AARCH64_HWCAP_CRC32, 0}, "crc32", &set_crc32},  //
};

static const size_t kConfigsSize = sizeof(kConfigs) / sizeof(CapabilityConfig);

static bool HandleAarch64Line(const LineResult result,
                              Aarch64Info* const info) {
  StringView line = result.line;
  StringView key, value;
  if (CpuFeatures_StringView_GetAttributeKeyValue(line, &key, &value)) {
    if (CpuFeatures_StringView_IsEquals(key, str("Features"))) {
      CpuFeatures_SetFromFlags(kConfigsSize, kConfigs, value, &info->features);
    } else if (CpuFeatures_StringView_IsEquals(key, str("CPU implementer"))) {
      info->implementer = CpuFeatures_StringView_ParsePositiveNumber(value);
    } else if (CpuFeatures_StringView_IsEquals(key, str("CPU variant"))) {
      info->variant = CpuFeatures_StringView_ParsePositiveNumber(value);
    } else if (CpuFeatures_StringView_IsEquals(key, str("CPU part"))) {
      info->part = CpuFeatures_StringView_ParsePositiveNumber(value);
    } else if (CpuFeatures_StringView_IsEquals(key, str("CPU revision"))) {
      info->revision = CpuFeatures_StringView_ParsePositiveNumber(value);
    }
  }
  return !result.eof;
}

static void FillProcCpuInfoData(Aarch64Info* const info) {
  const int fd = CpuFeatures_OpenFile("/proc/cpuinfo");
  if (fd >= 0) {
    StackLineReader reader;
    StackLineReader_Initialize(&reader, fd);
    for (;;) {
      if (!HandleAarch64Line(StackLineReader_NextLine(&reader), info)) {
        break;
      }
    }
    CpuFeatures_CloseFile(fd);
  }
}

static const Aarch64Info kEmptyAarch64Info;

Aarch64Info GetAarch64Info(void) {
  // capabilities are fetched from both getauxval and /proc/cpuinfo so we can
  // have some information if the executable is sandboxed (aka no access to
  // /proc/cpuinfo).
  Aarch64Info info = kEmptyAarch64Info;

  FillProcCpuInfoData(&info);
  CpuFeatures_OverrideFromHwCaps(kConfigsSize, kConfigs,
                                 CpuFeatures_GetHardwareCapabilities(),
                                 &info.features);

  return info;
}

////////////////////////////////////////////////////////////////////////////////
// Introspection functions

int GetAarch64FeaturesEnumValue(const Aarch64Features* features,
                                Aarch64FeaturesEnum value) {
  switch (value) {
    case AARCH64_FP:
      return features->fp;
    case AARCH64_ASIMD:
      return features->asimd;
    case AARCH64_AES:
      return features->aes;
    case AARCH64_PMULL:
      return features->pmull;
    case AARCH64_SHA1:
      return features->sha1;
    case AARCH64_SHA2:
      return features->sha2;
    case AARCH64_CRC32:
      return features->crc32;
    case AARCH64_LAST_:
      break;
  }
  return false;
}

const char* GetAarch64FeaturesEnumName(Aarch64FeaturesEnum value) {
  switch (value) {
    case AARCH64_FP:
      return "fp";
    case AARCH64_ASIMD:
      return "asimd";
    case AARCH64_AES:
      return "aes";
    case AARCH64_PMULL:
      return "pmull";
    case AARCH64_SHA1:
      return "sha1";
    case AARCH64_SHA2:
      return "sha2";
    case AARCH64_CRC32:
      return "crc32";
    case AARCH64_LAST_:
      break;
  }
  return "unknown feature";
}
