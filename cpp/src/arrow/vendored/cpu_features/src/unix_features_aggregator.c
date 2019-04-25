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

#include "internal/unix_features_aggregator.h"
#include "internal/string_view.h"

void CpuFeatures_SetFromFlags(const size_t configs_size,
                              const CapabilityConfig* configs,
                              const StringView flags_line,
                              void* const features) {
  size_t i = 0;
  for (; i < configs_size; ++i) {
    const CapabilityConfig config = configs[i];
    config.set_bit(features, CpuFeatures_StringView_HasWord(
                                 flags_line, config.proc_cpuinfo_flag));
  }
}

static bool IsSet(const uint32_t mask, const uint32_t value) {
  if (mask == 0) return false;
  return (value & mask) == mask;
}

static bool IsHwCapsSet(const HardwareCapabilities hwcaps_mask,
                        const HardwareCapabilities hwcaps) {
  return IsSet(hwcaps_mask.hwcaps, hwcaps.hwcaps) ||
         IsSet(hwcaps_mask.hwcaps2, hwcaps.hwcaps2);
}

void CpuFeatures_OverrideFromHwCaps(const size_t configs_size,
                                    const CapabilityConfig* configs,
                                    const HardwareCapabilities hwcaps,
                                    void* const features) {
  size_t i = 0;
  for (; i < configs_size; ++i) {
    const CapabilityConfig* config = &configs[i];
    if (IsHwCapsSet(config->hwcaps_mask, hwcaps)) {
      config->set_bit(features, true);
    }
  }
}
