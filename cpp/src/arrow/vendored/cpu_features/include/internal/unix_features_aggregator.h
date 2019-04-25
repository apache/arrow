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

// CapabilityConfig provides a way to map cpu features to hardware caps and
// /proc/cpuinfo flags. We then provide functions to update capabilities from
// either source.
#ifndef CPU_FEATURES_INCLUDE_INTERNAL_LINUX_FEATURES_AGGREGATOR_H_
#define CPU_FEATURES_INCLUDE_INTERNAL_LINUX_FEATURES_AGGREGATOR_H_

#include <ctype.h>
#include <stdint.h>
#include "cpu_features_macros.h"
#include "internal/hwcaps.h"
#include "internal/string_view.h"

CPU_FEATURES_START_CPP_NAMESPACE

// Use the following macro to declare setter functions to be used in
// CapabilityConfig.
#define DECLARE_SETTER(FeatureType, FeatureName)                    \
  static void set_##FeatureName(void* const features, bool value) { \
    ((FeatureType*)features)->FeatureName = value;                  \
  }

// Describes the relationship between hardware caps and /proc/cpuinfo flags.
typedef struct {
  const HardwareCapabilities hwcaps_mask;
  const char* const proc_cpuinfo_flag;
  void (*set_bit)(void* const, bool);  // setter for the corresponding bit.
} CapabilityConfig;

// For every config, looks into flags_line for the presence of the
// corresponding proc_cpuinfo_flag, calls `set_bit` accordingly.
// Note: features is a pointer to the underlying Feature struct.
void CpuFeatures_SetFromFlags(const size_t configs_size,
                              const CapabilityConfig* configs,
                              const StringView flags_line,
                              void* const features);

// For every config, looks into hwcaps for the presence of the feature. Calls
// `set_bit` with true if the hardware capability is found.
// Note: features is a pointer to the underlying Feature struct.
void CpuFeatures_OverrideFromHwCaps(const size_t configs_size,
                                    const CapabilityConfig* configs,
                                    const HardwareCapabilities hwcaps,
                                    void* const features);

CPU_FEATURES_END_CPP_NAMESPACE
#endif  // CPU_FEATURES_INCLUDE_INTERNAL_LINUX_FEATURES_AGGREGATOR_H_
