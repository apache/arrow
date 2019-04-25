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

#ifndef CPU_FEATURES_INCLUDE_CPUINFO_AARCH64_H_
#define CPU_FEATURES_INCLUDE_CPUINFO_AARCH64_H_

#include "cpu_features_macros.h"

CPU_FEATURES_START_CPP_NAMESPACE

typedef struct {
  int fp : 1;     // Floating-point.
  int asimd : 1;  // Advanced SIMD.
  int aes : 1;    // Hardware-accelerated Advanced Encryption Standard.
  int pmull : 1;  // Polynomial multiply long.
  int sha1 : 1;   // Hardware-accelerated SHA1.
  int sha2 : 1;   // Hardware-accelerated SHA2-256.
  int crc32 : 1;  // Hardware-accelerated CRC-32.

  // Make sure to update Aarch64FeaturesEnum below if you add a field here.
} Aarch64Features;

typedef struct {
  Aarch64Features features;
  int implementer;
  int variant;
  int part;
  int revision;
} Aarch64Info;

Aarch64Info GetAarch64Info(void);

////////////////////////////////////////////////////////////////////////////////
// Introspection functions

typedef enum {
  AARCH64_FP,
  AARCH64_ASIMD,
  AARCH64_AES,
  AARCH64_PMULL,
  AARCH64_SHA1,
  AARCH64_SHA2,
  AARCH64_CRC32,
  AARCH64_LAST_,
} Aarch64FeaturesEnum;

int GetAarch64FeaturesEnumValue(const Aarch64Features* features,
                                Aarch64FeaturesEnum value);

const char* GetAarch64FeaturesEnumName(Aarch64FeaturesEnum);

CPU_FEATURES_END_CPP_NAMESPACE

#if !defined(CPU_FEATURES_ARCH_AARCH64)
#error "Including cpuinfo_aarch64.h from a non-aarch64 target."
#endif

#endif  // CPU_FEATURES_INCLUDE_CPUINFO_AARCH64_H_
