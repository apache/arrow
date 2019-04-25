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

#ifndef CPU_FEATURES_INCLUDE_CPUINFO_X86_H_
#define CPU_FEATURES_INCLUDE_CPUINFO_X86_H_

#include "cpu_features_macros.h"

CPU_FEATURES_START_CPP_NAMESPACE

// See https://en.wikipedia.org/wiki/CPUID for a list of x86 cpu features.
// The field names are based on the short name provided in the wikipedia tables.
typedef struct {
  int aes : 1;
  int erms : 1;
  int f16c : 1;
  int fma3 : 1;
  int vpclmulqdq : 1;
  int bmi1 : 1;
  int bmi2 : 1;

  int ssse3 : 1;
  int sse4_1 : 1;
  int sse4_2 : 1;

  int avx : 1;
  int avx2 : 1;

  int avx512f : 1;
  int avx512cd : 1;
  int avx512er : 1;
  int avx512pf : 1;
  int avx512bw : 1;
  int avx512dq : 1;
  int avx512vl : 1;
  int avx512ifma : 1;
  int avx512vbmi : 1;
  int avx512vbmi2 : 1;
  int avx512vnni : 1;
  int avx512bitalg : 1;
  int avx512vpopcntdq : 1;
  int avx512_4vnniw : 1;
  int avx512_4vbmi2 : 1;

  int smx : 1;
  int sgx : 1;
  int cx16 : 1;  // aka. CMPXCHG16B
  int sha : 1;
  int popcnt : 1;
  int movbe : 1;
  int rdrnd : 1;

  // Make sure to update X86FeaturesEnum below if you add a field here.
} X86Features;

typedef struct {
  X86Features features;
  int family;
  int model;
  int stepping;
  char vendor[13];  // 0 terminated string
} X86Info;

// Calls cpuid and returns an initialized X86info.
// This function is guaranteed to be malloc, memset and memcpy free.
X86Info GetX86Info(void);

typedef enum {
  X86_UNKNOWN,
  INTEL_CORE,      // CORE
  INTEL_PNR,       // PENRYN
  INTEL_NHM,       // NEHALEM
  INTEL_ATOM_BNL,  // BONNELL
  INTEL_WSM,       // WESTMERE
  INTEL_SNB,       // SANDYBRIDGE
  INTEL_IVB,       // IVYBRIDGE
  INTEL_ATOM_SMT,  // SILVERMONT
  INTEL_HSW,       // HASWELL
  INTEL_BDW,       // BROADWELL
  INTEL_SKL,       // SKYLAKE
  INTEL_ATOM_GMT,  // GOLDMONT
  INTEL_KBL,       // KABY LAKE
  INTEL_CFL,       // COFFEE LAKE
  INTEL_CNL,       // CANNON LAKE
  AMD_HAMMER,      // K8
  AMD_K10,         // K10
  AMD_BOBCAT,      // K14
  AMD_BULLDOZER,   // K15
  AMD_JAGUAR,      // K16
  AMD_ZEN,         // K17
} X86Microarchitecture;

// Returns the underlying microarchitecture by looking at X86Info's vendor,
// family and model.
X86Microarchitecture GetX86Microarchitecture(const X86Info* info);

// Calls cpuid and fills the brand_string.
// - brand_string *must* be of size 49 (beware of array decaying).
// - brand_string will be zero terminated.
// - This function calls memcpy.
void FillX86BrandString(char brand_string[49]);

////////////////////////////////////////////////////////////////////////////////
// Introspection functions

typedef enum {
  X86_AES,
  X86_ERMS,
  X86_F16C,
  X86_FMA3,
  X86_VPCLMULQDQ,
  X86_BMI1,
  X86_BMI2,
  X86_SSSE3,
  X86_SSE4_1,
  X86_SSE4_2,
  X86_AVX,
  X86_AVX2,
  X86_AVX512F,
  X86_AVX512CD,
  X86_AVX512ER,
  X86_AVX512PF,
  X86_AVX512BW,
  X86_AVX512DQ,
  X86_AVX512VL,
  X86_AVX512IFMA,
  X86_AVX512VBMI,
  X86_AVX512VBMI2,
  X86_AVX512VNNI,
  X86_AVX512BITALG,
  X86_AVX512VPOPCNTDQ,
  X86_AVX512_4VNNIW,
  X86_AVX512_4VBMI2,
  X86_SMX,
  X86_SGX,
  X86_CX16,
  X86_SHA,
  X86_POPCNT,
  X86_MOVBE,
  X86_RDRND,
  X86_LAST_,
} X86FeaturesEnum;

int GetX86FeaturesEnumValue(const X86Features* features, X86FeaturesEnum value);

const char* GetX86FeaturesEnumName(X86FeaturesEnum);

const char* GetX86MicroarchitectureName(X86Microarchitecture);

CPU_FEATURES_END_CPP_NAMESPACE

#if !defined(CPU_FEATURES_ARCH_X86)
#error "Including cpuinfo_x86.h from a non-x86 target."
#endif

#endif  // CPU_FEATURES_INCLUDE_CPUINFO_X86_H_
