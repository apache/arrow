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

// Interface to retrieve hardware capabilities. It relies on Linux's getauxval
// or `/proc/self/auxval` under the hood.
#ifndef CPU_FEATURES_INCLUDE_INTERNAL_HWCAPS_H_
#define CPU_FEATURES_INCLUDE_INTERNAL_HWCAPS_H_

#include <stdint.h>
#include "cpu_features_macros.h"

CPU_FEATURES_START_CPP_NAMESPACE

// To avoid depending on the linux kernel we reproduce the architecture specific
// constants here.

// http://elixir.free-electrons.com/linux/latest/source/arch/arm64/include/uapi/asm/hwcap.h
#define AARCH64_HWCAP_FP (1UL << 0)
#define AARCH64_HWCAP_ASIMD (1UL << 1)
#define AARCH64_HWCAP_AES (1UL << 3)
#define AARCH64_HWCAP_PMULL (1UL << 4)
#define AARCH64_HWCAP_SHA1 (1UL << 5)
#define AARCH64_HWCAP_SHA2 (1UL << 6)
#define AARCH64_HWCAP_CRC32 (1UL << 7)

// http://elixir.free-electrons.com/linux/latest/source/arch/arm/include/uapi/asm/hwcap.h
#define ARM_HWCAP_VFP (1UL << 6)
#define ARM_HWCAP_IWMMXT (1UL << 9)
#define ARM_HWCAP_NEON (1UL << 12)
#define ARM_HWCAP_VFPV3 (1UL << 13)
#define ARM_HWCAP_VFPV3D16 (1UL << 14)
#define ARM_HWCAP_VFPV4 (1UL << 16)
#define ARM_HWCAP_IDIVA (1UL << 17)
#define ARM_HWCAP_IDIVT (1UL << 18)
#define ARM_HWCAP2_AES (1UL << 0)
#define ARM_HWCAP2_PMULL (1UL << 1)
#define ARM_HWCAP2_SHA1 (1UL << 2)
#define ARM_HWCAP2_SHA2 (1UL << 3)
#define ARM_HWCAP2_CRC32 (1UL << 4)

// http://elixir.free-electrons.com/linux/latest/source/arch/mips/include/uapi/asm/hwcap.h
#define MIPS_HWCAP_R6 (1UL << 0)
#define MIPS_HWCAP_MSA (1UL << 1)
#define MIPS_HWCAP_CRC32 (1UL << 2)

// http://elixir.free-electrons.com/linux/latest/source/arch/powerpc/include/uapi/asm/cputable.h
#ifndef _UAPI__ASM_POWERPC_CPUTABLE_H
/* in AT_HWCAP */
#define PPC_FEATURE_32 0x80000000
#define PPC_FEATURE_64 0x40000000
#define PPC_FEATURE_601_INSTR 0x20000000
#define PPC_FEATURE_HAS_ALTIVEC 0x10000000
#define PPC_FEATURE_HAS_FPU 0x08000000
#define PPC_FEATURE_HAS_MMU 0x04000000
#define PPC_FEATURE_HAS_4xxMAC 0x02000000
#define PPC_FEATURE_UNIFIED_CACHE 0x01000000
#define PPC_FEATURE_HAS_SPE 0x00800000
#define PPC_FEATURE_HAS_EFP_SINGLE 0x00400000
#define PPC_FEATURE_HAS_EFP_DOUBLE 0x00200000
#define PPC_FEATURE_NO_TB 0x00100000
#define PPC_FEATURE_POWER4 0x00080000
#define PPC_FEATURE_POWER5 0x00040000
#define PPC_FEATURE_POWER5_PLUS 0x00020000
#define PPC_FEATURE_CELL 0x00010000
#define PPC_FEATURE_BOOKE 0x00008000
#define PPC_FEATURE_SMT 0x00004000
#define PPC_FEATURE_ICACHE_SNOOP 0x00002000
#define PPC_FEATURE_ARCH_2_05 0x00001000
#define PPC_FEATURE_PA6T 0x00000800
#define PPC_FEATURE_HAS_DFP 0x00000400
#define PPC_FEATURE_POWER6_EXT 0x00000200
#define PPC_FEATURE_ARCH_2_06 0x00000100
#define PPC_FEATURE_HAS_VSX 0x00000080

#define PPC_FEATURE_PSERIES_PERFMON_COMPAT 0x00000040

/* Reserved - do not use                0x00000004 */
#define PPC_FEATURE_TRUE_LE 0x00000002
#define PPC_FEATURE_PPC_LE 0x00000001

/* in AT_HWCAP2 */
#define PPC_FEATURE2_ARCH_2_07 0x80000000
#define PPC_FEATURE2_HTM 0x40000000
#define PPC_FEATURE2_DSCR 0x20000000
#define PPC_FEATURE2_EBB 0x10000000
#define PPC_FEATURE2_ISEL 0x08000000
#define PPC_FEATURE2_TAR 0x04000000
#define PPC_FEATURE2_VEC_CRYPTO 0x02000000
#define PPC_FEATURE2_HTM_NOSC 0x01000000
#define PPC_FEATURE2_ARCH_3_00 0x00800000
#define PPC_FEATURE2_HAS_IEEE128 0x00400000
#define PPC_FEATURE2_DARN 0x00200000
#define PPC_FEATURE2_SCV 0x00100000
#define PPC_FEATURE2_HTM_NO_SUSPEND 0x00080000
#endif

typedef struct {
  unsigned long hwcaps;
  unsigned long hwcaps2;
} HardwareCapabilities;

HardwareCapabilities CpuFeatures_GetHardwareCapabilities(void);

typedef struct {
  char platform[64];       // 0 terminated string
  char base_platform[64];  // 0 terminated string
} PlatformType;

PlatformType CpuFeatures_GetPlatformType(void);

CPU_FEATURES_END_CPP_NAMESPACE

#endif  // CPU_FEATURES_INCLUDE_INTERNAL_HWCAPS_H_
