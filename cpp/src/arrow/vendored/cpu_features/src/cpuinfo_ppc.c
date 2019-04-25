// Copyright 2018 IBM.
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

#include <stdbool.h>
#include <string.h>

#include "cpuinfo_ppc.h"
#include "internal/bit_utils.h"
#include "internal/filesystem.h"
#include "internal/stack_line_reader.h"
#include "internal/string_view.h"
#include "internal/unix_features_aggregator.h"

DECLARE_SETTER(PPCFeatures, ppc32)
DECLARE_SETTER(PPCFeatures, ppc64)
DECLARE_SETTER(PPCFeatures, ppc601)
DECLARE_SETTER(PPCFeatures, altivec)
DECLARE_SETTER(PPCFeatures, fpu)
DECLARE_SETTER(PPCFeatures, mmu)
DECLARE_SETTER(PPCFeatures, mac_4xx)
DECLARE_SETTER(PPCFeatures, unifiedcache)
DECLARE_SETTER(PPCFeatures, spe)
DECLARE_SETTER(PPCFeatures, efpsingle)
DECLARE_SETTER(PPCFeatures, efpdouble)
DECLARE_SETTER(PPCFeatures, no_tb)
DECLARE_SETTER(PPCFeatures, power4)
DECLARE_SETTER(PPCFeatures, power5)
DECLARE_SETTER(PPCFeatures, power5plus)
DECLARE_SETTER(PPCFeatures, cell)
DECLARE_SETTER(PPCFeatures, booke)
DECLARE_SETTER(PPCFeatures, smt)
DECLARE_SETTER(PPCFeatures, icachesnoop)
DECLARE_SETTER(PPCFeatures, arch205)
DECLARE_SETTER(PPCFeatures, pa6t)
DECLARE_SETTER(PPCFeatures, dfp)
DECLARE_SETTER(PPCFeatures, power6ext)
DECLARE_SETTER(PPCFeatures, arch206)
DECLARE_SETTER(PPCFeatures, vsx)
DECLARE_SETTER(PPCFeatures, pseries_perfmon_compat)
DECLARE_SETTER(PPCFeatures, truele)
DECLARE_SETTER(PPCFeatures, ppcle)
DECLARE_SETTER(PPCFeatures, arch207)
DECLARE_SETTER(PPCFeatures, htm)
DECLARE_SETTER(PPCFeatures, dscr)
DECLARE_SETTER(PPCFeatures, ebb)
DECLARE_SETTER(PPCFeatures, isel)
DECLARE_SETTER(PPCFeatures, tar)
DECLARE_SETTER(PPCFeatures, vcrypto)
DECLARE_SETTER(PPCFeatures, htm_nosc)
DECLARE_SETTER(PPCFeatures, arch300)
DECLARE_SETTER(PPCFeatures, ieee128)
DECLARE_SETTER(PPCFeatures, darn)
DECLARE_SETTER(PPCFeatures, scv)
DECLARE_SETTER(PPCFeatures, htm_no_suspend)

static const CapabilityConfig kConfigs[] = {
    {{PPC_FEATURE_32, 0}, "ppc32", &set_ppc32},
    {{PPC_FEATURE_64, 0}, "ppc64", &set_ppc64},
    {{PPC_FEATURE_601_INSTR, 0}, "ppc601", &set_ppc601},
    {{PPC_FEATURE_HAS_ALTIVEC, 0}, "altivec", &set_altivec},
    {{PPC_FEATURE_HAS_FPU, 0}, "fpu", &set_fpu},
    {{PPC_FEATURE_HAS_MMU, 0}, "mmu", &set_mmu},
    {{PPC_FEATURE_HAS_4xxMAC, 0}, "4xxmac", &set_mac_4xx},
    {{PPC_FEATURE_UNIFIED_CACHE, 0}, "ucache", &set_unifiedcache},
    {{PPC_FEATURE_HAS_SPE, 0}, "spe", &set_spe},
    {{PPC_FEATURE_HAS_EFP_SINGLE, 0}, "efpsingle", &set_efpsingle},
    {{PPC_FEATURE_HAS_EFP_DOUBLE, 0}, "efpdouble", &set_efpdouble},
    {{PPC_FEATURE_NO_TB, 0}, "notb", &set_no_tb},
    {{PPC_FEATURE_POWER4, 0}, "power4", &set_power4},
    {{PPC_FEATURE_POWER5, 0}, "power5", &set_power5},
    {{PPC_FEATURE_POWER5_PLUS, 0}, "power5+", &set_power5plus},
    {{PPC_FEATURE_CELL, 0}, "cellbe", &set_cell},
    {{PPC_FEATURE_BOOKE, 0}, "booke", &set_booke},
    {{PPC_FEATURE_SMT, 0}, "smt", &set_smt},
    {{PPC_FEATURE_ICACHE_SNOOP, 0}, "ic_snoop", &set_icachesnoop},
    {{PPC_FEATURE_ARCH_2_05, 0}, "arch_2_05", &set_arch205},
    {{PPC_FEATURE_PA6T, 0}, "pa6t", &set_pa6t},
    {{PPC_FEATURE_HAS_DFP, 0}, "dfp", &set_dfp},
    {{PPC_FEATURE_POWER6_EXT, 0}, "power6x", &set_power6ext},
    {{PPC_FEATURE_ARCH_2_06, 0}, "arch_2_06", &set_arch206},
    {{PPC_FEATURE_HAS_VSX, 0}, "vsx", &set_vsx},
    {{PPC_FEATURE_PSERIES_PERFMON_COMPAT, 0},
     "archpmu",
     &set_pseries_perfmon_compat},
    {{PPC_FEATURE_TRUE_LE, 0}, "true_le", &set_truele},
    {{PPC_FEATURE_PPC_LE, 0}, "ppcle", &set_ppcle},
    {{0, PPC_FEATURE2_ARCH_2_07}, "arch_2_07", &set_arch207},
    {{0, PPC_FEATURE2_HTM}, "htm", &set_htm},
    {{0, PPC_FEATURE2_DSCR}, "dscr", &set_dscr},
    {{0, PPC_FEATURE2_EBB}, "ebb", &set_ebb},
    {{0, PPC_FEATURE2_ISEL}, "isel", &set_isel},
    {{0, PPC_FEATURE2_TAR}, "tar", &set_tar},
    {{0, PPC_FEATURE2_VEC_CRYPTO}, "vcrypto", &set_vcrypto},
    {{0, PPC_FEATURE2_HTM_NOSC}, "htm-nosc", &set_htm_nosc},
    {{0, PPC_FEATURE2_ARCH_3_00}, "arch_3_00", &set_arch300},
    {{0, PPC_FEATURE2_HAS_IEEE128}, "ieee128", &set_ieee128},
    {{0, PPC_FEATURE2_DARN}, "darn", &set_darn},
    {{0, PPC_FEATURE2_SCV}, "scv", &set_scv},
    {{0, PPC_FEATURE2_HTM_NO_SUSPEND}, "htm-no-suspend", &set_htm_no_suspend},
};
static const size_t kConfigsSize = sizeof(kConfigs) / sizeof(CapabilityConfig);

static bool HandlePPCLine(const LineResult result,
                          PPCPlatformStrings* const strings) {
  StringView line = result.line;
  StringView key, value;
  if (CpuFeatures_StringView_GetAttributeKeyValue(line, &key, &value)) {
    if (CpuFeatures_StringView_HasWord(key, "platform")) {
      CpuFeatures_StringView_CopyString(value, strings->platform,
                                        sizeof(strings->platform));
    } else if (CpuFeatures_StringView_IsEquals(key, str("model"))) {
      CpuFeatures_StringView_CopyString(value, strings->model,
                                        sizeof(strings->platform));
    } else if (CpuFeatures_StringView_IsEquals(key, str("machine"))) {
      CpuFeatures_StringView_CopyString(value, strings->machine,
                                        sizeof(strings->platform));
    } else if (CpuFeatures_StringView_IsEquals(key, str("cpu"))) {
      CpuFeatures_StringView_CopyString(value, strings->cpu,
                                        sizeof(strings->platform));
    }
  }
  return !result.eof;
}

static void FillProcCpuInfoData(PPCPlatformStrings* const strings) {
  const int fd = CpuFeatures_OpenFile("/proc/cpuinfo");
  if (fd >= 0) {
    StackLineReader reader;
    StackLineReader_Initialize(&reader, fd);
    for (;;) {
      if (!HandlePPCLine(StackLineReader_NextLine(&reader), strings)) {
        break;
      }
    }
    CpuFeatures_CloseFile(fd);
  }
}

static const PPCInfo kEmptyPPCInfo;

PPCInfo GetPPCInfo(void) {
  /*
   * On Power feature flags aren't currently in cpuinfo so we only look at
   * the auxilary vector.
   */
  PPCInfo info = kEmptyPPCInfo;

  CpuFeatures_OverrideFromHwCaps(kConfigsSize, kConfigs,
                                 CpuFeatures_GetHardwareCapabilities(),
                                 &info.features);
  return info;
}

static const PPCPlatformStrings kEmptyPPCPlatformStrings;

PPCPlatformStrings GetPPCPlatformStrings(void) {
  PPCPlatformStrings strings = kEmptyPPCPlatformStrings;

  FillProcCpuInfoData(&strings);
  strings.type = CpuFeatures_GetPlatformType();
  return strings;
}

////////////////////////////////////////////////////////////////////////////////
// Introspection functions

int GetPPCFeaturesEnumValue(const PPCFeatures* features,
                            PPCFeaturesEnum value) {
  switch (value) {
    case PPC_32:
      return features->ppc32;
    case PPC_64:
      return features->ppc64;
    case PPC_601_INSTR:
      return features->ppc601;
    case PPC_HAS_ALTIVEC:
      return features->altivec;
    case PPC_HAS_FPU:
      return features->fpu;
    case PPC_HAS_MMU:
      return features->mmu;
    case PPC_HAS_4xxMAC:
      return features->mac_4xx;
    case PPC_UNIFIED_CACHE:
      return features->unifiedcache;
    case PPC_HAS_SPE:
      return features->spe;
    case PPC_HAS_EFP_SINGLE:
      return features->efpsingle;
    case PPC_HAS_EFP_DOUBLE:
      return features->efpdouble;
    case PPC_NO_TB:
      return features->no_tb;
    case PPC_POWER4:
      return features->power4;
    case PPC_POWER5:
      return features->power5;
    case PPC_POWER5_PLUS:
      return features->power5plus;
    case PPC_CELL:
      return features->cell;
    case PPC_BOOKE:
      return features->booke;
    case PPC_SMT:
      return features->smt;
    case PPC_ICACHE_SNOOP:
      return features->icachesnoop;
    case PPC_ARCH_2_05:
      return features->arch205;
    case PPC_PA6T:
      return features->pa6t;
    case PPC_HAS_DFP:
      return features->dfp;
    case PPC_POWER6_EXT:
      return features->power6ext;
    case PPC_ARCH_2_06:
      return features->arch206;
    case PPC_HAS_VSX:
      return features->vsx;
    case PPC_PSERIES_PERFMON_COMPAT:
      return features->pseries_perfmon_compat;
    case PPC_TRUE_LE:
      return features->truele;
    case PPC_PPC_LE:
      return features->ppcle;
    case PPC_ARCH_2_07:
      return features->arch207;
    case PPC_HTM:
      return features->htm;
    case PPC_DSCR:
      return features->dscr;
    case PPC_EBB:
      return features->ebb;
    case PPC_ISEL:
      return features->isel;
    case PPC_TAR:
      return features->tar;
    case PPC_VEC_CRYPTO:
      return features->vcrypto;
    case PPC_HTM_NOSC:
      return features->htm_nosc;
    case PPC_ARCH_3_00:
      return features->arch300;
    case PPC_HAS_IEEE128:
      return features->ieee128;
    case PPC_DARN:
      return features->darn;
    case PPC_SCV:
      return features->scv;
    case PPC_HTM_NO_SUSPEND:
      return features->htm_no_suspend;
    case PPC_LAST_:
      break;
  }
  return false;
}

/* Have used the same names as glibc  */
const char* GetPPCFeaturesEnumName(PPCFeaturesEnum value) {
  switch (value) {
    case PPC_32:
      return "ppc32";
    case PPC_64:
      return "ppc64";
    case PPC_601_INSTR:
      return "ppc601";
    case PPC_HAS_ALTIVEC:
      return "altivec";
    case PPC_HAS_FPU:
      return "fpu";
    case PPC_HAS_MMU:
      return "mmu";
    case PPC_HAS_4xxMAC:
      return "4xxmac";
    case PPC_UNIFIED_CACHE:
      return "ucache";
    case PPC_HAS_SPE:
      return "spe";
    case PPC_HAS_EFP_SINGLE:
      return "efpsingle";
    case PPC_HAS_EFP_DOUBLE:
      return "efpdouble";
    case PPC_NO_TB:
      return "notb";
    case PPC_POWER4:
      return "power4";
    case PPC_POWER5:
      return "power5";
    case PPC_POWER5_PLUS:
      return "power5+";
    case PPC_CELL:
      return "cellbe";
    case PPC_BOOKE:
      return "booke";
    case PPC_SMT:
      return "smt";
    case PPC_ICACHE_SNOOP:
      return "ic_snoop";
    case PPC_ARCH_2_05:
      return "arch_2_05";
    case PPC_PA6T:
      return "pa6t";
    case PPC_HAS_DFP:
      return "dfp";
    case PPC_POWER6_EXT:
      return "power6x";
    case PPC_ARCH_2_06:
      return "arch_2_06";
    case PPC_HAS_VSX:
      return "vsx";
    case PPC_PSERIES_PERFMON_COMPAT:
      return "archpmu";
    case PPC_TRUE_LE:
      return "true_le";
    case PPC_PPC_LE:
      return "ppcle";
    case PPC_ARCH_2_07:
      return "arch_2_07";
    case PPC_HTM:
      return "htm";
    case PPC_DSCR:
      return "dscr";
    case PPC_EBB:
      return "ebb";
    case PPC_ISEL:
      return "isel";
    case PPC_TAR:
      return "tar";
    case PPC_VEC_CRYPTO:
      return "vcrypto";
    case PPC_HTM_NOSC:
      return "htm-nosc";
    case PPC_ARCH_3_00:
      return "arch_3_00";
    case PPC_HAS_IEEE128:
      return "ieee128";
    case PPC_DARN:
      return "darn";
    case PPC_SCV:
      return "scv";
    case PPC_HTM_NO_SUSPEND:
      return "htm-no-suspend";
    case PPC_LAST_:
      break;
  }
  return "unknown_feature";
}
