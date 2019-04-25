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

#include "cpuinfo_mips.h"
#include "filesystem_for_testing.h"
#include "hwcaps_for_testing.h"
#include "internal/stack_line_reader.h"
#include "internal/string_view.h"

#include "gtest/gtest.h"

namespace cpu_features {

namespace {

void DisableHardwareCapabilities() { SetHardwareCapabilities(0, 0); }

TEST(CpuinfoMipsTest, FromHardwareCapBoth) {
  SetHardwareCapabilities(MIPS_HWCAP_MSA | MIPS_HWCAP_R6, 0);
  GetEmptyFilesystem();  // disabling /proc/cpuinfo
  const auto info = GetMipsInfo();
  EXPECT_TRUE(info.features.msa);
  EXPECT_FALSE(info.features.eva);
  EXPECT_TRUE(info.features.r6);
}

TEST(CpuinfoMipsTest, FromHardwareCapOnlyOne) {
  SetHardwareCapabilities(MIPS_HWCAP_MSA, 0);
  GetEmptyFilesystem();  // disabling /proc/cpuinfo
  const auto info = GetMipsInfo();
  EXPECT_TRUE(info.features.msa);
  EXPECT_FALSE(info.features.eva);
}

TEST(CpuinfoMipsTest, Ci40) {
  DisableHardwareCapabilities();
  auto& fs = GetEmptyFilesystem();
  fs.CreateFile("/proc/cpuinfo", R"(system type : IMG Pistachio SoC (B0)
machine : IMG Marduk – Ci40 with cc2520
processor : 0
cpu model : MIPS interAptiv (multi) V2.0 FPU V0.0
BogoMIPS : 363.72
wait instruction : yes
microsecond timers : yes
tlb_entries : 64
extra interrupt vector : yes
hardware watchpoint : yes, count: 4, address/irw mask: [0x0ffc, 0x0ffc, 0x0ffb, 0x0ffb]
isa : mips1 mips2 mips32r1 mips32r2
ASEs implemented : mips16 dsp mt eva
shadow register sets : 1
kscratch registers : 0
package : 0
core : 0
VCED exceptions : not available
VCEI exceptions : not available
VPE : 0
)");
  const auto info = GetMipsInfo();
  EXPECT_FALSE(info.features.msa);
  EXPECT_TRUE(info.features.eva);
}

TEST(CpuinfoMipsTest, AR7161) {
  DisableHardwareCapabilities();
  auto& fs = GetEmptyFilesystem();
  fs.CreateFile("/proc/cpuinfo",
                R"(system type             : Atheros AR7161 rev 2
machine                 : NETGEAR WNDR3700/WNDR3800/WNDRMAC
processor               : 0
cpu model               : MIPS 24Kc V7.4
BogoMIPS                : 452.19
wait instruction        : yes
microsecond timers      : yes
tlb_entries             : 16
extra interrupt vector  : yes
hardware watchpoint     : yes, count: 4, address/irw mask: [0x0000, 0x0f98, 0x0f78, 0x0df8]
ASEs implemented        : mips16
shadow register sets    : 1
kscratch registers      : 0
core                    : 0
VCED exceptions         : not available
VCEI exceptions         : not available
)");
  const auto info = GetMipsInfo();
  EXPECT_FALSE(info.features.msa);
  EXPECT_FALSE(info.features.eva);
}

TEST(CpuinfoMipsTest, Goldfish) {
  DisableHardwareCapabilities();
  auto& fs = GetEmptyFilesystem();
  fs.CreateFile("/proc/cpuinfo", R"(system type		: MIPS-Goldfish
Hardware		: goldfish
Revison		: 1
processor		: 0
cpu model		: MIPS 24Kc V0.0  FPU V0.0
BogoMIPS		: 1042.02
wait instruction	: yes
microsecond timers	: yes
tlb_entries		: 16
extra interrupt vector	: yes
hardware watchpoint	: yes, count: 1, address/irw mask: [0x0ff8]
ASEs implemented	:
shadow register sets	: 1
core			: 0
VCED exceptions		: not available
VCEI exceptions		: not available
)");
  const auto info = GetMipsInfo();
  EXPECT_FALSE(info.features.msa);
  EXPECT_FALSE(info.features.eva);
}

}  // namespace
}  // namespace cpu_features
