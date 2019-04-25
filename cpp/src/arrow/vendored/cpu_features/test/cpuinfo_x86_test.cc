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

#include <cassert>
#include <cstdio>
#include <map>

#include "gtest/gtest.h"

#include "cpuinfo_x86.h"
#include "internal/cpuid_x86.h"

namespace cpu_features {

class FakeCpu {
 public:
  Leaf CpuId(uint32_t leaf_id) const {
    const auto itr = cpuid_leaves_.find(leaf_id);
    EXPECT_TRUE(itr != cpuid_leaves_.end()) << "Missing leaf " << leaf_id;
    return itr->second;
  }

  uint32_t GetXCR0Eax() const { return xcr0_eax_; }

  void SetLeaves(std::map<uint32_t, Leaf> configuration) {
    cpuid_leaves_ = std::move(configuration);
  }

  void SetOsBackupsExtendedRegisters(bool os_backups_extended_registers) {
    xcr0_eax_ = os_backups_extended_registers ? -1 : 0;
  }

 private:
  std::map<uint32_t, Leaf> cpuid_leaves_;
  uint32_t xcr0_eax_;
};

auto* g_fake_cpu = new FakeCpu();

extern "C" Leaf CpuId(uint32_t leaf_id) { return g_fake_cpu->CpuId(leaf_id); }
extern "C" uint32_t GetXCR0Eax(void) { return g_fake_cpu->GetXCR0Eax(); }

namespace {

TEST(CpuidX86Test, SandyBridge) {
  g_fake_cpu->SetOsBackupsExtendedRegisters(true);
  g_fake_cpu->SetLeaves({
      {0x00000000, Leaf{0x0000000D, 0x756E6547, 0x6C65746E, 0x49656E69}},
      {0x00000001, Leaf{0x000206A6, 0x00100800, 0x1F9AE3BF, 0xBFEBFBFF}},
      {0x00000007, Leaf{0x00000000, 0x00000000, 0x00000000, 0x00000000}},
  });
  const auto info = GetX86Info();
  EXPECT_STREQ(info.vendor, "GenuineIntel");
  EXPECT_EQ(info.family, 0x06);
  EXPECT_EQ(info.model, 0x02A);
  EXPECT_EQ(info.stepping, 0x06);
  // Leaf 7 is zeroed out so none of the Leaf 7 flags are set.
  const auto features = info.features;
  EXPECT_FALSE(features.erms);
  EXPECT_FALSE(features.avx2);
  EXPECT_FALSE(features.avx512f);
  EXPECT_FALSE(features.avx512cd);
  EXPECT_FALSE(features.avx512er);
  EXPECT_FALSE(features.avx512pf);
  EXPECT_FALSE(features.avx512bw);
  EXPECT_FALSE(features.avx512dq);
  EXPECT_FALSE(features.avx512vl);
  EXPECT_FALSE(features.avx512ifma);
  EXPECT_FALSE(features.avx512vbmi);
  EXPECT_FALSE(features.avx512vbmi2);
  EXPECT_FALSE(features.avx512vnni);
  EXPECT_FALSE(features.avx512bitalg);
  EXPECT_FALSE(features.avx512vpopcntdq);
  EXPECT_FALSE(features.avx512_4vnniw);
  EXPECT_FALSE(features.avx512_4vbmi2);
  // All old cpu features should be set.
  EXPECT_TRUE(features.aes);
  EXPECT_TRUE(features.ssse3);
  EXPECT_TRUE(features.sse4_1);
  EXPECT_TRUE(features.sse4_2);
  EXPECT_TRUE(features.avx);
  EXPECT_FALSE(features.sha);
  EXPECT_TRUE(features.popcnt);
  EXPECT_FALSE(features.movbe);
  EXPECT_FALSE(features.rdrnd);
}

TEST(CpuidX86Test, SandyBridgeTestOsSupport) {
  g_fake_cpu->SetLeaves({
      {0x00000000, Leaf{0x0000000D, 0x756E6547, 0x6C65746E, 0x49656E69}},
      {0x00000001, Leaf{0x000206A6, 0x00100800, 0x1F9AE3BF, 0xBFEBFBFF}},
      {0x00000007, Leaf{0x00000000, 0x00000000, 0x00000000, 0x00000000}},
  });
  // avx is disabled if os does not support backing up ymm registers.
  g_fake_cpu->SetOsBackupsExtendedRegisters(false);
  EXPECT_FALSE(GetX86Info().features.avx);
  // avx is disabled if os does not support backing up ymm registers.
  g_fake_cpu->SetOsBackupsExtendedRegisters(true);
  EXPECT_TRUE(GetX86Info().features.avx);
}

TEST(CpuidX86Test, SkyLake) {
  g_fake_cpu->SetOsBackupsExtendedRegisters(true);
  g_fake_cpu->SetLeaves({
      {0x00000000, Leaf{0x00000016, 0x756E6547, 0x6C65746E, 0x49656E69}},
      {0x00000001, Leaf{0x000406E3, 0x00100800, 0x7FFAFBBF, 0xBFEBFBFF}},
      {0x00000007, Leaf{0x00000000, 0x029C67AF, 0x00000000, 0x00000000}},
  });
  const auto info = GetX86Info();
  EXPECT_STREQ(info.vendor, "GenuineIntel");
  EXPECT_EQ(info.family, 0x06);
  EXPECT_EQ(info.model, 0x04E);
  EXPECT_EQ(info.stepping, 0x03);
  EXPECT_EQ(GetX86Microarchitecture(&info), X86Microarchitecture::INTEL_SKL);
}

TEST(CpuidX86Test, Branding) {
  g_fake_cpu->SetLeaves({
      {0x00000000, Leaf{0x00000016, 0x756E6547, 0x6C65746E, 0x49656E69}},
      {0x00000001, Leaf{0x000406E3, 0x00100800, 0x7FFAFBBF, 0xBFEBFBFF}},
      {0x00000007, Leaf{0x00000000, 0x029C67AF, 0x00000000, 0x00000000}},
      {0x80000000, Leaf{0x80000008, 0x00000000, 0x00000000, 0x00000000}},
      {0x80000001, Leaf{0x00000000, 0x00000000, 0x00000121, 0x2C100000}},
      {0x80000002, Leaf{0x65746E49, 0x2952286C, 0x726F4320, 0x4D542865}},
      {0x80000003, Leaf{0x37692029, 0x3035362D, 0x43205530, 0x40205550}},
      {0x80000004, Leaf{0x352E3220, 0x7A484730, 0x00000000, 0x00000000}},
  });
  char brand_string[49];
  FillX86BrandString(brand_string);
  EXPECT_STREQ(brand_string, "Intel(R) Core(TM) i7-6500U CPU @ 2.50GHz");
}

// http://users.atw.hu/instlatx64/AuthenticAMD0630F81_K15_Godavari_CPUID.txt
TEST(CpuidX86Test, AMD_K15) {
  g_fake_cpu->SetLeaves({
      {0x00000000, Leaf{0x0000000D, 0x68747541, 0x444D4163, 0x69746E65}},
      {0x00000001, Leaf{0x00630F81, 0x00040800, 0x3E98320B, 0x178BFBFF}},
      {0x00000007, Leaf{0x00000000, 0x00000000, 0x00000000, 0x00000000}},
      {0x80000000, Leaf{0x8000001E, 0x68747541, 0x444D4163, 0x69746E65}},
      {0x80000001, Leaf{0x00630F81, 0x10000000, 0x0FEBBFFF, 0x2FD3FBFF}},
      {0x80000002, Leaf{0x20444D41, 0x372D3841, 0x4B303736, 0x64615220}},
      {0x80000003, Leaf{0x206E6F65, 0x202C3752, 0x43203031, 0x75706D6F}},
      {0x80000004, Leaf{0x43206574, 0x7365726F, 0x2B433420, 0x00204736}},
      {0x80000005, Leaf{0xFF40FF18, 0xFF40FF30, 0x10040140, 0x60030140}},
  });
  const auto info = GetX86Info();

  EXPECT_STREQ(info.vendor, "AuthenticAMD");
  EXPECT_EQ(info.family, 0x15);
  EXPECT_EQ(info.model, 0x38);
  EXPECT_EQ(info.stepping, 0x01);
  EXPECT_EQ(GetX86Microarchitecture(&info),
            X86Microarchitecture::AMD_BULLDOZER);

  char brand_string[49];
  FillX86BrandString(brand_string);
  EXPECT_STREQ(brand_string, "AMD A8-7670K Radeon R7, 10 Compute Cores 4C+6G ");
}

// TODO(user): test what happens when xsave/osxsave are not present.
// TODO(user): test what happens when xmm/ymm/zmm os support are not
// present.

}  // namespace
}  // namespace cpu_features
