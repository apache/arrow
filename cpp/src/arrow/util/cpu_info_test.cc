#include "arrow/util/cpu_info.h"

#include <gtest/gtest.h>

TEST(CpuInfoTest, CpuAffinity) {
  auto cpu_info = arrow::internal::CpuInfo::GetInstance();

  ASSERT_LE(cpu_info->num_affinity_cores(), cpu_info->num_cores());
#ifdef __linux__
  ASSERT_GE(cpu_info->num_affinity_cores(), 1);
#else
  ASSERT_EQ(cpu_info->num_affinity_cores(), -1);
#endif
}
