#include <gtest/gtest.h>
#include "arrow/util/cpu_info.h"

#ifdef __linux__
#include <sched.h>
#endif

TEST(CpuInfoTest, CpuAffinity) {
#ifdef __linux__
  auto cpu_info = arrow::internal::CpuInfo::GetInstance();
  int affinity_cores = cpu_info->num_cores();

  cpu_set_t mask;
  ASSERT_EQ(sched_getaffinity(0, sizeof(mask), &mask), 0);
  int expected = CPU_COUNT(&mask);

  ASSERT_EQ(affinity_cores, expected);
#else
  GTEST_SKIP() << "CpuInfo affinity check only applies on Linux.";
#endif
}