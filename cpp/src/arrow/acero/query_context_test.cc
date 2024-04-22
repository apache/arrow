#include <gtest/gtest.h>

#include "arrow/acero/query_context.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

namespace arrow {
namespace acero {

TEST(TestTempStack, TestGetTempStackSizeFromEnvVar) {
  // Not set.
  ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);

  // Empty.
  ASSERT_OK(::arrow::internal::SetEnvVar(internal::kTempStackSizeEnvVar, ""));
  ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);

  // Non-number.
  ASSERT_OK(::arrow::internal::SetEnvVar(internal::kTempStackSizeEnvVar, "invalid"));
  ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);

  // Valid positive number.
  ASSERT_OK(::arrow::internal::SetEnvVar(internal::kTempStackSizeEnvVar, "42"));
  ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), 42);

  // Int64 max.
  {
    auto str = std::to_string(std::numeric_limits<int64_t>::max());
    ASSERT_OK(::arrow::internal::SetEnvVar(internal::kTempStackSizeEnvVar, str));
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(),
              std::numeric_limits<int64_t>::max());
  }

  // Over int64 max.
  {
    auto str = std::to_string(std::numeric_limits<int64_t>::max()) + "0";
    ASSERT_OK(::arrow::internal::SetEnvVar(internal::kTempStackSizeEnvVar, str));
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);
  }

  // Zero.
  ASSERT_OK(::arrow::internal::SetEnvVar(internal::kTempStackSizeEnvVar, "0"));
  ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);

  // Negative number.
  ASSERT_OK(::arrow::internal::SetEnvVar(internal::kTempStackSizeEnvVar, "-1"));
  ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);
}

}  // namespace acero
}  // namespace arrow