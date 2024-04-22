#include <gtest/gtest.h>

#include "arrow/acero/query_context.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

namespace arrow {
namespace acero {

TEST(TestTempStack, GetTempStackSizeFromEnvVar) {
  // Uncleared env var may have side-effect to subsequent tests. Use a structure to help
  // clearing the env var when leaving the scope.
  struct ScopedEnvVar {
    ScopedEnvVar(const char* name, const char* value) : name_(std::move(name)) {
      ARROW_CHECK_OK(::arrow::internal::SetEnvVar(name_, value));
    }
    ~ScopedEnvVar() { ARROW_CHECK_OK(::arrow::internal::DelEnvVar(name_)); }

   private:
    const char* name_;
  };

  // Not set.
  ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);

  // Empty.
  {
    ScopedEnvVar env(internal::kTempStackSizeEnvVar, "");
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);
  }

  // Non-number.
  {
    ScopedEnvVar env(internal::kTempStackSizeEnvVar, "invalid");
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);
  }

  // Number with invalid suffix.
  {
    ScopedEnvVar env(internal::kTempStackSizeEnvVar, "42MB");
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);
  }

  // Valid positive number.
  {
    ScopedEnvVar env(internal::kTempStackSizeEnvVar, "42");
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), 42);
  }

  // Int64 max.
  {
    auto str = std::to_string(std::numeric_limits<int64_t>::max());
    ScopedEnvVar env(internal::kTempStackSizeEnvVar, str.c_str());
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(),
              std::numeric_limits<int64_t>::max());
  }

  // Zero.
  {
    ScopedEnvVar env(internal::kTempStackSizeEnvVar, "0");
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);
  }

  // Negative number.
  {
    ScopedEnvVar env(internal::kTempStackSizeEnvVar, "-1");
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);
  }

  // Over int64 max.
  {
    auto str = std::to_string(std::numeric_limits<int64_t>::max()) + "0";
    ScopedEnvVar env(internal::kTempStackSizeEnvVar, str.c_str());
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kDefaultTempStackSize);
  }
}

}  // namespace acero
}  // namespace arrow