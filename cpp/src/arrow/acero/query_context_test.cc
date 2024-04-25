// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

  // Smaller than minimal temp stack size.
  {
    ScopedEnvVar env(internal::kTempStackSizeEnvVar, "65535");
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kMinTempStackSize);
  }

  // Between minimal and maximal temp stack size.
  {
    ScopedEnvVar env(internal::kTempStackSizeEnvVar, "65537");
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), 65537);
  }

  // Bigger than maximal temp stack size.
  {
    ScopedEnvVar env(internal::kTempStackSizeEnvVar, "67108865");
    ASSERT_EQ(internal::GetTempStackSizeFromEnvVar(), internal::kMaxTempStackSize);
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
