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

#include "gandiva/cache.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

#include <gtest/gtest.h>

namespace gandiva {

class TestCacheKey {
 public:
  explicit TestCacheKey(int value) : value_(value) {}
  std::size_t Hash() const { return value_; }
  bool operator==(const TestCacheKey& other) const { return value_ == other.value_; }

 private:
  int value_;
};

TEST(TestCache, TestGetPut) {
  Cache<TestCacheKey, std::string> cache(2);
  cache.PutObjectCode(TestCacheKey(1), "hello");
  cache.PutObjectCode(TestCacheKey(2), "world");
  ASSERT_EQ(cache.GetObjectCode(TestCacheKey(1)), "hello");
  ASSERT_EQ(cache.GetObjectCode(TestCacheKey(2)), "world");
}

namespace {
constexpr auto capacity_env_var = "GANDIVA_CACHE_SIZE";
constexpr auto default_capacity = 5000;
}  // namespace

TEST(TestCache, TestGetCacheCapacityDefault) {
  ASSERT_EQ(GetCapacity(), default_capacity);
}

TEST(TestCache, TestGetCacheCapacityEnvVar) {
  // Empty.
  ASSERT_OK(::arrow::internal::SetEnvVar(capacity_env_var, ""));
  ASSERT_EQ(internal::GetCapacityFromEnvVar(), default_capacity);

  // Non-number.
  ASSERT_OK(::arrow::internal::SetEnvVar(capacity_env_var, "invalid"));
  ASSERT_EQ(internal::GetCapacityFromEnvVar(), default_capacity);

  // Valid positive number.
  ASSERT_OK(::arrow::internal::SetEnvVar(capacity_env_var, "42"));
  ASSERT_EQ(internal::GetCapacityFromEnvVar(), 42);

  // Int max.
  {
    auto str = std::to_string(std::numeric_limits<int>::max());
    ASSERT_OK(::arrow::internal::SetEnvVar(capacity_env_var, str));
    ASSERT_EQ(internal::GetCapacityFromEnvVar(), std::numeric_limits<int>::max());
  }

  // Over int max.
  {
    auto str = std::to_string(static_cast<int64_t>(std::numeric_limits<int>::max()) + 1);
    ASSERT_OK(::arrow::internal::SetEnvVar(capacity_env_var, str));
    ASSERT_EQ(internal::GetCapacityFromEnvVar(), default_capacity);
  }

  // Zero.
  ASSERT_OK(::arrow::internal::SetEnvVar(capacity_env_var, "0"));
  ASSERT_EQ(internal::GetCapacityFromEnvVar(), default_capacity);

  // Negative number.
  ASSERT_OK(::arrow::internal::SetEnvVar(capacity_env_var, "-1"));
  ASSERT_EQ(internal::GetCapacityFromEnvVar(), default_capacity);
}

}  // namespace gandiva
