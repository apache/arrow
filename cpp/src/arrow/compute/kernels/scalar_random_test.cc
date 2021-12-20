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

#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace compute {

namespace {

void TestRandomWithOptions(const RandomOptions& random_options) {
  ASSERT_OK_AND_ASSIGN(Datum result, CallFunction("random", {}, &random_options));
  const auto result_array = result.make_array();
  ValidateOutput(*result_array);
  ASSERT_EQ(result_array->length(), random_options.length);
  ASSERT_EQ(result_array->null_count(), 0);
  AssertTypeEqual(result_array->type(), float64());

  if (random_options.length > 0) {
    // verify E(X), E(X^2) is near theory
    double sum = 0, square_sum = 0;
    const double* values = result_array->data()->GetValues<double>(1);
    for (int64_t i = 0; i < random_options.length; ++i) {
      const double value = values[i];
      ASSERT_GE(value, 0);
      ASSERT_LT(value, 1);
      sum += value;
      square_sum += value * value;
    }
    const double E_X = 0.5;
    const double E_X2 = 1.0 / 12 + E_X * E_X;
    ASSERT_NEAR(sum / random_options.length, E_X, E_X * 0.02);
    ASSERT_NEAR(square_sum / random_options.length, E_X2, E_X2 * 0.02);
  }
}

}  // namespace

TEST(TestRandom, Seed) {
  const int kCount = 100000;
  auto random_options = RandomOptions::FromSeed(/*length=*/kCount, /*seed=*/0);
  TestRandomWithOptions(random_options);
}

TEST(TestRandom, SystemRandom) {
  const int kCount = 100000;
  auto random_options = RandomOptions::FromSystemRandom(/*length=*/kCount);
  TestRandomWithOptions(random_options);
}

TEST(TestRandom, SeedIsDeterministic) {
  const int kCount = 100;
  auto random_options = RandomOptions::FromSeed(/*length=*/kCount, /*seed=*/0);
  ASSERT_OK_AND_ASSIGN(Datum first_call, CallFunction("random", {}, &random_options));
  ASSERT_OK_AND_ASSIGN(Datum second_call, CallFunction("random", {}, &random_options));
  AssertDatumsEqual(first_call, second_call);
}

TEST(TestRandom, Length) {
  auto random_options = RandomOptions::FromSystemRandom(/*length=*/0);
  TestRandomWithOptions(random_options);

  random_options = RandomOptions::FromSystemRandom(/*length=*/-1);
  ASSERT_RAISES(Invalid, CallFunction("random", {}, &random_options));
}

}  // namespace compute
}  // namespace arrow
