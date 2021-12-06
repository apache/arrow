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

#include <cstdlib>

#include "arrow/compute/api.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace compute {

TEST(TestRandom, Basic) {
  const int kCount = 100000;
  double sum = 0;
  double square_sum = 0;
  for (int i = 0; i < kCount; ++i) {
    ASSERT_OK_AND_ASSIGN(Datum result, CallFunction("random", {}));
    const auto& result_scalar = result.scalar_as<DoubleScalar>();
    ASSERT_GE(result_scalar.value, 0);
    ASSERT_LT(result_scalar.value, 1);
    sum += result_scalar.value;
    square_sum += result_scalar.value * result_scalar.value;
  }

  // verify E(X), E(X^2) is near theory
  const double E_X = 0.5;
  const double E_X2 = 1.0 / 12 + E_X * E_X;
  ASSERT_NEAR(sum / kCount, E_X, std::abs(E_X) * 0.02);
  ASSERT_NEAR(square_sum / kCount, E_X2, E_X2 * 0.02);
}

}  // namespace compute
}  // namespace arrow
