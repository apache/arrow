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

#include <cmath>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/math_internal.h"

namespace arrow::internal {

TEST(NormalPPF, Basics) {
  struct PPFTestCase {
    double input;
    double expected;
  };
  // Test vectors obtained using Scipy's norm.ppf
  std::vector<PPFTestCase> cases = {
      {0.0, -HUGE_VAL},
      {0.001, -3.090232306167813},
      {0.01, -2.3263478740408408},
      {0.02, -2.053748910631823},
      {0.03, -1.880793608151251},
      {0.04, -1.75068607125217},
      {0.05, -1.6448536269514729},
      {0.06, -1.5547735945968535},
      {0.07, -1.4757910281791706},
      {0.08, -1.4050715603096329},
      {0.09, -1.3407550336902165},
      {0.1, -1.2815515655446004},
      {0.2, -0.8416212335729142},
      {0.3, -0.5244005127080409},
      {0.4, -0.2533471031357997},
      {0.5, 0.0},
      {0.6, 0.2533471031357997},
      {0.7, 0.5244005127080407},
      {0.8, 0.8416212335729143},
      {0.9, 1.2815515655446004},
      {0.91, 1.3407550336902165},
      {0.92, 1.4050715603096329},
      {0.93, 1.475791028179171},
      {0.94, 1.5547735945968535},
      {0.95, 1.6448536269514722},
      {0.96, 1.7506860712521692},
      {0.97, 1.8807936081512509},
      {0.98, 2.0537489106318225},
      {0.99, 2.3263478740408408},
      {0.999, 3.090232306167813},
      {1.0, HUGE_VAL},
  };
  for (auto test_case : cases) {
    ARROW_SCOPED_TRACE("p = ", test_case.input);
    EXPECT_DOUBLE_EQ(NormalPPF(test_case.input), test_case.expected);
  }
  // Test vectors from https://doi.org/10.2307/2347330
  cases = {
      {0.25, -0.6744897501960817},
      {0.001, -3.090232306167814},
      {1e-20, -9.262340089798408},
  };
  for (auto test_case : cases) {
    ARROW_SCOPED_TRACE("p = ", test_case.input);
    EXPECT_DOUBLE_EQ(NormalPPF(test_case.input), test_case.expected);
  }
}

}  // namespace arrow::internal
