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

#include "arrow/compute/initialize.h"
#include "arrow/testing/gtest_util.h"

namespace arrow::compute {

namespace {

class ComputeKernelEnvironment : public ::testing::Environment {
 public:
  // This must be done before using the compute kernels in order to
  // register them to the FunctionRegistry.
  ComputeKernelEnvironment() : ::testing::Environment() {}

  void SetUp() override { ASSERT_OK(arrow::compute::Initialize()); }
};

}  // namespace

#ifdef _MSC_VER
// Initialize the compute module
::testing::Environment* compute_kernels_env =
    ::testing::AddGlobalTestEnvironment(new ComputeKernelEnvironment);
#endif

}  // namespace arrow::compute

#ifndef _MSC_VER
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new arrow::compute::ComputeKernelEnvironment);
  return RUN_ALL_TESTS();
}
#endif
