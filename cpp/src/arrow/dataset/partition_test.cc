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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/dataset/api.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/test_util.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

namespace arrow {
namespace dataset {

TEST(PartitionScheme, Simple) {
  auto expr = std::make_shared<Expression>();
  expr->values["alpha"] = std::make_shared<Int16Scalar>(3);
  SimplePartitionScheme scheme(expr);
  ASSERT_TRUE(scheme.CanParse("/hello/world"));

  std::shared_ptr<Expression> parsed;
  ASSERT_OK(scheme.Parse("/hello/world", &parsed));
  ASSERT_NE(parsed, nullptr);
  ASSERT_TRUE(parsed->Equals(*expr));
}

}  // namespace dataset
}  // namespace arrow
