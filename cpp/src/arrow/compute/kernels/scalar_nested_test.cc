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
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace compute {

static std::shared_ptr<DataType> GetOffsetType(const DataType& type) {
  return type.id() == Type::LIST ? int32() : int64();
}

TEST(TestScalarNested, ListValueLength) {
  for (auto ty : {list(int32()), large_list(int32())}) {
    CheckScalarUnary("list_value_length", ty, "[[0, null, 1], null, [2, 3], []]",
                     GetOffsetType(*ty), "[3, null, 2, 0]");
  }
}

}  // namespace compute
}  // namespace arrow
