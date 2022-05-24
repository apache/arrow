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

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/testing/gtest_util.h"

class TestRunLengthEncode : public ::testing::Test {};

namespace arrow {
namespace compute {

TEST_F(TestRunLengthEncode, EncodeInt32Array) {
  NumericBuilder<Int32Type> builder(default_memory_pool());
  ASSERT_OK(builder.Append(1));
  ASSERT_OK(builder.Append(1));
  ASSERT_OK(builder.Append(2));
  ASSERT_OK(builder.Append(-5));
  ASSERT_OK(builder.Append(-5));
  ASSERT_OK(builder.Append(-5));
  ASSERT_OK_AND_ASSIGN(auto input, builder.Finish());

  std::array<uint64_t, 3> expected_indices{0, 2, 3};
  std::array<int32_t, 3> expected_values{1, 2, -5};
  uint8_t expected_null_bitmap{0b111};

  ASSERT_OK_AND_ASSIGN(Datum result_datum, RunLengthEncode(input));

  auto result = result_datum.array();
  ASSERT_EQ(memcmp(result->GetMutableValues<uint8_t>(2), static_cast<void *>(&expected_indices), 3 * sizeof(uint64_t)), 0);
  ASSERT_EQ(memcmp(result->GetMutableValues<uint8_t>(1), static_cast<void *>(&expected_values), 3 * sizeof(int32_t)), 0);
  ASSERT_EQ(*result->GetMutableValues<uint8_t>(0), expected_null_bitmap);
}

}  // namespace compute
}  // namespace arrow
