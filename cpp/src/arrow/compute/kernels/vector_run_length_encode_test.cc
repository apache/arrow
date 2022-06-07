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

  std::array<uint64_t, 3> expected_run_lengths{2, 3, 6};
  std::array<int32_t, 3> expected_values{1, 2, -5};

  ASSERT_OK_AND_ASSIGN(Datum result_datum, RunLengthEncode(input));

  auto result = result_datum.array();
  ASSERT_EQ(*(result->GetMutableValues<std::array<uint64_t, 3>>(0)),
            expected_run_lengths);
  ASSERT_EQ(*(result->child_data[0]->GetMutableValues<std::array<int32_t, 3>>(1)),
            expected_values);
  ASSERT_EQ(result->child_data[0]->GetMutableValues<uint8_t>(0), nullptr);
  ASSERT_EQ(result->buffers[0]->size(), 3 * sizeof(uint64_t));
  ASSERT_EQ(result->child_data[0]->buffers[1]->size(), 3 * sizeof(int32_t));
  ASSERT_EQ(result->length, 3);
  ASSERT_EQ(*result->type, RunLengthEncodedType(int32()));
  ASSERT_EQ(result->null_count, 0);
  ASSERT_EQ(result->child_data[0]->null_count, 0);
}

TEST_F(TestRunLengthEncode, EncodeArrayWithNull) {
  NumericBuilder<Int32Type> builder(default_memory_pool());
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Append(1));
  ASSERT_OK(builder.Append(1));
  ASSERT_OK(builder.AppendNulls(2));
  ASSERT_OK(builder.Append(-5));
  ASSERT_OK_AND_ASSIGN(auto input, builder.Finish());

  std::array<uint64_t, 4> expected_run_lengths{1, 3, 5, 6};
  uint8_t expected_null_bitmap{0b1010};

  ASSERT_OK_AND_ASSIGN(Datum result_datum, RunLengthEncode(input));

  auto result = result_datum.array();

  ASSERT_EQ(*(result->GetMutableValues<std::array<uint64_t, 4>>(0)),
            expected_run_lengths);
  ASSERT_EQ(result->child_data[0]->GetMutableValues<int32_t>(1)[1], 1);
  ASSERT_EQ(result->child_data[0]->GetMutableValues<int32_t>(1)[3], -5);
  ASSERT_EQ(*(result->child_data[0]->GetMutableValues<uint8_t>(0)), expected_null_bitmap);
  ASSERT_EQ(result->buffers[0]->size(), 4 * sizeof(uint64_t));
  ASSERT_EQ(result->child_data[0]->buffers[1]->size(), 4 * sizeof(int32_t));
  ASSERT_EQ(result->length, 4);
  ASSERT_EQ(*result->type, RunLengthEncodedType(int32()));
  ASSERT_EQ(result->null_count, 2);
  ASSERT_EQ(result->child_data[0]->null_count, 2);
}

}  // namespace compute
}  // namespace arrow
