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

  NumericBuilder<Int32Type> builder2(default_memory_pool());
  ASSERT_OK(builder2.Append(1));
  ASSERT_OK(builder2.Append(2));
  ASSERT_OK(builder2.Append(-5));
  ASSERT_OK_AND_ASSIGN(auto expected_values, builder2.Finish());

  NumericBuilder<Int32Type> builder3(default_memory_pool());
  ASSERT_OK(builder3.Append(1));
  ASSERT_OK(builder3.Append(2));
  ASSERT_OK(builder3.Append(-5));
  ASSERT_OK_AND_ASSIGN(auto expected_indexes, builder3.Finish());

  ASSERT_OK_AND_ASSIGN(Datum result_datum, RunLengthEncode(input));

  AssertDatumsEqual(result_datum.array()->child_data[0], expected_indexes);
  AssertDatumsEqual(result_datum.array()->child_data[0], expected_values);
}

}  // namespace compute
}  // namespace arrow
