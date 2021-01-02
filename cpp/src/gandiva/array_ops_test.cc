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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "gandiva/execution_context.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

TEST(TestArrayOps, TestUtf8ContainsUtf8) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  const char* entry_buf = "trianglecirclerectangle";
  int32_t entry_child_offsets[] = {0, 8, 14, 24};
  int32_t entry_offsets_len = 3;
  const char* contains_data = "triangle";
  int32_t contains_data_length = 8;

  EXPECT_EQ(
      array_utf8_contains_utf8(ctx_ptr, entry_buf, entry_child_offsets, entry_offsets_len,
                               contains_data, contains_data_length),
      true);
}

TEST(TestArrayOps, TestUtf8Length) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  const char* entry_buf = "trianglecirclerectangle";
  int32_t entry_child_offsets[] = {0, 8, 14, 24};
  int32_t entry_offsets_len = 3;

  EXPECT_EQ(array_utf8_length(ctx_ptr, entry_buf, entry_child_offsets, entry_offsets_len),
            3);
}

}  // namespace gandiva
