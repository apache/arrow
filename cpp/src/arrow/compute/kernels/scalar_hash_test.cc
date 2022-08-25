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

#include "arrow/chunked_array.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace compute {

TEST(TestScalarHash, FastHash64Primitive) {
  for (auto input_dtype : {int32(), uint32(), int8(), uint8()}) {
    auto input_arr = ArrayFromJSON(input_dtype, "[3, null, 2, 0, 127, 64]");

    ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("fast_hash_64", {input_arr}));
  }
}

TEST(TestScalarHash, FastHash64Strings) {
  auto test_strarr = ArrayFromJSON(utf8(), R"(["first-A", "second-A", "third-A",
                                               "first-B", "second-B", "third-B"])");

  ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("fast_hash_64", {test_strarr}));
}

TEST(TestScalarHash, FastHash64List) {
  auto test_list = ArrayFromJSON(list(utf8()),
                                 R"([["first-A", "second-A", "third-A"],
                                     ["first-B", "second-B", "third-B"]])");

  ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("fast_hash_64", {test_list}));
}

TEST(TestScalarHash, FastHash64Map) {
  auto test_map = ArrayFromJSON(map(utf8(), uint8()),
                                R"([[["first-A", 1], ["second-A", 2], ["third-A", 3]],
                                    [["first-B", 10], ["second-B", 20], ["third-B", 30]]
                                    ])");

  ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("fast_hash_64", {test_map}));
}

}  // namespace compute
}  // namespace arrow
