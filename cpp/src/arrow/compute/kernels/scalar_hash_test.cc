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
#include "arrow/compute/exec/util.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace compute {

namespace {

// combining based on key_hash.h:CombineHashesImp (96a3af4)
static const uint64_t combiner_const = 0x9e3779b9UL;
static inline uint64_t hash_combine(uint64_t h1, uint64_t h2) {
  uint64_t combiner_result = combiner_const + h2 + (h1 << 6) + (h1 >> 2);
  return h1 ^ combiner_result;
}

// hash_int based on key_hash.cc:HashIntImp (672431b)
template <typename T>
uint64_t hash_int(T val) {
  constexpr uint64_t int_const = 11400714785074694791ULL;
  uint64_t cast_val = static_cast<uint64_t>(val);

  return static_cast<uint64_t>(BYTESWAP(cast_val * int_const));
}

template <typename T>
uint64_t hash_int_add(T val, uint64_t first_hash) {
  return hash_combine(first_hash, hash_int(val));
}

}  // namespace

TEST(TestScalarHash, Hash64Primitive) {
  constexpr int data_bufndx{1};
  std::vector<int32_t> test_values{3, 1, 2, 0, 127, 64};
  std::string test_inputs_str{"[3, 1, 2, 0, 127, 64]"};

  for (auto input_dtype : {int32(), uint32(), int8(), uint8()}) {
    auto test_inputs = ArrayFromJSON(input_dtype, test_inputs_str);

    ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("hash_64", {test_inputs}));
    auto result_data = *(hash_result.array());

    // validate each value
    for (int val_ndx = 0; val_ndx < test_inputs->length(); ++val_ndx) {
      uint64_t expected_hash = hash_int<int32_t>(test_values[val_ndx]);
      uint64_t actual_hash = result_data.GetValues<uint64_t>(data_bufndx)[val_ndx];
      ASSERT_EQ(expected_hash, actual_hash);
    }
  }
}

// NOTE: oddly, if int32_t or uint64_t is used for hash_int<>, this fails
TEST(TestScalarHash, Hash64Negative) {
  constexpr int data_bufndx{1};
  std::vector<int32_t> test_values{-3, 1, -2, 0, -127, 64};

  Int32Builder input_builder;
  ASSERT_OK(input_builder.Reserve(test_values.size()));
  ASSERT_OK(input_builder.AppendValues(test_values));
  ASSERT_OK_AND_ASSIGN(auto test_inputs, input_builder.Finish());

  ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("hash_64", {test_inputs}));
  auto result_data = *(hash_result.array());

  // validate each value
  for (int val_ndx = 0; val_ndx < test_inputs->length(); ++val_ndx) {
    uint64_t expected_hash = hash_int<uint32_t>(test_values[val_ndx]);
    uint64_t actual_hash = result_data.GetValues<uint64_t>(data_bufndx)[val_ndx];
    ASSERT_EQ(expected_hash, actual_hash);
  }
}

TEST(TestScalarHash, Hash64IntMap) {
  constexpr int data_bufndx{1};
  std::vector<uint16_t> test_vals_first{7, 67, 3, 31, 17, 29};
  std::vector<int16_t> test_vals_second{67, 7, 31, 3, 29, 17};

  auto test_map = ArrayFromJSON(map(uint16(), int16()),
                                R"([[[ 7, 67]], [[67,  7]], [[ 3, 31]],
                                    [[31,  3]], [[17, 29]], [[29, 17]]])");

  ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("hash_64", {test_map}));
  auto result_data = *(hash_result.array());

  // validate each value
  for (int val_ndx = 0; val_ndx < test_vals_first.size(); ++val_ndx) {
    uint64_t expected_hash = hash_combine(hash_int<uint16_t>(test_vals_first[val_ndx]),
                                          hash_int<int16_t>(test_vals_second[val_ndx]));
    uint64_t actual_hash = result_data.GetValues<uint64_t>(data_bufndx)[val_ndx];
    ASSERT_EQ(expected_hash, actual_hash);
  }
}

TEST(TestScalarHash, Hash64Strings) {
  auto test_strarr = ArrayFromJSON(utf8(), R"(["first-A", "second-A", "third-A",
                                               "first-B", "second-B", "third-B"])");

  ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("hash_64", {test_strarr}));
}

TEST(TestScalarHash, Hash64List) {
  auto test_list = ArrayFromJSON(list(utf8()),
                                 R"([["first-A", "second-A", "third-A"],
                                     ["first-B", "second-B", "third-B"]])");

  ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("hash_64", {test_list}));
}

TEST(TestScalarHash, Hash64Map) {
  auto test_map = ArrayFromJSON(map(utf8(), uint8()),
                                R"([[["first-A", 1], ["second-A", 2], ["third-A", 3]],
                                    [["first-B", 10], ["second-B", 20], ["third-B", 30]]
                                    ])");

  ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("hash_64", {test_map}));
}

}  // namespace compute
}  // namespace arrow
