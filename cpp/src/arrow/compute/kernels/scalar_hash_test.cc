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
#include "arrow/compute/util.h"
#include "arrow/compute/key_hash.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace compute {

/** A test helper that is a friend function for Hashing64 **/
void TestHashVarLen(bool should_incr, uint32_t row_count,
                    const uint32_t* var_offsets, const uint8_t* var_data,
                    uint64_t* hash_results) {
  Hashing64::HashVarLen(should_incr, row_count, var_offsets, var_data, hash_results);
}

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
  for (size_t val_ndx = 0; val_ndx < test_vals_first.size(); ++val_ndx) {
    uint64_t expected_hash = hash_combine(hash_int<uint16_t>(test_vals_first[val_ndx]),
                                          hash_int<int16_t>(test_vals_second[val_ndx]));
    uint64_t actual_hash = result_data.GetValues<uint64_t>(data_bufndx)[val_ndx];
    ASSERT_EQ(expected_hash, actual_hash);
  }
}

TEST(TestScalarHash, Hash64StringMap) {
  constexpr int data_bufndx{1};
  constexpr int varoffs_bufndx{1};
  constexpr int vardata_bufndx{2};

  // for expected values
  auto test_vals_first  = ArrayFromJSON(utf8(), R"(["first-A", "second-A", "third-A"])");
  auto test_vals_second = ArrayFromJSON(utf8(), R"(["first-B", "second-B", "third-B"])");
  uint64_t expected_hashes[test_vals_first->length()];

  TestHashVarLen(/*combine_hashes=*/false, /*num_rows=*/3,
                 test_vals_first->data()->GetValues<uint32_t>(varoffs_bufndx),
                 test_vals_first->data()->GetValues<uint8_t>(vardata_bufndx),
                 expected_hashes);

  TestHashVarLen(/*combine_hashes=*/true, /*num_rows=*/3,
                 test_vals_second->data()->GetValues<uint32_t>(varoffs_bufndx),
                 test_vals_second->data()->GetValues<uint8_t>(vardata_bufndx),
                 expected_hashes);

  // for actual values
  auto test_map = ArrayFromJSON(map(utf8(), utf8()),
                                R"([[["first-A", "first-B"]],
                                    [["second-A", "second-B"]],
                                    [["third-A", "third-B"]]])");

  ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("hash_64", {test_map}));
  auto result_data = *(hash_result.array());

  // compare actual and expected
  for (int64_t val_ndx = 0; val_ndx < test_vals_first->length(); ++val_ndx) {
    uint64_t actual_hash = result_data.GetValues<uint64_t>(data_bufndx)[val_ndx];

    ASSERT_EQ(expected_hashes[val_ndx], actual_hash);
  }
}

TEST(TestScalarHash, Hash64Map) {
  constexpr int data_bufndx{1};
  constexpr int varoffs_bufndx{1};
  constexpr int vardata_bufndx{2};

  // For expected values
  auto test_vals_first = ArrayFromJSON(utf8(),
                                       R"(["first-A", "second-A", "first-B",
                                           "second-B", "first-C", "second-C"])");

  std::vector<uint8_t> test_vals_second{1, 3, 11, 23, 111, 223};
  uint64_t expected_hashes[test_vals_first->length()];

  // compute initial hashes from the string column (array)
  TestHashVarLen(/*combine_hashes=*/false, /*num_rows=*/6,
                 test_vals_first->data()->GetValues<uint32_t>(varoffs_bufndx),
                 test_vals_first->data()->GetValues<uint8_t>(vardata_bufndx),
                 expected_hashes);

  // For actual values
  auto test_map = ArrayFromJSON(map(utf8(), uint8()),
                                R"([[["first-A", 1]], [["second-A", 3]],
                                    [["first-B", 11]], [["second-B", 23]],
                                    [["first-C", 111]], [["second-C", 223]]])");

  ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("hash_64", {test_map}));
  auto result_data = *(hash_result.array());

  // compare actual and expected
  for (int64_t val_ndx = 0; val_ndx < test_vals_first->length(); ++val_ndx) {
    // compute final hashes by combining int hashes with initial string hashes
    expected_hashes[val_ndx] = hash_combine(expected_hashes[val_ndx],
                                            hash_int<uint8_t>(test_vals_second[val_ndx]));

    uint64_t actual_hash = result_data.GetValues<uint64_t>(data_bufndx)[val_ndx];

    ASSERT_EQ(expected_hashes[val_ndx], actual_hash);
  }
}

TEST(TestScalarHash, Hash64List) {
  constexpr int data_bufndx{1};
  constexpr int varoffs_bufndx{1};
  constexpr int vardata_bufndx{2};

  // for expected values
  auto test_vals1 = ArrayFromJSON(utf8(), R"(["first-A", "second-A", "third-A"])");
  auto test_vals2 = ArrayFromJSON(utf8(), R"(["first-B", "second-B", "third-B"])");
  auto test_vals3 = ArrayFromJSON(utf8(), R"(["first-A", "first-B",
                                              "second-A", "second-B",
                                              "third-A", "third-B"])");
  uint64_t expected_hashes[test_vals1->length()];
  uint64_t test_hashes[test_vals3->length()];

  TestHashVarLen(/*combine_hashes=*/false, /*num_rows=*/3,
                 test_vals1->data()->GetValues<uint32_t>(varoffs_bufndx),
                 test_vals1->data()->GetValues<uint8_t>(vardata_bufndx),
                 expected_hashes);

  TestHashVarLen(/*combine_hashes=*/true, /*num_rows=*/3,
                 test_vals2->data()->GetValues<uint32_t>(varoffs_bufndx),
                 test_vals2->data()->GetValues<uint8_t>(vardata_bufndx),
                 expected_hashes);

  TestHashVarLen(/*combine_hashes=*/false, /*num_rows=*/6,
                 test_vals3->data()->GetValues<uint32_t>(varoffs_bufndx),
                 test_vals3->data()->GetValues<uint8_t>(vardata_bufndx),
                 test_hashes);

  // for actual values
  auto test_list = ArrayFromJSON(list(utf8()),
                                 R"([["first-A", "first-B"],
                                     ["second-A", "second-B"],
                                     ["third-A", "third-B"]])");

  ARROW_LOG(INFO) << "size: " << test_list->length();
  ARROW_LOG(INFO) << test_list->ToString();

  ARROW_LOG(INFO) << "Test Hashes:";
  for (uint64_t hash_val : test_hashes) {
    ARROW_LOG(INFO) << "\t" << hash_val;
  }

  ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("hash_64", {test_list}));
  auto result_data = *(hash_result.array());

  // compare actual and expected
  // for (int64_t val_ndx = 0; val_ndx < test_list->length(); ++val_ndx) {
  for (int64_t val_ndx = 0; val_ndx < result_data.length; ++val_ndx) {
    uint64_t actual_hash = result_data.GetValues<uint64_t>(data_bufndx)[val_ndx];
    ARROW_LOG(INFO) << "actual hash: " << actual_hash;

    // ASSERT_EQ(expected_hashes[val_ndx], actual_hash);
    // ARROW_LOG(INFO) << "expected hash: " << expected_hashes[val_ndx] << "\tactual hash: " << actual_hash;
  }
}

}  // namespace compute
}  // namespace arrow
