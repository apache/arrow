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
#include "arrow/compute/key_hash_internal.h"
#include "arrow/compute/util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x94378165;
constexpr auto array_lengths = {0, 10, 100};
constexpr auto null_probabilities = {0.0, 0.5, 1.0};

class TestScalarHash : public ::testing::Test {
 public:
  std::vector<uint64_t> HashPrimitive(const std::shared_ptr<Array>& arr) {
    std::vector<uint64_t> hashes(arr->length());
    Hashing64::HashFixed(false, static_cast<uint32_t>(arr->length()),
                         arr->type()->bit_width() / 8, arr->data()->GetValues<uint8_t>(1),
                         hashes.data());
    return hashes;
  }

  std::vector<uint64_t> HashBinaryLike(const std::shared_ptr<Array>& arr) {
    std::vector<uint64_t> hashes(arr->length());
    if (arr->type_id() == Type::LARGE_BINARY || arr->type_id() == Type::LARGE_STRING) {
      Hashing64::HashVarLen(false, static_cast<uint32_t>(arr->length()),
                            arr->data()->GetValues<uint64_t>(1),
                            arr->data()->GetValues<uint8_t>(2), hashes.data());
    } else {
      Hashing64::HashVarLen(false, static_cast<uint32_t>(arr->length()),
                            arr->data()->GetValues<uint32_t>(1),
                            arr->data()->GetValues<uint8_t>(2), hashes.data());
    }
    return hashes;
  }

  void CheckDeterminisic(const std::shared_ptr<Array>& arr) {
    // Check that the hash is deterministic
    ASSERT_OK_AND_ASSIGN(Datum res1, CallFunction("hash64", {arr}));
    ASSERT_OK_AND_ASSIGN(Datum res2, CallFunction("hash64", {arr}));
    ASSERT_EQ(res1.length(), arr->length());
    ASSERT_EQ(res2.type()->id(), Type::UINT64);
    AssertDatumsEqual(res1, res2);
  }

  void CheckPrimitive(const std::shared_ptr<Array>& arr) {
    CheckDeterminisic(arr);

    ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("hash64", {arr}));
    auto result_data = hash_result.array();
    auto expected_hashes = HashPrimitive(arr);

    for (int64_t val_ndx = 0; val_ndx < arr->length(); ++val_ndx) {
      uint64_t actual_hash = result_data->GetValues<uint64_t>(1)[val_ndx];
      if (arr->IsNull(val_ndx)) {
        ASSERT_EQ(0, actual_hash);
      } else {
        ASSERT_EQ(expected_hashes[val_ndx], actual_hash);
      }
    }
  }

  void CheckBinary(const std::shared_ptr<Array>& arr) {
    CheckDeterminisic(arr);

    ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction("hash64", {arr}));
    auto result_data = hash_result.array();
    auto expected_hashes = HashBinaryLike(arr);

    for (int64_t val_ndx = 0; val_ndx < arr->length(); ++val_ndx) {
      uint64_t actual_hash = result_data->GetValues<uint64_t>(1)[val_ndx];
      if (arr->IsNull(val_ndx)) {
        ASSERT_EQ(0, actual_hash);
      } else {
        ASSERT_EQ(expected_hashes[val_ndx], actual_hash);
      }
    }
  }
};

// TODO(kszucs): test null, bool, fixed size binary, dictionary encoded

TEST_F(TestScalarHash, NumericLike) {
  auto types = {int8(),   int16(),  int32(),  int64(),   uint8(),
                uint16(), uint32(), uint64(), float32(), float64()};
  for (auto type : types) {
    CheckPrimitive(ArrayFromJSON(type, R"([])"));
    CheckPrimitive(ArrayFromJSON(type, R"([null])"));
    CheckPrimitive(ArrayFromJSON(type, R"([1])"));
    CheckPrimitive(ArrayFromJSON(type, R"([1, 2, null])"));
    CheckPrimitive(ArrayFromJSON(type, R"([null, 2, 3])"));
    CheckPrimitive(ArrayFromJSON(type, R"([1, 2, 3, 4])"));
  }
}

TEST_F(TestScalarHash, BinaryLike) {
  auto types = {binary(), utf8(), large_binary(), large_utf8()};
  for (auto type : types) {
    CheckBinary(ArrayFromJSON(type, R"([])"));
    CheckBinary(ArrayFromJSON(type, R"([null])"));
    CheckBinary(ArrayFromJSON(type, R"([""])"));
    CheckBinary(ArrayFromJSON(type, R"(["first", "second", null])"));
    CheckBinary(ArrayFromJSON(type, R"(["first", "second", "third"])"));
  }
}

TEST_F(TestScalarHash, RandomBinaryLike) {
  auto rand = random::RandomArrayGenerator(kSeed);
  auto types = {binary(), utf8(), large_binary(), large_utf8()};
  for (auto type : types) {
    for (auto length : array_lengths) {
      for (auto null_probability : null_probabilities) {
        CheckBinary(rand.ArrayOf(type, length, null_probability));
      }
    }
  }
}

TEST_F(TestScalarHash, RandomNumericLike) {
  auto rand = random::RandomArrayGenerator(kSeed);
  auto types = {int8(),   int16(),  int32(),  int64(),   uint8(),
                uint16(), uint32(), uint64(), float32(), float64()};
  for (auto type : types) {
    for (auto length : array_lengths) {
      for (auto null_probability : null_probabilities) {
        CheckPrimitive(rand.ArrayOf(type, length, null_probability));
      }
    }
  }
}

TEST_F(TestScalarHash, RandomNested) {
  auto rand = random::RandomArrayGenerator(kSeed);
  auto types = {
      list(int32()),
      list(float64()),
      list(utf8()),
      list(large_binary()),
      large_list(int64()),
      large_list(utf8()),
      large_list(large_binary()),
      list(list(int16())),
      list(list(list(uint8()))),
      fixed_size_list(int32(), 3),
      map(int32(), int32()),
      map(int32(), utf8()),
      map(utf8(), list(int16())),
      struct_({field("f0", int32()), field("f1", utf8())}),
      struct_({field("f0", list(int32())), field("f1", large_binary())}),
  };
  for (auto type : types) {
    for (auto length : array_lengths) {
      for (auto null_probability : null_probabilities) {
        CheckDeterminisic(rand.ArrayOf(type, length, null_probability));
      }
    }
  }
}

}  // namespace compute
}  // namespace arrow
