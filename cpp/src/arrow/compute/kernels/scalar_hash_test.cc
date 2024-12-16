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
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x94378165;
constexpr auto array_lengths = {0, 50, 100};
constexpr auto null_probabilities = {0.0, 0.5, 1.0};

class TestScalarHash : public ::testing::Test {
 public:
  template <typename c_type>
  void AssertHashesEqual(const std::shared_ptr<Array>& arr, Datum res,
                         std::vector<c_type> exp) {
    auto res_array = res.array();
    for (int64_t val_ndx = 0; val_ndx < arr->length(); ++val_ndx) {
      c_type actual_hash = res_array->GetValues<c_type>(1)[val_ndx];
      if (arr->IsNull(val_ndx)) {
        ASSERT_EQ(0, actual_hash);
      } else {
        ASSERT_EQ(exp[val_ndx], actual_hash);
      }
    }
  }

  template <typename c_type>
  std::vector<c_type> HashPrimitive(const std::shared_ptr<Array>& arr) {
    std::vector<c_type> hashes(arr->length());
    // Choose the Hasher type conditionally based on c_type

    if constexpr (std::is_same<c_type, uint64_t>::value) {
      Hashing64::HashFixed(false, static_cast<uint32_t>(arr->length()),
                           arr->type()->bit_width() / 8,
                           arr->data()->GetValues<uint8_t>(1), hashes.data());
    } else {
      Hashing32::HashFixed(::arrow::internal::CpuInfo::GetInstance()->hardware_flags(),
                           false, static_cast<uint32_t>(arr->length()),
                           arr->type()->bit_width() / 8,
                           arr->data()->GetValues<uint8_t>(1), hashes.data(), nullptr);
    }

    return hashes;
  }

  template <typename c_type>
  std::vector<c_type> HashBinaryLike(const std::shared_ptr<Array>& arr) {
    std::vector<c_type> hashes(arr->length());
    auto length = static_cast<uint32_t>(arr->length());
    auto values = arr->data()->GetValues<uint8_t>(2);
    if constexpr (std::is_same<c_type, uint64_t>::value) {
      if (arr->type_id() == Type::LARGE_BINARY || arr->type_id() == Type::LARGE_STRING) {
        Hashing64::HashVarLen(false, length, arr->data()->GetValues<uint64_t>(1), values,
                              hashes.data());
      } else {
        Hashing64::HashVarLen(false, length, arr->data()->GetValues<uint32_t>(1), values,
                              hashes.data());
      }
    } else {
      auto hw_flags = ::arrow::internal::CpuInfo::GetInstance()->hardware_flags();
      if (arr->type_id() == Type::LARGE_BINARY || arr->type_id() == Type::LARGE_STRING) {
        Hashing32::HashVarLen(hw_flags, false, length,
                              arr->data()->GetValues<uint64_t>(1), values, hashes.data(),
                              nullptr);
      } else {
        Hashing32::HashVarLen(hw_flags, false, length,
                              arr->data()->GetValues<uint32_t>(1), values, hashes.data(),
                              nullptr);
      }
    }
    return hashes;
  }

  void CheckDeterminisic(const std::string& func, const std::shared_ptr<Array>& arr) {
    // Check that the hash is deterministic
    ASSERT_OK_AND_ASSIGN(Datum res1, CallFunction(func, {arr}));
    ASSERT_OK_AND_ASSIGN(Datum res2, CallFunction(func, {arr}));
    ASSERT_EQ(res1.length(), arr->length());
    if (func == "hash64") {
      ASSERT_EQ(res1.type()->id(), Type::UINT64);
    } else if (func == "hash32") {
      ASSERT_EQ(res1.type()->id(), Type::UINT32);
    } else {
      FAIL() << "Unknown function: " << func;
    }
    AssertDatumsEqual(res1, res2, true);
  }

  void CheckPrimitive(const std::string& func, const std::shared_ptr<Array>& arr) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction(func, {arr}));
    CheckDeterminisic(func, arr);
    if (func == "hash64") {
      AssertHashesEqual<uint64_t>(arr, hash_result, HashPrimitive<uint64_t>(arr));
    } else if (func == "hash32") {
      AssertHashesEqual<uint32_t>(arr, hash_result, HashPrimitive<uint32_t>(arr));
    } else {
      FAIL() << "Unknown function: " << func;
    }
  }

  void CheckBinary(const std::string& func, const std::shared_ptr<Array>& arr) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction(func, {arr}));
    CheckDeterminisic(func, arr);
    if (func == "hash64") {
      AssertHashesEqual<uint64_t>(arr, hash_result, HashBinaryLike<uint64_t>(arr));
    } else if (func == "hash32") {
      AssertHashesEqual<uint32_t>(arr, hash_result, HashBinaryLike<uint32_t>(arr));
    } else {
      FAIL() << "Unknown function: " << func;
    }
  }
};

TEST_F(TestScalarHash, Null) {
  Datum res;
  std::shared_ptr<Array> arr;
  std::shared_ptr<Array> exp;

  arr = ArrayFromJSON(null(), R"([])");
  exp = ArrayFromJSON(uint32(), "[]");
  ASSERT_OK_AND_ASSIGN(res, CallFunction("hash32", {arr}));
  AssertArraysEqual(*res.make_array(), *exp);
  CheckDeterminisic("hash32", arr);

  arr = ArrayFromJSON(null(), R"([])");
  exp = ArrayFromJSON(uint64(), "[]");
  ASSERT_OK_AND_ASSIGN(res, CallFunction("hash64", {arr}));
  AssertArraysEqual(*res.make_array(), *exp);
  CheckDeterminisic("hash64", arr);

  arr = ArrayFromJSON(null(), R"([null, null, null])");
  exp = ArrayFromJSON(uint32(), "[0, 0, 0]");
  ASSERT_OK_AND_ASSIGN(res, CallFunction("hash32", {arr}));
  AssertArraysEqual(*res.make_array(), *exp);
  CheckDeterminisic("hash32", arr);

  arr = ArrayFromJSON(null(), R"([null, null, null])");
  exp = ArrayFromJSON(uint64(), "[0, 0, 0]");
  ASSERT_OK_AND_ASSIGN(res, CallFunction("hash64", {arr}));
  AssertArraysEqual(*res.make_array(), *exp);
  CheckDeterminisic("hash64", arr);
}

TEST_F(TestScalarHash, NullHashIsZero) {
  auto arr1 = ArrayFromJSON(int32(), R"([null, 0, 1])");
  ASSERT_OK_AND_ASSIGN(auto res1, CallFunction("hash64", {arr1}));
  auto buf1 = res1.array()->GetValues<uint64_t>(1);
  ASSERT_EQ(buf1[0], 0);
  ASSERT_NE(buf1[1], 0);
  ASSERT_NE(buf1[2], 0);
  ASSERT_NE(buf1[1], buf1[2]);

  auto arr2 = ArrayFromJSON(int8(), R"([null, 0, 1])");
  ASSERT_OK_AND_ASSIGN(auto res2, CallFunction("hash32", {arr2}));
  auto buf2 = res2.array()->GetValues<uint32_t>(1);
  ASSERT_EQ(buf2[0], 0);
  ASSERT_NE(buf2[1], 0);
  ASSERT_NE(buf2[2], 0);
  ASSERT_NE(buf2[1], buf2[2]);
}

TEST_F(TestScalarHash, Boolean) {
  auto arr = ArrayFromJSON(boolean(), R"([true, false, null, true, null])");
  CheckDeterminisic("hash32", arr);
  CheckDeterminisic("hash64", arr);
}

TEST_F(TestScalarHash, NumericLike) {
  auto types = {int8(),   int16(),  int32(),  int64(),   uint8(),
                uint16(), uint32(), uint64(), float32(), float64()};
  for (auto func : {"hash32", "hash64"}) {
    for (auto type : types) {
      CheckPrimitive(func, ArrayFromJSON(type, R"([])"));
      CheckPrimitive(func, ArrayFromJSON(type, R"([null])"));
      CheckPrimitive(func, ArrayFromJSON(type, R"([1])"));
      CheckPrimitive(func, ArrayFromJSON(type, R"([1, 2, null])"));
      CheckPrimitive(func, ArrayFromJSON(type, R"([null, 2, 3])"));
      CheckPrimitive(func, ArrayFromJSON(type, R"([1, 2, 3, 4])"));
    }
  }
}

TEST_F(TestScalarHash, BinaryLike) {
  auto types = {binary(), utf8(), large_binary(), large_utf8()};
  for (auto func : {"hash32", "hash64"}) {
    for (auto type : types) {
      CheckBinary(func, ArrayFromJSON(type, R"([])"));
      CheckBinary(func, ArrayFromJSON(type, R"([null])"));
      CheckBinary(func, ArrayFromJSON(type, R"([""])"));
      CheckBinary(func, ArrayFromJSON(type, R"(["first", "second", null])"));
      CheckBinary(func, ArrayFromJSON(type, R"(["first", "second", "third"])"));
    }
  }
}

TEST_F(TestScalarHash, ExtensionType) {
  auto storage = ArrayFromJSON(int16(), R"([1, 2, 3, 4, null])");
  auto extension = ExtensionType::WrapArray(smallint(), storage);
  CheckPrimitive("hash32", extension);
  CheckPrimitive("hash64", extension);
}

TEST_F(TestScalarHash, DictionaryType) {
  auto dict_type = dictionary(int8(), utf8());
  auto dict = DictArrayFromJSON(dict_type, "[1, 2, null, 3, 0]",
                                "[\"A0\", \"A1\", \"C2\", \"C3\"]");
  CheckPrimitive("hash32", dict);
  CheckPrimitive("hash64", dict);
}

TEST_F(TestScalarHash, RandomBinaryLike) {
  auto rand = random::RandomArrayGenerator(kSeed);
  auto types = {binary(), utf8(), large_binary(), large_utf8()};
  for (auto func : {"hash32", "hash64"}) {
    for (auto type : types) {
      for (auto length : array_lengths) {
        for (auto null_probability : null_probabilities) {
          CheckBinary(func, rand.ArrayOf(type, length, null_probability));
        }
      }
    }
  }
}

TEST_F(TestScalarHash, RandomNumericLike) {
  auto rand = random::RandomArrayGenerator(kSeed);
  auto types = {int8(),   int16(),  int32(),  int64(),   uint8(),
                uint16(), uint32(), uint64(), float32(), float64()};
  for (auto func : {"hash32", "hash64"}) {
    for (auto type : types) {
      for (auto length : array_lengths) {
        for (auto null_probability : null_probabilities) {
          CheckPrimitive(func, rand.ArrayOf(type, length, null_probability));
        }
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
      list(boolean()),
      list(list(int16())),
      list(list(list(uint8()))),
      fixed_size_list(int32(), 3),
      map(int32(), int32()),
      map(int32(), utf8()),
      map(utf8(), list(int16())),
      struct_({field("f0", int32()), field("f1", utf8())}),
      struct_({field("f0", list(int32()))}),
      struct_({field("f0", struct_({field("f0", int32()), field("f1", utf8())}))}),
  };
  for (auto type : types) {
    for (auto length : array_lengths) {
      for (auto null_probability : null_probabilities) {
        auto arr = rand.ArrayOf(type, length, null_probability);
        CheckDeterminisic("hash32", arr);
        CheckDeterminisic("hash64", arr);
      }
    }
  }
}

}  // namespace compute
}  // namespace arrow
