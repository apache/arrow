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
#include <unordered_set>

#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util_internal.h"
#include "arrow/compute/key_hash_internal.h"
#include "arrow/compute/util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x94378165;
constexpr auto kArrayLengths = {0, 50, 100};
constexpr auto kNullProbabilities = {0.0, 0.5, 1.0};

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

    if constexpr (std::is_same_v<c_type, uint64_t>) {
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
    if constexpr (std::is_same_v<c_type, uint64_t>) {
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

  void CheckDeterministic(const std::string& func, const std::shared_ptr<Array>& arr) {
    // Check that the hash is deterministic between different runs
    ASSERT_OK_AND_ASSIGN(Datum res1, CallFunction(func, {arr}));
    ASSERT_OK_AND_ASSIGN(Datum res2, CallFunction(func, {arr}));
    ValidateOutput(res1);
    ValidateOutput(res2);
    ASSERT_EQ(res1.length(), arr->length());
    ASSERT_EQ(res2.length(), arr->length());
    if (func == "hash64") {
      ASSERT_EQ(res1.type()->id(), Type::UINT64);
    } else if (func == "hash32") {
      ASSERT_EQ(res1.type()->id(), Type::UINT32);
    } else {
      FAIL() << "Unknown function: " << func;
    }
    AssertDatumsEqual(res1, res2);

    // Check that slicing the array does not affect the hash
    auto hashes = res1.make_array();
    if (arr->length() >= 1) {
      auto in1 = arr->Slice(1);
      ASSERT_OK_AND_ASSIGN(Datum out1, CallFunction(func, {in1}));
      ValidateOutput(out1);
      AssertArraysEqual(*out1.make_array(), *hashes->Slice(1));
    }
    if (arr->length() >= 4) {
      auto in2 = arr->Slice(2, 2);
      ASSERT_OK_AND_ASSIGN(Datum out2, CallFunction(func, {in2}));
      ValidateOutput(out2);
      AssertArraysEqual(*out2.make_array(), *hashes->Slice(2, 2));
    }
  }

  void CheckHashQuality(const std::string& func, const std::shared_ptr<Array>& arr,
                        double tolerance = 1.0) {
    ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func, {arr}));
    auto hashes = result.make_array();

    auto expected = arr->length();
    if (arr->null_count()) {
      expected -= (arr->null_count() - 1);
    }
    if (func == "hash64") {
      auto hashes64 = dynamic_cast<const UInt64Array*>(hashes.get());
      std::unordered_set<uint64_t> hash_set;
      for (int64_t i = 0; i < hashes64->length(); ++i) {
        hash_set.insert(hashes64->Value(i));
      }
      ASSERT_LE(hash_set.size(), expected);
      ASSERT_GE(hash_set.size(), expected * tolerance);
    } else if (func == "hash32") {
      auto hashes32 = dynamic_cast<const UInt32Array*>(hashes.get());
      std::unordered_set<uint32_t> hash_set;
      for (int64_t i = 0; i < hashes32->length(); ++i) {
        if (hashes32->IsValid(i)) {
          hash_set.insert(hashes32->Value(i));
        }
      }
      ASSERT_LE(hash_set.size(), expected);
      ASSERT_GE(hash_set.size(), expected * tolerance);
    } else {
      FAIL() << "Unknown function: " << func;
    }
  }

  void CheckPrimitive(const std::string& func, const std::shared_ptr<Array>& arr) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result, CallFunction(func, {arr}));
    CheckDeterministic(func, arr);
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
    CheckDeterministic(func, arr);
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
  CheckDeterministic("hash32", arr);

  arr = ArrayFromJSON(null(), R"([])");
  exp = ArrayFromJSON(uint64(), "[]");
  ASSERT_OK_AND_ASSIGN(res, CallFunction("hash64", {arr}));
  AssertArraysEqual(*res.make_array(), *exp);
  CheckDeterministic("hash64", arr);

  arr = ArrayFromJSON(null(), R"([null, null, null])");
  exp = ArrayFromJSON(uint32(), "[0, 0, 0]");
  ASSERT_OK_AND_ASSIGN(res, CallFunction("hash32", {arr}));
  AssertArraysEqual(*res.make_array(), *exp);
  CheckDeterministic("hash32", arr);

  arr = ArrayFromJSON(null(), R"([null, null, null])");
  exp = ArrayFromJSON(uint64(), "[0, 0, 0]");
  ASSERT_OK_AND_ASSIGN(res, CallFunction("hash64", {arr}));
  AssertArraysEqual(*res.make_array(), *exp);
  CheckDeterministic("hash64", arr);
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
  Datum result;
  std::shared_ptr<Array> array;
  auto input = ArrayFromJSON(boolean(), R"([true, false, null, true, null, false])");
  CheckDeterministic("hash32", input);
  CheckDeterministic("hash64", input);

  ASSERT_OK_AND_ASSIGN(result, CallFunction("hash32", {input}));

  array = result.make_array();
  auto array32 = checked_cast<const UInt32Array*>(array.get());
  ASSERT_NE(array32->Value(0), array32->Value(1));
  ASSERT_NE(array32->Value(0), array32->Value(2));
  ASSERT_NE(array32->Value(1), array32->Value(2));
  ASSERT_EQ(array32->Value(0), array32->Value(3));
  ASSERT_EQ(array32->Value(2), array32->Value(4));
  ASSERT_EQ(array32->Value(1), array32->Value(5));

  ASSERT_OK_AND_ASSIGN(result, CallFunction("hash64", {input}));
  array = result.make_array();
  auto array64 = checked_cast<const UInt64Array*>(array.get());
  ASSERT_NE(array64->Value(0), array64->Value(1));
  ASSERT_NE(array64->Value(0), array64->Value(2));
  ASSERT_NE(array64->Value(1), array64->Value(2));
  ASSERT_EQ(array64->Value(0), array64->Value(3));
  ASSERT_EQ(array64->Value(2), array64->Value(4));
  ASSERT_EQ(array64->Value(1), array64->Value(5));
}

TEST_F(TestScalarHash, Primitive) {
  auto types = {int8(),
                int16(),
                int32(),
                int64(),
                uint8(),
                uint16(),
                uint32(),
                uint64(),
                float16(),
                float32(),
                float64(),
                time32(TimeUnit::SECOND),
                time64(TimeUnit::NANO),
                date32(),
                date64(),
                timestamp(TimeUnit::SECOND),
                duration(TimeUnit::MILLI)};

  for (auto func : {"hash32", "hash64"}) {
    for (auto type : types) {
      CheckPrimitive(func, ArrayFromJSON(type, R"([])"));
      CheckPrimitive(func, ArrayFromJSON(type, R"([null])"));
      CheckPrimitive(func, ArrayFromJSON(type, R"([1])"));
      CheckPrimitive(func, ArrayFromJSON(type, R"([1, 2])"));
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
      CheckBinary(func, ArrayFromJSON(type, R"(["first", "second", "third"])"));
    }
  }
  for (auto func : {"hash32", "hash64"}) {
    auto type = fixed_size_binary(1);
    CheckPrimitive(func, ArrayFromJSON(type, R"([])"));
    CheckPrimitive(func, ArrayFromJSON(type, R"([null])"));
    CheckPrimitive(func, ArrayFromJSON(type, R"(["a", "b"])"));
    CheckPrimitive(func, ArrayFromJSON(type, R"([null, "b"])"));

    type = fixed_size_binary(3);
    CheckPrimitive(func, ArrayFromJSON(type, R"([])"));
    CheckPrimitive(func, ArrayFromJSON(type, R"([null])"));
    CheckPrimitive(func, ArrayFromJSON(type, R"(["alt", "blt"])"));
    CheckPrimitive(func, ArrayFromJSON(type, R"([null, "blt"])"));
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

  for (auto length : kArrayLengths) {
    for (auto null_probability : kNullProbabilities) {
      for (auto type : types) {
        auto arr = rand.ArrayOf(type, length, null_probability);
        CheckBinary("hash32", arr);
        CheckBinary("hash64", arr);
      }
      for (auto type : {fixed_size_binary(1), fixed_size_binary(3)}) {
        auto arr = rand.ArrayOf(type, length, null_probability);
        CheckPrimitive("hash32", arr);
        CheckPrimitive("hash64", arr);
      }
      auto arr = rand.ArrayOf(fixed_size_binary(0), length, null_probability);
      CheckDeterministic("hash32", arr);
      CheckDeterministic("hash64", arr);
    }
  }
}

TEST_F(TestScalarHash, RandomPrimitive) {
  auto rand = random::RandomArrayGenerator(kSeed);
  auto types = {int8(),
                int16(),
                int32(),
                int64(),
                uint8(),
                uint16(),
                uint32(),
                uint64(),
                float16(),
                float32(),
                float64(),
                decimal128(18, 5),
                decimal256(38, 5),
                time32(TimeUnit::SECOND),
                time64(TimeUnit::NANO),
                date32(),
                date64(),
                timestamp(TimeUnit::SECOND),
                duration(TimeUnit::MILLI)};

  for (auto type : types) {
    for (auto length : kArrayLengths) {
      for (auto null_probability : kNullProbabilities) {
        auto arr = rand.ArrayOf(type, length, null_probability);
        CheckPrimitive("hash32", arr);
        CheckPrimitive("hash64", arr);
        if (type->bit_width() >= 16) {
          // the generated arrays contain unique values at the given lengths
          CheckHashQuality("hash32", arr, 0.98);
          CheckHashQuality("hash64", arr, 0.98);
        }
      }
    }
  }
}

TEST_F(TestScalarHash, RandomList) {
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
  };
  for (auto type : types) {
    for (auto length : kArrayLengths) {
      for (auto null_probability : kNullProbabilities) {
        auto arr = rand.ArrayOf(type, length, null_probability);
        CheckDeterministic("hash32", arr);
        CheckDeterministic("hash64", arr);
      }
    }
  }
}

// GH-17211: hashing nested (list-like) child values reused the parent's element
// offsets directly as byte offsets into the hashed-child buffer, without
// rescaling by the width of the hashed code (4 bytes for hash32, 8 for hash64).
// This corrupted results in a way that depended on row position, so two
// occurrences of the exact same nested value at different rows would hash
// differently.
void CheckIdenticalRowsHashEqually(const std::string& func,
                                   const std::shared_ptr<Array>& arr, int64_t row_a,
                                   int64_t row_b) {
  ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func, {arr}));
  ASSERT_OK_AND_ASSIGN(auto scalar_a, result.make_array()->GetScalar(row_a));
  ASSERT_OK_AND_ASSIGN(auto scalar_b, result.make_array()->GetScalar(row_b));
  ASSERT_TRUE(scalar_a->Equals(*scalar_b))
      << "row " << row_a << " and row " << row_b << " have the same value in "
      << arr->ToString() << " and should hash identically";
}

TEST_F(TestScalarHash, ListLikeDuplicateRowsHashEqually) {
  for (const std::string& func : {"hash32", "hash64"}) {
    CheckIdenticalRowsHashEqually(
        func,
        ArrayFromJSON(fixed_size_list(int32(), 3),
                      "[[7, 8, 9], [100, 101, 102], [7, 8, 9], [200, 201, 202]]"),
        0, 2);
    CheckIdenticalRowsHashEqually(
        func,
        ArrayFromJSON(list(int32()),
                      "[[7, 8, 9], [100, 101], [7, 8, 9], [200, 201, 202, 203]]"),
        0, 2);
    CheckIdenticalRowsHashEqually(
        func,
        ArrayFromJSON(large_list(int32()),
                      "[[7, 8, 9], [100, 101], [7, 8, 9], [200, 201, 202, 203]]"),
        0, 2);
    CheckIdenticalRowsHashEqually(
        func,
        ArrayFromJSON(list(list(int16())),
                      "[[[7, 8], [9]], [[1], [2, 3]], [[7, 8], [9]], [[4]]]"),
        0, 2);
    CheckIdenticalRowsHashEqually(
        func,
        ArrayFromJSON(
            map(utf8(), int32()),
            R"([[["a", 1], ["b", 2]], [["c", 3]], [["a", 1], ["b", 2]], [["d", 4]]])"),
        0, 2);
    CheckIdenticalRowsHashEqually(
        func,
        ArrayFromJSON(
            struct_({field("f0", list(int32()))}),
            R"([{"f0": [7, 8, 9]}, {"f0": [1, 2]}, {"f0": [7, 8, 9]}, {"f0": [4]}])"),
        0, 2);
  }
}

// Same as above, but with a large array and the duplicated rows far apart, as a
// stress test of the row-folding loop in HashArray's is_list_like branch beyond
// the handful of rows exercised above.
TEST_F(TestScalarHash, ListLikeDuplicateRowsFarApartHashEqually) {
  constexpr int64_t kRowA = 10;
  constexpr int64_t kRowB = 2 * util::MiniBatch::kMiniBatchLength + 10;
  constexpr int64_t kLength = kRowB + 100;

  Int32Builder value_builder;
  ListBuilder list_builder(default_memory_pool(), std::make_shared<Int32Builder>());
  auto* values = checked_cast<Int32Builder*>(list_builder.value_builder());
  for (int64_t row = 0; row < kLength; row++) {
    ASSERT_OK(list_builder.Append());
    int64_t content = row == kRowB ? kRowA : row;
    ASSERT_OK(values->Append(static_cast<int32_t>(content)));
    ASSERT_OK(values->Append(static_cast<int32_t>(content + 1)));
  }
  ASSERT_OK_AND_ASSIGN(auto arr, list_builder.Finish());

  for (const std::string& func : {"hash32", "hash64"}) {
    CheckIdenticalRowsHashEqually(func, arr, kRowA, kRowB);
  }
}

void CheckRowsHashDifferently(const std::string& func, const std::shared_ptr<Array>& arr,
                              int64_t row_a, int64_t row_b) {
  ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func, {arr}));
  ASSERT_OK_AND_ASSIGN(auto scalar_a, result.make_array()->GetScalar(row_a));
  ASSERT_OK_AND_ASSIGN(auto scalar_b, result.make_array()->GetScalar(row_b));
  ASSERT_FALSE(scalar_a->Equals(*scalar_b))
      << "row " << row_a << " and row " << row_b << " have different values in "
      << arr->ToString() << " and should (in practice) hash differently";
}

// Guards against a degenerate fold (e.g. one that ignores element order, or only
// looks at the first/last element) that would satisfy the "identical content hashes
// identically" tests above while still being a broken hash function.
TEST_F(TestScalarHash, ListLikeDistinctContentHashesDifferently) {
  for (const std::string& func : {"hash32", "hash64"}) {
    // Reordering elements should (in practice) change the hash.
    CheckRowsHashDifferently(func, ArrayFromJSON(list(int32()), "[[1, 2, 3], [3, 2, 1]]"),
                             0, 1);
    // Changing one element's value should (in practice) change the hash.
    CheckRowsHashDifferently(func, ArrayFromJSON(list(int32()), "[[1, 2, 3], [1, 2, 4]]"),
                             0, 1);
    // A shorter list shouldn't be a prefix-consistent truncation of a longer one.
    CheckRowsHashDifferently(func, ArrayFromJSON(list(int32()), "[[1, 2], [1, 2, 3]]"), 0,
                             1);
    // Swapping map values between keys should (in practice) change the hash.
    CheckRowsHashDifferently(
        func,
        ArrayFromJSON(map(utf8(), int32()),
                      R"([[["a", 1], ["b", 2]], [["a", 2], ["b", 1]]])"),
        0, 1);
  }
}

// The seed used to fold a list-like row's child hashes together (see
// FastHashScalar::CombineRange) is deliberately not 0, so that an empty (but
// non-null) list doesn't collide with a null list, which hashes to 0 (see
// NullHashIsZero).
TEST_F(TestScalarHash, ListLikeEmptyDiffersFromNull) {
  for (const std::string& func : {"hash32", "hash64"}) {
    for (auto arr : {
             ArrayFromJSON(list(int32()), "[[], null]"),
             ArrayFromJSON(large_list(int32()), "[[], null]"),
             ArrayFromJSON(map(utf8(), int32()), "[[], null]"),
         }) {
      ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func, {arr}));
      auto hashes = result.make_array();
      ASSERT_OK_AND_ASSIGN(auto empty_hash, hashes->GetScalar(0));
      ASSERT_OK_AND_ASSIGN(auto null_hash, hashes->GetScalar(1));
      ASSERT_FALSE(empty_hash->Equals(*null_hash))
          << "hash of an empty " << arr->type()->ToString()
          << " should not collide with hash of a null one";
    }
  }
}

// Mirrors NullHashIsZero, but for list-like types, whose null handling is a
// dedicated masking pass in HashArray's is_list_like branch rather than the
// generic path the other types go through.
TEST_F(TestScalarHash, ListLikeNullHashIsZero) {
  for (const std::string& func : {"hash32", "hash64"}) {
    for (auto arr : {
             ArrayFromJSON(fixed_size_list(int32(), 2), "[null, [1, 2]]"),
             ArrayFromJSON(list(int32()), "[null, [1, 2]]"),
             ArrayFromJSON(large_list(int32()), "[null, [1, 2]]"),
             ArrayFromJSON(map(utf8(), int32()), R"([null, [["a", 1]]])"),
         }) {
      ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func, {arr}));
      auto hashes = result.make_array();
      ASSERT_OK_AND_ASSIGN(auto null_hash, hashes->GetScalar(0));
      ASSERT_OK_AND_ASSIGN(auto non_null_hash, hashes->GetScalar(1));
      auto zero = func == "hash32" ? MakeScalar(uint32_t{0}) : MakeScalar(uint64_t{0});
      ASSERT_TRUE(null_hash->Equals(*zero))
          << "null " << arr->type()->ToString() << " should hash to 0";
      ASSERT_FALSE(non_null_hash->Equals(*zero))
          << "non-null " << arr->type()->ToString() << " should not hash to 0";
    }
  }
}

// Per the columnar format spec, a null slot may have a positive slot length over
// undefined memory. Build a LIST array where the null row's offsets span 3 real
// (non-garbage, but logically "don't care") values instead of the canonical empty
// range, to make sure CombineRange's output for that row is still discarded by the
// masking pass rather than leaking into the result.
TEST_F(TestScalarHash, ListNullWithNonEmptyOffsetRangeHashesToZero) {
  auto offsets = ArrayFromJSON(int32(), "[0, 2, 5, 6]");
  auto values = ArrayFromJSON(int32(), "[10, 20, 30, 40, 50, 60]");
  ASSERT_OK_AND_ASSIGN(auto validity, AllocateEmptyBitmap(3));
  bit_util::SetBit(validity->mutable_data(), 0);
  // Row 1 is null but its offset range [2, 5) is non-empty.
  bit_util::SetBit(validity->mutable_data(), 2);
  ASSERT_OK_AND_ASSIGN(
      auto arr, ListArray::FromArrays(*offsets, *values, default_memory_pool(), validity,
                                      /*null_count=*/1));
  ASSERT_TRUE(arr->IsNull(1));

  for (const std::string& func : {"hash32", "hash64"}) {
    ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func, {arr}));
    auto hashes = result.make_array();
    ASSERT_OK_AND_ASSIGN(auto null_hash, hashes->GetScalar(1));
    auto zero = func == "hash32" ? MakeScalar(uint32_t{0}) : MakeScalar(uint64_t{0});
    ASSERT_TRUE(null_hash->Equals(*zero))
        << "null row with a non-empty offset range should still hash to 0";
  }
}

// The generic path (bool, int, string, ...) zeroes nulls via HashMultiColumn, while
// list-like types are zeroed by HashArray's own is_list_like branch (see
// ListLikeNullHashIsZero) and struct by recursing into per-field columns fed back
// into HashMultiColumn. Check they all agree on the same sentinel (0), not just each
// individually hashing null to *something* self-consistent.
TEST_F(TestScalarHash, NullHashIsZeroAcrossTypes) {
  for (const std::string& func : {"hash32", "hash64"}) {
    auto zero = func == "hash32" ? MakeScalar(uint32_t{0}) : MakeScalar(uint64_t{0});
    for (auto arr : {
             ArrayFromJSON(boolean(), "[null]"),
             ArrayFromJSON(int32(), "[null]"),
             ArrayFromJSON(utf8(), "[null]"),
             ArrayFromJSON(list(int32()), "[null]"),
             ArrayFromJSON(struct_({field("f0", int32())}), "[null]"),
             ArrayFromJSON(map(utf8(), int32()), "[null]"),
         }) {
      ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func, {arr}));
      ASSERT_OK_AND_ASSIGN(auto null_hash, result.make_array()->GetScalar(0));
      ASSERT_TRUE(null_hash->Equals(*zero))
          << "null " << arr->type()->ToString() << " should hash to the same 0 "
          << "sentinel as every other type";
    }
  }
}

// The EXTENSION unwrapping at the top of HashArray should compose with the
// is_list_like recursion; this combination was otherwise untested (ExtensionType
// above only wraps a primitive).
TEST_F(TestScalarHash, ExtensionTypeWrappingList) {
  auto storage = ArrayFromJSON(list(int32()), "[[7, 8, 9], [1, 2], [7, 8, 9]]");
  auto extension = ExtensionType::WrapArray(list_extension_type(), storage);
  CheckIdenticalRowsHashEqually("hash32", extension, 0, 2);
  CheckIdenticalRowsHashEqually("hash64", extension, 0, 2);
}

TEST_F(TestScalarHash, RandomStruct) {
  auto rand = random::RandomArrayGenerator(kSeed);
  auto types = {
      struct_({field("f0", int32())}),
      struct_({field("f0", int32()), field("f1", utf8())}),
      struct_({field("f0", list(int32()))}),
      struct_({field("f0", struct_({field("f0", int32()), field("f1", utf8())}))}),
  };
  for (auto type : types) {
    for (auto length : kArrayLengths) {
      for (auto null_probability : kNullProbabilities) {
        auto arr = rand.ArrayOf(type, length, null_probability);
        CheckDeterministic("hash32", arr);
        CheckDeterministic("hash64", arr);
      }
    }
  }
}

TEST_F(TestScalarHash, RandomMap) {
  auto rand = random::RandomArrayGenerator(kSeed);
  auto types = {
      map(int32(), int32()),
      map(int32(), utf8()),
      map(utf8(), list(int16())),
      map(utf8(), map(int32(), int32())),
  };
  for (auto type : types) {
    for (auto length : kArrayLengths) {
      for (auto null_probability : kNullProbabilities) {
        auto arr = rand.ArrayOf(type, length, null_probability);
        CheckDeterministic("hash32", arr);
        CheckDeterministic("hash64", arr);
      }
    }
  }
}

TEST_F(TestScalarHash, UnsupportedTypes) {
  auto rand = random::RandomArrayGenerator(kSeed);
  auto types = {list_view(int64()),
                large_list_view(int64()),
                binary_view(),
                utf8_view(),
                dense_union({field("a", int64()), field("b", binary())}),
                sparse_union({field("a", int64()), field("b", binary())}),
                run_end_encoded(int16(), utf8())};
  for (auto type : types) {
    auto arr = rand.ArrayOf(type, 1, 0);
    ASSERT_RAISES(NotImplemented, CallFunction("hash32", {arr}));
    ASSERT_RAISES(NotImplemented, CallFunction("hash64", {arr}));
  }
}

// copied from cpp/src/arrow/util/hashing_test.cc
template <typename Integer>
static std::unordered_set<Integer> MakeSequentialIntegers(int32_t n_values) {
  std::unordered_set<Integer> values;
  values.reserve(n_values);

  for (int32_t i = 0; i < n_values; ++i) {
    values.insert(static_cast<Integer>(i));
  }
  ARROW_DCHECK_EQ(values.size(), static_cast<uint32_t>(n_values));
  return values;
}

// copied from cpp/src/arrow/util/hashing_test.cc
static std::unordered_set<std::string> MakeDistinctStrings(int32_t n_values) {
  std::unordered_set<std::string> values;
  values.reserve(n_values);

  // Generate strings between 0 and 24 bytes, with ASCII characters
  std::default_random_engine gen(42);
  std::uniform_int_distribution<int32_t> length_dist(0, 24);
  std::uniform_int_distribution<uint32_t> char_dist('0', 'z');

  while (values.size() < static_cast<uint32_t>(n_values)) {
    auto length = length_dist(gen);
    std::string s(length, 'X');
    for (int32_t i = 0; i < length; ++i) {
      s[i] = static_cast<uint8_t>(char_dist(gen));
    }
    values.insert(std::move(s));
  }
  return values;
}

TEST_F(TestScalarHash, HashQuality) {
  for (auto& func : {"hash32", "hash64"}) {
    std::shared_ptr<Array> arr;
    auto integer_values = MakeSequentialIntegers<int32_t>(100000);
    auto integer_vector =
        std::vector<int32_t>(integer_values.begin(), integer_values.end());
    arrow::ArrayFromVector<Int32Type>(integer_vector, &arr);
    CheckHashQuality(func, arr);

    auto string_values = MakeDistinctStrings(10000);
    auto string_vector =
        std::vector<std::string>(string_values.begin(), string_values.end());
    arrow::ArrayFromVector<StringType>(string_vector, &arr);
    CheckHashQuality(func, arr);
  }
}

}  // namespace compute
}  // namespace arrow
