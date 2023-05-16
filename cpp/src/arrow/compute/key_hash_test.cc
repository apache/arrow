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

#include <gmock/gmock-matchers.h>

#include <chrono>
#include <map>
#include <random>
#include <unordered_set>
#include "arrow/array/builder_binary.h"
#include "arrow/compute/key_hash.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/pcg_random.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace compute {

class TestVectorHash {
 private:
  template <typename Type, typename ArrayType = typename TypeTraits<Type>::ArrayType>
  static enable_if_base_binary<Type, Result<std::shared_ptr<ArrayType>>>
  GenerateUniqueRandomBinary(random::pcg32_fast* random, int num, int min_length,
                             int max_length) {
    using BuilderType = typename TypeTraits<Type>::BuilderType;
    BuilderType builder;
    std::unordered_set<std::string> unique_key_strings;
    std::vector<uint8_t> temp_buffer;
    temp_buffer.resize(max_length);
    std::uniform_int_distribution<int> length_gen(min_length, max_length);
    std::uniform_int_distribution<uint32_t> byte_gen(0,
                                                     std::numeric_limits<uint8_t>::max());

    int num_inserted = 0;
    while (num_inserted < num) {
      int length = length_gen(*random);
      std::generate(temp_buffer.begin(), temp_buffer.end(),
                    [&] { return static_cast<uint8_t>(byte_gen(*random)); });
      std::string buffer_as_str(reinterpret_cast<char*>(temp_buffer.data()),
                                static_cast<std::size_t>(length));
      if (unique_key_strings.insert(buffer_as_str).second) {
        num_inserted++;
        ARROW_RETURN_NOT_OK(builder.Append(temp_buffer.data(), length));
      }
    }
    ARROW_ASSIGN_OR_RAISE(auto uniques, builder.Finish());
    return checked_pointer_cast<ArrayType>(uniques);
  }

  template <typename Type, typename ArrayType = typename TypeTraits<Type>::ArrayType>
  static Result<std::pair<std::vector<int>, std::shared_ptr<ArrayType>>>
  SampleUniqueBinary(random::pcg32_fast* random, int num, const ArrayType& uniques) {
    using BuilderType = typename TypeTraits<Type>::BuilderType;
    BuilderType builder;
    std::vector<int> row_ids;
    row_ids.resize(num);

    std::uniform_int_distribution<int> row_id_gen(0,
                                                  static_cast<int>(uniques.length()) - 1);
    for (int i = 0; i < num; ++i) {
      int row_id = row_id_gen(*random);
      row_ids[i] = row_id;
      ARROW_RETURN_NOT_OK(builder.Append(uniques.GetView(row_id)));
    }
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> sampled, builder.Finish());
    return std::pair<std::vector<int>, std::shared_ptr<ArrayType>>{
        std::move(row_ids), checked_pointer_cast<ArrayType>(sampled)};
  }

 public:
  template <typename Type>
  static void RunSingle(random::pcg32_fast* random, bool use_32bit_hash,
                        bool use_varlen_input, int min_length, int max_length) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    using OffsetType = typename TypeTraits<Type>::OffsetType;
    using offset_t = typename std::make_unsigned<typename OffsetType::c_type>::type;

    constexpr int min_num_unique = 100;
    constexpr int max_num_unique = 1000;
    constexpr int min_num_rows = 4000;
    constexpr int max_num_rows = 8000;

    std::uniform_int_distribution<int> num_unique_gen(min_num_unique, max_num_unique);
    std::uniform_int_distribution<int> num_rows_gen(min_num_rows, max_num_rows);

    int num_unique = num_unique_gen(*random);
    int num_rows = num_rows_gen(*random);

    SCOPED_TRACE("num_bits = " + std::to_string(use_32bit_hash ? 32 : 64) +
                 " varlen = " + std::string(use_varlen_input ? "yes" : "no") +
                 " num_unique " + std::to_string(num_unique) + " num_rows " +
                 std::to_string(num_rows) + " min_length " + std::to_string(min_length) +
                 " max_length " + std::to_string(max_length));

    // The hash can only support 2^(length_in_bits-1) unique values
    if (max_length == 1) {
      num_unique &= 0x7f;
    }

    std::uniform_int_distribution<int> fixed_length_gen(min_length, max_length);
    int fixed_length = use_varlen_input ? 0 : std::max(2, fixed_length_gen(*random));
    if (!use_varlen_input) {
      min_length = max_length = fixed_length;
    }

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ArrayType> uniques,
        GenerateUniqueRandomBinary<Type>(random, num_unique, min_length, max_length));
    ASSERT_OK_AND_ASSIGN(auto sampled,
                         SampleUniqueBinary<Type>(random, num_rows, *uniques));
    const std::vector<int>& row_ids = sampled.first;
    const std::shared_ptr<ArrayType>& keys_array = sampled.second;
    const uint8_t* keys = keys_array->raw_data();
    const offset_t* key_offsets =
        reinterpret_cast<const offset_t*>(keys_array->raw_value_offsets());

    std::vector<uint32_t> hashes_scalar32;
    std::vector<uint64_t> hashes_scalar64;
    hashes_scalar32.resize(num_rows);
    hashes_scalar64.resize(num_rows);
    std::vector<uint32_t> hashes_simd32;
    std::vector<uint64_t> hashes_simd64;
    hashes_simd32.resize(num_rows);
    hashes_simd64.resize(num_rows);

    int64_t hardware_flags_scalar = 0LL;
    int64_t hardware_flags_simd = ::arrow::internal::CpuInfo::AVX2;

    constexpr int mini_batch_size = 1024;
    std::vector<uint32_t> temp_buffer;
    temp_buffer.resize(mini_batch_size * 4);

    for (bool use_simd : {false, true}) {
      if (use_32bit_hash) {
        if (!use_varlen_input) {
          Hashing32::HashFixed(use_simd ? hardware_flags_simd : hardware_flags_scalar,
                               /*combine_hashes=*/false, num_rows, fixed_length, keys,
                               use_simd ? hashes_simd32.data() : hashes_scalar32.data(),
                               temp_buffer.data());
        } else {
          for (int first_row = 0; first_row < num_rows;) {
            int batch_size_next = std::min(num_rows - first_row, mini_batch_size);

            Hashing32::HashVarLen(
                use_simd ? hardware_flags_simd : hardware_flags_scalar,
                /*combine_hashes=*/false, batch_size_next, key_offsets + first_row, keys,
                (use_simd ? hashes_simd32.data() : hashes_scalar32.data()) + first_row,
                temp_buffer.data());

            first_row += batch_size_next;
          }
        }
      } else {
        if (!use_varlen_input) {
          Hashing64::HashFixed(
              /*combine_hashes=*/false, num_rows, fixed_length, keys,
              use_simd ? hashes_simd64.data() : hashes_scalar64.data());
        } else {
          Hashing64::HashVarLen(
              /*combine_hashes=*/false, num_rows, key_offsets, keys,
              use_simd ? hashes_simd64.data() : hashes_scalar64.data());
        }
      }
    }

    if (use_32bit_hash) {
      for (int i = 0; i < num_rows; ++i) {
        hashes_scalar64[i] = hashes_scalar32[i];
        hashes_simd64[i] = hashes_simd32[i];
      }
    }

    // Verify that both scalar and AVX2 implementations give the same hashes
    //
    for (int i = 0; i < num_rows; ++i) {
      ASSERT_EQ(hashes_scalar64[i], hashes_simd64[i])
          << "scalar and simd approaches yielded different hashes";
    }

    // Verify that the same key appearing multiple times generates the same hash
    // each time. Measure the number of unique hashes and compare to the number
    // of unique keys.
    //
    std::map<int, uint64_t> unique_key_to_hash;
    std::set<uint64_t> unique_hashes;
    for (int i = 0; i < num_rows; ++i) {
      std::map<int, uint64_t>::iterator iter = unique_key_to_hash.find(row_ids[i]);
      if (iter == unique_key_to_hash.end()) {
        unique_key_to_hash.insert(std::make_pair(row_ids[i], hashes_scalar64[i]));
      } else {
        ASSERT_EQ(iter->second, hashes_scalar64[i]);
      }
      if (unique_hashes.find(hashes_scalar64[i]) == unique_hashes.end()) {
        unique_hashes.insert(hashes_scalar64[i]);
      }
    }
    float percent_hash_collisions =
        100.0f * static_cast<float>(num_unique - unique_hashes.size()) /
        static_cast<float>(num_unique);
    SCOPED_TRACE("percent_hash_collisions " + std::to_string(percent_hash_collisions));
    ASSERT_LT(percent_hash_collisions, 5.0f) << "hash collision rate was too high";
  }
};

template <typename Type>
void RunTestVectorHash() {
  random::pcg32_fast gen(/*seed=*/0);

#ifdef ARROW_VALGRIND
  constexpr int kNumTests = 5;
#else
  constexpr int kNumTests = 40;
#endif

  constexpr int min_length = 0;
  constexpr int max_length = 50;

  for (bool use_32bit_hash : {true, false}) {
    for (bool use_varlen_input : {false, true}) {
      for (int itest = 0; itest < kNumTests; ++itest) {
        TestVectorHash::RunSingle<Type>(&gen, use_32bit_hash, use_varlen_input,
                                        min_length, max_length);
      }
    }
  }
}

TEST(VectorHash, BasicBinary) { RunTestVectorHash<BinaryType>(); }

TEST(VectorHash, BasicLargeBinary) { RunTestVectorHash<LargeBinaryType>(); }

TEST(VectorHash, BasicString) { RunTestVectorHash<StringType>(); }

TEST(VectorHash, BasicLargeString) { RunTestVectorHash<LargeStringType>(); }

}  // namespace compute
}  // namespace arrow
