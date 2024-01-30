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
#include "arrow/testing/util.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/pcg_random.h"

namespace arrow {

using internal::checked_pointer_cast;
using internal::CpuInfo;

namespace compute {

std::vector<int64_t> HardwareFlagsForTesting() {
  // Our key-hash and key-map routines currently only have AVX2 optimizations
  return GetSupportedHardwareFlags({CpuInfo::AVX2});
}

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

    // For each tested hardware flags, we will compute the hashes and check
    // them for consistency.
    const auto hardware_flags_for_testing = HardwareFlagsForTesting();
    ASSERT_GT(hardware_flags_for_testing.size(), 0);
    std::vector<std::vector<uint32_t>> hashes32(hardware_flags_for_testing.size());
    std::vector<std::vector<uint64_t>> hashes64(hardware_flags_for_testing.size());
    for (auto& h : hashes32) {
      h.resize(num_rows);
    }
    for (auto& h : hashes64) {
      h.resize(num_rows);
    }

    constexpr int mini_batch_size = 1024;
    std::vector<uint32_t> temp_buffer;
    temp_buffer.resize(mini_batch_size * 4);

    for (int i = 0; i < static_cast<int>(hardware_flags_for_testing.size()); ++i) {
      const auto hardware_flags = hardware_flags_for_testing[i];
      if (use_32bit_hash) {
        if (!use_varlen_input) {
          Hashing32::HashFixed(hardware_flags,
                               /*combine_hashes=*/false, num_rows, fixed_length, keys,
                               hashes32[i].data(), temp_buffer.data());
        } else {
          for (int first_row = 0; first_row < num_rows;) {
            int batch_size_next = std::min(num_rows - first_row, mini_batch_size);

            Hashing32::HashVarLen(hardware_flags,
                                  /*combine_hashes=*/false, batch_size_next,
                                  key_offsets + first_row, keys,
                                  hashes32[i].data() + first_row, temp_buffer.data());

            first_row += batch_size_next;
          }
        }
        for (int j = 0; j < num_rows; ++j) {
          hashes64[i][j] = hashes32[i][j];
        }
      } else {
        if (!use_varlen_input) {
          Hashing64::HashFixed(
              /*combine_hashes=*/false, num_rows, fixed_length, keys, hashes64[i].data());
        } else {
          Hashing64::HashVarLen(
              /*combine_hashes=*/false, num_rows, key_offsets, keys, hashes64[i].data());
        }
      }
    }

    // Verify that all implementations (scalar, SIMD) give the same hashes
    //
    const auto& hashes_scalar64 = hashes64[0];
    for (int i = 0; i < static_cast<int>(hardware_flags_for_testing.size()); ++i) {
      for (int j = 0; j < num_rows; ++j) {
        ASSERT_EQ(hashes64[i][j], hashes_scalar64[j])
            << "scalar and simd approaches yielded different hashes";
      }
    }

    // Verify that the same key appearing multiple times generates the same hash
    // each time. Measure the number of unique hashes and compare to the number
    // of unique keys.
    //
    std::unordered_map<int, uint64_t> unique_key_to_hash;
    std::unordered_set<uint64_t> unique_hashes;
    for (int i = 0; i < num_rows; ++i) {
      auto [it, inserted] =
          unique_key_to_hash.try_emplace(row_ids[i], hashes_scalar64[i]);
      if (!inserted) {
        ASSERT_EQ(it->second, hashes_scalar64[i]);
      }
      unique_hashes.insert(hashes_scalar64[i]);
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

void HashFixedLengthFrom(int key_length, int num_rows, int start_row) {
  int num_rows_to_hash = num_rows - start_row;
  auto num_bytes_aligned = arrow::bit_util::RoundUpToMultipleOf64(key_length * num_rows);

  const auto hardware_flags_for_testing = HardwareFlagsForTesting();
  ASSERT_GT(hardware_flags_for_testing.size(), 0);

  std::vector<std::vector<uint32_t>> hashes32(hardware_flags_for_testing.size());
  std::vector<std::vector<uint64_t>> hashes64(hardware_flags_for_testing.size());
  for (auto& h : hashes32) {
    h.resize(num_rows_to_hash);
  }
  for (auto& h : hashes64) {
    h.resize(num_rows_to_hash);
  }

  FixedSizeBinaryBuilder keys_builder(fixed_size_binary(key_length));
  for (int j = 0; j < num_rows; ++j) {
    ASSERT_OK(keys_builder.Append(std::string(key_length, 42)));
  }
  ASSERT_OK_AND_ASSIGN(auto keys, keys_builder.Finish());
  // Make sure the buffer is aligned as expected.
  ASSERT_EQ(keys->data()->buffers[1]->capacity(), num_bytes_aligned);

  constexpr int mini_batch_size = 1024;
  std::vector<uint32_t> temp_buffer;
  temp_buffer.resize(mini_batch_size * 4);

  for (int i = 0; i < static_cast<int>(hardware_flags_for_testing.size()); ++i) {
    const auto hardware_flags = hardware_flags_for_testing[i];
    Hashing32::HashFixed(hardware_flags,
                         /*combine_hashes=*/false, num_rows_to_hash, key_length,
                         keys->data()->GetValues<uint8_t>(1) + start_row * key_length,
                         hashes32[i].data(), temp_buffer.data());
    Hashing64::HashFixed(
        /*combine_hashes=*/false, num_rows_to_hash, key_length,
        keys->data()->GetValues<uint8_t>(1) + start_row * key_length, hashes64[i].data());
  }

  // Verify that all implementations (scalar, SIMD) give the same hashes.
  for (int i = 1; i < static_cast<int>(hardware_flags_for_testing.size()); ++i) {
    for (int j = 0; j < num_rows_to_hash; ++j) {
      ASSERT_EQ(hashes32[i][j], hashes32[0][j])
          << "scalar and simd approaches yielded different 32-bit hashes";
      ASSERT_EQ(hashes64[i][j], hashes64[0][j])
          << "scalar and simd approaches yielded different 64-bit hashes";
    }
  }
}

// Some carefully chosen cases that may cause troubles like GH-39778.
TEST(VectorHash, FixedLengthTailByteSafety) {
  // Tow cases of key_length < stripe (16-byte).
  HashFixedLengthFrom(/*key_length=*/3, /*num_rows=*/1450, /*start_row=*/1447);
  HashFixedLengthFrom(/*key_length=*/5, /*num_rows=*/883, /*start_row=*/858);
  // Case of key_length > stripe (16-byte).
  HashFixedLengthFrom(/*key_length=*/19, /*num_rows=*/64, /*start_row=*/63);
}

}  // namespace compute
}  // namespace arrow
