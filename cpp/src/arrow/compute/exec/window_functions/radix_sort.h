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

#pragma once

#include <cstdint>
#include "arrow/util/bit_util.h"

namespace arrow {
namespace compute {

class RadixSort {
 public:
  static void Sort(int64_t num_rows, int64_t value_max, const int64_t* values,
                   int64_t* values_sorted, int64_t* permutation) {
    int num_bits = std::max(
        1, 64 - arrow::bit_util::CountLeadingZeros(static_cast<uint64_t>(value_max)));
    constexpr int num_bits_per_group = 8;
    constexpr int num_bit_groups_max = 8;
    const int64_t bit_group_mask = ((1LL << num_bits_per_group) - 1LL);
    int num_bit_groups =
        static_cast<int>(arrow::bit_util::CeilDiv(num_bits, num_bits_per_group));
    int64_t counters[num_bit_groups_max << num_bits_per_group];
    memset(counters, 0, sizeof(counters[0]) * (num_bit_groups << num_bits_per_group));
    for (int64_t i = 0; i < num_rows; ++i) {
      int64_t value = values[i];
      for (int bit_group = 0; bit_group < num_bit_groups; ++bit_group) {
        int64_t bucket_id = (value >> (num_bits_per_group * bit_group)) & bit_group_mask;
        ++counters[(bit_group << num_bits_per_group) + bucket_id];
      }
    }
    for (int bit_group = 0; bit_group < num_bit_groups; ++bit_group) {
      int64_t sum = 0;
      int64_t* counters_base = counters + (bit_group << num_bits_per_group);
      for (int i = 0; i < (1 << num_bits_per_group); ++i) {
        int64_t sum_next = sum + counters_base[i];
        counters_base[i] = sum;
        sum = sum_next;
      }
    }
    std::vector<int64_t> values_sorted_2nd;
    std::vector<int64_t> permutation_2nd;
    if (num_bit_groups > 1) {
      values_sorted_2nd.resize(num_rows);
      permutation_2nd.resize(num_rows);
    }
    for (int bit_group = 0; bit_group < num_bit_groups; ++bit_group) {
      int64_t* values_sorted_target =
          ((num_bit_groups - bit_group) & 1) ? values_sorted : values_sorted_2nd.data();
      int64_t* permutation_target =
          ((num_bit_groups - bit_group) & 1) ? permutation : permutation_2nd.data();
      int64_t* counters_base = counters + (bit_group << num_bits_per_group);
      if (bit_group == 0) {
        for (int64_t i = 0; i < num_rows; ++i) {
          int64_t value = values[i];
          int64_t bucket = value & bit_group_mask;
          int64_t pos = counters_base[bucket]++;
          values_sorted_target[pos] = value;
          permutation_target[pos] = i;
        }
      } else {
        const int64_t* values_sorted_source = values_sorted_target == values_sorted
                                                  ? values_sorted_2nd.data()
                                                  : values_sorted;
        const int64_t* permutation_source =
            permutation_target == permutation ? permutation_2nd.data() : permutation;
        for (int64_t i = 0; i < num_rows; ++i) {
          int64_t value = values_sorted_source[i];
          int64_t bucket = ((value >> (num_bits_per_group * bit_group)) & bit_group_mask);
          int64_t pos = counters_base[bucket]++;
          values_sorted_target[pos] = value;
          permutation_target[pos] = permutation_source[i];
        }
      }
    }
  }
};

}  // namespace compute
}  // namespace arrow
