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

#include <immintrin.h>

#include "arrow/acero/util.h"
#include "arrow/util/bit_util.h"

namespace arrow {
namespace util {

#if defined(ARROW_HAVE_AVX2)

void bit_util::bits_to_indexes_avx2(int bit_to_search, const int num_bits,
                                    const uint8_t* bits, int* num_indexes,
                                    uint16_t* indexes, uint16_t base_index) {
  if (bit_to_search == 0) {
    bits_to_indexes_imp_avx2<0>(num_bits, bits, num_indexes, indexes, base_index);
  } else {
    ARROW_DCHECK(bit_to_search == 1);
    bits_to_indexes_imp_avx2<1>(num_bits, bits, num_indexes, indexes, base_index);
  }
}

template <int bit_to_search>
void bit_util::bits_to_indexes_imp_avx2(const int num_bits, const uint8_t* bits,
                                        int* num_indexes, uint16_t* indexes,
                                        uint16_t base_index) {
  // 64 bits at a time
  constexpr int unroll = 64;

  // The caller takes care of processing the remaining bits at the end outside of the
  // multiples of 64
  ARROW_DCHECK(num_bits % unroll == 0);

  constexpr uint64_t kEachByteIs1 = 0X0101010101010101ULL;
  constexpr uint64_t kEachByteIs8 = 0x0808080808080808ULL;
  constexpr uint64_t kByteSequence0To7 = 0x0706050403020100ULL;

  uint8_t byte_indexes[64];
  const uint64_t incr = kEachByteIs8;
  const uint64_t mask = kByteSequence0To7;
  *num_indexes = 0;
  for (int i = 0; i < num_bits / unroll; ++i) {
    uint64_t word = reinterpret_cast<const uint64_t*>(bits)[i];
    if (bit_to_search == 0) {
      word = ~word;
    }
    uint64_t base = 0;
    int num_indexes_loop = 0;
    while (word) {
      uint64_t byte_indexes_next =
          _pext_u64(mask, _pdep_u64(word, kEachByteIs1) * 0xff) + base;
      *reinterpret_cast<uint64_t*>(byte_indexes + num_indexes_loop) = byte_indexes_next;
      base += incr;
      num_indexes_loop += static_cast<int>(arrow::bit_util::PopCount(word & 0xff));
      word >>= 8;
    }
    // Unpack indexes to 16-bits and either add the base of i * 64 or shuffle input
    // indexes
    for (int j = 0; j < (num_indexes_loop + 15) / 16; ++j) {
      __m256i output = _mm256_cvtepi8_epi16(
          _mm_loadu_si128(reinterpret_cast<const __m128i*>(byte_indexes) + j));
      output = _mm256_add_epi16(output, _mm256_set1_epi16(i * 64 + base_index));
      _mm256_storeu_si256(((__m256i*)(indexes + *num_indexes)) + j, output);
    }
    *num_indexes += num_indexes_loop;
  }
}

void bit_util::bits_filter_indexes_avx2(int bit_to_search, const int num_bits,
                                        const uint8_t* bits,
                                        const uint16_t* input_indexes, int* num_indexes,
                                        uint16_t* indexes) {
  if (bit_to_search == 0) {
    bits_filter_indexes_imp_avx2<0>(num_bits, bits, input_indexes, num_indexes, indexes);
  } else {
    bits_filter_indexes_imp_avx2<1>(num_bits, bits, input_indexes, num_indexes, indexes);
  }
}

template <int bit_to_search>
void bit_util::bits_filter_indexes_imp_avx2(const int num_bits, const uint8_t* bits,
                                            const uint16_t* input_indexes,
                                            int* out_num_indexes, uint16_t* indexes) {
  // 64 bits at a time
  constexpr int unroll = 64;

  // The caller takes care of processing the remaining bits at the end outside of the
  // multiples of 64
  ARROW_DCHECK(num_bits % unroll == 0);

  constexpr uint64_t kRepeatedBitPattern0001 = 0x1111111111111111ULL;
  constexpr uint64_t k4BitSequence0To15 = 0xfedcba9876543210ULL;
  constexpr uint64_t kByteSequence_0_0_1_1_2_2_3_3 = 0x0303020201010000ULL;
  constexpr uint64_t kByteSequence_4_4_5_5_6_6_7_7 = 0x0707060605050404ULL;
  constexpr uint64_t kByteSequence_0_2_4_6_8_10_12_14 = 0x0e0c0a0806040200ULL;
  constexpr uint64_t kByteSequence_1_3_5_7_9_11_13_15 = 0x0f0d0b0907050301ULL;
  constexpr uint64_t kByteSequence_0_8_1_9_2_10_3_11 = 0x0b030a0209010800ULL;
  constexpr uint64_t kByteSequence_4_12_5_13_6_14_7_15 = 0x0f070e060d050c04ULL;

  const uint64_t mask = k4BitSequence0To15;
  int num_indexes = 0;
  for (int i = 0; i < num_bits / unroll; ++i) {
    uint64_t word = reinterpret_cast<const uint64_t*>(bits)[i];
    if (bit_to_search == 0) {
      word = ~word;
    }

    int loop_id = 0;
    while (word) {
      uint64_t indexes_4bit =
          _pext_u64(mask, _pdep_u64(word, kRepeatedBitPattern0001) * 0xf);
      // Unpack 4 bit indexes to 8 bits
      __m256i indexes_8bit = _mm256_set1_epi64x(indexes_4bit);
      indexes_8bit = _mm256_shuffle_epi8(
          indexes_8bit,
          _mm256_setr_epi64x(kByteSequence_0_0_1_1_2_2_3_3, kByteSequence_4_4_5_5_6_6_7_7,
                             kByteSequence_0_0_1_1_2_2_3_3,
                             kByteSequence_4_4_5_5_6_6_7_7));
      indexes_8bit = _mm256_blendv_epi8(
          _mm256_and_si256(indexes_8bit, _mm256_set1_epi8(0x0f)),
          _mm256_and_si256(_mm256_srli_epi32(indexes_8bit, 4), _mm256_set1_epi8(0x0f)),
          _mm256_set1_epi16(static_cast<uint16_t>(0xff00)));
      __m256i input =
          _mm256_loadu_si256(((const __m256i*)input_indexes) + 4 * i + loop_id);
      // Shuffle bytes to get low bytes in the first 128-bit lane and high bytes in the
      // second
      input = _mm256_shuffle_epi8(
          input, _mm256_setr_epi64x(
                     kByteSequence_0_2_4_6_8_10_12_14, kByteSequence_1_3_5_7_9_11_13_15,
                     kByteSequence_0_2_4_6_8_10_12_14, kByteSequence_1_3_5_7_9_11_13_15));
      input = _mm256_permute4x64_epi64(input, 0xd8);  // 0b11011000
      // Apply permutation
      __m256i output = _mm256_shuffle_epi8(input, indexes_8bit);
      // Move low and high bytes across 128-bit lanes to assemble back 16-bit indexes.
      // (This is the reverse of the byte permutation we did on the input)
      output = _mm256_permute4x64_epi64(output,
                                        0xd8);  // The reverse of swapping 2nd and 3rd
                                                // 64-bit element is the same permutation
      output = _mm256_shuffle_epi8(output,
                                   _mm256_setr_epi64x(kByteSequence_0_8_1_9_2_10_3_11,
                                                      kByteSequence_4_12_5_13_6_14_7_15,
                                                      kByteSequence_0_8_1_9_2_10_3_11,
                                                      kByteSequence_4_12_5_13_6_14_7_15));
      _mm256_storeu_si256((__m256i*)(indexes + num_indexes), output);
      num_indexes += static_cast<int>(arrow::bit_util::PopCount(word & 0xffff));
      word >>= 16;
      ++loop_id;
    }
  }

  *out_num_indexes = num_indexes;
}

void bit_util::bits_to_bytes_avx2(const int num_bits, const uint8_t* bits,
                                  uint8_t* bytes) {
  constexpr int unroll = 32;

  constexpr uint64_t kEachByteIs1 = 0x0101010101010101ULL;
  constexpr uint64_t kEachByteIs2 = 0x0202020202020202ULL;
  constexpr uint64_t kEachByteIs3 = 0x0303030303030303ULL;
  constexpr uint64_t kByteSequencePowersOf2 = 0x8040201008040201ULL;

  // Processing 32 bits at a time
  for (int i = 0; i < num_bits / unroll; ++i) {
    __m256i unpacked = _mm256_set1_epi32(reinterpret_cast<const uint32_t*>(bits)[i]);
    unpacked = _mm256_shuffle_epi8(
        unpacked, _mm256_setr_epi64x(0ULL, kEachByteIs1, kEachByteIs2, kEachByteIs3));
    __m256i bits_in_bytes = _mm256_set1_epi64x(kByteSequencePowersOf2);
    unpacked =
        _mm256_cmpeq_epi8(bits_in_bytes, _mm256_and_si256(unpacked, bits_in_bytes));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(bytes) + i, unpacked);
  }
}

void bit_util::bytes_to_bits_avx2(const int num_bits, const uint8_t* bytes,
                                  uint8_t* bits) {
  constexpr int unroll = 32;
  // Processing 32 bits at a time
  for (int i = 0; i < num_bits / unroll; ++i) {
    reinterpret_cast<uint32_t*>(bits)[i] = _mm256_movemask_epi8(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bytes) + i));
  }
}

bool bit_util::are_all_bytes_zero_avx2(const uint8_t* bytes, uint32_t num_bytes) {
  __m256i result_or = _mm256_setzero_si256();
  uint32_t i;
  for (i = 0; i < num_bytes / 32; ++i) {
    __m256i x = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bytes) + i);
    result_or = _mm256_or_si256(result_or, x);
  }
  result_or = _mm256_cmpeq_epi8(result_or, _mm256_set1_epi8(0));
  result_or =
      _mm256_andnot_si256(result_or, _mm256_set1_epi8(static_cast<uint8_t>(0xff)));
  uint32_t result_or32 = _mm256_movemask_epi8(result_or);
  if (num_bytes % 32 > 0) {
    uint64_t tail[4] = {0, 0, 0, 0};
    result_or32 |= memcmp(bytes + i * 32, tail, num_bytes % 32);
  }
  return result_or32 == 0;
}

#endif  // ARROW_HAVE_AVX2

}  // namespace util
}  // namespace arrow
