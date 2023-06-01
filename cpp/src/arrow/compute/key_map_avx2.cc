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

#include "arrow/compute/key_map.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

#if defined(ARROW_HAVE_AVX2)

// This is more or less translation of equivalent scalar code, adjusted for a
// different instruction set (e.g. missing leading zero count instruction).
//
// Returns the number of hashes actually processed, which may be less than
// requested due to alignment required by SIMD.
//
int SwissTable::early_filter_imp_avx2_x8(const int num_hashes, const uint32_t* hashes,
                                         uint8_t* out_match_bitvector,
                                         uint8_t* out_local_slots) const {
  // Number of inputs processed together in a loop
  constexpr int unroll = 8;

  const int num_group_id_bits = num_groupid_bits_from_log_blocks(log_blocks_);
  const __m256i* vhash_ptr = reinterpret_cast<const __m256i*>(hashes);
  const __m256i vstamp_mask = _mm256_set1_epi32((1 << bits_stamp_) - 1);

  for (int i = 0; i < num_hashes / unroll; ++i) {
    constexpr uint64_t kEachByteIs8 = 0x0808080808080808ULL;
    constexpr uint64_t kByteSequenceOfPowersOf2 = 0x8040201008040201ULL;

    // Calculate block index and hash stamp for a byte in a block
    //
    __m256i vhash = _mm256_loadu_si256(vhash_ptr + i);
    __m256i vblock_id = _mm256_srlv_epi32(
        vhash, _mm256_set1_epi32(bits_hash_ - bits_stamp_ - log_blocks_));
    __m256i vstamp = _mm256_and_si256(vblock_id, vstamp_mask);
    vblock_id = _mm256_srli_epi32(vblock_id, bits_stamp_);

    // We now split inputs and process 4 at a time,
    // in order to process 64-bit blocks
    //
    __m256i vblock_offset =
        _mm256_mullo_epi32(vblock_id, _mm256_set1_epi32(num_group_id_bits + 8));
    __m256i voffset_A = _mm256_and_si256(vblock_offset, _mm256_set1_epi64x(0xffffffff));
    __m256i vstamp_A = _mm256_and_si256(vstamp, _mm256_set1_epi64x(0xffffffff));
    __m256i voffset_B = _mm256_srli_epi64(vblock_offset, 32);
    __m256i vstamp_B = _mm256_srli_epi64(vstamp, 32);

    auto blocks_i64 =
        reinterpret_cast<arrow::util::int64_for_gather_t*>(blocks_->mutable_data());
    auto vblock_A = _mm256_i64gather_epi64(blocks_i64, voffset_A, 1);
    auto vblock_B = _mm256_i64gather_epi64(blocks_i64, voffset_B, 1);
    __m256i vblock_highbits_A =
        _mm256_cmpeq_epi8(vblock_A, _mm256_set1_epi8(static_cast<unsigned char>(0x80)));
    __m256i vblock_highbits_B =
        _mm256_cmpeq_epi8(vblock_B, _mm256_set1_epi8(static_cast<unsigned char>(0x80)));
    __m256i vbyte_repeat_pattern =
        _mm256_setr_epi64x(0ULL, kEachByteIs8, 0ULL, kEachByteIs8);
    vstamp_A = _mm256_shuffle_epi8(
        vstamp_A, _mm256_or_si256(vbyte_repeat_pattern, vblock_highbits_A));
    vstamp_B = _mm256_shuffle_epi8(
        vstamp_B, _mm256_or_si256(vbyte_repeat_pattern, vblock_highbits_B));
    __m256i vmatches_A = _mm256_cmpeq_epi8(vblock_A, vstamp_A);
    __m256i vmatches_B = _mm256_cmpeq_epi8(vblock_B, vstamp_B);

    // In case when there are no matches in slots and the block is full (no empty slots),
    // pretend that there is a match in the last slot.
    //
    vmatches_A = _mm256_or_si256(
        vmatches_A, _mm256_andnot_si256(vblock_highbits_A, _mm256_set1_epi64x(0xff)));
    vmatches_B = _mm256_or_si256(
        vmatches_B, _mm256_andnot_si256(vblock_highbits_B, _mm256_set1_epi64x(0xff)));

    __m256i vmatch_found = _mm256_andnot_si256(
        _mm256_blend_epi32(_mm256_cmpeq_epi64(vmatches_A, _mm256_setzero_si256()),
                           _mm256_cmpeq_epi64(vmatches_B, _mm256_setzero_si256()),
                           0xaa),  // 0b10101010
        _mm256_set1_epi8(static_cast<unsigned char>(0xff)));
    vmatches_A =
        _mm256_sad_epu8(_mm256_and_si256(_mm256_or_si256(vmatches_A, vblock_highbits_A),
                                         _mm256_set1_epi64x(kByteSequenceOfPowersOf2)),
                        _mm256_setzero_si256());
    vmatches_B =
        _mm256_sad_epu8(_mm256_and_si256(_mm256_or_si256(vmatches_B, vblock_highbits_B),
                                         _mm256_set1_epi64x(kByteSequenceOfPowersOf2)),
                        _mm256_setzero_si256());
    __m256i vmatches = _mm256_or_si256(vmatches_A, _mm256_slli_epi64(vmatches_B, 32));

    // We are now back to processing 8 at a time.
    // Each lane contains 8-bit bit vector marking slots that are matches.
    // We need to find leading zeroes count for all slots.
    //
    // Emulating lzcnt in lowest bytes of 32-bit elements
    __m256i vgt = _mm256_cmpgt_epi32(_mm256_set1_epi32(16), vmatches);
    __m256i vlocal_slot =
        _mm256_blendv_epi8(_mm256_srli_epi32(vmatches, 4),
                           _mm256_and_si256(vmatches, _mm256_set1_epi32(0x0f)), vgt);
    vlocal_slot = _mm256_shuffle_epi8(
        _mm256_setr_epi8(4, 3, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 4, 3, 2, 2, 1, 1,
                         1, 1, 0, 0, 0, 0, 0, 0, 0, 0),
        vlocal_slot);
    vlocal_slot = _mm256_add_epi32(_mm256_and_si256(vlocal_slot, _mm256_set1_epi32(0xff)),
                                   _mm256_and_si256(vgt, _mm256_set1_epi32(4)));

    // Convert slot id relative to the block to slot id relative to the beginnning of the
    // table
    //
    uint64_t local_slot = _mm256_extract_epi64(
        _mm256_permutevar8x32_epi32(
            _mm256_shuffle_epi8(
                vlocal_slot, _mm256_setr_epi32(0x0c080400, 0, 0, 0, 0x0c080400, 0, 0, 0)),
            _mm256_setr_epi32(0, 4, 0, 0, 0, 0, 0, 0)),
        0);
    (reinterpret_cast<uint64_t*>(out_local_slots))[i] = local_slot;

    // Convert match found vector from 32-bit elements to bit vector
    out_match_bitvector[i] = _pext_u32(_mm256_movemask_epi8(vmatch_found),
                                       0x11111111);  // 0b00010001 repeated 4x
  }

  return num_hashes - (num_hashes % unroll);
}

// Take a set of 16 64-bit elements,
// Output one AVX2 register per byte (0 to 7), containing a sequence of 16 bytes,
// one from each input 64-bit word, all from the same position in 64-bit word.
// 16 bytes are replicated in lower and upper half of each output register.
//
inline void split_bytes_avx2(__m256i word0, __m256i word1, __m256i word2, __m256i word3,
                             __m256i& byte0, __m256i& byte1, __m256i& byte2,
                             __m256i& byte3, __m256i& byte4, __m256i& byte5,
                             __m256i& byte6, __m256i& byte7) {
  __m256i word01lo = _mm256_unpacklo_epi8(
      word0, word1);  // {a0, e0, a1, e1, ... a7, e7, c0, g0, c1, g1, ... c7, g7}
  __m256i word23lo = _mm256_unpacklo_epi8(
      word2, word3);  // {i0, m0, i1, m1, ... i7, m7, k0, o0, k1, o1, ... k7, o7}
  __m256i word01hi = _mm256_unpackhi_epi8(
      word0, word1);  // {b0, f0, b1, f1, ... b7, f1, d0, h0, d1, h1, ... d7, h7}
  __m256i word23hi = _mm256_unpackhi_epi8(
      word2, word3);  // {j0, n0, j1, n1, ... j7, n7, l0, p0, l1, p1, ... l7, p7}

  __m256i a =
      _mm256_unpacklo_epi16(word01lo, word01hi);  // {a0, e0, b0, f0, ... a3, e3, b3, f3,
                                                  // c0, g0, d0, h0, ... c3, g3, d3, h3}
  __m256i b =
      _mm256_unpacklo_epi16(word23lo, word23hi);  // {i0, m0, j0, n0, ... i3, m3, j3, n3,
                                                  // k0, o0, l0, p0, ... k3, o3, l3, p3}
  __m256i c =
      _mm256_unpackhi_epi16(word01lo, word01hi);  // {a4, e4, b4, f4, ... a7, e7, b7, f7,
                                                  // c4, g4, d4, h4, ... c7, g7, d7, h7}
  __m256i d =
      _mm256_unpackhi_epi16(word23lo, word23hi);  // {i4, m4, j4, n4, ... i7, m7, j7, n7,
                                                  // k4, o4, l4, p4, ... k7, o7, l7, p7}

  __m256i byte01 = _mm256_unpacklo_epi32(
      a, b);  // {a0, e0, b0, f0, i0, m0, j0, n0, a1, e1, b1, f1, i1, m1, j1, n1,
              // c0, g0, d0, h0, k0, o0, l0, p0, ...}
  __m256i shuffle_const =
      _mm256_setr_epi8(0, 2, 8, 10, 1, 3, 9, 11, 4, 6, 12, 14, 5, 7, 13, 15, 0, 2, 8, 10,
                       1, 3, 9, 11, 4, 6, 12, 14, 5, 7, 13, 15);
  byte01 = _mm256_permute4x64_epi64(
      byte01, 0xd8);  // 11011000 b - swapping middle two 64-bit elements
  byte01 = _mm256_shuffle_epi8(byte01, shuffle_const);
  __m256i byte23 = _mm256_unpackhi_epi32(a, b);
  byte23 = _mm256_permute4x64_epi64(byte23, 0xd8);
  byte23 = _mm256_shuffle_epi8(byte23, shuffle_const);
  __m256i byte45 = _mm256_unpacklo_epi32(c, d);
  byte45 = _mm256_permute4x64_epi64(byte45, 0xd8);
  byte45 = _mm256_shuffle_epi8(byte45, shuffle_const);
  __m256i byte67 = _mm256_unpackhi_epi32(c, d);
  byte67 = _mm256_permute4x64_epi64(byte67, 0xd8);
  byte67 = _mm256_shuffle_epi8(byte67, shuffle_const);

  byte0 = _mm256_permute4x64_epi64(byte01, 0x44);  // 01000100 b
  byte1 = _mm256_permute4x64_epi64(byte01, 0xee);  // 11101110 b
  byte2 = _mm256_permute4x64_epi64(byte23, 0x44);  // 01000100 b
  byte3 = _mm256_permute4x64_epi64(byte23, 0xee);  // 11101110 b
  byte4 = _mm256_permute4x64_epi64(byte45, 0x44);  // 01000100 b
  byte5 = _mm256_permute4x64_epi64(byte45, 0xee);  // 11101110 b
  byte6 = _mm256_permute4x64_epi64(byte67, 0x44);  // 01000100 b
  byte7 = _mm256_permute4x64_epi64(byte67, 0xee);  // 11101110 b
}

// This one can only process a multiple of 32 values.
// The caller needs to process the remaining tail, if the input is not divisible by 32,
// using a different method.
// TODO: Explain the idea behind storing arrays in SIMD registers.
// Explain why it is faster with SIMD than using memory loads.
//
// Returns the number of hashes actually processed, which may be less than
// requested due to alignment required by SIMD.
//
int SwissTable::early_filter_imp_avx2_x32(const int num_hashes, const uint32_t* hashes,
                                          uint8_t* out_match_bitvector,
                                          uint8_t* out_local_slots) const {
  constexpr int unroll = 32;

  // There is a limit on the number of input blocks,
  // because we want to store all their data in a set of AVX2 registers.
  ARROW_DCHECK(log_blocks_ <= 4);

  // Remember that block bytes and group id bytes are in opposite orders in memory of hash
  // table. We put them in the same order.
  __m256i vblock_byte0, vblock_byte1, vblock_byte2, vblock_byte3, vblock_byte4,
      vblock_byte5, vblock_byte6, vblock_byte7;
  // What we output if there is no match in the block
  __m256i vslot_empty_or_end;

  constexpr uint32_t k4ByteSequence_0_4_8_12 = 0x0c080400;
  constexpr uint32_t k4ByteSequence_1_5_9_13 = 0x0d090501;
  constexpr uint32_t k4ByteSequence_2_6_10_14 = 0x0e0a0602;
  constexpr uint32_t k4ByteSequence_3_7_11_15 = 0x0f0b0703;
  constexpr uint64_t kByteSequence7DownTo0 = 0x0001020304050607ULL;
  constexpr uint64_t kByteSequence15DownTo8 = 0x08090A0B0C0D0E0FULL;

  // Bit unpack group ids into 1B.
  // Assemble the sequence of block bytes.
  uint64_t block_bytes[16];
  const int num_groupid_bits = num_groupid_bits_from_log_blocks(log_blocks_);
  for (int i = 0; i < (1 << log_blocks_); ++i) {
    uint64_t in_blockbytes =
        *reinterpret_cast<const uint64_t*>(blocks_->data() + (8 + num_groupid_bits) * i);
    block_bytes[i] = in_blockbytes;
  }

  // Split a sequence of 64-bit words into SIMD vectors holding individual bytes
  __m256i vblock_words0 =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block_bytes) + 0);
  __m256i vblock_words1 =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block_bytes) + 1);
  __m256i vblock_words2 =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block_bytes) + 2);
  __m256i vblock_words3 =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block_bytes) + 3);
  // Reverse the bytes in blocks
  __m256i vshuffle_const =
      _mm256_setr_epi64x(kByteSequence7DownTo0, kByteSequence15DownTo8,
                         kByteSequence7DownTo0, kByteSequence15DownTo8);
  vblock_words0 = _mm256_shuffle_epi8(vblock_words0, vshuffle_const);
  vblock_words1 = _mm256_shuffle_epi8(vblock_words1, vshuffle_const);
  vblock_words2 = _mm256_shuffle_epi8(vblock_words2, vshuffle_const);
  vblock_words3 = _mm256_shuffle_epi8(vblock_words3, vshuffle_const);
  split_bytes_avx2(vblock_words0, vblock_words1, vblock_words2, vblock_words3,
                   vblock_byte0, vblock_byte1, vblock_byte2, vblock_byte3, vblock_byte4,
                   vblock_byte5, vblock_byte6, vblock_byte7);

  // Calculate the slot to output when there is no match in a block.
  // It will be the index of the first empty slot or 7 (the number of slots in block)
  // if there are no empty slots.
  vslot_empty_or_end = _mm256_set1_epi8(7);
  {
    __m256i vis_empty;
#define CMP(VBLOCKBYTE, BYTENUM)                                                         \
  vis_empty =                                                                            \
      _mm256_cmpeq_epi8(VBLOCKBYTE, _mm256_set1_epi8(static_cast<unsigned char>(0x80))); \
  vslot_empty_or_end =                                                                   \
      _mm256_blendv_epi8(vslot_empty_or_end, _mm256_set1_epi8(BYTENUM), vis_empty);
    CMP(vblock_byte7, 7);
    CMP(vblock_byte6, 6);
    CMP(vblock_byte5, 5);
    CMP(vblock_byte4, 4);
    CMP(vblock_byte3, 3);
    CMP(vblock_byte2, 2);
    CMP(vblock_byte1, 1);
    CMP(vblock_byte0, 0);
#undef CMP
  }
  __m256i vblock_is_full = _mm256_andnot_si256(
      _mm256_cmpeq_epi8(vblock_byte7, _mm256_set1_epi8(static_cast<unsigned char>(0x80))),
      _mm256_set1_epi8(static_cast<unsigned char>(0xff)));

  const int block_id_mask = (1 << log_blocks_) - 1;

  for (int i = 0; i < num_hashes / unroll; ++i) {
    __m256i vhash0 =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes) + 4 * i + 0);
    __m256i vhash1 =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes) + 4 * i + 1);
    __m256i vhash2 =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes) + 4 * i + 2);
    __m256i vhash3 =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes) + 4 * i + 3);

    // We will get input in byte lanes in the order: [0, 8, 16, 24, 1, 9, 17, 25, 2, 10,
    // 18, 26, ...]
    vhash0 = _mm256_or_si256(_mm256_srli_epi32(vhash0, 16),
                             _mm256_and_si256(vhash2, _mm256_set1_epi32(0xffff0000)));
    vhash1 = _mm256_or_si256(_mm256_srli_epi32(vhash1, 16),
                             _mm256_and_si256(vhash3, _mm256_set1_epi32(0xffff0000)));
    __m256i vstamp_A = _mm256_and_si256(
        _mm256_srlv_epi32(vhash0, _mm256_set1_epi32(16 - log_blocks_ - 7)),
        _mm256_set1_epi16(0x7f));
    __m256i vstamp_B = _mm256_and_si256(
        _mm256_srlv_epi32(vhash1, _mm256_set1_epi32(16 - log_blocks_ - 7)),
        _mm256_set1_epi16(0x7f));
    __m256i vstamp = _mm256_or_si256(vstamp_A, _mm256_slli_epi16(vstamp_B, 8));
    __m256i vblock_id_A =
        _mm256_and_si256(_mm256_srlv_epi32(vhash0, _mm256_set1_epi32(16 - log_blocks_)),
                         _mm256_set1_epi16(block_id_mask));
    __m256i vblock_id_B =
        _mm256_and_si256(_mm256_srlv_epi32(vhash1, _mm256_set1_epi32(16 - log_blocks_)),
                         _mm256_set1_epi16(block_id_mask));
    __m256i vblock_id = _mm256_or_si256(vblock_id_A, _mm256_slli_epi16(vblock_id_B, 8));

    // Visit all block bytes in reverse order (overwriting data on multiple matches)
    //
    // Always set match found to true for full blocks.
    //
    __m256i vmatch_found = _mm256_shuffle_epi8(vblock_is_full, vblock_id);
    __m256i vslot_id = _mm256_shuffle_epi8(vslot_empty_or_end, vblock_id);
#define CMP(VBLOCK_BYTE, BYTENUM)                                               \
  {                                                                             \
    __m256i vcmp =                                                              \
        _mm256_cmpeq_epi8(_mm256_shuffle_epi8(VBLOCK_BYTE, vblock_id), vstamp); \
    vmatch_found = _mm256_or_si256(vmatch_found, vcmp);                         \
    vslot_id = _mm256_blendv_epi8(vslot_id, _mm256_set1_epi8(BYTENUM), vcmp);   \
  }
    CMP(vblock_byte7, 7);
    CMP(vblock_byte6, 6);
    CMP(vblock_byte5, 5);
    CMP(vblock_byte4, 4);
    CMP(vblock_byte3, 3);
    CMP(vblock_byte2, 2);
    CMP(vblock_byte1, 1);
    CMP(vblock_byte0, 0);
#undef CMP

    // So far the output is in the order: [0, 8, 16, 24, 1, 9, 17, 25, 2, 10, 18, 26, ...]
    vmatch_found = _mm256_shuffle_epi8(
        vmatch_found,
        _mm256_setr_epi32(k4ByteSequence_0_4_8_12, k4ByteSequence_1_5_9_13,
                          k4ByteSequence_2_6_10_14, k4ByteSequence_3_7_11_15,
                          k4ByteSequence_0_4_8_12, k4ByteSequence_1_5_9_13,
                          k4ByteSequence_2_6_10_14, k4ByteSequence_3_7_11_15));
    // Now it is: [0, 1, 2, 3, 8, 9, 10, 11, 16, 17, 18, 19, 24, 25, 26, 27, | 4, 5, 6, 7,
    // 12, 13, 14, 15, ...]
    vmatch_found = _mm256_permutevar8x32_epi32(vmatch_found,
                                               _mm256_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7));

    // Repeat the same permutation for slot ids
    vslot_id = _mm256_shuffle_epi8(
        vslot_id, _mm256_setr_epi32(k4ByteSequence_0_4_8_12, k4ByteSequence_1_5_9_13,
                                    k4ByteSequence_2_6_10_14, k4ByteSequence_3_7_11_15,
                                    k4ByteSequence_0_4_8_12, k4ByteSequence_1_5_9_13,
                                    k4ByteSequence_2_6_10_14, k4ByteSequence_3_7_11_15));
    vslot_id =
        _mm256_permutevar8x32_epi32(vslot_id, _mm256_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(out_local_slots) + i, vslot_id);

    reinterpret_cast<uint32_t*>(out_match_bitvector)[i] =
        _mm256_movemask_epi8(vmatch_found);
  }

  return num_hashes - (num_hashes % unroll);
}

int SwissTable::extract_group_ids_avx2(const int num_keys, const uint32_t* hashes,
                                       const uint8_t* local_slots,
                                       uint32_t* out_group_ids, int byte_offset,
                                       int byte_multiplier, int byte_size) const {
  ARROW_DCHECK(byte_size == 1 || byte_size == 2 || byte_size == 4);
  uint32_t mask = byte_size == 1 ? 0xFF : byte_size == 2 ? 0xFFFF : 0xFFFFFFFF;
  auto elements = reinterpret_cast<const int*>(blocks_->data() + byte_offset);
  constexpr int unroll = 8;
  if (log_blocks_ == 0) {
    ARROW_DCHECK(byte_size == 1 && byte_offset == 8 && byte_multiplier == 16);
    __m256i block_group_ids =
        _mm256_set1_epi64x(reinterpret_cast<const uint64_t*>(blocks_->data())[1]);
    for (int i = 0; i < num_keys / unroll; ++i) {
      __m256i local_slot =
          _mm256_set1_epi64x(reinterpret_cast<const uint64_t*>(local_slots)[i]);
      __m256i group_id = _mm256_shuffle_epi8(block_group_ids, local_slot);
      group_id = _mm256_shuffle_epi8(
          group_id, _mm256_setr_epi32(0x80808000, 0x80808001, 0x80808002, 0x80808003,
                                      0x80808004, 0x80808005, 0x80808006, 0x80808007));
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(out_group_ids) + i, group_id);
    }
  } else {
    for (int i = 0; i < num_keys / unroll; ++i) {
      __m256i hash = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes) + i);
      __m256i local_slot =
          _mm256_set1_epi64x(reinterpret_cast<const uint64_t*>(local_slots)[i]);
      local_slot = _mm256_shuffle_epi8(
          local_slot, _mm256_setr_epi32(0x80808000, 0x80808001, 0x80808002, 0x80808003,
                                        0x80808004, 0x80808005, 0x80808006, 0x80808007));
      local_slot = _mm256_mullo_epi32(local_slot, _mm256_set1_epi32(byte_size));
      __m256i pos = _mm256_srlv_epi32(hash, _mm256_set1_epi32(bits_hash_ - log_blocks_));
      pos = _mm256_mullo_epi32(pos, _mm256_set1_epi32(byte_multiplier));
      pos = _mm256_add_epi32(pos, local_slot);
      __m256i group_id = _mm256_i32gather_epi32(elements, pos, 1);
      group_id = _mm256_and_si256(group_id, _mm256_set1_epi32(mask));
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(out_group_ids) + i, group_id);
    }
  }
  return num_keys - (num_keys % unroll);
}

#endif

}  // namespace compute
}  // namespace arrow
