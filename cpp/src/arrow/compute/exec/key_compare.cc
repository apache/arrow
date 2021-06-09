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

#include "arrow/compute/exec/key_compare.h"

#include <algorithm>
#include <cstdint>

#include "arrow/compute/exec/util.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace compute {

void KeyCompare::CompareRows(uint32_t num_rows_to_compare,
                             const uint16_t* sel_left_maybe_null,
                             const uint32_t* left_to_right_map,
                             KeyEncoder::KeyEncoderContext* ctx, uint32_t* out_num_rows,
                             uint16_t* out_sel_left_maybe_same,
                             const KeyEncoder::KeyRowArray& rows_left,
                             const KeyEncoder::KeyRowArray& rows_right) {
  ARROW_DCHECK(rows_left.metadata().is_compatible(rows_right.metadata()));

  if (num_rows_to_compare == 0) {
    *out_num_rows = 0;
    return;
  }

  // Allocate temporary byte and bit vectors
  auto bytevector_holder =
      util::TempVectorHolder<uint8_t>(ctx->stack, num_rows_to_compare);
  auto bitvector_holder =
      util::TempVectorHolder<uint8_t>(ctx->stack, num_rows_to_compare);

  uint8_t* match_bytevector = bytevector_holder.mutable_data();
  uint8_t* match_bitvector = bitvector_holder.mutable_data();

  // All comparison functions called here will update match byte vector
  // (AND it with comparison result) instead of overwriting it.
  memset(match_bytevector, 0xff, num_rows_to_compare);

  if (rows_left.metadata().is_fixed_length) {
    CompareFixedLength(num_rows_to_compare, sel_left_maybe_null, left_to_right_map,
                       match_bytevector, ctx, rows_left.metadata().fixed_length,
                       rows_left.data(1), rows_right.data(1));
  } else {
    CompareVaryingLength(num_rows_to_compare, sel_left_maybe_null, left_to_right_map,
                         match_bytevector, ctx, rows_left.data(2), rows_right.data(2),
                         rows_left.offsets(), rows_right.offsets());
  }

  // CompareFixedLength can be used to compare nulls as well
  bool nulls_present = rows_left.has_any_nulls(ctx) || rows_right.has_any_nulls(ctx);
  if (nulls_present) {
    CompareFixedLength(num_rows_to_compare, sel_left_maybe_null, left_to_right_map,
                       match_bytevector, ctx,
                       rows_left.metadata().null_masks_bytes_per_row,
                       rows_left.null_masks(), rows_right.null_masks());
  }

  util::BitUtil::bytes_to_bits(ctx->hardware_flags, num_rows_to_compare, match_bytevector,
                               match_bitvector);
  if (sel_left_maybe_null) {
    int out_num_rows_int;
    util::BitUtil::bits_filter_indexes(0, ctx->hardware_flags, num_rows_to_compare,
                                       match_bitvector, sel_left_maybe_null,
                                       &out_num_rows_int, out_sel_left_maybe_same);
    *out_num_rows = out_num_rows_int;
  } else {
    int out_num_rows_int;
    util::BitUtil::bits_to_indexes(0, ctx->hardware_flags, num_rows_to_compare,
                                   match_bitvector, &out_num_rows_int,
                                   out_sel_left_maybe_same);
    *out_num_rows = out_num_rows_int;
  }
}

void KeyCompare::CompareFixedLength(uint32_t num_rows_to_compare,
                                    const uint16_t* sel_left_maybe_null,
                                    const uint32_t* left_to_right_map,
                                    uint8_t* match_bytevector,
                                    KeyEncoder::KeyEncoderContext* ctx,
                                    uint32_t fixed_length, const uint8_t* rows_left,
                                    const uint8_t* rows_right) {
  bool use_selection = (sel_left_maybe_null != nullptr);

  uint32_t num_rows_already_processed = 0;

#if defined(ARROW_HAVE_AVX2)
  if (ctx->has_avx2() && !use_selection) {
    // Choose between up-to-8B length, up-to-16B length and any size versions
    if (fixed_length <= 8) {
      num_rows_already_processed = CompareFixedLength_UpTo8B_avx2(
          num_rows_to_compare, left_to_right_map, match_bytevector, fixed_length,
          rows_left, rows_right);
    } else if (fixed_length <= 16) {
      num_rows_already_processed = CompareFixedLength_UpTo16B_avx2(
          num_rows_to_compare, left_to_right_map, match_bytevector, fixed_length,
          rows_left, rows_right);
    } else {
      num_rows_already_processed =
          CompareFixedLength_avx2(num_rows_to_compare, left_to_right_map,
                                  match_bytevector, fixed_length, rows_left, rows_right);
    }
  }
#endif

  typedef void (*CompareFixedLengthImp_t)(uint32_t, uint32_t, const uint16_t*,
                                          const uint32_t*, uint8_t*, uint32_t,
                                          const uint8_t*, const uint8_t*);
  static const CompareFixedLengthImp_t CompareFixedLengthImp_fn[] = {
      CompareFixedLengthImp<false, 1>, CompareFixedLengthImp<false, 2>,
      CompareFixedLengthImp<false, 0>, CompareFixedLengthImp<true, 1>,
      CompareFixedLengthImp<true, 2>,  CompareFixedLengthImp<true, 0>};
  int dispatch_const = (use_selection ? 3 : 0) +
                       ((fixed_length <= 8) ? 0 : ((fixed_length <= 16) ? 1 : 2));
  CompareFixedLengthImp_fn[dispatch_const](
      num_rows_already_processed, num_rows_to_compare, sel_left_maybe_null,
      left_to_right_map, match_bytevector, fixed_length, rows_left, rows_right);
}

template <bool use_selection, int num_64bit_words>
void KeyCompare::CompareFixedLengthImp(uint32_t num_rows_already_processed,
                                       uint32_t num_rows,
                                       const uint16_t* sel_left_maybe_null,
                                       const uint32_t* left_to_right_map,
                                       uint8_t* match_bytevector, uint32_t length,
                                       const uint8_t* rows_left,
                                       const uint8_t* rows_right) {
  // Key length (for encoded key) has to be non-zero
  ARROW_DCHECK(length > 0);

  // Non-zero length guarantees no underflow
  int32_t num_loops_less_one = (static_cast<int32_t>(length) + 7) / 8 - 1;

  // Length remaining in last loop can only be zero for input length equal to zero
  uint32_t length_remaining_last_loop = length - num_loops_less_one * 8;
  uint64_t tail_mask = (~0ULL) >> (8 * (8 - length_remaining_last_loop));

  for (uint32_t id_input = num_rows_already_processed; id_input < num_rows; ++id_input) {
    uint32_t irow_left = use_selection ? sel_left_maybe_null[id_input] : id_input;
    uint32_t irow_right = left_to_right_map[irow_left];
    uint32_t begin_left = length * irow_left;
    uint32_t begin_right = length * irow_right;
    const uint64_t* key_left_ptr =
        reinterpret_cast<const uint64_t*>(rows_left + begin_left);
    const uint64_t* key_right_ptr =
        reinterpret_cast<const uint64_t*>(rows_right + begin_right);
    uint64_t result_or = 0ULL;
    int32_t istripe = 0;

    // Specializations for keys up to 8 bytes and between 9 and 16 bytes to
    // avoid internal loop over words in the value for short ones.
    //
    // Template argument 0 means arbitrarily many 64-bit words,
    // 1 means up to 1 and 2 means up to 2.
    //
    if (num_64bit_words == 0) {
      for (; istripe < num_loops_less_one; ++istripe) {
        uint64_t key_left = util::SafeLoad(&key_left_ptr[istripe]);
        uint64_t key_right = util::SafeLoad(&key_right_ptr[istripe]);
        result_or |= (key_left ^ key_right);
      }
    } else if (num_64bit_words == 2) {
      uint64_t key_left = util::SafeLoad(&key_left_ptr[istripe]);
      uint64_t key_right = util::SafeLoad(&key_right_ptr[istripe]);
      result_or |= (key_left ^ key_right);
      ++istripe;
    }

    uint64_t key_left = util::SafeLoad(&key_left_ptr[istripe]);
    uint64_t key_right = util::SafeLoad(&key_right_ptr[istripe]);
    result_or |= (tail_mask & (key_left ^ key_right));

    int result = (result_or == 0 ? 0xff : 0);
    match_bytevector[id_input] &= result;
  }
}

void KeyCompare::CompareVaryingLength(uint32_t num_rows_to_compare,
                                      const uint16_t* sel_left_maybe_null,
                                      const uint32_t* left_to_right_map,
                                      uint8_t* match_bytevector,
                                      KeyEncoder::KeyEncoderContext* ctx,
                                      const uint8_t* rows_left, const uint8_t* rows_right,
                                      const uint32_t* offsets_left,
                                      const uint32_t* offsets_right) {
  bool use_selection = (sel_left_maybe_null != nullptr);

#if defined(ARROW_HAVE_AVX2)
  if (ctx->has_avx2() && !use_selection) {
    CompareVaryingLength_avx2(num_rows_to_compare, left_to_right_map, match_bytevector,
                              rows_left, rows_right, offsets_left, offsets_right);
  } else {
#endif
    if (use_selection) {
      CompareVaryingLengthImp<true>(num_rows_to_compare, sel_left_maybe_null,
                                    left_to_right_map, match_bytevector, rows_left,
                                    rows_right, offsets_left, offsets_right);
    } else {
      CompareVaryingLengthImp<false>(num_rows_to_compare, sel_left_maybe_null,
                                     left_to_right_map, match_bytevector, rows_left,
                                     rows_right, offsets_left, offsets_right);
    }
#if defined(ARROW_HAVE_AVX2)
  }
#endif
}

template <bool use_selection>
void KeyCompare::CompareVaryingLengthImp(
    uint32_t num_rows, const uint16_t* sel_left_maybe_null,
    const uint32_t* left_to_right_map, uint8_t* match_bytevector,
    const uint8_t* rows_left, const uint8_t* rows_right, const uint32_t* offsets_left,
    const uint32_t* offsets_right) {
  static const uint64_t tail_masks[] = {
      0x0000000000000000ULL, 0x00000000000000ffULL, 0x000000000000ffffULL,
      0x0000000000ffffffULL, 0x00000000ffffffffULL, 0x000000ffffffffffULL,
      0x0000ffffffffffffULL, 0x00ffffffffffffffULL, 0xffffffffffffffffULL};
  for (uint32_t i = 0; i < num_rows; ++i) {
    uint32_t irow_left = use_selection ? sel_left_maybe_null[i] : i;
    uint32_t irow_right = left_to_right_map[irow_left];
    uint32_t begin_left = offsets_left[irow_left];
    uint32_t begin_right = offsets_right[irow_right];
    uint32_t length_left = offsets_left[irow_left + 1] - begin_left;
    uint32_t length_right = offsets_right[irow_right + 1] - begin_right;
    uint32_t length = std::min(length_left, length_right);
    const uint64_t* key_left_ptr =
        reinterpret_cast<const uint64_t*>(rows_left + begin_left);
    const uint64_t* key_right_ptr =
        reinterpret_cast<const uint64_t*>(rows_right + begin_right);
    uint64_t result_or = 0;
    int32_t istripe;
    // length can be zero
    for (istripe = 0; istripe < (static_cast<int32_t>(length) + 7) / 8 - 1; ++istripe) {
      uint64_t key_left = util::SafeLoad(&key_left_ptr[istripe]);
      uint64_t key_right = util::SafeLoad(&key_right_ptr[istripe]);
      result_or |= (key_left ^ key_right);
    }

    uint32_t length_remaining = length - static_cast<uint32_t>(istripe) * 8;
    uint64_t tail_mask = tail_masks[length_remaining];

    uint64_t key_left = util::SafeLoad(&key_left_ptr[istripe]);
    uint64_t key_right = util::SafeLoad(&key_right_ptr[istripe]);
    result_or |= (tail_mask & (key_left ^ key_right));

    int result = (result_or == 0 ? 0xff : 0);
    match_bytevector[i] &= result;
  }
}

}  // namespace compute
}  // namespace arrow
