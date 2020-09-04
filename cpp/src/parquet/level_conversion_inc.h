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

#include "parquet/level_conversion.h"

#include <algorithm>
#include <limits>
#if defined(ARROW_HAVE_BMI2)
#include <x86intrin.h>
#endif

#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "parquet/exception.h"
#include "parquet/level_comparison.h"

namespace parquet {
namespace internal {
namespace BMI_RUNTIME_VERSION {

using ::arrow::internal::BitRun;
using ::arrow::internal::BitRunReader;

/// Algorithm to simulate pext using BitRunReader for cases where all bits
/// not set or set.
uint64_t RunBasedExtractMixed(uint64_t bitmap, uint64_t select_bitmap) {
  bitmap = arrow::BitUtil::FromLittleEndian(bitmap);
  uint64_t new_bitmap = 0;
  ::arrow::internal::BitRunReader selection(reinterpret_cast<uint8_t*>(&select_bitmap),
                                            /*start_offset=*/0, /*length=*/64);
  ::arrow::internal::BitRun run = selection.NextRun();
  int64_t selected_bits = 0;
  while (run.length != 0) {
    if (run.set) {
      new_bitmap |= (bitmap & ::arrow::BitUtil::LeastSignficantBitMask(run.length))
                    << selected_bits;
      selected_bits += run.length;
    }
    bitmap = bitmap >> run.length;
    run = selection.NextRun();
  }
  return arrow::BitUtil::ToLittleEndian(new_bitmap);
}

inline uint64_t RunBasedExtractImpl(uint64_t bitmap, uint64_t select_bitmap) {
  /// These checks should be inline and are likely to be common cases.
  if (select_bitmap == ~uint64_t{0}) {
    return bitmap;
  } else if (select_bitmap == 0) {
    return 0;
  }
  /// Fallback to the slow method.
  return RunBasedExtractMixed(bitmap, select_bitmap);
}

inline uint64_t ExtractBits(uint64_t bitmap, uint64_t select_bitmap) {
#if defined(ARROW_HAVE_BMI2)
  return _pext_u64(bitmap, select_bitmap);
#else
  return RunBasedExtractImpl(bitmap, select_bitmap);
#endif
}

// Converts a bitmaps to list lengths.
//
// eq_rep_level_bitmap - bitmap indicate repetition level is equal to
//   the repetition level for the given list.
// ge_def_level_bitmap - bitmap indicating the definition level is greater than the level
//   for an element to be present.
template <typename LengthType>
LengthType* ToListLengths(const uint8_t* eq_rep_level_bitmap,
                          const uint8_t* ge_def_level_bitmap, int64_t bitmap_length,
                          LengthType* lengths) {
  BitRunReader rep_levels(eq_rep_level_bitmap,
                          /*start_offset=*/0, bitmap_length);
  BitRun rep_run = rep_levels.NextRun();
  int64_t current_pos = 0;
  while (rep_run.length != 0) {
    if (rep_run.set) {
      // Values equal to rep-level indicate a continuation for an already begun list.
      *lengths += rep_run.length;
    } else {
      // Run lengths of unset values occur for only empty lists or lists of one element.
      // Single value runs always correspond to a present list since they are succeeded by
      // values in the at list (see set condition above).
      // The one exception is if the the last element happens to be
      // unset, this is fixed up below outside of the loop.
      ++lengths;
      if (rep_run.length > 1) {
        BitRunReader def_levels(ge_def_level_bitmap, /*start_offset=*/current_pos,
                                rep_run.length - 1);
        BitRun def_run = def_levels.NextRun();
        while (def_run.length != 0) {
          std::fill(lengths, lengths + def_run.length, def_run.set ? 1 : 0);
          lengths += def_run.length;
          def_run = def_levels.NextRun();
        }
      }
      *lengths = 1;
    }
    current_pos += rep_run.length;
    rep_run = rep_levels.NextRun();
  }
  // Fixup the case when the last value observed was unset.  In which case it would have
  // been set to one above but it could in fact be set to zero.
  if (bitmap_length > 0 && *lengths == 1) {
    *lengths = arrow::BitUtil::GetBit(ge_def_level_bitmap, bitmap_length - 1) ? 1 : 0;
  }
  return lengths;
}

template <bool has_repeated_parent>
int64_t DefinitionLevelsBatchToBitmap(const int16_t* def_levels, const int64_t batch_size,
                                      LevelInfo level_info,
                                      ::arrow::internal::FirstTimeBitmapWriter* writer) {
  // Greater than level_info.def_level - 1 implies >= the def_level
  uint64_t defined_bitmap =
      internal::GreaterThanBitmap(def_levels, batch_size, level_info.def_level - 1);

  DCHECK_LE(batch_size, 64);
  if (has_repeated_parent) {
    // Greater than level_info.repeated_ancestor_def_level - 1 implies >= the
    // repeated_ancenstor_def_level
    uint64_t present_bitmap = internal::GreaterThanBitmap(
        def_levels, batch_size, level_info.repeated_ancestor_def_level - 1);
    uint64_t selected_bits = ExtractBits(defined_bitmap, present_bitmap);
    writer->AppendWord(selected_bits, ::arrow::BitUtil::PopCount(present_bitmap));
    return ::arrow::BitUtil::PopCount(selected_bits);
  } else {
    writer->AppendWord(defined_bitmap, batch_size);
    return ::arrow::BitUtil::PopCount(defined_bitmap);
  }
}

template <bool has_repeated_parent>
void DefinitionLevelsToBitmapSimd(const int16_t* def_levels, int64_t num_def_levels,
                                  LevelInfo level_info, int64_t* values_read,
                                  int64_t* null_count, uint8_t* valid_bits,
                                  int64_t valid_bits_offset) {
  constexpr int64_t kBitMaskSize = 64;
  ::arrow::internal::FirstTimeBitmapWriter writer(valid_bits,
                                                  /*start_offset=*/valid_bits_offset,
                                                  /*length=*/num_def_levels);
  int64_t set_count = 0;
  *values_read = 0;
  while (num_def_levels > kBitMaskSize) {
    set_count += DefinitionLevelsBatchToBitmap<has_repeated_parent>(
        def_levels, kBitMaskSize, level_info, &writer);
    def_levels += kBitMaskSize;
    num_def_levels -= kBitMaskSize;
  }
  set_count += DefinitionLevelsBatchToBitmap<has_repeated_parent>(
      def_levels, num_def_levels, level_info, &writer);

  *values_read = writer.position();
  *null_count += *values_read - set_count;
  writer.Finish();
}

template <typename LengthType>
inline LengthType* BitmapsToListLengths(const ListLengthBitmaps& bitmaps,
                                        LengthType* lengths) {
  int64_t bits_to_process = bitmaps.ge_rep_level.length();
  if (bitmaps.gt_rep_level.has_value() && bitmaps.ge_rep_ancestor_def_level.has_value()) {
    auto visitor = [&](const std::array<uint64_t, 4> words) {
      constexpr int kGeRepLevel = 0;
      constexpr int kGeDefLevel = 1;
      constexpr int kGtRepLevel = 2;
      constexpr int kGeRepAncensorLevel = 3;
      // This eliminates elements that have a rep-level greater then the target (those
      // entries belong to a sub list) and elements that correspond to a empty parent list
      // (not applicable to this list).
      uint64_t selection_bits = (~words[kGtRepLevel] & words[kGeRepAncensorLevel]);
      // Correct for partial words that are negated.
      if (ARROW_PREDICT_FALSE(bits_to_process < 64)) {
        selection_bits &= arrow::BitUtil::LeastSignficantBitMask(bits_to_process);
      } else {
        bits_to_process -= 64;
      }
      uint64_t eq_rep_level = ExtractBits(words[kGeRepLevel], selection_bits);
      uint64_t present_def_level = ExtractBits(words[kGeDefLevel], selection_bits);
      lengths = ToListLengths(reinterpret_cast<uint8_t*>(&eq_rep_level),
                              reinterpret_cast<uint8_t*>(&present_def_level),
                              arrow::BitUtil::PopCount(selection_bits), lengths);
    };
    arrow::internal::Bitmap::VisitWords(
        {bitmaps.ge_rep_level, bitmaps.ge_def_level, *bitmaps.gt_rep_level,
         *bitmaps.ge_rep_ancestor_def_level},
        visitor);
    return lengths;
  } else if (bitmaps.gt_rep_level.has_value()) {
    auto visitor = [&](const std::array<uint64_t, 3> words) {
      constexpr int kGeRepLevel = 0;
      constexpr int kGeDefLevel = 1;
      constexpr int kGtRepLevel = 2;
      // This eliminates elements that have a rep-level greater then the target (those
      // entries belong to a sub list).
      uint64_t selection_bits = ~words[kGtRepLevel];
      if (ARROW_PREDICT_FALSE(bits_to_process < 64)) {
        selection_bits &= arrow::BitUtil::LeastSignficantBitMask(bits_to_process);
      } else {
        bits_to_process -= 64;
      }
      uint64_t eq_rep_level = ExtractBits(words[kGeRepLevel], selection_bits);
      uint64_t present_def_level = ExtractBits(words[kGeDefLevel], selection_bits);
      lengths = ToListLengths(reinterpret_cast<uint8_t*>(&eq_rep_level),
                              reinterpret_cast<uint8_t*>(&present_def_level),
                              arrow::BitUtil::PopCount(selection_bits), lengths);
    };
    arrow::internal::Bitmap::VisitWords(
        {bitmaps.ge_rep_level, bitmaps.ge_def_level, *bitmaps.gt_rep_level}, visitor);
    return lengths;
  } else if (bitmaps.ge_rep_ancestor_def_level.has_value()) {
    auto visitor = [&](const std::array<uint64_t, 3> words) {
      constexpr int kGeRepLevel = 0;
      constexpr int kGeDefLevel = 1;
      constexpr int kGeRepAncensorLevel = 2;
      // Eliminate elements that correspond to a empty parent list.
      uint64_t selection_bits = words[kGeRepAncensorLevel];
      uint64_t eq_rep_level = ExtractBits(words[kGeRepLevel], selection_bits);
      uint64_t present_def_level = ExtractBits(words[kGeDefLevel], selection_bits);
      lengths = ToListLengths(reinterpret_cast<uint8_t*>(&eq_rep_level),
                              reinterpret_cast<uint8_t*>(&present_def_level),
                              arrow::BitUtil::PopCount(selection_bits), lengths);
    };
    arrow::internal::Bitmap::VisitWords(
        {bitmaps.ge_rep_level, bitmaps.ge_def_level, *bitmaps.ge_rep_ancestor_def_level},
        visitor);

    return lengths;
  }
  // Final case is a list that doesn't nest with any other lists.
  // In this case ge_rep_level translates is equivelant to eq_rep_level.
  return ToListLengths(bitmaps.ge_rep_level.buffer()->data(),
                       bitmaps.ge_def_level.buffer()->data(),
                       bitmaps.ge_rep_level.length(), lengths);
}

inline ::arrow::util::variant<int32_t*, int64_t*> PopulateListLengthsListTypeDispatch(
    const ListLengthBitmaps& bitmaps,
    ::arrow::util::variant<int32_t*, int64_t*> lengths) {
  if (arrow::util::holds_alternative<int32_t*>(lengths)) {
    return BitmapsToListLengths<int32_t>(bitmaps, ::arrow::util::get<int32_t*>(lengths));

  } else if (arrow::util::holds_alternative<int64_t*>(lengths)) {
    return BitmapsToListLengths<int64_t>(bitmaps, ::arrow::util::get<int64_t*>(lengths));

  } else {
    throw ParquetException("Unrecognized variant");
  }
}

inline void ResolveNestedValidityBitmap(const NestedValidityBitmaps& bitmaps,
                                        int null_slot_count,
                                        ValidityBitmapInputOutput* output) {
  ::arrow::internal::FirstTimeBitmapWriter writer(
      output->valid_bits, output->valid_bits_offset, bitmaps.ge_def_level.length());

  DCHECK_EQ(bitmaps.ge_def_level.offset(), 0);
  int64_t bits_to_process = bitmaps.ge_def_level.length();
  int64_t set_count = 0;
  output->values_read = 0;
  if (bitmaps.ge_rep_ancestor_def_level.has_value() && bitmaps.ge_rep_level.has_value()) {
    DCHECK_EQ(bitmaps.ge_rep_ancestor_def_level->length(), bitmaps.ge_def_level.length());
    DCHECK_EQ(bitmaps.ge_rep_level->length(), bitmaps.ge_def_level.length());
    DCHECK_EQ(bitmaps.ge_rep_ancestor_def_level->offset(), 0);
    DCHECK_EQ(bitmaps.ge_rep_level->offset(), 0);
    auto visitor = [&](std::array<uint64_t, 3> words) {
      constexpr int kGeAncestorWord = 0;
      constexpr int kGeDefLevelWord = 1;
      constexpr int kGeRepLevelWord = 2;
      // Starts of lists are demarcated by values less then the current
      // rep-level.  Lists are only present if the values were greater
      // then the ancenstor def level.
      uint64_t selection = words[kGeAncestorWord] & ~words[kGeRepLevelWord];
      uint64_t present_bits = ExtractBits(words[kGeDefLevelWord], selection);

      writer.AppendWord(present_bits, arrow::BitUtil::PopCount(selection));
      set_count += arrow::BitUtil::PopCount(present_bits);
    };
    arrow::internal::Bitmap to_visit[] = {*bitmaps.ge_rep_ancestor_def_level,
                                          bitmaps.ge_def_level, *bitmaps.ge_rep_level};
    arrow::internal::Bitmap::VisitWords(to_visit, visitor);
  } else if (bitmaps.ge_rep_ancestor_def_level.has_value()) {
    DCHECK_EQ(bitmaps.ge_rep_ancestor_def_level->length(), bitmaps.ge_def_level.length());
    DCHECK_EQ(bitmaps.ge_rep_ancestor_def_level->offset(), 0);

    auto visitor = [&](std::array<uint64_t, 2> words) {
      constexpr int kGeAncestorWord = 0;
      constexpr int kGeDefLevelWord = 1;
      uint64_t present_bits = ExtractBits(words[kGeDefLevelWord], words[kGeAncestorWord]);

      writer.AppendWord(present_bits, arrow::BitUtil::PopCount(words[kGeAncestorWord]));
      set_count += arrow::BitUtil::PopCount(present_bits);
    };
    const arrow::internal::Bitmap to_visit[] = {*bitmaps.ge_rep_ancestor_def_level,
                                                bitmaps.ge_def_level};
    arrow::internal::Bitmap::VisitWords(to_visit, visitor);
  } else if (bitmaps.ge_rep_level.has_value()) {
    DCHECK_EQ(bitmaps.ge_rep_level->length(), bitmaps.ge_def_level.length());
    DCHECK_EQ(bitmaps.ge_rep_level->offset(), 0);

    auto visitor = [&](std::array<uint64_t, 2> words) {
      constexpr int kGeDefLevelWord = 0;
      constexpr int kGeRepLevelWord = 1;
      // Starts of lists are demarcated by values less then the current
      // rep-level.
      uint64_t selection = ~words[kGeRepLevelWord];
      uint64_t present_bits = ExtractBits(words[kGeDefLevelWord], selection);
      // Correct for partial words that are negated.
      if (ARROW_PREDICT_FALSE(bits_to_process < 64)) {
        selection &= arrow::BitUtil::LeastSignficantBitMask(bits_to_process);
      } else {
        bits_to_process -= 64;
      }

      writer.AppendWord(present_bits, arrow::BitUtil::PopCount(selection));
      set_count += arrow::BitUtil::PopCount(present_bits);
    };
    arrow::internal::Bitmap to_visit[] = {bitmaps.ge_def_level, *bitmaps.ge_rep_level};
    arrow::internal::Bitmap::VisitWords(to_visit, visitor);
  } else {
    DCHECK(false)
        << "should only be called with one optional parameter passed. if this case is "
           "reached the passed in bitmap is equal to the validity bitmap";
  }
  output->values_read = writer.position();
  output->null_count = output->values_read - set_count;
  if (output->null_count > 0 && null_slot_count > 1) {
    throw ParquetException(
        "Null values with null_slot_count > 1 not supported."
        "(i.e. FixedSizeLists with null values are not supported");
  }
}

}  // namespace BMI_RUNTIME_VERSION
}  // namespace internal
}  // namespace parquet
