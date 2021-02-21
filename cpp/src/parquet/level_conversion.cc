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
#include "parquet/level_conversion.h"

#include <algorithm>
#include <limits>

#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "parquet/exception.h"

#include "parquet/level_comparison.h"
#define PARQUET_IMPL_NAMESPACE standard
#include "parquet/level_conversion_inc.h"
#undef PARQUET_IMPL_NAMESPACE

namespace parquet {
namespace internal {
namespace {

using ::arrow::internal::CpuInfo;
using ::arrow::util::optional;

template <typename OffsetType>
void DefRepLevelsToListInfo(const int16_t* def_levels, const int16_t* rep_levels,
                            int64_t num_def_levels, LevelInfo level_info,
                            ValidityBitmapInputOutput* output, OffsetType* offsets) {
  OffsetType* orig_pos = offsets;
  optional<::arrow::internal::FirstTimeBitmapWriter> valid_bits_writer;
  if (output->valid_bits) {
    valid_bits_writer.emplace(output->valid_bits, output->valid_bits_offset,
                              output->values_read_upper_bound);
  }
  for (int x = 0; x < num_def_levels; x++) {
    // Skip items that belong to empty or null ancestor lists and further nested lists.
    if (def_levels[x] < level_info.repeated_ancestor_def_level ||
        rep_levels[x] > level_info.rep_level) {
      continue;
    }

    if (rep_levels[x] == level_info.rep_level) {
      // A continuation of an existing list.
      // offsets can be null for structs with repeated children (we don't need to know
      // offsets until we get to the children).
      if (offsets != nullptr) {
        if (ARROW_PREDICT_FALSE(*offsets == std::numeric_limits<OffsetType>::max())) {
          throw ParquetException("List index overflow.");
        }
        *offsets += 1;
      }
    } else {
      if (ARROW_PREDICT_FALSE(
              (valid_bits_writer.has_value() &&
               valid_bits_writer->position() >= output->values_read_upper_bound) ||
              (offsets - orig_pos) >= output->values_read_upper_bound)) {
        std::stringstream ss;
        ss << "Definition levels exceeded upper bound: "
           << output->values_read_upper_bound;
        throw ParquetException(ss.str());
      }

      // current_rep < list rep_level i.e. start of a list (ancestor empty lists are
      // filtered out above).
      // offsets can be null for structs with repeated children (we don't need to know
      // offsets until we get to the children).
      if (offsets != nullptr) {
        ++offsets;
        // Use cumulative offsets because variable size lists are more common then
        // fixed size lists so it should be cheaper to make these cumulative and
        // subtract when validating fixed size lists.
        *offsets = *(offsets - 1);
        if (def_levels[x] >= level_info.def_level) {
          if (ARROW_PREDICT_FALSE(*offsets == std::numeric_limits<OffsetType>::max())) {
            throw ParquetException("List index overflow.");
          }
          *offsets += 1;
        }
      }

      if (valid_bits_writer.has_value()) {
        // the level_info def level for lists reflects element present level.
        // the prior level distinguishes between empty lists.
        if (def_levels[x] >= level_info.def_level - 1) {
          valid_bits_writer->Set();
        } else {
          output->null_count++;
          valid_bits_writer->Clear();
        }
        valid_bits_writer->Next();
      }
    }
  }
  if (valid_bits_writer.has_value()) {
    valid_bits_writer->Finish();
  }
  if (offsets != nullptr) {
    output->values_read = offsets - orig_pos;
  } else if (valid_bits_writer.has_value()) {
    output->values_read = valid_bits_writer->position();
  }
  if (output->null_count > 0 && level_info.null_slot_usage > 1) {
    throw ParquetException(
        "Null values with null_slot_usage > 1 not supported."
        "(i.e. FixedSizeLists with null values are not supported)");
  }
}

}  // namespace

#if defined(ARROW_HAVE_RUNTIME_BMI2)
// defined in level_conversion_bmi2.cc for dynamic dispatch.
void DefLevelsToBitmapBmi2WithRepeatedParent(const int16_t* def_levels,
                                             int64_t num_def_levels, LevelInfo level_info,
                                             ValidityBitmapInputOutput* output);
#endif

void DefLevelsToBitmap(const int16_t* def_levels, int64_t num_def_levels,
                       LevelInfo level_info, ValidityBitmapInputOutput* output) {
  // It is simpler to rely on rep_level here until PARQUET-1899 is done and the code
  // is deleted in a follow-up release.
  if (level_info.rep_level > 0) {
#if defined(ARROW_HAVE_RUNTIME_BMI2)
    if (CpuInfo::GetInstance()->HasEfficientBmi2()) {
      return DefLevelsToBitmapBmi2WithRepeatedParent(def_levels, num_def_levels,
                                                     level_info, output);
    }
#endif
    standard::DefLevelsToBitmapSimd</*has_repeated_parent=*/true>(
        def_levels, num_def_levels, level_info, output);
  } else {
    standard::DefLevelsToBitmapSimd</*has_repeated_parent=*/false>(
        def_levels, num_def_levels, level_info, output);
  }
}

uint64_t TestOnlyExtractBitsSoftware(uint64_t bitmap, uint64_t select_bitmap) {
  return standard::ExtractBitsSoftware(bitmap, select_bitmap);
}

void DefRepLevelsToList(const int16_t* def_levels, const int16_t* rep_levels,
                        int64_t num_def_levels, LevelInfo level_info,
                        ValidityBitmapInputOutput* output, int32_t* offsets) {
  DefRepLevelsToListInfo<int32_t>(def_levels, rep_levels, num_def_levels, level_info,
                                  output, offsets);
}

void DefRepLevelsToList(const int16_t* def_levels, const int16_t* rep_levels,
                        int64_t num_def_levels, LevelInfo level_info,
                        ValidityBitmapInputOutput* output, int64_t* offsets) {
  DefRepLevelsToListInfo<int64_t>(def_levels, rep_levels, num_def_levels, level_info,
                                  output, offsets);
}

void DefRepLevelsToBitmap(const int16_t* def_levels, const int16_t* rep_levels,
                          int64_t num_def_levels, LevelInfo level_info,
                          ValidityBitmapInputOutput* output) {
  // DefReplevelsToListInfo assumes it for the actual list method and this
  // method is for parent structs, so we need to bump def and ref level.
  level_info.rep_level += 1;
  level_info.def_level += 1;
  DefRepLevelsToListInfo<int32_t>(def_levels, rep_levels, num_def_levels, level_info,
                                  output, /*offsets=*/nullptr);
}

}  // namespace internal
}  // namespace parquet
