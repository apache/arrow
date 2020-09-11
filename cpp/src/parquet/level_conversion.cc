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
#include "parquet/exception.h"
#include "parquet/level_comparison.h"

#define BMI_RUNTIME_VERSION standard
#include "parquet/level_conversion_inc.h"
#undef BMI_RUNTIME_VERSION

namespace parquet {
namespace internal {
namespace {

using ::arrow::internal::CpuInfo;

#if !defined(ARROW_HAVE_RUNTIME_BMI2)
void DefinitionLevelsToBitmapScalar(const int16_t* def_levels, int64_t num_def_levels,
                                    LevelInfo level_info, int64_t* values_read,
                                    int64_t* null_count, uint8_t* valid_bits,
                                    int64_t valid_bits_offset) {
  ::arrow::internal::FirstTimeBitmapWriter valid_bits_writer(
      valid_bits,
      /*start_offset=*/valid_bits_offset,
      /*length=*/num_def_levels);
  for (int x = 0; x < num_def_levels; x++) {
    if (def_levels[x] < level_info.repeated_ancestor_def_level) {
      continue;
    }
    if (def_levels[x] >= level_info.def_level) {
      valid_bits_writer.Set();
    } else {
      valid_bits_writer.Clear();
      *null_count += 1;
    }
    valid_bits_writer.Next();
  }
  valid_bits_writer.Finish();
  *values_read = valid_bits_writer.position();
  if (*null_count > 0 && level_info.null_slot_usage > 1) {
    throw ParquetException(
        "Null values with null_slot_usage > 1 not supported."
        "(i.e. FixedSizeLists with null values are not supported");
  }
}
#endif

template <typename LengthType>
void DefRepLevelsToListInfo(const int16_t* def_levels, const int16_t* rep_levels,
                            int64_t num_def_levels, LevelInfo level_info,
                            ValidityBitmapInputOutput* output, LengthType* lengths) {
  LengthType* orig_pos = lengths;
  std::unique_ptr<::arrow::internal::FirstTimeBitmapWriter> valid_bits_writer;
  if (output->valid_bits) {
    valid_bits_writer.reset(new ::arrow::internal::FirstTimeBitmapWriter(
        output->valid_bits, output->valid_bits_offset, num_def_levels));
  }
  for (int x = 0; x < num_def_levels; x++) {
    // Skip items that belong to empty ancenstor lists and futher nested lists.
    if (def_levels[x] < level_info.repeated_ancestor_def_level ||
        rep_levels[x] > level_info.rep_level) {
      continue;
    }
    if (rep_levels[x] == level_info.rep_level) {
      // A continuation of an existing list.
      *lengths += 1;
    } else {
      // current_rep < list rep_level i.e. start of a list (ancenstor empty lists are
      // filtered out above).
      ++lengths;
      *lengths = (def_levels[x] >= level_info.def_level) ? 1 : 0;

      if (valid_bits_writer != nullptr) {
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
  if (valid_bits_writer != nullptr) {
    valid_bits_writer->Finish();
  }
  output->values_read = lengths - orig_pos;
  if (output->null_count > 0 && level_info.null_slot_usage > 1) {
    throw ParquetException(
        "Null values with null_slot_usage > 1 not supported."
        "(i.e. FixedSizeLists with null values are not supported)");
  }
}

}  // namespace

void DefinitionLevelsToBitmap(const int16_t* def_levels, int64_t num_def_levels,
                              LevelInfo level_info, int64_t* values_read,
                              int64_t* null_count, uint8_t* valid_bits,
                              int64_t valid_bits_offset) {
  if (level_info.rep_level > 0) {
#if defined(ARROW_HAVE_RUNTIME_BMI2)
    using FunctionType = decltype(&standard::DefinitionLevelsToBitmapSimd<true>);
    static FunctionType fn =
        CpuInfo::GetInstance()->HasEfficientBmi2()
            ? DefinitionLevelsToBitmapBmi2WithRepeatedParent
            : standard::DefinitionLevelsToBitmapSimd</*has_repeated_parent=*/true>;
    fn(def_levels, num_def_levels, level_info, values_read, null_count, valid_bits,
       valid_bits_offset);
#else
    DefinitionLevelsToBitmapScalar(def_levels, num_def_levels, level_info, values_read,
                                   null_count, valid_bits, valid_bits_offset);

#endif
  } else {
    standard::DefinitionLevelsToBitmapSimd</*has_repeated_parent=*/false>(
        def_levels, num_def_levels, level_info, values_read, null_count, valid_bits,
        valid_bits_offset);
  }
}

uint64_t RunBasedExtract(uint64_t bitmap, uint64_t select_bitmap) {
  return standard::RunBasedExtractImpl(bitmap, select_bitmap);
}

void ConvertDefRepLevelsToList(const int16_t* def_levels, const int16_t* rep_levels,
                               int64_t num_def_levels, LevelInfo level_info,
                               ValidityBitmapInputOutput* output,
                               ::arrow::util::variant<int32_t*, int64_t*> lengths) {
  if (arrow::util::holds_alternative<int32_t*>(lengths)) {
    auto int32_lengths = ::arrow::util::get<int32_t*>(lengths);
    DefRepLevelsToListInfo<int32_t>(def_levels, rep_levels, num_def_levels, level_info,
                                    output, int32_lengths);
  } else if (arrow::util::holds_alternative<int64_t*>(lengths)) {
    auto int64_lengths = ::arrow::util::get<int64_t*>(lengths);
    DefRepLevelsToListInfo<int64_t>(def_levels, rep_levels, num_def_levels, level_info,
                                    output, int64_lengths);
  } else {
    throw ParquetException("Unrecognized variant");
  }
}

}  // namespace internal
}  // namespace parquet
