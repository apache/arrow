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
#include <cstdint>
#include <limits>

#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bit_util_extract.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/logging.h"
#include "arrow/util/simd.h"
#include "parquet/exception.h"
#include "parquet/level_comparison.h"

#ifndef PARQUET_IMPL_NAMESPACE
#error "PARQUET_IMPL_NAMESPACE must be defined"
#endif

namespace parquet::internal::PARQUET_IMPL_NAMESPACE {

namespace bit_util = ::arrow::bit_util;

static constexpr int64_t kExtractBitsSize = 8 * sizeof(bit_util::extract_bitmap_t);

template <bool has_repeated_parent>
int64_t DefLevelsBatchToBitmap(const int16_t* def_levels, const int64_t batch_size,
                               int64_t upper_bound_remaining, LevelInfo level_info,
                               ::arrow::internal::FirstTimeBitmapWriter* writer) {
  DCHECK_LE(batch_size, kExtractBitsSize);

  // Greater than level_info.def_level - 1 implies >= the def_level
  auto defined_bitmap = static_cast<bit_util::extract_bitmap_t>(
      internal::GreaterThanBitmap(def_levels, batch_size, level_info.def_level - 1));

  if (has_repeated_parent) {
    // Greater than level_info.repeated_ancestor_def_level - 1 implies >= the
    // repeated_ancestor_def_level
    auto present_bitmap =
        static_cast<bit_util::extract_bitmap_t>(internal::GreaterThanBitmap(
            def_levels, batch_size, level_info.repeated_ancestor_def_level - 1));
    auto selected_bits = bit_util::ExtractBits(defined_bitmap, present_bitmap);
    int64_t selected_count = ::arrow::bit_util::PopCount(present_bitmap);
    if (ARROW_PREDICT_FALSE(selected_count > upper_bound_remaining)) {
      throw ParquetException("Values read exceeded upper bound");
    }
    writer->AppendWord(selected_bits, selected_count);
    return ::arrow::bit_util::PopCount(selected_bits);
  } else {
    if (ARROW_PREDICT_FALSE(batch_size > upper_bound_remaining)) {
      std::stringstream ss;
      ss << "Values read exceeded upper bound";
      throw ParquetException(ss.str());
    }

    writer->AppendWord(defined_bitmap, batch_size);
    return ::arrow::bit_util::PopCount(defined_bitmap);
  }
}

template <bool has_repeated_parent>
void DefLevelsToBitmapSimd(const int16_t* def_levels, int64_t num_def_levels,
                           LevelInfo level_info, ValidityBitmapInputOutput* output) {
  ::arrow::internal::FirstTimeBitmapWriter writer(
      output->valid_bits,
      /*start_offset=*/output->valid_bits_offset,
      /*length=*/output->values_read_upper_bound);
  int64_t set_count = 0;
  output->values_read = 0;
  int64_t values_read_remaining = output->values_read_upper_bound;
  while (num_def_levels > kExtractBitsSize) {
    set_count += DefLevelsBatchToBitmap<has_repeated_parent>(
        def_levels, kExtractBitsSize, values_read_remaining, level_info, &writer);
    def_levels += kExtractBitsSize;
    num_def_levels -= kExtractBitsSize;
    values_read_remaining = output->values_read_upper_bound - writer.position();
  }
  set_count += DefLevelsBatchToBitmap<has_repeated_parent>(
      def_levels, num_def_levels, values_read_remaining, level_info, &writer);

  output->values_read = writer.position();
  output->null_count += output->values_read - set_count;
  writer.Finish();
}

}  // namespace parquet::internal::PARQUET_IMPL_NAMESPACE
