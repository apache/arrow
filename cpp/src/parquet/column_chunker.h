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

#include <cmath>
#include <string>
#include <vector>
#include "arrow/array.h"
#include "parquet/level_conversion.h"

using arrow::internal::checked_cast;

namespace parquet {
namespace internal {

struct Chunk {
  int64_t level_offset;
  int64_t value_offset;
  int64_t levels_to_write;

  Chunk(int64_t level_offset, int64_t value_offset, int64_t levels_to_write)
      : level_offset(level_offset),
        value_offset(value_offset),
        levels_to_write(levels_to_write) {}
};

// have a chunker here

// rename it since it is not FastCDC anymore
class FastCDC {
 public:
  FastCDC(const LevelInfo& level_info, uint64_t avg_len, uint8_t granurality_level = 5);

  const ::arrow::Result<std::vector<Chunk>> GetBoundaries(const int16_t* def_levels,
                                                          const int16_t* rep_levels,
                                                          int64_t num_levels,
                                                          const ::arrow::Array& values);

 private:
  template <typename T>
  bool Roll(const T value);
  bool Roll(std::string_view value);
  inline bool Check(bool match);

  template <typename T>
  const std::vector<Chunk> Calculate(const int16_t* def_levels, const int16_t* rep_levels,
                                     int64_t num_levels, const T& leaf_array);

  const internal::LevelInfo& level_info_;
  const uint64_t avg_len_;
  const uint64_t min_len_;
  const uint64_t max_len_;
  const uint64_t hash_mask_;

  uint8_t nth_run_ = 0;
  uint64_t chunk_size_ = 0;
  uint64_t rolling_hash_ = 0;
};

}  // namespace internal
}  // namespace parquet
