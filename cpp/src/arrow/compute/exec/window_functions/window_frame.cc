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

#include "arrow/compute/exec/window_functions/window_frame.h"
#include <cstdint>
#include <limits>
#include "arrow/compute/exec/window_functions/bit_vector_navigator.h"

namespace arrow {
namespace compute {

void WindowFrameGenerator::GenTies(InputValues vals, InputTies* ties) {
  ties->num_rows = vals.num_rows;

  uint64_t bits = 0ULL;
  int64_t i = 0;

  auto push_bit = [&bits, &i, ties](uint64_t bit) {
    bits |= bit << (i & 63);
    if ((i & 63) == 63) {
      ties->bitvec[i / 64] = bits;
      bits = 0ULL;
    }
    ++i;
  };

  if (vals.nulls_before && vals.num_nulls > 0) {
    push_bit(1);
    for (int64_t j = 1; j < vals.num_nulls; ++j) {
      push_bit(0);
    }
  }

  if (vals.num_rows > vals.num_nulls) {
    push_bit(1);
    for (int64_t j = 1; j < vals.num_rows - vals.num_nulls; ++j) {
      push_bit(vals.vals[i] == vals.vals[i - 1] ? 0 : 1);
    }
  }

  if (!vals.nulls_before && vals.num_nulls > 0) {
    push_bit(1);
    for (int64_t j = 1; j < vals.num_nulls; ++j) {
      push_bit(0);
    }
  }

  if (vals.num_rows % 64 > 0) {
    ties->bitvec[vals.num_rows / 64] = bits;
  }
}

void WindowFrameGenerator::UnboundedRanges(InputBatch* batch, bool unbounded_left,
                                           bool unbounded_right, int64_t num_rows) {
  if (unbounded_left) {
    memset(batch->frame_begins, 0,
           (batch->end - batch->begin) * sizeof(batch->frame_begins[0]));
  }
  if (unbounded_right) {
    for (int64_t irow = batch->begin; irow < batch->end; ++irow) {
      int64_t ibatch = irow - batch->begin;
      batch->frame_ends[ibatch] = num_rows;
    }
  }
}

void WindowFrameGenerator::Groups(InputBatch* batch, bool constant_deltas,
                                  const int64_t* left_delta, const int64_t* right_delta,
                                  InputTies ties, int64_t hardware_flags,
                                  util::TempVectorStack* temp_vector_stack) {
  // The case of empty batch
  if (batch->end == batch->begin) {
    return;
  }

  // The case of unbounded ranges
  UnboundedRanges(batch, !left_delta, !right_delta, ties.num_rows);

  // The case of constant deltas
  if (constant_deltas) {
    for (int side = 0; side < 2; ++side) {
      const int64_t* delta = side == 0 ? left_delta : right_delta;
      int64_t incr = side == 0 ? 0 : 1;
      if (delta) {
        BitVectorNavigator::SelectsForRelativeRanksForRangeOfRows(
            batch->begin, batch->end, delta[0] + incr, ties.num_rows, ties.bitvec,
            ties.popcounts, batch->frame_begins, hardware_flags, temp_vector_stack);
      }
    }
  }
  // The case of varying deltas
  else {
    for (int side = 0; side < 2; ++side) {
      const int64_t* delta = side == 0 ? left_delta : right_delta;
      int64_t incr = side == 0 ? 0 : 1;
      if (delta) {
        for (int64_t irow = batch->begin; irow < batch->end; ++irow) {
          int64_t ibatch = irow - batch->begin;
          int64_t rank =
              BitVectorNavigator::RankNext(irow, ties.bitvec, ties.popcounts) - 1LL;
          int64_t select = BitVectorNavigator::Select(
              rank + delta[ibatch] + incr, ties.num_rows, ties.bitvec, ties.popcounts);
          batch->frame_begins[ibatch] = std::max(static_cast<int64_t>(0LL), select);
        }
      }
    }
  }
}

void WindowFrameGenerator::Rows(InputBatch* batch, bool constant_deltas,
                                const int64_t* left_delta, const int64_t* right_delta,
                                int64_t num_rows) {
  // The case of empty batch
  if (batch->end == batch->begin) {
    return;
  }

  // The case of unbounded ranges
  UnboundedRanges(batch, !left_delta, !right_delta, num_rows);

  int64_t mask = constant_deltas ? 0LL : ~0LL;
  for (int side = 0; side < 2; ++side) {
    const int64_t* delta = side == 0 ? left_delta : right_delta;
    int64_t incr = side == 0 ? 0 : 1;
    if (delta) {
      for (int64_t irow = batch->begin; irow < batch->end; ++irow) {
        int64_t ibatch = irow - batch->begin;
        batch->frame_begins[ibatch] = std::min(
            num_rows,
            std::max(static_cast<int64_t>(0LL), irow + delta[ibatch & mask] + incr));
      }
    }
  }
}

void WindowFrameGenerator::Range(InputBatch* batch, bool constant_deltas,
                                 const int64_t* left_delta, const int64_t* right_delta,
                                 InputValues vals) {
  // The case of empty batch
  if (batch->end == batch->begin) {
    return;
  }

  // The case of unbounded ranges
  UnboundedRanges(batch, !left_delta, !right_delta, vals.num_rows);

  // Handle nulls. Every null row in a batch will be assigned a frame that
  // consist of all the nulls.
  int64_t rows_begin = 0;
  int64_t rows_end = vals.num_rows;
  int64_t batch_begin = batch->begin;
  int64_t batch_end = batch->end;
  if (vals.num_nulls > 0) {
    if (vals.nulls_before) {
      rows_begin += vals.num_nulls;
      if (batch->begin < vals.num_nulls) {
        for (int64_t irow = batch->begin; irow < std::min(batch->end, vals.num_nulls);
             ++irow) {
          int64_t ibatch = irow - batch->begin;
          batch->frame_begins[ibatch] = 0;
          batch->frame_ends[ibatch] = vals.num_nulls;
        }
        batch_begin = std::min(batch->end, vals.num_nulls);
      }
    } else {
      rows_end -= vals.num_nulls;
      if (batch->end > vals.num_rows - vals.num_nulls) {
        for (int64_t irow = std::max(batch->begin, vals.num_rows - vals.num_nulls);
             irow < batch->end; ++irow) {
          int64_t ibatch = irow - batch->begin;
          batch->frame_begins[ibatch] = vals.num_rows - vals.num_nulls;
          batch->frame_ends[ibatch] = vals.num_rows;
        }
        batch_end = std::max(batch->begin, vals.num_rows - vals.num_nulls);
      }
    }
    if (rows_begin == rows_end) {
      for (int64_t ibatch = batch_begin; ibatch < batch_end; ++ibatch) {
        batch->frame_begins[ibatch] = vals.nulls_before ? vals.num_rows : 0;
        batch->frame_ends[ibatch] = batch->frame_begins[ibatch];
      }
      return;
    }
    if (batch_begin == batch_end) {
      return;
    }
  }

  // The case of constant deltas
  if (constant_deltas) {
    if (left_delta) {
      int64_t current_frame_begin =
          rows_begin + (std::lower_bound(vals.vals + rows_begin, vals.vals + rows_end,
                                         vals.vals[batch_begin] + left_delta[0]) -
                        vals.vals);

      batch->frame_begins[0] = current_frame_begin;

      for (int64_t irow = batch_begin + 1; irow < batch_end; ++irow) {
        int64_t ibatch = irow - batch->begin;
        int64_t current_frame_left_bound = vals.vals[irow] + left_delta[0];
        while (current_frame_begin < vals.num_rows &&
               vals.vals[current_frame_begin] < current_frame_left_bound) {
          ++current_frame_begin;
        }
        batch->frame_begins[ibatch] = current_frame_begin;
      }
    }
    if (right_delta) {
      int64_t current_frame_end =
          rows_begin + (std::upper_bound(vals.vals + rows_begin, vals.vals + rows_end,
                                         vals.vals[batch->begin] + right_delta[0]) -
                        vals.vals);

      batch->frame_ends[0] = current_frame_end;

      for (int64_t irow = batch_begin + 1; irow < batch_end; ++irow) {
        int64_t ibatch = irow - batch->begin;
        int64_t current_frame_left_bound = vals.vals[irow] + left_delta[0];
        while (current_frame_end < vals.num_rows &&
               vals.vals[current_frame_end] <= current_frame_left_bound) {
          ++current_frame_end;
        }
        batch->frame_ends[ibatch] = current_frame_end;
      }
    }
  }
  // The case of varying deltas
  else {
    if (left_delta) {
      for (int64_t irow = batch_begin; irow < batch_end; ++irow) {
        int64_t ibatch = irow - batch->begin;

        int64_t frame_left_bound = vals.vals[irow] + left_delta[ibatch];
        int64_t frame_begin =
            rows_begin + (std::lower_bound(vals.vals + rows_begin, vals.vals + rows_end,
                                           frame_left_bound) -
                          vals.vals);

        batch->frame_begins[ibatch] = frame_begin;
      }
    }
    if (right_delta) {
      for (int64_t irow = batch_begin; irow < batch_end; ++irow) {
        int64_t ibatch = irow - batch->begin;

        int64_t frame_right_bound = vals.vals[irow] + right_delta[ibatch];
        int64_t frame_end =
            rows_begin + (std::upper_bound(vals.vals + rows_begin, vals.vals + rows_end,
                                           frame_right_bound) -
                          vals.vals);

        batch->frame_ends[ibatch] = frame_end;
      }
    }
  }
}

void WindowFrameGeneratorBasic::Groups(WindowFrameGenerator::InputBatch* batch,
                                       bool constant_deltas, const int64_t* left_delta,
                                       const int64_t* right_delta,
                                       WindowFrameGenerator::InputValues vals) {
  const int64_t mask = constant_deltas ? 0LL : ~0LL;
  for (int64_t i = batch->begin; i < batch->end; ++i) {
    if (left_delta) {
      batch->frame_begins[i - batch->begin] =
          GroupBegin(i, left_delta[(i - batch->begin) & mask], vals);
    } else {
      batch->frame_begins[i - batch->begin] = 0;
    }
    if (right_delta) {
      batch->frame_ends[i - batch->begin] =
          GroupBegin(i, right_delta[(i - batch->begin) & mask] + 1, vals);
    } else {
      batch->frame_ends[i - batch->begin] = vals.num_rows;
    }
  }
}

void WindowFrameGeneratorBasic::Rows(WindowFrameGenerator::InputBatch* batch,
                                     bool constant_deltas, const int64_t* left_delta,
                                     const int64_t* right_delta, int64_t num_rows) {
  const int64_t mask = constant_deltas ? 0LL : ~0LL;
  if (left_delta) {
    for (int64_t i = batch->begin; i < batch->end; ++i) {
      batch->frame_begins[i - batch->begin] = std::min(
          num_rows,
          std::max(static_cast<int64_t>(0LL), i + left_delta[(i - batch->begin) & mask]));
    }
  } else {
    for (int64_t i = batch->begin; i < batch->end; ++i) {
      batch->frame_begins[i - batch->begin] = 0LL;
    }
  }
  if (right_delta) {
    for (int64_t i = batch->begin; i < batch->end; ++i) {
      batch->frame_ends[i - batch->begin] =
          std::min(num_rows, std::max(static_cast<int64_t>(0LL),
                                      i + right_delta[(i - batch->begin) & mask] + 1));
    }
  } else {
    for (int64_t i = batch->begin; i < batch->end; ++i) {
      batch->frame_ends[i - batch->begin] = num_rows;
    }
  }
}

void WindowFrameGeneratorBasic::Range(WindowFrameGenerator::InputBatch* batch,
                                      bool constant_deltas, const int64_t* left_delta,
                                      const int64_t* right_delta,
                                      WindowFrameGenerator::InputValues vals) {
  auto equals = [vals](int64_t l, int64_t r) {
    bool is_null_l =
        vals.nulls_before ? l < vals.num_nulls : l >= vals.num_rows - vals.num_nulls;
    bool is_null_r =
        vals.nulls_before ? r < vals.num_nulls : r >= vals.num_rows - vals.num_nulls;
    return (is_null_l && is_null_r) ||
           (!is_null_l && !is_null_r && (vals.vals[l] == vals.vals[r]));
  };

  const int64_t mask = constant_deltas ? 0LL : ~0LL;

  if (left_delta) {
    for (int64_t i = batch->begin; i < batch->end; ++i) {
      batch->frame_begins[i - batch->begin] =
          RangeBegin(vals.vals[i] + left_delta[(i - batch->begin) & mask], vals);
    }
  } else {
    for (int64_t i = batch->begin; i < batch->end; ++i) {
      batch->frame_begins[i - batch->begin] = 0LL;
    }
  }

  if (right_delta) {
    for (int64_t i = batch->begin; i < batch->end; ++i) {
      int64_t last =
          RangeBegin(vals.vals[i] + right_delta[(i - batch->begin) & mask], vals);
      while (last + 1 < vals.num_rows && equals(last + 1, last)) {
        ++last;
      }
      batch->frame_ends[i - batch->begin] = last + 1;
    }
  } else {
    for (int64_t i = batch->begin; i < batch->end; ++i) {
      batch->frame_ends[i - batch->begin] = vals.num_rows;
    }
  }
}

int64_t WindowFrameGeneratorBasic::GroupBegin(int64_t row_number,
                                              int64_t relative_group_number,
                                              WindowFrameGenerator::InputValues vals) {
  auto equals = [vals](int64_t l, int64_t r) {
    bool is_null_l =
        vals.nulls_before ? l < vals.num_nulls : l >= vals.num_rows - vals.num_nulls;
    bool is_null_r =
        vals.nulls_before ? r < vals.num_nulls : r >= vals.num_rows - vals.num_nulls;
    return (is_null_l && is_null_r) ||
           (!is_null_l && !is_null_r && (vals.vals[l] == vals.vals[r]));
  };

  // Find first row in current row's group
  int64_t begin = row_number;
  while (begin > 0 && equals(begin - 1, begin)) {
    --begin;
  }
  if (relative_group_number < 0) {
    // Skip a given number of preceding groups
    for (int64_t group = 0; group < -relative_group_number; ++group) {
      if (begin > 0) {
        --begin;
      }
      while (begin > 0 && equals(begin - 1, begin)) {
        --begin;
      }
    }
  } else {
    // Skip a given number of following groups
    for (int64_t group = 0; group < relative_group_number; ++group) {
      while (begin < vals.num_rows && equals(begin + 1, begin)) {
        ++begin;
      }
      if (begin < vals.num_rows) {
        ++begin;
      }
    }
  }
  return begin;
}

int64_t WindowFrameGeneratorBasic::RangeBegin(int64_t val,
                                              WindowFrameGenerator::InputValues vals) {
  auto less = [vals](int64_t index, int64_t value) {
    bool is_null_l = vals.nulls_before ? index < vals.num_nulls
                                       : index >= vals.num_rows - vals.num_nulls;
    bool is_null_r = false;
    if (vals.nulls_before) {
      if (is_null_l && !is_null_r) {
        return true;
      }
      if (is_null_r) {
        return false;
      }
    } else {
      if (!is_null_l && is_null_r) {
        return true;
      }
      if (is_null_l) {
        return false;
      }
    }
    return (vals.vals[index] < value);
  };

  int64_t result = 0;
  while (result < vals.num_rows && less(result, val)) {
    ++result;
  }
  return result;
  // return std::lower_bound(vals, vals + num_rows, val) - vals;
}

}  // namespace compute
}  // namespace arrow
