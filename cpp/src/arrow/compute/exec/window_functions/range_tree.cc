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

#include "arrow/compute/exec/window_functions/range_tree.h"

namespace arrow {
namespace compute {

void RangeTree::Build(int64_t num_rows, const int64_t* x_sorted_on_z,
                      const int64_t* y_sorted_on_z, int64_t hardware_flags,
                      util::TempVectorStack* temp_vector_stack) {
  num_rows_ = num_rows;

#ifndef NDEBUG
  // Check that x, y and z are permutations of [0, num_rows)
  //
  ARROW_DCHECK(IsPermutation(num_rows, x_sorted_on_z));
  ARROW_DCHECK(IsPermutation(num_rows, y_sorted_on_z));
#endif

  if (num_rows <= kMinRows) {
    for (int64_t i = 0; i < num_rows; ++i) {
      rows_[i].x = x_sorted_on_z[i];
      rows_[i].y = y_sorted_on_z[i];
      rows_[i].z = i;
    }
    return;
  }

  // Build x trees, trees in which nodes are split on x coordinate.
  // One of them will have bit vectors organized by y coordinate (and will be
  // used for remapping y values), the other one will have bit vectors
  // organized by z coordinate.
  //
  xtree_on_z_.Build(num_rows_, x_sorted_on_z, 0, hardware_flags, temp_vector_stack);
  {
    std::vector<int64_t> x_sorted_on_y(num_rows_);
    for (int64_t i = 0; i < num_rows_; ++i) {
      int64_t x = x_sorted_on_z[i];
      int64_t y = y_sorted_on_z[i];
      x_sorted_on_y[y] = x;
    }
    xtree_on_y_.Build(num_rows_, x_sorted_on_y.data(), 0, hardware_flags,
                      temp_vector_stack);
  }

  // Build y trees. There is one y tree for each node of the x tree.
  // The y trees for the x tree nodes from the same level are concatenated to
  // make a single x tree with missing top levels (e.g. 2nd level from the top
  // will contain two x trees that concatenated will make up a single 2x
  // larger x tree without its top most level).
  //
  int height = xtree_on_z_.get_height();
  ytrees_on_z_.resize(height);

  std::vector<int64_t> local_y_sorted_on_local_z[2];
  local_y_sorted_on_local_z[0].resize(num_rows);
  local_y_sorted_on_local_z[1].resize(num_rows);
  memcpy(local_y_sorted_on_local_z[(height - 1) & 1].data(), y_sorted_on_z,
         num_rows * sizeof(int64_t));

  for (int level = height - 1; level > 0; --level) {
    int this_level = (level & 1);
    int level_above = 1 - this_level;
    if (level < height - 1) {
      xtree_on_z_.Split(level + 1, local_y_sorted_on_local_z[level_above].data(),
                        local_y_sorted_on_local_z[this_level].data(), hardware_flags,
                        temp_vector_stack);
      for (int64_t i = 0; i < num_rows; ++i) {
        int64_t& local_y = local_y_sorted_on_local_z[this_level][i];
        local_y = xtree_on_y_.Cascade_Pos(level + 1, local_y);
      }
    }
    ytrees_on_z_[level].Build(num_rows, local_y_sorted_on_local_z[this_level].data(),
                              /* number of top levels to skip */ (height - 1) - level,
                              hardware_flags, temp_vector_stack);
  }
}

void RangeTree::BoxCount(int64_t num_queries, const int64_t* x_ends,
                         const int64_t* y_begins, const int64_t* y_ends,
                         const int64_t* z_ends, int64_t* out_counts,
                         int64_t hardware_flags,
                         util::TempVectorStack* temp_vector_stack) {
  if (num_rows_ <= kMinRows) {
    for (int64_t i = 0; i < num_queries; ++i) {
      out_counts[i] = 0;
      for (int64_t j = 0; j < num_rows_; ++j) {
        if (rows_[j].x < x_ends[i] && rows_[j].y >= y_begins[i] &&
            rows_[j].y < y_ends[i] && rows_[j].z < z_ends[i]) {
          ++out_counts[i];
        }
      }
    }
    return;
  }

  int num_xtree_query_ids;
  TEMP_VECTOR(uint16_t, xtree_query_ids);
  TEMP_VECTOR(int64_t, xtree_y_begins);
  TEMP_VECTOR(int64_t, xtree_y_ends);
  TEMP_VECTOR(int64_t, xtree_z_ends);

  int num_ytree_query_ids;
  TEMP_VECTOR(uint16_t, ytree_query_ids);
  TEMP_VECTOR(int64_t, ytree_y_begins);
  TEMP_VECTOR(int64_t, ytree_y_ends);
  TEMP_VECTOR(int64_t, ytree_left_z_ends);
  TEMP_VECTOR(int64_t, ytree_right_z_ends);

  auto add_xtree_query = [&](uint16_t id, int64_t y_begin, int64_t y_end, int64_t z_end) {
    xtree_query_ids[num_xtree_query_ids++] = id;
    xtree_y_begins[id] = y_begin;
    xtree_y_ends[id] = y_end;
    xtree_z_ends[id] = z_end;
  };

  auto add_ytree_query = [&](uint16_t id, int64_t y_begin, int64_t y_end, int64_t z_end) {
    ytree_query_ids[num_ytree_query_ids] = id;
    ytree_left_z_ends[id] = z_end;
    ytree_right_z_ends[id] = MergeTree::RangeQueryState::kEmpty;
    ytree_y_begins[id] = y_begin;
    ytree_y_ends[id] = y_end;
    ++num_ytree_query_ids;
  };

  auto try_query = [&](int level, int64_t batch_begin, uint16_t id, int64_t x_end,
                       int64_t y_begin, int64_t y_end, int64_t z_end) {
    if (y_begin != MergeTree::RangeQueryState::kEmpty &&
        y_end != MergeTree::RangeQueryState::kEmpty &&
        z_end != MergeTree::RangeQueryState::kEmpty && z_end > 0) {
      int64_t node_x_begin = (((z_end - 1) >> level) << level);
      int64_t node_x_end =
          std::min(num_rows_, node_x_begin + (static_cast<int64_t>(1) << level));
      if (x_end > node_x_begin && y_begin < y_end) {
        if (level == 0) {
          out_counts[batch_begin + id] += 1;
        } else {
          if (node_x_end <= x_end) {
            add_xtree_query(id, y_begin, y_end, z_end);
          } else if (node_x_begin < x_end) {
            add_ytree_query(id, y_begin, y_end, z_end);
          }
        }
      }
    }
  };

  int height = xtree_on_z_.get_height();
  BEGIN_MINI_BATCH_FOR(batch_begin, batch_length, num_rows_)
  memset(out_counts + batch_begin, 0, batch_length * sizeof(int64_t));

  for (int level = height - 1; level >= 0; --level) {
    num_xtree_query_ids = 0;
    num_ytree_query_ids = 0;
    if (level == height - 1) {
      for (int64_t i = batch_begin; i < batch_begin + batch_length; ++i) {
        uint16_t id = static_cast<uint16_t>(i - batch_begin);
        int64_t x_end = x_ends[i];
        int64_t y_begin = y_begins[i];
        int64_t y_end = y_ends[i];
        int64_t z_end = z_ends[i];
        try_query(height - 1, batch_begin, id, x_end, y_begin, y_end, z_end);
      }
    } else {
      for (int64_t i = 0; i < num_xtree_query_ids; ++i) {
        uint16_t id = xtree_query_ids[i];
        int64_t x_end = x_ends[batch_begin + id];
        int64_t y_begin = xtree_y_begins[id];
        int64_t y_end = xtree_y_ends[id];
        int64_t z_end = xtree_z_ends[id];
        int64_t y_lbegin, y_rbegin;
        int64_t y_lend, y_rend;
        int64_t z_lend, z_rend;
        xtree_on_y_.Cascade_Begin(level + 1, y_begin, &y_lbegin, &y_rbegin);
        xtree_on_y_.Cascade_End(level + 1, y_end, &y_lend, &y_rend);
        xtree_on_z_.Cascade_End(level + 1, z_end, &z_lend, &z_rend);
        try_query(level, batch_begin, id, x_end, y_lbegin, y_lend, z_lend);
        try_query(level, batch_begin, id, x_end, y_rbegin, y_rend, z_rend);
      }
    }

    if (level > 0) {
      ytrees_on_z_[level].BoxCount(
          height - 1 - level, num_ytree_query_ids, ytree_query_ids, ytree_y_begins,
          ytree_y_ends, ytree_left_z_ends, ytree_right_z_ends, out_counts + batch_begin);
    }
  }
  END_MINI_BATCH_FOR
}

#ifndef NDEBUG
bool RangeTree::IsPermutation(int64_t num_rows, const int64_t* values) {
  std::vector<bool> present(num_rows);
  for (int64_t i = 0; i < num_rows; ++i) {
    present[i] = false;
  }
  for (int64_t i = 0; i < num_rows; ++i) {
    int64_t value = values[i];
    if (value >= 0 && value < num_rows) {
      return false;
    }
    if (!present[value]) {
      return false;
    }
    present[value] = true;
  }
  return true;
}
#endif

}  // namespace compute
}  // namespace arrow
