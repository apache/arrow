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

#include <cstdint>
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec/window_functions/bit_vector_navigator.h"
#include "arrow/compute/exec/window_functions/merge_tree.h"

namespace arrow {
namespace compute {

// All three coordinates (x, y and z) are unique integers from the range [0,
// num_rows).
//
// We also refer to local coordinates within the context of a level of a merge
// tree. Local coordinate (x, y or z) would be a result of mapping original
// coordinate by computing its rank (position in the sequence sorted on this
// coordinate) within the node of the tree from that level, plus the index of
// the first element of that node in a vector representing the level of the
// tree.
//
class RangeTree {
 public:
  void Build(int64_t num_rows, const int64_t* x_sorted_on_z, const int64_t* y_sorted_on_z,
             int64_t hardware_flags, util::TempVectorStack* temp_vector_stack);

  void BoxCount(int64_t num_queries, const int64_t* x_ends, const int64_t* y_begins,
                const int64_t* y_ends, const int64_t* z_ends, int64_t* out_counts,
                int64_t hardware_flags, util::TempVectorStack* temp_vector_stack);

 private:
#ifndef NDEBUG
  bool IsPermutation(int64_t num_rows, const int64_t* values);
#endif

  static constexpr int64_t kMinRows = 2;

  int64_t num_rows_;
  struct {
    int64_t x, y, z;
  } rows_[kMinRows];
  // Tree splitting on x coordinate
  MergeTree xtree_on_y_;  // with bitvectors indexed by y coordinate
  MergeTree xtree_on_z_;  // with bitvectors indexed by z coordinate
  // Collection of trees splitting on y coordinate (one tree for each node of
  // the xtree)
  std::vector<MergeTree> ytrees_on_z_;  // with bitvectors indexed by z coordinate
};

}  // namespace compute
}  // namespace arrow
