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
#include <numeric>  // for std::iota
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec/window_functions/bit_vector_navigator.h"
#include "arrow/compute/exec/window_functions/merge_tree.h"
#include "arrow/compute/exec/window_functions/window_frame.h"

// TODO: Add support for CUME_DIST and NTILE
// TODO: Add support for rank with limit
// TODO: Add support for rank with global filter

namespace arrow {
namespace compute {

enum class RankType : int {
  ROW_NUMBER = 0,
  RANK_TIES_LOW = 1,
  RANK_TIES_HIGH = 2,
  DENSE_RANK = 3
};

class ARROW_EXPORT WindowRank_Global {
 public:
  static void Eval(RankType rank_type, const BitVectorNavigator& tie_begins,
                   int64_t batch_begin, int64_t batch_end, int64_t* results);
};

class ARROW_EXPORT WindowRank_Framed1D {
 public:
  static void Eval(RankType rank_type, const BitVectorNavigator& tie_begins,
                   const WindowFrames& frames, int64_t* results);
};

class ARROW_EXPORT WindowRank_Framed2D {
 public:
  static Status Eval(RankType rank_type, const BitVectorNavigator& rank_key_tie_begins,
                     const int64_t* order_by_rank_key, const WindowFrames& frames,
                     int64_t* results, ThreadContext& thread_context);

 private:
  static Status DenseRankWithRangeTree() {
    // TODO: Implement
    ARROW_DCHECK(false);
    return Status::OK();
  }
  static Status DenseRankWithSplayTree() {
    // TODO: Implement
    ARROW_DCHECK(false);
    return Status::OK();
  }
};

// Reference implementations used for testing.
//
// May also be useful for understanding the expected behaviour of the actual
// implementations, which trade simplicity for efficiency.
//
class ARROW_EXPORT WindowRank_Global_Ref {
 public:
  static void Eval(RankType rank_type, const BitVectorNavigator& tie_begins,
                   int64_t* results);
};

class ARROW_EXPORT WindowRank_Framed_Ref {
 public:
  // For 1D variant use null pointer for the permutation of rows ordered by
  // ranking key. That will assume that the permutation is an identity mapping.
  //
  static void Eval(RankType rank_type, const BitVectorNavigator& rank_key_tie_begins,
                   const int64_t* order_by_rank_key, const WindowFrames& frames,
                   int64_t* results);
};

}  // namespace compute
}  // namespace arrow
