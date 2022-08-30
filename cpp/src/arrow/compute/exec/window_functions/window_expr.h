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
#include <numeric>
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec/window_functions/bit_vector_navigator.h"
#include "arrow/compute/exec/window_functions/merge_tree.h"

// This file collects all interface used by window function implementation that
// need to be aware of different data types and operators operating on them
// (addition, subtraction, comparison).
//
// The rest of window function implementation does not ever need to access and
// interpret input values directly.
//

namespace arrow {
namespace compute {

class WindowAggregateFunc {
 public:
  class Vector {
   public:
    virtual ~Vector() {}
  };
  // virtual bool HasInverse() const;
  virtual Vector* Alloc(int64_t num) = 0;
  virtual void Zero(int64_t num, Vector* out_vec, int64_t out_begin) = 0;
  virtual void Sum(int64_t num, Vector* out_vec, const int64_t* out_ids,
                   // If in_vec is null then use source array values instead
                   const Vector* in_vec, const int64_t* in_ids) = 0;
  // virtual void Take(int64_t num, Vector *out_vec, int64_t out_begin,
  //                   const Vector *in_vec, const int64_t *in_ids,
  //                   const uint8_t *opt_in_invert_bitvec) = 0;
  // virtual void PrefixSum(int64_t num, Vector *out_vec, int64_t out_begin,
  //                        bool preceded_by_base_value,
  //                        int log_reset_distance = 62) = 0;
  virtual void Split(int64_t num, Vector* out_vec, const Vector* in_vec,
                     const MergeTree& merge_tree, int level, int64_t hardware_flags,
                     util::TempVectorStack* temp_vector_stack) = 0;
  // Inclusive prefix sum
  //
  virtual void PrefixSum(int64_t num, Vector* out_vec, const Vector* in_vec,
                         int log_segment_length) = 0;
};

class WinAggFun_SumInt64 : public WindowAggregateFunc {
 public:
  class VectorUint128 : public WindowAggregateFunc::Vector {
   public:
    VectorUint128(int64_t num) { vals_.resize(num); }
    ~VectorUint128() override {}
    std::vector<std::pair<uint64_t, uint64_t>> vals_;
  };
  WinAggFun_SumInt64() : num_rows_(0), source_(nullptr) {}
  void Init(int64_t num_rows, const int64_t* source) {
    num_rows_ = num_rows;
    source_ = source;
  }
  VectorUint128* Alloc(int64_t num) override { return new VectorUint128(num); }
  void Zero(int64_t num, Vector* out_vec, int64_t out_begin) override {
    VectorUint128* out_vec_cast = reinterpret_cast<VectorUint128*>(out_vec);
    for (int64_t i = 0; i < num; ++i) {
      out_vec_cast->vals_[i] = std::make_pair(0ULL, 0ULL);
    }
  }
  void Sum(int64_t num, Vector* out_vec, const int64_t* out_ids,
           // If in_vec is null then use source array values instead
           const Vector* in_vec, const int64_t* in_ids) override {
    VectorUint128* out_vec_cast = reinterpret_cast<VectorUint128*>(out_vec);
    if (in_vec) {
      const VectorUint128* in_vec_cast = reinterpret_cast<const VectorUint128*>(in_vec);
      for (int64_t i = 0; i < num; ++i) {
        std::pair<uint64_t, uint64_t>& dst = out_vec_cast->vals_[out_ids[i]];
        const std::pair<uint64_t, uint64_t>& src = in_vec_cast->vals_[in_ids[i]];
        dst.first += src.first;
        dst.second += src.second + (dst.first < src.first ? 1 : 0);
      }
    } else {
      for (int64_t i = 0; i < num; ++i) {
        std::pair<uint64_t, uint64_t>& dst = out_vec_cast->vals_[out_ids[i]];
        uint64_t src = static_cast<uint64_t>(source_[in_ids[i]]);
        dst.first += src;
        dst.second += static_cast<uint64_t>(-static_cast<int64_t>(src >> 63)) +
                      (dst.first < src ? 1 : 0);
      }
    }
  }

  void Split(int64_t num, Vector* out_vec, const Vector* in_vec,
             const MergeTree& merge_tree, int level, int64_t hardware_flags,
             util::TempVectorStack* temp_vector_stack) override {
    const VectorUint128* in_vec_cast = reinterpret_cast<const VectorUint128*>(in_vec);
    VectorUint128* out_vec_cast = reinterpret_cast<VectorUint128*>(out_vec);
    merge_tree.Split(level, in_vec_cast->vals_.data(), out_vec_cast->vals_.data(),
                     hardware_flags, temp_vector_stack);
  }

  void PrefixSum(int64_t num, Vector* out_vec, const Vector* in_vec,
                 int log_segment_length) override {
    const VectorUint128* in_vec_cast = reinterpret_cast<const VectorUint128*>(in_vec);
    VectorUint128* out_vec_cast = reinterpret_cast<VectorUint128*>(out_vec);

    int64_t mask = (1LL << log_segment_length) - 1;

    uint64_t sum_lo, sum_hi;
    for (int64_t i = 0; i < num; ++i) {
      if ((i & mask) == 0) {
        sum_lo = sum_hi = 0;
      }
      const std::pair<uint64_t, uint64_t>& src = in_vec_cast->vals_[i];
      std::pair<uint64_t, uint64_t>& dst = out_vec_cast->vals_[i];
      sum_lo += src.first;
      sum_hi += src.second + (sum_lo < src.first ? 1 : 0);
      dst.first = sum_lo;
      dst.second = sum_hi;
    }
  }

 private:
  int64_t num_rows_;
  const int64_t* source_;
};

}  // namespace compute
}  // namespace arrow
