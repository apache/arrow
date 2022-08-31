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
#include <set>
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec/window_functions/bit_vector_navigator.h"
#include "arrow/compute/exec/window_functions/merge_path.h"
#include "arrow/compute/exec/window_functions/merge_tree.h"
#include "arrow/compute/exec/window_functions/window_frame.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {
namespace compute {

class WindowQuantiles {
 public:
  struct Quantiles {
    int num_quantiles;
    const int* numerators;
    const int* denominators;
  };

  static void ForFramesWithSeparateOrder(
      int64_t num_rows, const int64_t* permutation, const Quantiles& quantiles,
      const WindowFrames& frames,
      // TODO: output a pair of vectors instead of vector of pairs
      std::vector<std::vector<int64_t>>& output_l,
      std::vector<std::vector<int64_t>>& output_r,
      std::vector<std::vector<int>>& output_lerp, int64_t hardware_flags,
      util::TempVectorStack* temp_vector_stack);

  // Nulls are filtered out before computation.
  template <typename T>
  static bool ForEntirePartition(int64_t num_rows, const T* vals, const uint8_t* validity,
                                 const Quantiles& quantiles, T* output_l, T* output_r,
                                 int* output_lerp_nominator) {
    ARROW_DCHECK(num_rows > 0);

    int64_t num_nulls =
        validity ? num_rows - arrow::internal::CountSetBits(validity, /*offset=*/0,
                                                            static_cast<int>(num_rows))
                 : 0LL;
    if (num_nulls == num_rows) {
      return false;
    }

    // Filter out nulls
    std::vector<T> vals_reordered;
    if (validity && num_nulls > 0) {
      for (int64_t i = 0; i < num_rows; ++i) {
        if (bit_util::GetBit(validity, i)) {
          vals_reordered.push_back(vals[i]);
        }
      }
    } else {
      vals_reordered.resize(num_rows);
      memcpy(vals_reordered.data(), vals, num_rows * sizeof(T));
    }

    ForEntirePartitionNoNullsInPlace(num_rows - num_nulls, vals_reordered.data(),
                                     quantiles, output_l, output_r,
                                     output_lerp_nominator);

    return true;
  }

  template <typename T>
  static void ForEntirePartitionNoNullsInPlace(int64_t num_rows, T* vals,
                                               const Quantiles& quantiles, T* output_l,
                                               T* output_r, int* output_lerp_nominator) {
    ARROW_DCHECK(num_rows > 0);

    int num_quantiles = quantiles.num_quantiles;
    std::vector<int64_t> begins(num_quantiles);
    std::vector<int64_t> ends(num_quantiles);
    std::vector<int> permutation(num_quantiles);
    SortQuantiles(quantiles, num_rows, begins.data(), ends.data(), permutation.data());

    for (int i = 0; i < num_quantiles; ++i) {
      int id = permutation[i];
      int64_t begin = begins[id];
      int64_t end = ends[id];
      int64_t row_number;
      int lerp_nominator;
      RowNumberFromQuantile(quantiles.numerators[id], quantiles.denominators[id],
                            num_rows, &row_number, &lerp_nominator);

      if (end - begin > 1 && row_number >= begin && row_number < end) {
        std::nth_element(vals + begin, vals + row_number, vals + end);
        if (lerp_nominator < quantiles.denominators[id] && row_number + 1 < end) {
          int64_t min_row_number =
              (std::min_element(vals + row_number + 1, vals + num_rows) - vals);
          if (min_row_number != row_number + 1) {
            std::swap(vals[row_number + 1], vals[min_row_number]);
          }
        }
      }

      output_l[id] = vals[row_number];
      output_r[id] = (lerp_nominator < quantiles.denominators[id] ? vals[row_number + 1]
                                                                  : vals[row_number]);
      output_lerp_nominator[id] = lerp_nominator;
    }
  }

  // The result is the average of *output_value_l and *output_value_r.
  //
  static bool MADForEntirePartition(int64_t num_rows, const int64_t* vals,
                                    const uint8_t* validity, uint64_t* output_value_l,
                                    uint64_t* output_value_r,
                                    bool use_merge_path = false);

  static void MADForFramesWithSeparateOrder(int64_t num_rows, const int64_t* vals,
                                            const int64_t* permutation,
                                            const WindowFrames& frames, uint64_t* mad_l,
                                            uint64_t* mad_r, int64_t hardware_flags,
                                            util::TempVectorStack* temp_vector_stack);

  static void ForFramesWithSameOrder(
      int64_t num_rows, const int64_t* frame_begins, const int64_t* frame_ends,
      const Quantiles& quantiles,
      /* pair of left row_number and lerp_nominator, indexed
         by quantile id and row number */
      std::vector<std::vector<std::pair<int64_t, int>>>& output);

 private:
  static void RowNumberFromQuantile(int numerator, int denominator, int64_t num_rows,
                                    int64_t* row_number, int* lerp_nominator);

  static bool IsInterpolationNeeded(int interpolation_numerator,
                                    int interpolation_denominator);

  static bool IsInterpolationNeeded(int64_t num_rows, int quantile_numerator,
                                    int quantile_denominator);

  static void BuildMergeTree(int64_t num_rows, const int64_t* permutation,
                             MergeTree* tree, int64_t hardware_flags,
                             util::TempVectorStack* temp_vector_stack);

  static void QuantileMulti(int64_t batch_begin, int64_t batch_length,
                            const WindowFrames& frames, int quantile_numerator,
                            int quantile_denominator, const MergeTree& merge_tree,
                            std::vector<int64_t>& output_l,
                            std::vector<int64_t>& output_r, std::vector<int>& output_lerp,
                            util::TempVectorStack* temp_vector_stack);

  // Organize requested list of quantiles in order to minimize the combined
  // cost of the sequence of std::nth_element() calls.
  //
  static void SortQuantiles(const Quantiles& quantiles, int64_t num_rows,
                            /* Range of rows for which std::nth_element() will be called
                               for the kth quantile in the given original order */
                            int64_t* begins, int64_t* ends,
                            /* Permutation of given quantiles representing order in which
                               to call std::nth_element() */
                            int* permutation);
};

class WindowQuantilesBasic {
 public:
  static bool ForEntirePartition(int64_t num_rows, const int64_t* vals,
                                 const uint8_t* validity,
                                 const WindowQuantiles::Quantiles& quantiles,
                                 int64_t* output_l, int64_t* output_r,
                                 int* output_lerp_nominator);

  static bool MADForEntirePartition(int64_t num_rows, const int64_t* vals,
                                    const uint8_t* optional_validity,
                                    uint64_t* output_value_l, uint64_t* output_value_r);

  static void MADForFramesWithSeparateOrder(int64_t num_rows, const int64_t* vals,
                                            const int64_t* permutation,
                                            const WindowFrames& frames, uint64_t* mad_l,
                                            uint64_t* mad_r);

  // For empty frames the result is undefined.
  //
  static void ForFramesWithSeparateOrder(int64_t num_rows, const int64_t* permutation,
                                         const WindowQuantiles::Quantiles& quantiles,
                                         const WindowFrames& frames,
                                         std::vector<std::vector<int64_t>>& output_l,
                                         std::vector<std::vector<int64_t>>& output_r,
                                         std::vector<std::vector<int>>& output_lerp);
};

// TODO: For small number of rows (e.g. less than 100) and entire partition
// quantiles sorting can be faster than nth_element.
//

class WindowQuantilesTests {
 public:
  static void TestQuantilesForEntirePartition();

  static void TestQuantilesForFramesWithSeparateOrder();

  static void TestMADForEntirePartition();

  static void TestMADForFramesWithSeparateOrder();
};

}  // namespace compute
}  // namespace arrow
