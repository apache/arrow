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

#include "arrow/compute/exec/window_functions/window_quantiles.h"

namespace arrow {
namespace compute {

void WindowQuantiles::ForFramesWithSeparateOrder(
    int64_t num_rows, const int64_t* permutation, const Quantiles& quantiles,
    const WindowFrames& frames,
    // TODO: output a pair of vectors instead of vector of pairs
    std::vector<std::vector<int64_t>>& output_l,
    std::vector<std::vector<int64_t>>& output_r,
    std::vector<std::vector<int>>& output_lerp, int64_t hardware_flags,
    util::TempVectorStack* temp_vector_stack) {
  const int64_t* frame_begins = frames.begins[0];
  const int64_t* frame_ends = frames.ends[0];

  // Build pure merge tree - with no data in the nodes
  //
  MergeTree tree;
  BuildMergeTree(num_rows, permutation, &tree, hardware_flags, temp_vector_stack);

  int num_quantiles = quantiles.num_quantiles;

  // Reserve space for output
  //
  output_l.resize(num_quantiles);
  output_r.resize(num_quantiles);
  output_lerp.resize(num_quantiles);
  for (int i = 0; i < num_quantiles; ++i) {
    output_l[i].resize(num_rows);
    output_r[i].resize(num_rows);
    output_lerp[i].resize(num_rows);
  }

  // Run queries one quantile at a time, one mini-batch at a time.
  //
  int64_t batch_length_max = util::MiniBatch::kMiniBatchLength;

  for (int quantile = 0; quantile < num_quantiles; ++quantile) {
    for (int64_t batch_begin = 0; batch_begin < num_rows;
         batch_begin += batch_length_max / 2) {
      int64_t batch_length = std::min(num_rows - batch_begin, batch_length_max / 2);

      QuantileMulti(batch_begin, batch_length, frames, quantiles.numerators[quantile],
                    quantiles.denominators[quantile], tree, output_l[quantile],
                    output_r[quantile], output_lerp[quantile], temp_vector_stack);

      // At this point we have row numbers relative to the sorted order
      // represented by permutation array. To get original row numbers we have
      // to map using permutation array.
      //
      for (int64_t i = batch_begin; i < batch_begin + batch_length; ++i) {
        if (frame_begins[i] < frame_ends[i]) {
          output_l[quantile][i] = permutation[output_l[quantile][i]];
          output_r[quantile][i] = permutation[output_r[quantile][i]];
        }
      }
    }
  }
}

// The result is the average of *output_value_l and *output_value_r.
//
bool WindowQuantiles::MADForEntirePartition(int64_t num_rows, const int64_t* vals,
                                            const uint8_t* validity,
                                            uint64_t* output_value_l,
                                            uint64_t* output_value_r,
                                            bool use_merge_path) {
  ARROW_DCHECK(num_rows > 0);

  int64_t num_nulls =
      validity ? num_rows - arrow::internal::CountSetBits(validity, /*offset=*/0,
                                                          static_cast<int>(num_rows))
               : 0LL;
  if (num_nulls == num_rows) {
    return false;
  }

  // Filter out nulls
  std::vector<int64_t> vals_reordered;
  if (validity && num_nulls > 0) {
    for (int64_t i = 0; i < num_rows; ++i) {
      if (bit_util::GetBit(validity, i)) {
        vals_reordered.push_back(vals[i]);
      }
    }
  } else {
    vals_reordered.resize(num_rows);
    memcpy(vals_reordered.data(), vals, num_rows * sizeof(int64_t));
  }

  Quantiles median;
  int numerator = 1;
  int denominator = 2;
  median.num_quantiles = 1;
  median.numerators = &numerator;
  median.denominators = &denominator;
  int64_t median_l;
  int64_t median_r;
  int ignored;
  ForEntirePartitionNoNullsInPlace(num_rows - num_nulls, vals_reordered.data(), median,
                                   &median_l, &median_r, &ignored);

  std::vector<uint64_t> absolute_differences(vals_reordered.size());
  for (size_t i = 0; i < vals_reordered.size(); ++i) {
    int64_t value = vals_reordered[i];
    if (i < (vals_reordered.size() + 1) / 2) {
      ARROW_DCHECK(value <= median_l);
      absolute_differences[i] = median_r - value;
    } else {
      ARROW_DCHECK(value >= median_r);
      absolute_differences[i] = value - median_l;
    }
    // absolute_differences[i] =
    //    value <= median_l ? (median_r - value) : (value - median_l);
  }

  uint64_t mad_l;
  uint64_t mad_r;

  if (use_merge_path) {
    int64_t length_l = (vals_reordered.size() + 1) / 2;
    int64_t length_r = vals_reordered.size() - length_l;
    bool first_from_l, second_from_l;
    int64_t first_offset, second_offset;
    if (vals_reordered.size() % 2 == 1) {
      MergeUtil::NthElement(
          length_l - 1, length_l, length_r, &first_from_l, &first_offset,
          [&](int64_t offset_l, int64_t offset_r) {
            int64_t row_number_l = length_l - 1 - offset_l;
            int64_t row_number_r = length_l + offset_r;
            std::nth_element(vals_reordered.data(), vals_reordered.data() + row_number_l,
                             vals_reordered.data() + vals_reordered.size());
            std::nth_element(vals_reordered.data(), vals_reordered.data() + row_number_r,
                             vals_reordered.data() + vals_reordered.size());
            uint64_t left_value = median_r - vals_reordered[row_number_l];
            uint64_t right_value = vals_reordered[row_number_r] - median_l;
            return left_value < right_value;
          });
      second_from_l = first_from_l;
      second_offset = first_offset;
    } else {
      MergeUtil::NthPair(
          length_l - 1, length_l, length_r, &first_from_l, &first_offset, &second_from_l,
          &second_offset, [&](int64_t offset_l, int64_t offset_r) {
            int64_t row_number_l = length_l - 1 - offset_l;
            int64_t row_number_r = length_l + offset_r;
            std::nth_element(vals_reordered.data(), vals_reordered.data() + row_number_l,
                             vals_reordered.data() + vals_reordered.size());
            std::nth_element(vals_reordered.data(), vals_reordered.data() + row_number_r,
                             vals_reordered.data() + vals_reordered.size());
            uint64_t left_value = median_r - vals_reordered[row_number_l];
            uint64_t right_value = vals_reordered[row_number_r] - median_l;
            return left_value < right_value;
          });
    }
    mad_l = (first_from_l ? median_r - vals_reordered[length_l - 1 - first_offset]
                          : vals_reordered[length_l + first_offset] - median_l);
    mad_r = (second_from_l ? median_r - vals_reordered[length_l - 1 - second_offset]
                           : vals_reordered[length_l + second_offset] - median_l);
  } else {
    ForEntirePartitionNoNullsInPlace(num_rows - num_nulls, absolute_differences.data(),
                                     median, &mad_l, &mad_r, &ignored);
  }

  *output_value_l = mad_l - (median_r - median_l);
  *output_value_r = mad_r;

  return true;
}

void WindowQuantiles::MADForFramesWithSeparateOrder(
    int64_t num_rows, const int64_t* vals, const int64_t* permutation,
    const WindowFrames& frames, uint64_t* mad_l, uint64_t* mad_r, int64_t hardware_flags,
    util::TempVectorStack* temp_vector_stack) {
  // Build pure merge tree - with no data in the nodes
  //
  MergeTree tree;
  BuildMergeTree(num_rows, permutation, &tree, hardware_flags, temp_vector_stack);

  auto nth_value_in_frame = [&](int64_t frame_begin, int64_t frame_end, int64_t n) {
    return vals[permutation[tree.NthElement(frame_begin, frame_end, n)]];
  };

  for (int64_t i = 0; i < num_rows; ++i) {
    int64_t frame_begin = frames.begins[0][i];
    int64_t frame_end = frames.ends[0][i];
    int64_t length = frame_end - frame_begin;
    ARROW_DCHECK(length > 0);

    int64_t median_l = nth_value_in_frame(frame_begin, frame_end, (length - 1) / 2);
    int64_t median_r = (length % 2 == 1)
                           ? median_l
                           : nth_value_in_frame(frame_begin, frame_end, (length + 1) / 2);

    int64_t length_l = (length + 1) / 2;
    int64_t length_r = length - length_l;
    bool first_from_l, second_from_l;
    int64_t first_offset, second_offset;
    if (length % 2 == 1) {
      MergeUtil::NthElement(length_l - 1, length_l, length_r, &first_from_l,
                            &first_offset, [&](int64_t offset_l, int64_t offset_r) {
                              int64_t val_l = nth_value_in_frame(frame_begin, frame_end,
                                                                 length_l - 1 - offset_l);
                              int64_t val_r = nth_value_in_frame(frame_begin, frame_end,
                                                                 length_l + offset_r);
                              uint64_t diff_l = median_r - val_l;
                              uint64_t diff_r = val_r - median_l;
                              return diff_l < diff_r;
                            });
      second_from_l = first_from_l;
      second_offset = first_offset;
    } else {
      MergeUtil::NthPair(
          length_l - 1, length_l, length_r, &first_from_l, &first_offset, &second_from_l,
          &second_offset, [&](int64_t offset_l, int64_t offset_r) {
            int64_t val_l =
                nth_value_in_frame(frame_begin, frame_end, length_l - 1 - offset_l);
            int64_t val_r =
                nth_value_in_frame(frame_begin, frame_end, length_l + offset_r);
            uint64_t diff_l = median_r - val_l;
            uint64_t diff_r = val_r - median_l;
            return diff_l < diff_r;
          });
    }
    mad_l[i] =
        (first_from_l
             ? median_r -
                   nth_value_in_frame(frame_begin, frame_end, length_l - 1 - first_offset)
             : nth_value_in_frame(frame_begin, frame_end, length_l + first_offset) -
                   median_l);
    mad_r[i] =
        (second_from_l
             ? median_r - nth_value_in_frame(frame_begin, frame_end,
                                             length_l - 1 - second_offset)
             : nth_value_in_frame(frame_begin, frame_end, length_l + second_offset) -
                   median_l);

    mad_l[i] -= (median_r - median_l);
  }
}

void WindowQuantiles::ForFramesWithSameOrder(
    int64_t num_rows, const int64_t* frame_begins, const int64_t* frame_ends,
    const Quantiles& quantiles,
    /* pair of left row_number and lerp_nominator, indexed
       by quantile id and row number */
    std::vector<std::vector<std::pair<int64_t, int>>>& output) {
  int num_quantiles = quantiles.num_quantiles;
  output.resize(num_quantiles);
  for (int i = 0; i < num_quantiles; ++i) {
    output[i].resize(num_rows);
  }
  for (int64_t row = 0; row < num_rows; ++row) {
    int64_t frame_begin = frame_begins[row];
    int64_t frame_end = frame_ends[row];
    if (frame_end == frame_begin) {
      for (int quantile = 0; quantile < num_quantiles; ++quantile) {
        output[quantile][row] = std::make_pair(-1, -1);
      }
    }
    for (int quantile = 0; quantile < num_quantiles; ++quantile) {
      int64_t output_row_number;
      int lerp_nominator;
      RowNumberFromQuantile(quantiles.numerators[quantile],
                            quantiles.denominators[quantile], frame_end - frame_begin,
                            &output_row_number, &lerp_nominator);
      output_row_number += frame_begin;
      output[quantile][row] = std::make_pair(output_row_number, lerp_nominator);
    }
  }
}

void WindowQuantiles::RowNumberFromQuantile(int numerator, int denominator,
                                            int64_t num_rows, int64_t* row_number,
                                            int* lerp_nominator) {
  ARROW_DCHECK(numerator >= 0 && numerator <= denominator && denominator > 0);
  *row_number = (num_rows - 1) * numerator / denominator;
  *lerp_nominator = denominator - (((num_rows - 1) * numerator) % denominator);
}

bool WindowQuantiles::IsInterpolationNeeded(int interpolation_numerator,
                                            int interpolation_denominator) {
  return interpolation_numerator == interpolation_denominator;
}

bool WindowQuantiles::IsInterpolationNeeded(int64_t num_rows, int quantile_numerator,
                                            int quantile_denominator) {
  return (num_rows - 1) * quantile_numerator % quantile_denominator == 0;
}

void WindowQuantiles::BuildMergeTree(int64_t num_rows, const int64_t* permutation,
                                     MergeTree* tree, int64_t hardware_flags,
                                     util::TempVectorStack* temp_vector_stack) {
  // Build inverse permutation
  //
  std::vector<int64_t> inverse_permutation(num_rows);
  for (int64_t i = 0; i < num_rows; ++i) {
    inverse_permutation[permutation[i]] = i;
  }

  // Build merge bit vectors with counters (bitvec and popcounts) for each
  // level of the tree above 0.
  //
  tree->Build(num_rows, inverse_permutation.data(), 0, hardware_flags, temp_vector_stack);
}

void WindowQuantiles::QuantileMulti(int64_t batch_begin, int64_t batch_length,
                                    const WindowFrames& frames, int quantile_numerator,
                                    int quantile_denominator, const MergeTree& merge_tree,
                                    std::vector<int64_t>& output_l,
                                    std::vector<int64_t>& output_r,
                                    std::vector<int>& output_lerp,
                                    util::TempVectorStack* temp_vector_stack) {
  const int64_t* frame_begins = frames.begins[0];
  const int64_t* frame_ends = frames.ends[0];

  // Allocate temporary buffers
  //
  auto ids_buf = util::TempVectorHolder<uint16_t>(temp_vector_stack,
                                                  static_cast<uint32_t>(batch_length));
  uint16_t* ids = ids_buf.mutable_data();
  int num_ids = 0;
  auto frames_ns_buf = util::TempVectorHolder<int64_t>(
      temp_vector_stack, static_cast<uint32_t>(batch_length));
  int64_t* frames_ns = frames_ns_buf.mutable_data();

  // Filter out empty frames
  //
  for (int64_t i = 0; i < batch_length; ++i) {
    int64_t frame_begin = frame_begins[batch_begin + i];
    int64_t frame_end = frame_ends[batch_begin + i];
    if (frame_end > frame_begin) {
      ids[num_ids++] = static_cast<uint16_t>(i);
    }
  }

  // Compute n for nth element based on quantile and frames.
  //
  for (int64_t i = 0; i < num_ids; ++i) {
    uint16_t id = ids[i];
    int64_t frame_number = batch_begin + id;
    int64_t frame_begin = frame_begins[frame_number];
    int64_t frame_end = frame_ends[frame_number];
    int64_t row_number;
    int lerp_nominator;
    RowNumberFromQuantile(quantile_numerator, quantile_denominator,
                          frame_end - frame_begin, &row_number, &lerp_nominator);
    output_lerp[frame_number] = lerp_nominator;
    frames_ns[id] = row_number;
  }

  // For each frame we may run up to two queries: for finding nth row
  // and (n+1)th row in the merge tree sort order within the given frame.
  // Depending on the number of rows in the frame and the quantile the
  // second row may not be needed.
  //
  merge_tree.NthElement(num_ids, ids, frame_begins + batch_begin,
                        frame_ends + batch_begin, frames_ns,
                        output_l.data() + batch_begin, temp_vector_stack);

  // For the second pass filter out frames for which the quantile is
  // determined by a value of a single row (for which the interpolation
  // between a pair of rows is not needed).
  //
  int num_ids_new = 0;
  for (int64_t i = 0; i < num_ids; ++i) {
    uint16_t id = ids[i];
    int64_t frame_number = batch_begin + id;
    if (output_lerp[frame_number] < quantile_denominator) {
      ids[num_ids_new++] = id;
      ++frames_ns[id];
    } else {
      output_r[batch_begin + id] = output_l[batch_begin + id];
    }
  }
  num_ids = num_ids_new;

  merge_tree.NthElement(num_ids, ids, frame_begins + batch_begin,
                        frame_ends + batch_begin, frames_ns,
                        output_r.data() + batch_begin, temp_vector_stack);
}

// Organize requested list of quantiles in order to minimize the combined
// cost of the sequence of std::nth_element() calls.
//
void WindowQuantiles::SortQuantiles(
    const Quantiles& quantiles, int64_t num_rows,
    /* Range of rows for which std::nth_element() will be called for
       the kth quantile in the given original order */
    int64_t* begins, int64_t* ends,
    /* Permutation of given quantiles representing order in which to
       call std::nth_element() */
    int* permutation) {
  int num_quantiles = quantiles.num_quantiles;

  // Find row numbers corresponding to quantiles and sort quantile ids on
  // them.
  //
  std::vector<std::pair<int64_t, int>> row_numbers;
  for (int i = 0; i < num_quantiles; ++i) {
    int64_t row_number;
    int lerp_nominator;
    RowNumberFromQuantile(quantiles.numerators[i], quantiles.denominators[i], num_rows,
                          &row_number, &lerp_nominator);
    row_numbers.push_back(std::make_pair(row_number, i));
  }
  std::sort(row_numbers.begin(), row_numbers.end());

  const int left_boundary = num_quantiles;
  const int right_boundary = num_quantiles + 1;
  row_numbers.push_back(std::make_pair(0LL, left_boundary));
  row_numbers.push_back(std::make_pair(num_rows, right_boundary));

  // Connect into doubly linked list
  //
  std::vector<int> prev(num_quantiles + 2);
  std::vector<int> next(num_quantiles + 2);
  for (int i = 0; i < num_quantiles; ++i) {
    prev[i] = i == 0 ? left_boundary : i - 1;
    next[i] = i == num_quantiles - 1 ? right_boundary : i + 1;
  }

  prev[left_boundary] = left_boundary;
  next[left_boundary] = 0;
  prev[right_boundary] = num_quantiles - 1;
  next[right_boundary] = right_boundary;

  // We look for the quantile with the smallest sum of distances to its
  // direct neighbours (or range boundaries if it is the first or the
  // last). We remove it and repeat the process for the remaining list of
  // quantiles.
  //
  std::set<std::pair<int64_t, int>> ranges;
  auto add_range = [&](int i) {
    if (i != left_boundary && i != right_boundary) {
      ranges.insert(
          std::make_pair(row_numbers[next[i]].first - row_numbers[prev[i]].first, i));
    }
  };
  auto del_range = [&](int i) {
    if (i != left_boundary && i != right_boundary) {
      ranges.erase(ranges.find(
          std::make_pair(row_numbers[next[i]].first - row_numbers[prev[i]].first, i)));
    }
  };
  for (int i = 0; i < num_quantiles; ++i) {
    add_range(i);
  }
  for (int output_pos = 0; output_pos < num_quantiles; ++output_pos) {
    int i = ranges.begin()->second;
    int id = row_numbers[i].second;

    permutation[output_pos] = id;
    begins[id] = row_numbers[prev[i]].first;
    ends[id] = row_numbers[next[i]].first;

    ranges.erase(ranges.begin());
    del_range(prev[i]);
    del_range(next[i]);
    next[prev[i]] = next[i];
    prev[next[i]] = prev[i];
    add_range(prev[i]);
    add_range(next[i]);
  }
  std::reverse(permutation, permutation + num_quantiles);
}

bool WindowQuantilesBasic::ForEntirePartition(int64_t num_rows, const int64_t* vals,
                                              const uint8_t* validity,
                                              const WindowQuantiles::Quantiles& quantiles,
                                              int64_t* output_l, int64_t* output_r,
                                              int* output_lerp_nominator) {
  std::vector<int64_t> vals_sorted;
  for (int64_t i = 0; i < num_rows; ++i) {
    if (bit_util::GetBit(validity, i)) {
      vals_sorted.push_back(vals[i]);
    }
  }
  if (vals_sorted.empty()) {
    return false;
  }

  std::sort(vals_sorted.begin(), vals_sorted.end());

  int64_t num_vals_sorted = static_cast<int64_t>(vals_sorted.size());
  for (int quantile = 0; quantile < quantiles.num_quantiles; ++quantile) {
    int numerator = quantiles.numerators[quantile];
    int denominator = quantiles.denominators[quantile];
    int64_t id_l = (num_vals_sorted - 1) * numerator / denominator;
    int64_t id_r = id_l + 1;

    output_l[quantile] = vals_sorted[id_l];
    output_lerp_nominator[quantile] = static_cast<int>(
        denominator - ((num_vals_sorted - 1) * numerator - id_l * denominator));
    output_r[quantile] = output_lerp_nominator[quantile] == denominator
                             ? vals_sorted[id_l]
                             : vals_sorted[id_r];
  }

  return true;
}

bool WindowQuantilesBasic::MADForEntirePartition(int64_t num_rows, const int64_t* vals,
                                                 const uint8_t* optional_validity,
                                                 uint64_t* output_value_l,
                                                 uint64_t* output_value_r) {
  std::vector<int64_t> vals_sorted;
  if (optional_validity) {
    for (int64_t i = 0; i < num_rows; ++i) {
      if (bit_util::GetBit(optional_validity, i)) {
        vals_sorted.push_back(vals[i]);
      }
    }
  } else {
    vals_sorted.resize(num_rows);
    memcpy(vals_sorted.data(), vals, num_rows * sizeof(int64_t));
  }
  if (vals_sorted.empty()) {
    return false;
  }

  std::sort(vals_sorted.begin(), vals_sorted.end());

  int64_t num_vals_sorted = static_cast<int64_t>(vals_sorted.size());
  int64_t median_l, median_r;
  median_l = vals_sorted[(num_vals_sorted - 1) / 2];
  median_r = vals_sorted[num_vals_sorted / 2];

  std::vector<uint64_t> absolute_differences;
  for (int64_t i = 0; i < num_vals_sorted; ++i) {
    int64_t value = vals_sorted[i];
    if (value <= median_l) {
      absolute_differences.push_back(median_r - value);
    } else {
      absolute_differences.push_back(value - median_l);
    }
  }

  std::sort(absolute_differences.begin(), absolute_differences.end());

  *output_value_l =
      absolute_differences[(num_vals_sorted - 1) / 2] - (median_r - median_l);
  *output_value_r = absolute_differences[num_vals_sorted / 2];

  return true;
}

void WindowQuantilesBasic::MADForFramesWithSeparateOrder(
    int64_t num_rows, const int64_t* vals, const int64_t* permutation,
    const WindowFrames& frames, uint64_t* mad_l, uint64_t* mad_r) {
  const int64_t* frame_begins = frames.begins[0];
  const int64_t* frame_ends = frames.ends[0];
  for (int64_t i = 0; i < num_rows; ++i) {
    ARROW_DCHECK(frame_ends[i] > frame_begins[i]);
    MADForEntirePartition(frame_ends[i] - frame_begins[i], vals + frame_begins[i],
                          nullptr, &mad_l[i], &mad_r[i]);
  }
}

// For empty frames the result is undefined.
//
void WindowQuantilesBasic::ForFramesWithSeparateOrder(
    int64_t num_rows, const int64_t* permutation,
    const WindowQuantiles::Quantiles& quantiles, const WindowFrames& frames,
    std::vector<std::vector<int64_t>>& output_l,
    std::vector<std::vector<int64_t>>& output_r,
    std::vector<std::vector<int>>& output_lerp) {
  const int64_t* frame_begins = frames.begins[0];
  const int64_t* frame_ends = frames.ends[0];

  std::vector<int64_t> rank_of_row(num_rows);
  for (int64_t i = 0; i < num_rows; ++i) {
    rank_of_row[permutation[i]] = i;
  }
  for (int quantile = 0; quantile < quantiles.num_quantiles; ++quantile) {
    for (int64_t frame = 0; frame < num_rows; ++frame) {
      int64_t begin = frame_begins[frame];
      int64_t end = frame_ends[frame];
      int64_t num_values_frame = end - begin;
      if (num_values_frame == 0) {
        // The case of an empty frame
        continue;
      }
      // Each pair is: {rank, row_number}.
      std::vector<std::pair<int64_t, int64_t>> frame_values;
      for (int64_t i = begin; i < end; ++i) {
        frame_values.push_back(std::make_pair(rank_of_row[i], i));
      }
      std::sort(frame_values.begin(), frame_values.end());

      int numerator = quantiles.numerators[quantile];
      int denominator = quantiles.denominators[quantile];
      int64_t id_l = (end - begin - 1) * numerator / denominator;

      output_l[quantile][frame] = output_r[quantile][frame] = frame_values[id_l].second;
      output_lerp[quantile][frame] = static_cast<int>(
          denominator - ((num_values_frame - 1) * numerator - id_l * denominator));
      if (output_lerp[quantile][frame] < denominator) {
        output_r[quantile][frame] = frame_values[id_l + 1].second;
      }
    }
  }
}

void WindowQuantilesTests::TestQuantilesForEntirePartition() {
  Random64BitCopy rand;
  MemoryPool* pool = default_memory_pool();
  util::TempVectorStack temp_vector_stack;
  Status status = temp_vector_stack.Init(pool, 128 * util::MiniBatch::kMiniBatchLength);
  ARROW_DCHECK(status.ok());

  constexpr int num_tests = 100;
  const int num_tests_to_skip = 0;
  for (int test = 0; test < num_tests; ++test) {
    // Generate random quantile queries
    //
    constexpr int max_quantiles = 10;
    WindowQuantiles::Quantiles quantiles;
    quantiles.num_quantiles = rand.from_range(1, max_quantiles);
    std::vector<int> numerators(quantiles.num_quantiles);
    std::vector<int> denominators(quantiles.num_quantiles);
    quantiles.numerators = numerators.data();
    quantiles.denominators = denominators.data();
    for (int i = 0; i < quantiles.num_quantiles; ++i) {
      denominators[i] = rand.from_range(1, 100);
      numerators[i] = rand.from_range(0, denominators[i]);
    }

    // Generate random values
    //
    constexpr int64_t max_rows = 1100;
    int64_t num_rows = rand.from_range(static_cast<int64_t>(1), max_rows);
    std::vector<uint8_t> validity(bit_util::BytesForBits(num_rows));
    std::vector<int64_t> vals(num_rows);
    constexpr int64_t max_val = 65535;
    // TODO: test with nulls
    int null_probability = 0;  // rand.from_range(0, 256);
    int tie_probability = rand.from_range(0, 256);
    for (int64_t i = 0; i < num_rows; ++i) {
      bool null = rand.from_range(0, 255) < null_probability;
      bool tie = rand.from_range(0, 255) < tie_probability;
      if (null) {
        bit_util::ClearBit(validity.data(), i);
      } else {
        bit_util::SetBit(validity.data(), i);
        if (tie && i > 0) {
          vals[i] = vals[rand.from_range(static_cast<int64_t>(0), i - 1)];
        } else {
          vals[i] = rand.from_range(static_cast<int64_t>(0), max_val);
        }
      }
    }
    std::vector<int64_t> vals_sorted(num_rows);
    for (int64_t i = 0; i < num_rows; ++i) {
      vals_sorted[i] = vals[i];
    }
    std::sort(vals_sorted.begin(), vals_sorted.end());

    printf("num_quantiles %d num_rows %d ", quantiles.num_quantiles,
           static_cast<int>(num_rows));

    if (test < num_tests_to_skip) {
      continue;
    }

    bool out_flag[2];
    std::vector<int64_t> out_l[2];
    std::vector<int64_t> out_r[2];
    std::vector<int> out_lerp[2];
    for (int i = 0; i < 2; ++i) {
      out_l[i].resize(quantiles.num_quantiles);
      out_r[i].resize(quantiles.num_quantiles);
      out_lerp[i].resize(quantiles.num_quantiles);
    }
    int64_t num_repeats;
#ifndef NDEBUG
    num_repeats = 1;
#else
    num_repeats = std::max(1LL, 1024 * 1024LL / num_rows);
#endif
    printf("num_repeats %d ", static_cast<int>(num_repeats));
    // int64_t start = __rdtsc();
    for (int repeat = 0; repeat < num_repeats; ++repeat) {
      out_flag[0] = WindowQuantilesBasic::ForEntirePartition(
          num_rows, vals.data(), validity.data(), quantiles, out_l[0].data(),
          out_r[0].data(), out_lerp[0].data());
    }
    // int64_t end = __rdtsc();
    // printf("cpr basic %.1f ",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));
    // start = __rdtsc();
    for (int repeat = 0; repeat < num_repeats; ++repeat) {
      out_flag[1] = WindowQuantiles::ForEntirePartition(
          num_rows, vals.data(), validity.data(), quantiles, out_l[1].data(),
          out_r[1].data(), out_lerp[1].data());
    }
    // end = __rdtsc();
    // printf("cpr normal %.1f ",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));

    bool ok = true;
    if (out_flag[0] != out_flag[1]) {
      ARROW_DCHECK(false);
      ok = false;
    }
    for (int i = 0; i < quantiles.num_quantiles; ++i) {
      if (out_l[0] != out_l[1]) {
        ARROW_DCHECK(false);
        ok = false;
      }
      if (out_r[0] != out_r[1]) {
        ARROW_DCHECK(false);
        ok = false;
      }
      if (out_lerp[0] != out_lerp[1]) {
        ARROW_DCHECK(false);
        ok = false;
      }
    }
    printf("%s\n", ok ? "correct" : "wrong");
  }
}

void WindowQuantilesTests::TestQuantilesForFramesWithSeparateOrder() {
  Random64BitCopy rand;
  MemoryPool* pool = default_memory_pool();
  util::TempVectorStack temp_vector_stack;
  Status status = temp_vector_stack.Init(pool, 128 * util::MiniBatch::kMiniBatchLength);
  ARROW_DCHECK(status.ok());
  int64_t hardware_flags = 0LL;

  constexpr int num_tests = 100;
  const int num_tests_to_skip = 1;
  for (int test = 0; test < num_tests; ++test) {
    // Generate random quantile queries
    //
    constexpr int max_quantiles = 10;
    WindowQuantiles::Quantiles quantiles;
    quantiles.num_quantiles = rand.from_range(1, max_quantiles);
    std::vector<int> numerators(quantiles.num_quantiles);
    std::vector<int> denominators(quantiles.num_quantiles);
    quantiles.numerators = numerators.data();
    quantiles.denominators = denominators.data();
    for (int i = 0; i < quantiles.num_quantiles; ++i) {
      denominators[i] = rand.from_range(1, 100);
      numerators[i] = rand.from_range(0, denominators[i]);
    }

    // Generate random values
    //
    constexpr int64_t max_rows = 1100;
    int64_t num_rows = rand.from_range(static_cast<int64_t>(1), max_rows);
    std::vector<int64_t> vals(num_rows);
    constexpr int64_t max_val = 65535;
    int tie_probability = rand.from_range(0, 256);
    for (int64_t i = 0; i < num_rows; ++i) {
      bool tie = rand.from_range(0, 255) < tie_probability;
      if (tie && i > 0) {
        vals[i] = vals[rand.from_range(static_cast<int64_t>(0), i - 1)];
      } else {
        vals[i] = rand.from_range(static_cast<int64_t>(0), max_val);
      }
    }

    // Sort on values
    //
    std::vector<std::pair<int64_t, int64_t>> vals_sorted(num_rows);
    for (int64_t i = 0; i < num_rows; ++i) {
      vals_sorted[i] = std::make_pair(vals[i], i);
    }
    std::sort(vals_sorted.begin(), vals_sorted.end());
    std::vector<int64_t> permutation(num_rows);
    for (int64_t i = 0; i < num_rows; ++i) {
      permutation[i] = vals_sorted[i].second;
    }

    // Generate random frames
    //
    constexpr int64_t max_frame_length = 100;
    std::vector<int64_t> begins(num_rows);
    std::vector<int64_t> ends(num_rows);
    WindowFrames frames;
    frames.num_frames = num_rows;
    frames.begins[0] = begins.data();
    frames.ends[0] = ends.data();
    int64_t sum_frame_length = 0LL;
    for (int64_t i = 0; i < num_rows; ++i) {
      int64_t frame_length =
          rand.from_range(static_cast<int64_t>(0), std::min(num_rows, max_frame_length));
      begins[i] = rand.from_range(static_cast<int64_t>(0), num_rows - frame_length);
      ends[i] = begins[i] + frame_length;
      sum_frame_length += frame_length;
    }

    printf("num_quantiles %d num_rows %d avg_frame_length %.1f ", quantiles.num_quantiles,
           static_cast<int>(num_rows),
           static_cast<float>(sum_frame_length) / static_cast<float>(num_rows));

    if (test < num_tests_to_skip) {
      continue;
    }

    std::vector<std::vector<int64_t>> out_l[2];
    std::vector<std::vector<int64_t>> out_r[2];
    std::vector<std::vector<int>> out_lerp[2];
    for (int i = 0; i < 2; ++i) {
      out_l[i].resize(quantiles.num_quantiles);
      out_r[i].resize(quantiles.num_quantiles);
      out_lerp[i].resize(quantiles.num_quantiles);
      for (int j = 0; j < quantiles.num_quantiles; ++j) {
        out_l[i][j].resize(num_rows);
        out_r[i][j].resize(num_rows);
        out_lerp[i][j].resize(num_rows);
      }
    }

    int64_t num_repeats;
#ifndef NDEBUG
    num_repeats = 1;
#else
    num_repeats = std::max(1LL, 1024 * 1024LL / num_rows);
#endif
    printf("num_repeats %d ", static_cast<int>(num_repeats));

    // int64_t start = __rdtsc();
    for (int repeat = 0; repeat < num_repeats; ++repeat) {
      WindowQuantilesBasic::ForFramesWithSeparateOrder(num_rows, permutation.data(),
                                                       quantiles, frames, out_l[0],
                                                       out_r[0], out_lerp[0]);
    }
    // int64_t end = __rdtsc();
    // printf("cpr basic %.1f ",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));
    // start = __rdtsc();
    for (int repeat = 0; repeat < num_repeats; ++repeat) {
      WindowQuantiles::ForFramesWithSeparateOrder(num_rows, permutation.data(), quantiles,
                                                  frames, out_l[1], out_r[1], out_lerp[1],
                                                  hardware_flags, &temp_vector_stack);
    }
    // end = __rdtsc();
    // printf("cpr normal %.1f ",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));

    bool ok = true;
    for (int i = 0; i < quantiles.num_quantiles; ++i) {
      for (int64_t j = 0; j < num_rows; ++j) {
        if (frames.begins[0][j] == frames.ends[0][j]) {
          continue;
        }
        if (out_l[0][i][j] != out_l[1][i][j] || out_r[0][i][j] != out_r[1][i][j]) {
          ARROW_DCHECK(false);
          ok = false;
        }
        if (out_lerp[0][i][j] != out_lerp[1][i][j]) {
          ARROW_DCHECK(false);
          ok = false;
        }
      }
    }
    printf("%s\n", ok ? "correct" : "wrong");
  }
}

void WindowQuantilesTests::TestMADForEntirePartition() {
  Random64BitCopy rand;
  MemoryPool* pool = default_memory_pool();
  util::TempVectorStack temp_vector_stack;
  Status status = temp_vector_stack.Init(pool, 128 * util::MiniBatch::kMiniBatchLength);
  ARROW_DCHECK(status.ok());

  constexpr int num_tests = 100;
  const int num_tests_to_skip = 1;
  for (int test = 0; test < num_tests; ++test) {
    // Generate random values
    //
    constexpr int64_t max_rows = 1100;
    int64_t num_rows = rand.from_range(static_cast<int64_t>(1), max_rows);
    std::vector<int64_t> vals(num_rows);
    constexpr int64_t max_val = 65535;
    int tie_probability = rand.from_range(0, 256);
    for (int64_t i = 0; i < num_rows; ++i) {
      bool tie = rand.from_range(0, 255) < tie_probability;
      if (tie && i > 0) {
        vals[i] = vals[rand.from_range(static_cast<int64_t>(0), i - 1)];
      } else {
        vals[i] = rand.from_range(static_cast<int64_t>(0), max_val);
      }
    }
    std::vector<int64_t> vals_sorted(num_rows);
    for (int64_t i = 0; i < num_rows; ++i) {
      vals_sorted[i] = vals[i];
    }
    std::sort(vals_sorted.begin(), vals_sorted.end());

    printf("num_rows %d ", static_cast<int>(num_rows));

    if (test < num_tests_to_skip) {
      continue;
    }

    uint64_t mad_l[3];
    uint64_t mad_r[3];

    int64_t num_repeats;
#ifndef NDEBUG
    num_repeats = 1;
#else
    num_repeats = std::max(1LL, 1024 * 1024LL / num_rows);
#endif
    printf("num_repeats %d ", static_cast<int>(num_repeats));

    // int64_t start = __rdtsc();
    for (int repeat = 0; repeat < num_repeats; ++repeat) {
      WindowQuantilesBasic::MADForEntirePartition(num_rows, vals.data(), nullptr,
                                                  &mad_l[0], &mad_r[0]);
    }
    // int64_t end = __rdtsc();
    // printf("cpr basic %.1f ",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));

    for (bool use_merge_path : {false, true}) {
      //   start = __rdtsc();
      for (int repeat = 0; repeat < num_repeats; ++repeat) {
        WindowQuantiles::MADForEntirePartition(
            num_rows, vals.data(), nullptr, &mad_l[use_merge_path ? 2 : 1],
            &mad_r[use_merge_path ? 2 : 1], use_merge_path);
      }
      //   end = __rdtsc();
      //   printf(
      //       "cpr normal %s %.1f ", use_merge_path ? "merge_path" : "",
      //       static_cast<float>(end - start) / static_cast<float>(num_rows *
      //       num_repeats));
    }

    bool ok = true;
    for (int i = 1; i < 3; ++i) {
      if (mad_l[0] != mad_l[i]) {
        ARROW_DCHECK(false);
        ok = false;
      }
      if (mad_r[0] != mad_r[i]) {
        ARROW_DCHECK(false);
        ok = false;
      }
    }
    printf("%s\n", ok ? "correct" : "wrong");
  }
}

void WindowQuantilesTests::TestMADForFramesWithSeparateOrder() {
  Random64BitCopy rand;
  MemoryPool* pool = default_memory_pool();
  util::TempVectorStack temp_vector_stack;
  Status status = temp_vector_stack.Init(pool, 128 * util::MiniBatch::kMiniBatchLength);
  ARROW_DCHECK(status.ok());
  int64_t hardware_flags = 0LL;

  constexpr int num_tests = 100;
  const int num_tests_to_skip = 1;
  for (int test = 0; test < num_tests; ++test) {
    // Generate random values
    //
    constexpr int64_t max_rows = 1100;
    int64_t num_rows = rand.from_range(static_cast<int64_t>(1), max_rows);
    std::vector<int64_t> vals(num_rows);
    constexpr int64_t max_val = 65535;
    int tie_probability = rand.from_range(0, 256);
    for (int64_t i = 0; i < num_rows; ++i) {
      bool tie = rand.from_range(0, 255) < tie_probability;
      if (tie && i > 0) {
        vals[i] = vals[rand.from_range(static_cast<int64_t>(0), i - 1)];
      } else {
        vals[i] = rand.from_range(static_cast<int64_t>(0), max_val);
      }
    }

    // Sort on values
    //
    std::vector<std::pair<int64_t, int64_t>> vals_sorted(num_rows);
    for (int64_t i = 0; i < num_rows; ++i) {
      vals_sorted[i] = std::make_pair(vals[i], i);
    }
    std::sort(vals_sorted.begin(), vals_sorted.end());
    std::vector<int64_t> permutation(num_rows);
    for (int64_t i = 0; i < num_rows; ++i) {
      permutation[i] = vals_sorted[i].second;
    }

    // Generate random frames
    //
    constexpr int64_t max_frame_length = 100;
    std::vector<int64_t> begins(num_rows);
    std::vector<int64_t> ends(num_rows);
    WindowFrames frames;
    frames.num_frames = num_rows;
    frames.begins[0] = begins.data();
    frames.ends[0] = ends.data();
    int64_t sum_frame_length = 0LL;
    for (int64_t i = 0; i < num_rows; ++i) {
      int64_t frame_length =
          rand.from_range(static_cast<int64_t>(1), std::min(num_rows, max_frame_length));
      begins[i] = rand.from_range(static_cast<int64_t>(0), num_rows - frame_length);
      ends[i] = begins[i] + frame_length;
      sum_frame_length += frame_length;
    }

    printf("num_rows %d avg_frame_length %.1f ", static_cast<int>(num_rows),
           static_cast<float>(sum_frame_length) / static_cast<float>(num_rows));

    if (test < num_tests_to_skip) {
      continue;
    }

    std::vector<uint64_t> mad_l[2];
    std::vector<uint64_t> mad_r[2];
    for (int i = 0; i < 2; ++i) {
      mad_l[i].resize(num_rows);
      mad_r[i].resize(num_rows);
    }

    int64_t num_repeats;
#ifndef NDEBUG
    num_repeats = 1;
#else
    num_repeats = std::max(1LL, 1024 * 1024LL / num_rows);
#endif
    printf("num_repeats %d ", static_cast<int>(num_repeats));

    // int64_t start = __rdtsc();
    for (int repeat = 0; repeat < num_repeats; ++repeat) {
      WindowQuantilesBasic::MADForFramesWithSeparateOrder(
          num_rows, vals.data(), permutation.data(), frames, mad_l[0].data(),
          mad_r[0].data());
    }
    // int64_t end = __rdtsc();
    // printf("cpr basic %.1f ",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));
    // start = __rdtsc();
    for (int repeat = 0; repeat < num_repeats; ++repeat) {
      WindowQuantiles::MADForFramesWithSeparateOrder(
          num_rows, vals.data(), permutation.data(), frames, mad_l[1].data(),
          mad_r[1].data(), hardware_flags, &temp_vector_stack);
    }
    // end = __rdtsc();
    // printf("cpr normal %.1f ",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));

    bool ok = true;
    for (int64_t i = 0; i < num_rows; ++i) {
      if (mad_l[0][i] != mad_l[1][i] || mad_r[0][i] != mad_r[1][i]) {
        ARROW_DCHECK(false);
        ok = false;
      }
    }
    printf("%s\n", ok ? "correct" : "wrong");
  }
}

}  // namespace compute
}  // namespace arrow
