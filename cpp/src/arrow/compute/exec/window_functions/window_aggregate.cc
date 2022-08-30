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

#include "arrow/compute/exec/window_functions/window_aggregate.h"
#include <numeric>
#include <set>  // For test
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec/window_functions/bit_vector_navigator.h"

namespace arrow {
namespace compute {

IntervalTree::~IntervalTree() {
  for (size_t i = 0; i < levels_.size(); ++i) {
    if (levels_[i]) {
      delete levels_[i];
    }
  }
  if (temp_vector_) {
    delete temp_vector_;
  }
}

void IntervalTree::Build(int64_t num_rows, util::TempVectorStack* temp_vector_stack) {
  ARROW_DCHECK(num_rows_ == 0);
  num_rows_ = num_rows;
  if (num_rows_ == 0) {
    return;
  }

  // Allocate vectors for levels of the tree (starting from level 1).
  //
  levels_.clear();
  level_sizes_.clear();
  levels_.push_back(nullptr);  // Level 0 is ignored
  level_sizes_.push_back(0LL);
  int64_t last_level_size = num_rows_;
  while (last_level_size >= kFanout) {
    int64_t next_level_size = bit_util::CeilDiv(last_level_size, kFanout);
    levels_.push_back(func_->Alloc(next_level_size));
    level_sizes_.push_back(next_level_size);
    func_->Zero(level_sizes_.back(), levels_.back(), 0);
    last_level_size = next_level_size;
  }

  int64_t batch_length_max = util::MiniBatch::kMiniBatchLength;
  ARROW_DCHECK(batch_length_max % kFanout == 0);

  // Temp buffers
  //
  auto out_ids_buf = util::TempVectorHolder<int64_t>(
      temp_vector_stack, static_cast<uint32_t>(batch_length_max));
  int64_t* out_ids = out_ids_buf.mutable_data();
  auto in_ids_buf = util::TempVectorHolder<int64_t>(
      temp_vector_stack, static_cast<uint32_t>(batch_length_max));
  int64_t* in_ids = in_ids_buf.mutable_data();

  int num_levels = static_cast<int>(level_sizes_.size());
  for (int target_level = 1; target_level < num_levels; ++target_level) {
    int64_t num_rows_in =
        (target_level == 1) ? num_rows_ : level_sizes_[target_level - 1];
    for (int64_t batch_begin = 0; batch_begin < num_rows_in;
         batch_begin += batch_length_max) {
      int64_t batch_length = std::min(num_rows_in - batch_begin, batch_length_max);
      std::iota(in_ids, in_ids + batch_length, batch_begin);
      for (int64_t i = 0; i < batch_length; ++i) {
        out_ids[i] = ((batch_begin + i) >> kLogFanout);
      }
      func_->Sum(batch_length, levels_[target_level], out_ids,
                 target_level == 1 ? nullptr : levels_[target_level - 1], in_ids);
    }
  }

  delete temp_vector_;
  temp_vector_ = nullptr;
}

void IntervalTree::SumOfRange(int64_t num, const int64_t* begins, const int64_t* ends,
                              WindowAggregateFunc::Vector* results,
                              uint8_t* results_validity,
                              util::TempVectorStack* temp_vector_stack) {
  // Zero and mark all results as valid
  //
  memset(results_validity, 0xff, bit_util::CeilDiv(num, 8));
  func_->Zero(num, results, 0);

  // Bottom-up interval tree traversal
  //
  // Breaking queries into batches.
  // We use smaller batches to have room in temporary arrays for multiple
  // elements per query (up to kFanout * 2).
  //
  int64_t batch_length_max = (util::MiniBatch::kMiniBatchLength >> kLogFanout);

  // Ids of queries within a batch still participating in interval tree
  // traversal
  auto ids_buf = util::TempVectorHolder<uint16_t>(
      temp_vector_stack, static_cast<uint32_t>(batch_length_max));
  uint16_t* ids = ids_buf.mutable_data();
  int num_ids = 0;
  // Buffers for source and destination ids for requests to perform addition
  // (room for up to 2 * (kFanout - 1) sums per query)
  //
  auto sum_dst_buf = util::TempVectorHolder<int64_t>(
      temp_vector_stack, static_cast<uint32_t>(batch_length_max * (kFanout - 1) * 2));
  int64_t* sum_dst = sum_dst_buf.mutable_data();
  auto sum_src_buf = util::TempVectorHolder<int64_t>(
      temp_vector_stack, static_cast<uint32_t>(batch_length_max * (kFanout - 1) * 2));
  int64_t* sum_src = sum_src_buf.mutable_data();

  for (int64_t batch_begin = 0; batch_begin < num; batch_begin += batch_length_max) {
    int64_t batch_length = std::min(num - batch_begin, batch_length_max);

    num_ids = 0;
    for (int64_t i = batch_begin; i < batch_begin + batch_length; ++i) {
      if (begins[i] < ends[i]) {
        ids[num_ids++] = static_cast<uint16_t>(i - batch_begin);
      } else {
        bit_util::ClearBit(results_validity, i);
      }
    }

    int level = 0;
    while (num_ids > 0) {
      ARROW_DCHECK(level < static_cast<int>(levels_.size()));
      int64_t num_sums = 0;
      int num_ids_new = 0;
      int bit_shift = kLogFanout * level;
      int64_t cell_size = 1LL << bit_shift;
      for (int i = 0; i < num_ids; ++i) {
        uint16_t id = ids[i];
        int64_t begin = (begins[batch_begin + id] + cell_size - 1LL) >> bit_shift;
        int64_t end = ends[batch_begin + id] >> bit_shift;
        int64_t begin_round_up =
            ((begin + kFanout - 1) & ~static_cast<int64_t>(kFanout - 1));
        int64_t end_round_down = (end & ~static_cast<int64_t>(kFanout - 1));
        if (begin_round_up > end_round_down) {
          begin_round_up = end_round_down = end;
        }
        for (int64_t j = begin; j < begin_round_up; ++j) {
          sum_dst[num_sums] = batch_begin + id;
          sum_src[num_sums] = j;
          ++num_sums;
        }
        for (int64_t j = end_round_down; j < end; ++j) {
          sum_dst[num_sums] = batch_begin + id;
          sum_src[num_sums] = j;
          ++num_sums;
        }
        if (begin_round_up < end_round_down) {
          ids[num_ids_new++] = id;
        }
      }
      func_->Sum(num_sums, results, sum_dst, levels_[level], sum_src);
      num_ids = num_ids_new;
      ++level;
    }
  }
}

void WindowFrameDeltaStream::Init(int64_t num_frames, const int64_t* begins,
                                  const int64_t* ends) {
  num_frames_ = num_frames;
  begins_ = begins;
  ends_ = ends;
  frame_ = 0;
  begin_ = 0;
  end_ = 0;
  reset_next_ = true;
}

void WindowFrameDeltaStream::GetNextInputBatch(
    uint16_t max_batch_size, uint16_t* num_inputs, uint16_t* num_output_frames,
    uint8_t* reset_sum_bitvec, uint8_t* negate_input_bitvec, int64_t* input_row_number,
    uint16_t* input_prefix_length, int64_t* output_frame) {
  *num_inputs = 0;
  *num_output_frames = 0;

  int64_t num_frames = num_frames_;
  const int64_t* begins = begins_;
  const int64_t* ends = ends_;

  for (;;) {
    // Ignore empty frames
    //
    while (frame_ < num_frames && begins[frame_] == ends[frame_]) {
      ++frame_;
    }

    // Ignore frames that are the same as the previous one
    //
    while (frame_ < num_frames && begins[frame_] == begin_ && ends[frame_] == end_) {
      ++frame_;
    }

    if (frame_ == num_frames) {
      return;
    }

    int64_t target_begin = begins[frame_];
    int64_t target_end = ends[frame_];

    if (begin_ < target_begin && end_ <= target_begin) {
      reset_next_ = true;
      begin_ = end_ = target_begin;
    }

    while (begin_ < target_begin) {
      if (*num_inputs == max_batch_size) {
        return;
      }
      bit_util::SetBitTo(reset_sum_bitvec, *num_inputs, reset_next_);
      reset_next_ = false;
      bit_util::SetBit(negate_input_bitvec, *num_inputs);
      input_row_number[*num_inputs] = begin_;
      ++num_inputs;
      ++begin_;
    }

    while (end_ < target_end) {
      if (*num_inputs == max_batch_size) {
        return;
      }
      bit_util::SetBitTo(reset_sum_bitvec, *num_inputs, reset_next_);
      reset_next_ = false;
      bit_util::ClearBit(negate_input_bitvec, *num_inputs);
      input_row_number[*num_inputs] = end_;
      ++*num_inputs;
      ++end_;
    }

    input_prefix_length[*num_output_frames] = *num_inputs;
    output_frame[*num_output_frames] = frame_;
    ++*num_output_frames;
    ++frame_;
  }
}

void WindowAggregateBasic::Sum(int64_t num_rows, const int64_t* vals,
                               const int64_t* begins, const int64_t* ends,
                               std::vector<std::pair<uint64_t, uint64_t>>& results,
                               uint8_t* results_validity) {
  results.resize(num_rows);
  for (int64_t i = 0; i < num_rows; ++i) {
    int64_t begin = begins[i];
    int64_t end = ends[i];
    if (begin == end) {
      bit_util::ClearBit(results_validity, i);
      results[i] = std::make_pair(0ULL, 0ULL);
      continue;
    }
    bit_util::SetBit(results_validity, i);

    uint64_t overflow = 0ULL;
    uint64_t result = 0ULL;
    for (int64_t j = begin; j < end; ++j) {
      result += static_cast<uint64_t>(vals[j]);
      if (result < static_cast<uint64_t>(vals[j])) {
        ++overflow;
      }
      if (vals[j] < 0LL) {
        overflow += ~0ULL;
      }
    }
    results[i] = std::make_pair(result, overflow);
  }
}

void WindowAggregateTest::TestSum() {
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
    for (int64_t i = 0; i < num_rows; ++i) {
      vals[i] = rand.from_range(static_cast<int64_t>(0), max_val);
    }

    // Generate random frames
    //
    std::vector<int64_t> begins;
    std::vector<int64_t> ends;
    GenerateTestFrames(rand, num_rows, begins, ends, /*progressive=*/false,
                       /*expansive=*/false);

    printf("num_rows %d ", static_cast<int>(num_rows));

    if (test < num_tests_to_skip) {
      continue;
    }

    WinAggFun_SumInt64 func;
    func.Init(num_rows, vals.data());

    std::vector<std::pair<uint64_t, uint64_t>> out0(num_rows);
    WinAggFun_SumInt64::VectorUint128* out1;
    out1 = func.Alloc(num_rows);
    std::vector<uint8_t> out_validity[2];
    out_validity[0].resize(bit_util::BytesForBits(num_rows));
    out_validity[1].resize(bit_util::BytesForBits(num_rows));

    int64_t num_repeats;
#ifndef NDEBUG
    num_repeats = 1;
#else
    num_repeats = std::max(1LL, 1024 * 1024LL / num_rows);
#endif
    printf("num_repeats %d ", static_cast<int>(num_repeats));

    // int64_t start = __rdtsc();
    for (int repeat = 0; repeat < num_repeats; ++repeat) {
      WindowAggregateBasic::Sum(num_rows, vals.data(), begins.data(), ends.data(), out0,
                                out_validity[1].data());
    }
    // int64_t end = __rdtsc();
    // printf("cpr basic %.1f ",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));
    // start = __rdtsc();
    for (int repeat = 0; repeat < num_repeats; ++repeat) {
      WindowAggregate::Sum(num_rows, &func, begins.data(), ends.data(), out1,
                           out_validity[0].data(), &temp_vector_stack);
    }
    // end = __rdtsc();
    // printf("cpr normal %.1f ",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));

    bool ok = true;
    for (int64_t i = 0; i < num_rows; ++i) {
      bool valid[2];
      for (int j = 0; j < 2; ++j) {
        valid[j] = bit_util::GetBit(out_validity[j].data(), i);
      }
      if (valid[0] != valid[1]) {
        ARROW_DCHECK(false);
        ok = false;
      }
      if (out1->vals_[i].first != out0[i].first ||
          out1->vals_[i].second != out0[i].second) {
        ARROW_DCHECK(false);
        ok = false;
      }
    }
    printf("%s\n", ok ? "correct" : "wrong");

    delete out1;
  }
}

}  // namespace compute
}  // namespace arrow
