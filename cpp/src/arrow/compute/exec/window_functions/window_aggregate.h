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
#include "arrow/compute/exec/window_functions/window_expr.h"
#include "arrow/compute/exec/window_functions/window_frame.h"

namespace arrow {
namespace compute {

class IntervalTree {
 public:
  IntervalTree() : func_(nullptr), num_rows_(0), temp_vector_(nullptr) {}

  ~IntervalTree();

  void Init(WindowAggregateFunc* func) { func_ = func; }

  void Build(int64_t num_rows, util::TempVectorStack* temp_vector_stack);

  void SumOfRange(int64_t num, const int64_t* begins, const int64_t* ends,
                  WindowAggregateFunc::Vector* results, uint8_t* results_validity,
                  util::TempVectorStack* temp_vector_stack);

 private:
  static constexpr int kLogFanout = 2;
  static constexpr int64_t kFanout = 1LL << kLogFanout;
  WindowAggregateFunc* func_;
  int64_t num_rows_;
  std::vector<WindowAggregateFunc::Vector*> levels_;
  std::vector<int64_t> level_sizes_;
  WindowAggregateFunc::Vector* temp_vector_;
};

class WindowFrameDeltaStream {
 public:
  void Init(int64_t num_frames, const int64_t* begins, const int64_t* ends);
  void GetNextInputBatch(uint16_t max_batch_size, uint16_t* num_inputs,
                         uint16_t* num_output_frames, uint8_t* reset_sum_bitvec,
                         uint8_t* negate_input_bitvec, int64_t* input_row_number,
                         uint16_t* input_prefix_length, int64_t* output_frame);

 private:
  int64_t num_frames_;
  const int64_t* begins_;
  const int64_t* ends_;
  int64_t frame_;
  int64_t begin_, end_;
  bool reset_next_;

  int64_t next_frame_to_output_;
};

class WindowFramePeersEnumerator {};

class WindowAggregate {
 public:
  static void Sum(int64_t num_rows, WindowAggregateFunc* func, const int64_t* begins,
                  const int64_t* ends, WindowAggregateFunc::Vector* results,
                  uint8_t* results_validity, util::TempVectorStack* temp_vector_stack) {
    IntervalTree tree;
    tree.Init(func);
    tree.Build(num_rows, temp_vector_stack);
    tree.SumOfRange(num_rows, begins, ends, results, results_validity, temp_vector_stack);
  }
  //   static void SumProgressive(int64_t num_rows, WindowAggregateFunc* func,
  //                              const int64_t* begins, const int64_t* ends,
  //                              WindowAggregateFunc::Vector* results,
  //                              uint8_t* results_validity,
  //                              util::TempVectorStack* temp_vector_stack) {
  //     int64_t batch_length_max = util::MiniBatch::kMiniBatchLength;

  //     // TODO: Zero output
  //     //

  //     auto reset_sum_bitvec_buf = util::TempVectorHolder<uint8_t>(
  //         temp_vector_stack,
  //         static_cast<uint32_t>(bit_util::BytesForBits(batch_length_max)));
  //     uint8_t* reset_sum_bitvec = reset_sum_bitvec_buf.mutable_data();

  //     auto negate_input_bitvec_buf = util::TempVectorHolder<uint8_t>(
  //         temp_vector_stack,
  //         static_cast<uint32_t>(bit_util::BytesForBits(batch_length_max)));
  //     uint8_t* negate_input_bitvec = negate_input_bitvec_buf.mutable_data();

  //     auto input_row_number_buf = util::TempVectorHolder<int64_t>(
  //         temp_vector_stack, static_cast<uint32_t>(batch_length_max));
  //     int64_t* input_row_number = input_row_number_buf.mutable_data();

  //     auto input_prefix_length_buf = util::TempVectorHolder<uint16_t>(
  //         temp_vector_stack, static_cast<uint32_t>(batch_length_max));
  //     uint16_t* input_prefix_length = input_prefix_length_buf.mutable_data();

  //     auto output_frame_buf = util::TempVectorHolder<int64_t>(
  //         temp_vector_stack, static_cast<uint32_t>(batch_length_max));
  //     int64_t* output_frame = output_frame_buf.mutable_data();

  //     WindowFrameDeltaStream frame_deltas;
  //     frame_deltas.Init(num_rows, begins, ends);

  //     // TODO: Allocate temporary vector for prefix sums

  //     // TODO: Allocate out_ids and in_ids for sums

  //     int64_t num_frames_processed = 0;
  //     for (;;) {
  //       uint16_t num_inputs;
  //       uint16_t num_output_frames;
  //       frame_deltas.GetNextInputBatch(static_cast<uint16_t>(batch_length_max),
  //       &num_inputs,
  //                                      &num_output_frames, reset_sum_bitvec,
  //                                      negate_input_bitvec, input_row_number,
  //                                      input_prefix_length, output_frame);

  //       // TODO: Finish
  //     }
  //   }
};

class WindowAggregateBasic {
 public:
  static void Sum(int64_t num_rows, const int64_t* vals, const int64_t* begins,
                  const int64_t* ends,
                  std::vector<std::pair<uint64_t, uint64_t>>& results,
                  uint8_t* results_validity);
};

class WindowAggregateTest {
 public:
  static void TestSum();
};

}  // namespace compute
}  // namespace arrow
