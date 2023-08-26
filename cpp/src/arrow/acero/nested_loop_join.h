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

#include <set>
#include <vector>

#include "arrow/acero/accumulation_queue.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/schema_util.h"
#include "arrow/acero/task_util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {

namespace acero {

using util::AccumulationQueue;

struct MatchState {
  std::vector<int64_t>& no_match;
  std::vector<int64_t>& match;
  std::vector<int64_t>& match_left;
  std::vector<int64_t>& match_right;
};

class NestedLoopJoinImpl {
 public:
  using OutputBatchCallback = std::function<Status(int64_t, ExecBatch)>;
  using FinishedCallback = std::function<Status(int64_t)>;
  using RegisterTaskGroupCallback = std::function<int(
      std::function<Status(size_t, int64_t)>, std::function<Status(size_t)>)>;
  using StartTaskGroupCallback = std::function<Status(int, int64_t)>;
  static constexpr int64_t kRowIdNull = -1;

  Status Init(QueryContext* ctx, JoinType join_type, size_t num_threads,
              const NestedLoopJoinProjectionMaps* proj_map_left,
              const NestedLoopJoinProjectionMaps* proj_map_right, Expression filter,
              RegisterTaskGroupCallback register_task_group_callback,
              StartTaskGroupCallback start_task_group_callback,
              OutputBatchCallback output_batch_callback,
              FinishedCallback finished_callback);

  Status StartCrossProduct(size_t /*thread_index*/, AccumulationQueue inner_batches,
                           AccumulationQueue outer_batches);

  Status CrossProductTask(size_t thread_id, int64_t task_id);

  Status CrossProductInnerEmpty(size_t thread_id, const ExecBatch& left_batch);

  Status ExecuteFilter(const ExecBatch& cross_batch, const MatchState& state,
                       int64_t left_row_idx, bool is_last_row, int64_t left_rows,
                       std::set<int64_t>& match_idx);
  Status FinishCallBack(size_t thread_index);

  Status CrossBatchOutputAll(const ExecBatch& left_batch, const ExecBatch& right_batch,
                             const MatchState& state);

  void Abort(TaskScheduler::AbortContinuationImpl pos_abort_callback);

 protected:
  arrow::util::tracing::Span span_;

 private:
  void InitTaskGroups();
  Status CrossBatchOutputOne(const ExecBatch& left_batch, const ExecBatch& right_batch,
                             int64_t batch_size_next, const int64_t* opt_left_ids,
                             const int64_t* opt_right_ids);
  Status AppendOutputColumn(ExecBatch& result, const SchemaProjectionMap& from_input,
                            const ExecBatch& batch, int side, const int64_t* opt_ids,
                            int64_t nrows, int out_col_offset);

  static constexpr int64_t output_batch_size_ = 32 * 1024;

  // Metadata
  QueryContext* ctx_;
  JoinType join_type_;
  size_t num_threads_;
  const NestedLoopJoinProjectionMaps* schema_[2];
  Expression filter_;
  int task_cross_product_;
  MemoryPool* pool_;

  // Callbacks
  OutputBatchCallback output_batch_callback_;
  FinishedCallback finished_callback_;
  RegisterTaskGroupCallback register_task_group_callback_;
  StartTaskGroupCallback start_task_group_callback_;

  // Batches
  AccumulationQueue inner_batches_;
  AccumulationQueue outer_batches_;

  // Output batch num for next node
  int64_t output_batch_nums_;

  bool cancelled_;
};

}  // namespace acero
}  // namespace arrow