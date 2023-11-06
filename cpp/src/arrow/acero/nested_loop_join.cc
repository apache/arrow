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

#include "arrow/acero/nested_loop_join.h"

#include "arrow/acero/task_util.h"
#include "arrow/array/concatenate.h"
#include "arrow/builder.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {
namespace acero {
Status NestedLoopJoinImpl::Init(QueryContext* ctx, JoinType join_type, size_t num_threads,
                                const NestedLoopJoinProjectionMaps* proj_map_left,
                                const NestedLoopJoinProjectionMaps* proj_map_right,
                                Expression filter,
                                RegisterTaskGroupCallback register_task_group_callback,
                                StartTaskGroupCallback start_task_group_callback,
                                OutputBatchCallback output_batch_callback,
                                FinishedCallback finished_callback) {
  num_threads_ = num_threads;
  ctx_ = ctx;
  join_type_ = join_type;
  schema_[0] = proj_map_left;
  schema_[1] = proj_map_right;
  filter_ = std::move(filter);
  register_task_group_callback_ = std::move(register_task_group_callback);
  start_task_group_callback_ = std::move(start_task_group_callback);
  output_batch_callback_ = std::move(output_batch_callback);
  finished_callback_ = std::move(finished_callback);
  pool_ = default_memory_pool();

  output_batch_nums_ = 0;
  cancelled_ = false;

  task_cross_product_ = register_task_group_callback_(
      [this](size_t thread_index, int64_t task_id) -> Status {
        return CrossProductTask(thread_index, task_id);
      },
      [this](size_t thread_index) -> Status { return FinishCallBack(thread_index); });

  return Status::OK();
}

Status NestedLoopJoinImpl::StartCrossProduct(size_t /*thread_index*/,
                                             AccumulationQueue inner_batches,
                                             AccumulationQueue outer_batches) {
  inner_batches_ = std::move(inner_batches);
  outer_batches_ = std::move(outer_batches);
  return start_task_group_callback_(task_cross_product_,
                                    /*num_tasks=*/outer_batches_.batch_count());
}

Status NestedLoopJoinImpl::CrossProductTask(size_t thread_id, int64_t task_id) {
  const ExecBatch& left_batch = outer_batches_[task_id];
  int64_t left_rows = left_batch.length;
  ARROW_DCHECK(left_rows != 0);

  SchemaProjectionMap left_f_to_i =
      schema_[0]->map(HashJoinProjection::FILTER, HashJoinProjection::INPUT);
  SchemaProjectionMap right_f_to_i =
      schema_[1]->map(HashJoinProjection::FILTER, HashJoinProjection::INPUT);

  // There is no corresponding column, and it does not satisfy the true expression, we
  // should do nothing.
  if (filter_ != literal(true) && !left_f_to_i.num_cols && !right_f_to_i.num_cols) {
    // do nothing
    return Status::OK();
  }

  if (!inner_batches_.batch_count()) {
    return CrossProductInnerEmpty(thread_id, left_batch);
  }

  bool already_output = false;
  std::set<int64_t> match_idx;
  size_t right_total_batchs = inner_batches_.batch_count();
  // use match_idx to filter no match rows when we traverse to the last line of the outer
  // batch.
  bool is_last_row = false;
  for (size_t right_idx = 0; right_idx < right_total_batchs; right_idx++) {
    std::vector<int64_t> no_match, match, match_left, match_right;
    MatchState state{no_match, match, match_left, match_right};
    const ExecBatch& right_batch = inner_batches_[right_idx];
    int64_t right_rows = right_batch.length;
    ARROW_DCHECK(right_rows != 0);
    // right batch isn't an emtpy batch.
    for (int64_t i = 0; i < left_rows; i++) {
      // doesn't have filter.
      if (filter_ == literal(true)) {
        // for semi, we just output once.
        if (already_output && (join_type_ == JoinType::LEFT_SEMI)) {
          break;
        }
        state.match_left.clear();
        state.match_right.clear();
        state.match.clear();
        for (int64_t j = 0; j < right_rows; j++) {
          state.match_left.push_back(i);
          state.match_right.push_back(j);
        }
        state.match.push_back(i);
        if (i == left_rows - 1) {
          already_output = true;
        }
      } else {
        ExecBatch cross_batch;
        for (int left_col = 0; left_col < left_f_to_i.num_cols; left_col++) {
          int key_idx = left_f_to_i.get(left_col);
          std::shared_ptr<Array> result;
          auto type = left_batch.values[key_idx].type();
          std::unique_ptr<arrow::ArrayBuilder> builder;
          RETURN_NOT_OK(MakeBuilder(pool_, type, &builder));
          for (int64_t j = 0; j < right_rows; j++) {
            RETURN_NOT_OK(
                builder->AppendArraySlice(*left_batch.values[key_idx].array(), i, 1));
          }
          RETURN_NOT_OK(builder->Finish(&result));
          cross_batch.values.push_back(result);
        }
        for (int right_col = 0; right_col < right_f_to_i.num_cols; right_col++) {
          int key_idx = right_f_to_i.get(right_col);
          cross_batch.values.push_back(right_batch.values[key_idx]);
        }
        cross_batch.length = right_rows;
        is_last_row = i + 1 == left_rows && right_idx + 1 == right_total_batchs;
        ARROW_RETURN_NOT_OK(
            ExecuteFilter(cross_batch, state, i, is_last_row, left_rows, match_idx));
      }
      ARROW_RETURN_NOT_OK(CrossBatchOutputAll(left_batch, right_batch, state));
    }
  }
  return Status::OK();
}

Status NestedLoopJoinImpl::CrossProductInnerEmpty(size_t thread_id,
                                                  const ExecBatch& left_batch) {
  int64_t left_rows = left_batch.length;
  ARROW_DCHECK(left_rows != 0);
  std::vector<int64_t> no_match, match, match_left, match_right;
  MatchState state{no_match, match, match_left, match_right};
  for (int64_t i = 0; i < left_rows; i++) {
    state.no_match.push_back(i);
  }
  RETURN_NOT_OK(CrossBatchOutputAll(left_batch, ExecBatch{}, state));
  return Status::OK();
}

Status NestedLoopJoinImpl::ExecuteFilter(const ExecBatch& cross_batch,
                                         const MatchState& state, int64_t left_row_idx,
                                         bool is_last_row, int64_t left_rows,
                                         std::set<int64_t>& match_idx) {
  int64_t cross_rows = cross_batch.length;
  ARROW_ASSIGN_OR_RAISE(
      Datum mask, ExecuteScalarExpression(filter_, cross_batch, ctx_->exec_context()));
  ARROW_DCHECK(mask.is_array());
  state.no_match.clear();
  state.match.clear();
  state.match_left.clear();
  state.match_right.clear();
  ARROW_DCHECK_EQ(mask.array()->offset, 0);
  ARROW_DCHECK_EQ(mask.array()->length, cross_rows);
  const uint8_t* validity =
      mask.array()->buffers[0] ? mask.array()->buffers[0]->data() : nullptr;
  const uint8_t* comparisons = mask.array()->buffers[1]->data();

  bool passed = false;
  for (int64_t irow = 0; irow < cross_rows; irow++) {
    bool is_valid = !validity || bit_util::GetBit(validity, irow);
    bool is_cmp_true = bit_util::GetBit(comparisons, irow);
    // We treat a null comparison result as false, like in SQL
    if (is_valid && is_cmp_true) {
      state.match_left.push_back(left_row_idx);
      state.match_right.push_back(irow);
      match_idx.insert(left_row_idx);
      passed = true;
    }
  }

  if (passed) {
    state.match.push_back(left_row_idx);
  }

  // we can output no match when we scan last row in local left batch and right last batch
  // row.
  if (is_last_row) {
    for (int irow = 0; irow < left_rows; irow++) {
      if (!match_idx.count(irow)) state.no_match.push_back(irow);
    }
  }
  return Status::OK();
}
Status NestedLoopJoinImpl::CrossBatchOutputAll(const ExecBatch& left_batch,
                                               const ExecBatch& right_batch,
                                               const MatchState& state) {
  if (join_type_ == JoinType::RIGHT_SEMI || join_type_ == JoinType::RIGHT_ANTI) {
    // Nothing to output
    return Status::OK();
  }

  if (join_type_ == JoinType::LEFT_ANTI || join_type_ == JoinType::LEFT_SEMI) {
    const std::vector<int64_t>& out_ids =
        (join_type_ == JoinType::LEFT_SEMI) ? state.match : state.no_match;
    for (size_t start = 0; start < out_ids.size(); start += output_batch_size_) {
      int64_t batch_size_next = std::min(static_cast<int64_t>(out_ids.size() - start),
                                         static_cast<int64_t>(output_batch_size_));
      if (join_type_ == JoinType::LEFT_ANTI || join_type_ == JoinType::LEFT_SEMI) {
        RETURN_NOT_OK(CrossBatchOutputOne(left_batch, right_batch, batch_size_next,
                                          out_ids.data() + start, nullptr));
      }
    }
  } else {
    if (join_type_ == JoinType::LEFT_OUTER || join_type_ == JoinType::FULL_OUTER) {
      for (size_t i = 0; i < state.no_match.size(); ++i) {
        state.match_left.push_back(state.no_match[i]);
        state.match_right.push_back(kRowIdNull);
      }
    }
    ARROW_DCHECK(state.match_left.size() == state.match_right.size());

    for (size_t start = 0; start < state.match_left.size(); start += output_batch_size_) {
      int64_t batch_size_next =
          std::min(static_cast<int64_t>(state.match_left.size() - start),
                   static_cast<int64_t>(output_batch_size_));
      RETURN_NOT_OK(CrossBatchOutputOne(left_batch, right_batch, batch_size_next,
                                        state.match_left.data() + start,
                                        state.match_right.data() + start));
    }
  }
  return Status::OK();
}

Status NestedLoopJoinImpl::CrossBatchOutputOne(const ExecBatch& left_batch,
                                               const ExecBatch& right_batch,
                                               int64_t batch_size_next,
                                               const int64_t* opt_left_ids,
                                               const int64_t* opt_right_ids) {
  if (batch_size_next == 0 || (!opt_left_ids && !opt_right_ids)) {
    return Status::OK();
  }

  ExecBatch result({}, batch_size_next);

  int num_out_cols_left = schema_[0]->num_cols(HashJoinProjection::OUTPUT);
  int num_out_cols_right = schema_[1]->num_cols(HashJoinProjection::OUTPUT);

  result.values.resize(num_out_cols_left + num_out_cols_right);

  bool has_left =
      (join_type_ != JoinType::RIGHT_SEMI && join_type_ != JoinType::RIGHT_ANTI &&
       schema_[0]->num_cols(HashJoinProjection::OUTPUT) > 0);
  bool has_right =
      (join_type_ != JoinType::LEFT_SEMI && join_type_ != JoinType::LEFT_ANTI &&
       schema_[1]->num_cols(HashJoinProjection::OUTPUT) > 0);

  auto left_from_input =
      schema_[0]->map(HashJoinProjection::OUTPUT, HashJoinProjection::INPUT);
  auto right_from_input =
      schema_[1]->map(HashJoinProjection::OUTPUT, HashJoinProjection::INPUT);

  if (has_left) {
    RETURN_NOT_OK(AppendOutputColumn(result, left_from_input, left_batch, 0, opt_left_ids,
                                     batch_size_next, 0));
  }

  if (has_right) {
    RETURN_NOT_OK(AppendOutputColumn(result, right_from_input, right_batch, 1,
                                     opt_right_ids, batch_size_next, num_out_cols_left));
  }

  ARROW_RETURN_NOT_OK(output_batch_callback_(0, std::move(result)));
  output_batch_nums_++;

  return Status::OK();
}

Status NestedLoopJoinImpl::AppendOutputColumn(ExecBatch& result,
                                              const SchemaProjectionMap& from_input,
                                              const ExecBatch& batch, int side,
                                              const int64_t* opt_ids, int64_t nrows,
                                              int out_col_offset) {
  int num_cols = schema_[side]->num_cols(HashJoinProjection::OUTPUT);
  for (int icol = 0; icol < num_cols; ++icol) {
    std::shared_ptr<Array> col_array;
    std::unique_ptr<arrow::ArrayBuilder> builder;
    int input_field_id = from_input.get(icol);
    const std::shared_ptr<DataType>& input_data_type =
        schema_[side]->data_type(NestedLoopJoinProjection::INPUT, input_field_id);
    RETURN_NOT_OK(MakeBuilder(pool_, input_data_type, &builder));
    for (int i = 0; i < nrows; i++) {
      int64_t irow = opt_ids[i];
      if (irow == kRowIdNull || input_data_type->id() == Type::NA) {
        RETURN_NOT_OK(builder->AppendNull());
      } else {
        ARROW_DCHECK(batch.num_values() > 0);
        RETURN_NOT_OK(
            builder->AppendArraySlice(*batch.values[input_field_id].array(), irow, 1));
      }
    }
    RETURN_NOT_OK(builder->Finish(&col_array));
    result.values[icol + out_col_offset] = col_array;
  }
  return Status::OK();
}

Status NestedLoopJoinImpl::FinishCallBack(size_t thread_index) {
  return finished_callback_(output_batch_nums_);
}

void NestedLoopJoinImpl::Abort(TaskScheduler::AbortContinuationImpl pos_abort_callback) {
  EVENT(span_, "Abort");
  END_SPAN(span_);
  cancelled_ = true;
  pos_abort_callback();
}

}  // namespace acero
}  // namespace arrow