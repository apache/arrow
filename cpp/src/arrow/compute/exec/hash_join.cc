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

#include "arrow/compute/exec/hash_join.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <mutex>
#include <numeric>
#include <unordered_map>
#include <vector>

#include "arrow/compute/exec/hash_join_dict.h"
#include "arrow/compute/exec/task_util.h"
#include "arrow/compute/kernels/row_encoder_internal.h"
#include "arrow/compute/row/encode_internal.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {
namespace compute {

class HashJoinBasicImpl : public HashJoinImpl {
 private:
  struct ThreadLocalState;

 public:
  Status Init(QueryContext* ctx, JoinType join_type, size_t num_threads,
              const HashJoinProjectionMaps* proj_map_left,
              const HashJoinProjectionMaps* proj_map_right,
              std::vector<JoinKeyCmp> key_cmp, Expression filter,
              RegisterTaskGroupCallback register_task_group_callback,
              StartTaskGroupCallback start_task_group_callback,
              OutputBatchCallback output_batch_callback,
              FinishedCallback finished_callback) override {
    START_COMPUTE_SPAN(span_, "HashJoinBasicImpl",
                       {{"detail", filter.ToString()},
                        {"join.kind", arrow::compute::ToString(join_type)},
                        {"join.threads", static_cast<uint32_t>(num_threads)}});

    num_threads_ = num_threads;
    ctx_ = ctx;
    join_type_ = join_type;
    schema_[0] = proj_map_left;
    schema_[1] = proj_map_right;
    key_cmp_ = std::move(key_cmp);
    filter_ = std::move(filter);
    register_task_group_callback_ = std::move(register_task_group_callback);
    start_task_group_callback_ = std::move(start_task_group_callback);
    output_batch_callback_ = std::move(output_batch_callback);
    finished_callback_ = std::move(finished_callback);
    local_states_.resize(num_threads_);

    for (size_t i = 0; i < local_states_.size(); ++i) {
      local_states_[i].is_initialized = false;
      local_states_[i].is_has_match_initialized = false;
    }

    dict_probe_.Init(num_threads_);

    has_hash_table_ = false;
    num_batches_produced_.store(0);
    cancelled_ = false;

    RegisterBuildHashTable();
    RegisterScanHashTable();
    return Status::OK();
  }

  void Abort(AbortContinuationImpl pos_abort_callback) override {
    EVENT(span_, "Abort");
    END_SPAN(span_);
    cancelled_ = true;
    pos_abort_callback();
  }

  std::string ToString() const override { return "HashJoinBasicImpl"; }

 private:
  void InitEncoder(int side, HashJoinProjection projection_handle, RowEncoder* encoder) {
    std::vector<TypeHolder> data_types;
    int num_cols = schema_[side]->num_cols(projection_handle);
    data_types.resize(num_cols);
    for (int icol = 0; icol < num_cols; ++icol) {
      data_types[icol] = schema_[side]->data_type(projection_handle, icol);
    }
    encoder->Init(data_types, ctx_->exec_context());
    encoder->Clear();
  }

  Status InitLocalStateIfNeeded(size_t thread_index) {
    DCHECK_LT(thread_index, local_states_.size());
    ThreadLocalState& local_state = local_states_[thread_index];
    if (!local_state.is_initialized) {
      InitEncoder(0, HashJoinProjection::KEY, &local_state.exec_batch_keys);
      bool has_payload = (schema_[0]->num_cols(HashJoinProjection::PAYLOAD) > 0);
      if (has_payload) {
        InitEncoder(0, HashJoinProjection::PAYLOAD, &local_state.exec_batch_payloads);
      }
      local_state.is_initialized = true;
    }
    return Status::OK();
  }

  Status EncodeBatch(int side, HashJoinProjection projection_handle, RowEncoder* encoder,
                     const ExecBatch& batch, ExecBatch* opt_projected_batch = nullptr) {
    ExecBatch projected({}, batch.length);
    int num_cols = schema_[side]->num_cols(projection_handle);
    projected.values.resize(num_cols);

    auto to_input = schema_[side]->map(projection_handle, HashJoinProjection::INPUT);
    for (int icol = 0; icol < num_cols; ++icol) {
      projected.values[icol] = batch.values[to_input.get(icol)];
    }

    if (opt_projected_batch) {
      *opt_projected_batch = projected;
    }

    return encoder->EncodeAndAppend(ExecSpan(projected));
  }

  void ProbeBatch_Lookup(ThreadLocalState* local_state, const RowEncoder& exec_batch_keys,
                         const std::vector<const uint8_t*>& non_null_bit_vectors,
                         const std::vector<int64_t>& non_null_bit_vector_offsets,
                         std::vector<int32_t>* output_match,
                         std::vector<int32_t>* output_no_match,
                         std::vector<int32_t>* output_match_left,
                         std::vector<int32_t>* output_match_right) {
    InitHasMatchIfNeeded(local_state);

    ARROW_DCHECK(has_hash_table_);

    InitHasMatchIfNeeded(local_state);

    int num_cols = static_cast<int>(non_null_bit_vectors.size());
    for (int32_t irow = 0; irow < exec_batch_keys.num_rows(); ++irow) {
      // Apply null key filtering
      bool no_match = hash_table_empty_;
      for (int icol = 0; icol < num_cols; ++icol) {
        bool is_null = non_null_bit_vectors[icol] &&
                       !bit_util::GetBit(non_null_bit_vectors[icol],
                                         non_null_bit_vector_offsets[icol] + irow);
        if (key_cmp_[icol] == JoinKeyCmp::EQ && is_null) {
          no_match = true;
          break;
        }
      }
      if (no_match) {
        output_no_match->push_back(irow);
        continue;
      }
      // Get all matches from hash table
      bool has_match = false;

      auto range = hash_table_.equal_range(exec_batch_keys.encoded_row(irow));
      for (auto it = range.first; it != range.second; ++it) {
        output_match_left->push_back(irow);
        output_match_right->push_back(it->second);
        has_match = true;
      }
      if (!has_match) {
        output_no_match->push_back(irow);
      } else {
        output_match->push_back(irow);
      }
    }
  }

  Status ProbeBatch_OutputOne(int64_t batch_size_next, ExecBatch* opt_left_key,
                              ExecBatch* opt_left_payload, ExecBatch* opt_right_key,
                              ExecBatch* opt_right_payload) {
    ExecBatch result({}, batch_size_next);
    int num_out_cols_left = schema_[0]->num_cols(HashJoinProjection::OUTPUT);
    int num_out_cols_right = schema_[1]->num_cols(HashJoinProjection::OUTPUT);

    result.values.resize(num_out_cols_left + num_out_cols_right);
    auto from_key = schema_[0]->map(HashJoinProjection::OUTPUT, HashJoinProjection::KEY);
    auto from_payload =
        schema_[0]->map(HashJoinProjection::OUTPUT, HashJoinProjection::PAYLOAD);
    for (int icol = 0; icol < num_out_cols_left; ++icol) {
      bool is_from_key = (from_key.get(icol) != HashJoinProjectionMaps::kMissingField);
      bool is_from_payload =
          (from_payload.get(icol) != HashJoinProjectionMaps::kMissingField);
      ARROW_DCHECK(is_from_key != is_from_payload);
      ARROW_DCHECK(!is_from_key ||
                   (opt_left_key &&
                    from_key.get(icol) < static_cast<int>(opt_left_key->values.size()) &&
                    opt_left_key->length == batch_size_next));
      ARROW_DCHECK(
          !is_from_payload ||
          (opt_left_payload &&
           from_payload.get(icol) < static_cast<int>(opt_left_payload->values.size()) &&
           opt_left_payload->length == batch_size_next));
      result.values[icol] = is_from_key
                                ? opt_left_key->values[from_key.get(icol)]
                                : opt_left_payload->values[from_payload.get(icol)];
    }
    from_key = schema_[1]->map(HashJoinProjection::OUTPUT, HashJoinProjection::KEY);
    from_payload =
        schema_[1]->map(HashJoinProjection::OUTPUT, HashJoinProjection::PAYLOAD);
    for (int icol = 0; icol < num_out_cols_right; ++icol) {
      bool is_from_key = (from_key.get(icol) != HashJoinProjectionMaps::kMissingField);
      bool is_from_payload =
          (from_payload.get(icol) != HashJoinProjectionMaps::kMissingField);
      ARROW_DCHECK(is_from_key != is_from_payload);
      ARROW_DCHECK(!is_from_key ||
                   (opt_right_key &&
                    from_key.get(icol) < static_cast<int>(opt_right_key->values.size()) &&
                    opt_right_key->length == batch_size_next));
      ARROW_DCHECK(
          !is_from_payload ||
          (opt_right_payload &&
           from_payload.get(icol) < static_cast<int>(opt_right_payload->values.size()) &&
           opt_right_payload->length == batch_size_next));
      result.values[num_out_cols_left + icol] =
          is_from_key ? opt_right_key->values[from_key.get(icol)]
                      : opt_right_payload->values[from_payload.get(icol)];
    }

    ARROW_RETURN_NOT_OK(output_batch_callback_(0, std::move(result)));

    // Update the counter of produced batches
    //
    num_batches_produced_++;
    return Status::OK();
  }

  Status ProbeBatch_ResidualFilter(ThreadLocalState& local_state,
                                   std::vector<int32_t>& match,
                                   std::vector<int32_t>& no_match,
                                   std::vector<int32_t>& match_left,
                                   std::vector<int32_t>& match_right) {
    if (filter_ == literal(true)) {
      return Status::OK();
    }
    ARROW_DCHECK_EQ(match_left.size(), match_right.size());

    ExecBatch concatenated({}, match_left.size());

    ARROW_ASSIGN_OR_RAISE(ExecBatch left_key, local_state.exec_batch_keys.Decode(
                                                  match_left.size(), match_left.data()));
    ARROW_ASSIGN_OR_RAISE(
        ExecBatch right_key,
        hash_table_keys_.Decode(match_right.size(), match_right.data()));

    ExecBatch left_payload;
    if (!schema_[0]->is_empty(HashJoinProjection::PAYLOAD)) {
      ARROW_ASSIGN_OR_RAISE(left_payload, local_state.exec_batch_payloads.Decode(
                                              match_left.size(), match_left.data()));
    }

    ExecBatch right_payload;
    if (!schema_[1]->is_empty(HashJoinProjection::PAYLOAD)) {
      ARROW_ASSIGN_OR_RAISE(right_payload, hash_table_payloads_.Decode(
                                               match_right.size(), match_right.data()));
    }

    auto AppendFields = [&concatenated](const SchemaProjectionMap& to_key,
                                        const SchemaProjectionMap& to_pay,
                                        const ExecBatch& key, const ExecBatch& payload) {
      ARROW_DCHECK(to_key.num_cols == to_pay.num_cols);
      for (int i = 0; i < to_key.num_cols; i++) {
        if (to_key.get(i) != SchemaProjectionMap::kMissingField) {
          int key_idx = to_key.get(i);
          concatenated.values.push_back(key.values[key_idx]);
        } else if (to_pay.get(i) != SchemaProjectionMap::kMissingField) {
          int pay_idx = to_pay.get(i);
          concatenated.values.push_back(payload.values[pay_idx]);
        }
      }
    };

    SchemaProjectionMap left_to_key =
        schema_[0]->map(HashJoinProjection::FILTER, HashJoinProjection::KEY);
    SchemaProjectionMap left_to_pay =
        schema_[0]->map(HashJoinProjection::FILTER, HashJoinProjection::PAYLOAD);
    SchemaProjectionMap right_to_key =
        schema_[1]->map(HashJoinProjection::FILTER, HashJoinProjection::KEY);
    SchemaProjectionMap right_to_pay =
        schema_[1]->map(HashJoinProjection::FILTER, HashJoinProjection::PAYLOAD);

    AppendFields(left_to_key, left_to_pay, left_key, left_payload);
    AppendFields(right_to_key, right_to_pay, right_key, right_payload);

    ARROW_ASSIGN_OR_RAISE(
        Datum mask, ExecuteScalarExpression(filter_, concatenated, ctx_->exec_context()));

    size_t num_probed_rows = match.size() + no_match.size();
    if (mask.is_scalar()) {
      const auto& mask_scalar = mask.scalar_as<BooleanScalar>();
      if (mask_scalar.is_valid && mask_scalar.value) {
        // All rows passed, nothing left to do
        return Status::OK();
      } else {
        // Nothing passed, no_match becomes everything
        no_match.resize(num_probed_rows);
        std::iota(no_match.begin(), no_match.end(), 0);
        match_left.clear();
        match_right.clear();
        match.clear();
        return Status::OK();
      }
    }
    ARROW_DCHECK_EQ(mask.array()->offset, 0);
    ARROW_DCHECK_EQ(mask.array()->length, static_cast<int64_t>(match_left.size()));
    const uint8_t* validity =
        mask.array()->buffers[0] ? mask.array()->buffers[0]->data() : nullptr;
    const uint8_t* comparisons = mask.array()->buffers[1]->data();
    size_t num_rows = match_left.size();

    match.clear();
    no_match.clear();

    int32_t match_idx = 0;  // current size of new match_left
    int32_t irow = 0;       // index into match_left
    for (int32_t curr_left = 0; static_cast<size_t>(curr_left) < num_probed_rows;
         curr_left++) {
      int32_t advance_to = static_cast<size_t>(irow) < num_rows
                               ? match_left[irow]
                               : static_cast<int32_t>(num_probed_rows);
      while (curr_left < advance_to) {
        no_match.push_back(curr_left++);
      }
      bool passed = false;
      for (; static_cast<size_t>(irow) < num_rows && match_left[irow] == curr_left;
           irow++) {
        bool is_valid = !validity || bit_util::GetBit(validity, irow);
        bool is_cmp_true = bit_util::GetBit(comparisons, irow);
        // We treat a null comparison result as false, like in SQL
        if (is_valid && is_cmp_true) {
          match_left[match_idx] = match_left[irow];
          match_right[match_idx] = match_right[irow];
          match_idx++;
          passed = true;
        }
      }
      if (passed) {
        match.push_back(curr_left);
      } else if (static_cast<size_t>(curr_left) < num_probed_rows) {
        no_match.push_back(curr_left);
      }
    }
    match_left.resize(match_idx);
    match_right.resize(match_idx);
    return Status::OK();
  }

  Status ProbeBatch_OutputOne(size_t thread_index, int64_t batch_size_next,
                              const int32_t* opt_left_ids, const int32_t* opt_right_ids) {
    if (batch_size_next == 0 || (!opt_left_ids && !opt_right_ids)) {
      return Status::OK();
    }

    bool has_left =
        (join_type_ != JoinType::RIGHT_SEMI && join_type_ != JoinType::RIGHT_ANTI &&
         schema_[0]->num_cols(HashJoinProjection::OUTPUT) > 0);
    bool has_right =
        (join_type_ != JoinType::LEFT_SEMI && join_type_ != JoinType::LEFT_ANTI &&
         schema_[1]->num_cols(HashJoinProjection::OUTPUT) > 0);
    bool has_left_payload =
        has_left && (schema_[0]->num_cols(HashJoinProjection::PAYLOAD) > 0);
    bool has_right_payload =
        has_right && (schema_[1]->num_cols(HashJoinProjection::PAYLOAD) > 0);

    ThreadLocalState& local_state = local_states_[thread_index];
    RETURN_NOT_OK(InitLocalStateIfNeeded(thread_index));

    ExecBatch left_key;
    ExecBatch left_payload;
    ExecBatch right_key;
    ExecBatch right_payload;
    if (has_left) {
      ARROW_DCHECK(opt_left_ids);
      ARROW_ASSIGN_OR_RAISE(
          left_key, local_state.exec_batch_keys.Decode(batch_size_next, opt_left_ids));
    }
    if (has_left_payload) {
      ARROW_ASSIGN_OR_RAISE(left_payload, local_state.exec_batch_payloads.Decode(
                                              batch_size_next, opt_left_ids));
    }
    if (has_right) {
      ARROW_DCHECK(opt_right_ids);
      ARROW_ASSIGN_OR_RAISE(right_key,
                            hash_table_keys_.Decode(batch_size_next, opt_right_ids));
      // Post process build side keys that use dictionary
      RETURN_NOT_OK(
          dict_build_.PostDecode(*schema_[1], &right_key, ctx_->exec_context()));
    }
    if (has_right_payload) {
      ARROW_ASSIGN_OR_RAISE(right_payload,
                            hash_table_payloads_.Decode(batch_size_next, opt_right_ids));
    }

    return ProbeBatch_OutputOne(batch_size_next, has_left ? &left_key : nullptr,
                                has_left_payload ? &left_payload : nullptr,
                                has_right ? &right_key : nullptr,
                                has_right_payload ? &right_payload : nullptr);
  }

  Status ProbeBatch_OutputAll(size_t thread_index, const RowEncoder& exec_batch_keys,
                              const RowEncoder& exec_batch_payloads,
                              const std::vector<int32_t>& match,
                              const std::vector<int32_t>& no_match,
                              std::vector<int32_t>& match_left,
                              std::vector<int32_t>& match_right) {
    if (join_type_ == JoinType::RIGHT_SEMI || join_type_ == JoinType::RIGHT_ANTI) {
      // Nothing to output
      return Status::OK();
    }

    if (join_type_ == JoinType::LEFT_ANTI || join_type_ == JoinType::LEFT_SEMI) {
      const std::vector<int32_t>& out_ids =
          (join_type_ == JoinType::LEFT_SEMI) ? match : no_match;

      for (size_t start = 0; start < out_ids.size(); start += output_batch_size_) {
        int64_t batch_size_next = std::min(static_cast<int64_t>(out_ids.size() - start),
                                           static_cast<int64_t>(output_batch_size_));
        RETURN_NOT_OK(ProbeBatch_OutputOne(thread_index, batch_size_next,
                                           out_ids.data() + start, nullptr));
      }
    } else {
      if (join_type_ == JoinType::LEFT_OUTER || join_type_ == JoinType::FULL_OUTER) {
        for (size_t i = 0; i < no_match.size(); ++i) {
          match_left.push_back(no_match[i]);
          match_right.push_back(RowEncoder::kRowIdForNulls());
        }
      }

      ARROW_DCHECK(match_left.size() == match_right.size());

      for (size_t start = 0; start < match_left.size(); start += output_batch_size_) {
        int64_t batch_size_next =
            std::min(static_cast<int64_t>(match_left.size() - start),
                     static_cast<int64_t>(output_batch_size_));
        RETURN_NOT_OK(ProbeBatch_OutputOne(thread_index, batch_size_next,
                                           match_left.data() + start,
                                           match_right.data() + start));
      }
    }
    return Status::OK();
  }

  void NullInfoFromBatch(const ExecBatch& batch,
                         std::vector<const uint8_t*>* nn_bit_vectors,
                         std::vector<int64_t>* nn_offsets,
                         std::vector<uint8_t>* nn_bit_vector_all_nulls) {
    int num_cols = static_cast<int>(batch.values.size());
    nn_bit_vectors->resize(num_cols);
    nn_offsets->resize(num_cols);
    nn_bit_vector_all_nulls->clear();
    for (int64_t i = 0; i < num_cols; ++i) {
      const uint8_t* nn = nullptr;
      int64_t offset = 0;
      if (batch[i].is_array()) {
        if (batch[i].array()->buffers[0] != NULLPTR) {
          nn = batch[i].array()->buffers[0]->data();
          offset = batch[i].array()->offset;
        }
      } else {
        ARROW_DCHECK(batch[i].is_scalar());
        if (!batch[i].scalar_as<arrow::internal::PrimitiveScalarBase>().is_valid) {
          if (nn_bit_vector_all_nulls->empty()) {
            nn_bit_vector_all_nulls->resize(bit_util::BytesForBits(batch.length));
            memset(nn_bit_vector_all_nulls->data(), 0,
                   bit_util::BytesForBits(batch.length));
          }
          nn = nn_bit_vector_all_nulls->data();
        }
      }
      (*nn_bit_vectors)[i] = nn;
      (*nn_offsets)[i] = offset;
    }
  }

  Status ProbeSingleBatch(size_t thread_index, ExecBatch batch) override {
    ThreadLocalState& local_state = local_states_[thread_index];
    RETURN_NOT_OK(InitLocalStateIfNeeded(thread_index));

    local_state.exec_batch_keys.Clear();

    ExecBatch batch_key_for_lookups;

    RETURN_NOT_OK(EncodeBatch(0, HashJoinProjection::KEY, &local_state.exec_batch_keys,
                              batch, &batch_key_for_lookups));
    bool has_left_payload = (schema_[0]->num_cols(HashJoinProjection::PAYLOAD) > 0);
    if (has_left_payload) {
      local_state.exec_batch_payloads.Clear();
      RETURN_NOT_OK(EncodeBatch(0, HashJoinProjection::PAYLOAD,
                                &local_state.exec_batch_payloads, batch));
    }

    local_state.match.clear();
    local_state.no_match.clear();
    local_state.match_left.clear();
    local_state.match_right.clear();

    bool use_key_batch_for_dicts = dict_probe_.BatchRemapNeeded(
        thread_index, *schema_[0], *schema_[1], ctx_->exec_context());
    RowEncoder* row_encoder_for_lookups = &local_state.exec_batch_keys;
    if (use_key_batch_for_dicts) {
      RETURN_NOT_OK(dict_probe_.EncodeBatch(
          thread_index, *schema_[0], *schema_[1], dict_build_, batch,
          &row_encoder_for_lookups, &batch_key_for_lookups, ctx_->exec_context()));
    }

    // Collect information about all nulls in key columns.
    //
    std::vector<const uint8_t*> non_null_bit_vectors;
    std::vector<int64_t> non_null_bit_vector_offsets;
    std::vector<uint8_t> all_nulls;
    NullInfoFromBatch(batch_key_for_lookups, &non_null_bit_vectors,
                      &non_null_bit_vector_offsets, &all_nulls);

    ProbeBatch_Lookup(&local_state, *row_encoder_for_lookups, non_null_bit_vectors,
                      non_null_bit_vector_offsets, &local_state.match,
                      &local_state.no_match, &local_state.match_left,
                      &local_state.match_right);

    RETURN_NOT_OK(ProbeBatch_ResidualFilter(local_state, local_state.match,
                                            local_state.no_match, local_state.match_left,
                                            local_state.match_right));

    for (auto i : local_state.match_right) {
      // Mark row in hash table as having a match
      bit_util::SetBit(local_state.has_match.data(), i);
    }

    RETURN_NOT_OK(ProbeBatch_OutputAll(thread_index, local_state.exec_batch_keys,
                                       local_state.exec_batch_payloads, local_state.match,
                                       local_state.no_match, local_state.match_left,
                                       local_state.match_right));

    return Status::OK();
  }

  void RegisterBuildHashTable() {
    task_group_build_ = register_task_group_callback_(
        [this](size_t thread_index, int64_t task_id) -> Status {
          return BuildHashTable_exec_task(thread_index, task_id);
        },
        [this](size_t thread_index) -> Status {
          return BuildHashTable_on_finished(thread_index);
        });
  }

  Status BuildHashTable_exec_task(size_t thread_index, int64_t /*task_id*/) {
    AccumulationQueue batches = std::move(build_batches_);
    dict_build_.InitEncoder(*schema_[1], &hash_table_keys_, ctx_->exec_context());
    bool has_payload = (schema_[1]->num_cols(HashJoinProjection::PAYLOAD) > 0);
    if (has_payload) {
      InitEncoder(1, HashJoinProjection::PAYLOAD, &hash_table_payloads_);
    }
    hash_table_empty_ = true;

    for (size_t ibatch = 0; ibatch < batches.batch_count(); ++ibatch) {
      if (cancelled_) {
        return Status::Cancelled("Hash join cancelled");
      }
      const ExecBatch& batch = batches[ibatch];
      if (batch.length == 0) {
        continue;
      } else if (hash_table_empty_) {
        hash_table_empty_ = false;

        RETURN_NOT_OK(dict_build_.Init(*schema_[1], &batch, ctx_->exec_context()));
      }
      int32_t num_rows_before = hash_table_keys_.num_rows();
      RETURN_NOT_OK(dict_build_.EncodeBatch(thread_index, *schema_[1], batch,
                                            &hash_table_keys_, ctx_->exec_context()));
      if (has_payload) {
        RETURN_NOT_OK(
            EncodeBatch(1, HashJoinProjection::PAYLOAD, &hash_table_payloads_, batch));
      }
      int32_t num_rows_after = hash_table_keys_.num_rows();
      for (int32_t irow = num_rows_before; irow < num_rows_after; ++irow) {
        hash_table_.insert(std::make_pair(hash_table_keys_.encoded_row(irow), irow));
      }
    }

    if (hash_table_empty_) {
      RETURN_NOT_OK(dict_build_.Init(*schema_[1], nullptr, ctx_->exec_context()));
    }

    return Status::OK();
  }

  Status BuildHashTable_on_finished(size_t thread_index) {
    ARROW_DCHECK_EQ(build_batches_.batch_count(), 0);
    has_hash_table_ = true;
    return build_finished_callback_(thread_index);
  }

  Status BuildHashTable(size_t /*thread_index*/, AccumulationQueue batches,
                        BuildFinishedCallback on_finished) override {
    build_finished_callback_ = std::move(on_finished);
    build_batches_ = std::move(batches);
    return start_task_group_callback_(task_group_build_,
                                      /*num_tasks=*/1);
  }

  void RegisterScanHashTable() {
    task_group_scan_ = register_task_group_callback_(
        [this](size_t thread_index, int64_t task_id) -> Status {
          return ScanHashTable_exec_task(thread_index, task_id);
        },
        [this](size_t thread_index) -> Status {
          return ScanHashTable_on_finished(thread_index);
        });
  }

  int64_t ScanHashTable_num_tasks() {
    if (!has_hash_table_ || hash_table_empty_) {
      return 0;
    }
    if (join_type_ != JoinType::RIGHT_SEMI && join_type_ != JoinType::RIGHT_ANTI &&
        join_type_ != JoinType::RIGHT_OUTER && join_type_ != JoinType::FULL_OUTER) {
      return 0;
    }
    return bit_util::CeilDiv(hash_table_keys_.num_rows(), hash_table_scan_unit_);
  }

  Status ScanHashTable_exec_task(size_t thread_index, int64_t task_id) {
    if (cancelled_) {
      return Status::Cancelled("Hash join cancelled");
    }

    int32_t start_row_id = static_cast<int32_t>(hash_table_scan_unit_ * task_id);
    int32_t end_row_id =
        static_cast<int32_t>(std::min(static_cast<int64_t>(hash_table_keys_.num_rows()),
                                      hash_table_scan_unit_ * (task_id + 1)));

    ThreadLocalState& local_state = local_states_[thread_index];
    RETURN_NOT_OK(InitLocalStateIfNeeded(thread_index));

    std::vector<int32_t>& id_left = local_state.no_match;
    std::vector<int32_t>& id_right = local_state.match;
    id_left.clear();
    id_right.clear();
    bool use_left = false;

    bool match_search_value = (join_type_ == JoinType::RIGHT_SEMI);
    for (int32_t row_id = start_row_id; row_id < end_row_id; ++row_id) {
      if (bit_util::GetBit(has_match_.data(), row_id) == match_search_value) {
        id_right.push_back(row_id);
      }
    }

    if (id_right.empty()) {
      return Status::OK();
    }

    if (join_type_ != JoinType::RIGHT_SEMI && join_type_ != JoinType::RIGHT_ANTI) {
      use_left = true;
      id_left.resize(id_right.size());
      for (size_t i = 0; i < id_left.size(); ++i) {
        id_left[i] = RowEncoder::kRowIdForNulls();
      }
    }

    RETURN_NOT_OK(
        ProbeBatch_OutputOne(thread_index, static_cast<int64_t>(id_right.size()),
                             use_left ? id_left.data() : nullptr, id_right.data()));
    return Status::OK();
  }

  Status ScanHashTable_on_finished(size_t thread_index) {
    if (cancelled_) {
      return Status::Cancelled("Hash join cancelled");
    }
    END_SPAN(span_);
    return finished_callback_(num_batches_produced_.load());
  }

  Status ScanHashTable(size_t thread_index) {
    MergeHasMatch();
    return start_task_group_callback_(task_group_scan_, ScanHashTable_num_tasks());
  }

  Status ProbingFinished(size_t thread_index) override {
    return ScanHashTable(thread_index);
  }

  void InitHasMatchIfNeeded(ThreadLocalState* local_state) {
    if (local_state->is_has_match_initialized) {
      return;
    }
    if (!hash_table_empty_) {
      int32_t num_rows = hash_table_keys_.num_rows();
      local_state->has_match.resize(bit_util::BytesForBits(num_rows));
      memset(local_state->has_match.data(), 0, bit_util::BytesForBits(num_rows));
    }
    local_state->is_has_match_initialized = true;
  }

  void MergeHasMatch() {
    if (hash_table_empty_) {
      return;
    }

    int32_t num_rows = hash_table_keys_.num_rows();
    has_match_.resize(bit_util::BytesForBits(num_rows));
    memset(has_match_.data(), 0, bit_util::BytesForBits(num_rows));

    for (size_t tid = 0; tid < local_states_.size(); ++tid) {
      if (!local_states_[tid].is_initialized) {
        continue;
      }
      if (!local_states_[tid].is_has_match_initialized) {
        continue;
      }
      arrow::internal::BitmapOr(has_match_.data(), 0, local_states_[tid].has_match.data(),
                                0, num_rows, 0, has_match_.data());
    }
  }

  static constexpr int64_t hash_table_scan_unit_ = 32 * 1024;
  static constexpr int64_t output_batch_size_ = 32 * 1024;

  // Metadata
  //
  QueryContext* ctx_;
  JoinType join_type_;
  size_t num_threads_;
  const HashJoinProjectionMaps* schema_[2];
  std::vector<JoinKeyCmp> key_cmp_;
  Expression filter_;
  int task_group_build_;
  int task_group_scan_;

  // Callbacks
  //
  RegisterTaskGroupCallback register_task_group_callback_;
  StartTaskGroupCallback start_task_group_callback_;
  OutputBatchCallback output_batch_callback_;
  BuildFinishedCallback build_finished_callback_;
  FinishedCallback finished_callback_;

  // Thread local runtime state
  //
  struct ThreadLocalState {
    bool is_initialized;
    RowEncoder exec_batch_keys;
    RowEncoder exec_batch_payloads;
    std::vector<int32_t> match;
    std::vector<int32_t> no_match;
    std::vector<int32_t> match_left;
    std::vector<int32_t> match_right;
    bool is_has_match_initialized;
    std::vector<uint8_t> has_match;
  };
  std::vector<ThreadLocalState> local_states_;

  // Shared runtime state
  //
  RowEncoder hash_table_keys_;
  RowEncoder hash_table_payloads_;
  std::unordered_multimap<std::string, int32_t> hash_table_;
  std::vector<uint8_t> has_match_;
  bool hash_table_empty_;

  // Dictionary handling
  //
  HashJoinDictBuildMulti dict_build_;
  HashJoinDictProbeMulti dict_probe_;

  bool has_hash_table_;

  AccumulationQueue build_batches_;

  std::atomic<int64_t> num_batches_produced_;
  bool cancelled_;
};

Result<std::unique_ptr<HashJoinImpl>> HashJoinImpl::MakeBasic() {
  std::unique_ptr<HashJoinImpl> impl{new HashJoinBasicImpl()};
  return std::move(impl);
}

}  // namespace compute
}  // namespace arrow
