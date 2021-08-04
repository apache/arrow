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

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/exec_utils.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

struct HashSemiJoinNode : ExecNode {
  HashSemiJoinNode(ExecNode* build_input, ExecNode* probe_input, std::string label,
                   ExecContext* ctx, const std::vector<int>&& build_index_field_ids,
                   const std::vector<int>&& probe_index_field_ids)
      : ExecNode(build_input->plan(), std::move(label), {build_input, probe_input},
                 {"hash_join_build", "hash_join_probe"}, probe_input->output_schema(),
                 /*num_outputs=*/1),
        ctx_(ctx),
        build_index_field_ids_(build_index_field_ids),
        probe_index_field_ids_(probe_index_field_ids),
        build_result_index(-1),
        hash_table_built_(false),
        cached_probe_batches_consumed(false) {}

 private:
  struct ThreadLocalState;

 public:
  const char* kind_name() override { return "HashSemiJoinNode"; }

  Status InitLocalStateIfNeeded(ThreadLocalState* state) {
    ARROW_LOG(DEBUG) << "init state";

    // Get input schema
    auto build_schema = inputs_[0]->output_schema();

    if (state->grouper != nullptr) return Status::OK();

    // Build vector of key field data types
    std::vector<ValueDescr> key_descrs(build_index_field_ids_.size());
    for (size_t i = 0; i < build_index_field_ids_.size(); ++i) {
      auto build_type = build_schema->field(build_index_field_ids_[i])->type();
      key_descrs[i] = ValueDescr(build_type);
    }

    // Construct grouper
    ARROW_ASSIGN_OR_RAISE(state->grouper, internal::Grouper::Make(key_descrs, ctx_));

    return Status::OK();
  }

  // Finds an appropriate index which could accumulate all build indices (i.e. the grouper
  // which has the highest # of groups)
  void CalculateBuildResultIndex() {
    uint32_t curr_max = 0;
    for (int i = 0; i < static_cast<int>(local_states_.size()); i++) {
      auto* state = &local_states_[i];
      ARROW_DCHECK(state);
      if (state->grouper && curr_max < state->grouper->num_groups()) {
        curr_max = state->grouper->num_groups();
        build_result_index = i;
      }
    }
    ARROW_DCHECK(build_result_index > -1);
    ARROW_LOG(DEBUG) << "build_result_index " << build_result_index;
  }

  // Performs the housekeeping work after the build-side is completed.
  // Note: this method is not thread safe, and hence should be guaranteed that it is
  // not accessed concurrently!
  Status BuildSideCompleted() {
    ARROW_LOG(DEBUG) << "build side merge";

    // if the hash table has already been built, return
    if (hash_table_built_) return Status::OK();

    CalculateBuildResultIndex();

    // merge every group into the build_result_index grouper
    ThreadLocalState* result_state = &local_states_[build_result_index];
    for (int i = 0; i < static_cast<int>(local_states_.size()); ++i) {
      ThreadLocalState* state = &local_states_[i];
      ARROW_DCHECK(state);
      if (i == build_result_index || !state->grouper) {
        continue;
      }
      ARROW_ASSIGN_OR_RAISE(ExecBatch other_keys, state->grouper->GetUniques());

      // TODO(niranda) replace with void consume method
      ARROW_ASSIGN_OR_RAISE(Datum _, result_state->grouper->Consume(other_keys));
      state->grouper.reset();
    }

    // enable flag that build side is completed
    hash_table_built_ = true;

    // since the build side is completed, consume cached probe batches
    RETURN_NOT_OK(ConsumeCachedProbeBatches());

    return Status::OK();
  }

  // consumes a build batch and increments the build_batches count. if the build batches
  // total reached at the end of consumption, all the local states will be merged, before
  // incrementing the total batches
  Status ConsumeBuildBatch(ExecBatch batch) {
    size_t thread_index = get_thread_index_();
    ARROW_DCHECK(thread_index < local_states_.size());

    ARROW_LOG(DEBUG) << "ConsumeBuildBatch tid:" << thread_index
                     << " len:" << batch.length;

    auto state = &local_states_[thread_index];
    RETURN_NOT_OK(InitLocalStateIfNeeded(state));

    // Create a batch with key columns
    std::vector<Datum> keys(build_index_field_ids_.size());
    for (size_t i = 0; i < build_index_field_ids_.size(); ++i) {
      keys[i] = batch.values[build_index_field_ids_[i]];
    }
    ARROW_ASSIGN_OR_RAISE(ExecBatch key_batch, ExecBatch::Make(keys));

    // Create a batch with group ids
    // TODO(niranda) replace with void consume method
    ARROW_ASSIGN_OR_RAISE(Datum _, state->grouper->Consume(key_batch));

    if (build_counter_.Increment()) {
      // only one thread would get inside this block!
      // while incrementing, if the total is reached, call BuildSideCompleted.
      RETURN_NOT_OK(BuildSideCompleted());
    }

    return Status::OK();
  }

  // consumes cached probe batches by invoking executor::Spawn.
  Status ConsumeCachedProbeBatches() {
    ARROW_LOG(DEBUG) << "ConsumeCachedProbeBatches tid:" << get_thread_index_()
                     << " len:" << cached_probe_batches.size();

    // acquire the mutex to access cached_probe_batches, because while consuming, other
    // batches should not be cached!
    std::lock_guard<std::mutex> lck(cached_probe_batches_mutex);

    if (!cached_probe_batches.empty()) {
      auto executor = ctx_->executor();
      for (auto&& cached : cached_probe_batches) {
        if (executor) {
          Status lambda_status;
          RETURN_NOT_OK(executor->Spawn([&] {
            lambda_status = ConsumeProbeBatch(cached.first, std::move(cached.second));
          }));

          // if the lambda execution failed internally, return status
          RETURN_NOT_OK(lambda_status);
        } else {
          RETURN_NOT_OK(ConsumeProbeBatch(cached.first, std::move(cached.second)));
        }
      }
      // cached vector will be cleared. exec batches are expected to be moved to the
      // lambdas
      cached_probe_batches.clear();
    }

    // set flag
    cached_probe_batches_consumed = true;
    return Status::OK();
  }

  // consumes a probe batch and increment probe batches count. Probing would query the
  // grouper[build_result_index] which have been merged with all others.
  Status ConsumeProbeBatch(int seq, ExecBatch batch) {
    ARROW_LOG(DEBUG) << "ConsumeProbeBatch seq:" << seq;

    auto& final_grouper = *local_states_[build_result_index].grouper;

    // Create a batch with key columns
    std::vector<Datum> keys(probe_index_field_ids_.size());
    for (size_t i = 0; i < probe_index_field_ids_.size(); ++i) {
      keys[i] = batch.values[probe_index_field_ids_[i]];
    }
    ARROW_ASSIGN_OR_RAISE(ExecBatch key_batch, ExecBatch::Make(keys));

    // Query the grouper with key_batch. If no match was found, returning group_ids would
    // have null.
    ARROW_ASSIGN_OR_RAISE(Datum group_ids, final_grouper.Find(key_batch));
    auto group_ids_data = *group_ids.array();

    if (group_ids_data.MayHaveNulls()) {  // values need to be filtered
      auto filter_arr =
          std::make_shared<BooleanArray>(group_ids_data.length, group_ids_data.buffers[0],
                                         /*null_bitmap=*/nullptr, /*null_count=*/0,
                                         /*offset=*/group_ids_data.offset);
      ARROW_ASSIGN_OR_RAISE(auto rec_batch,
                            batch.ToRecordBatch(output_schema_, ctx_->memory_pool()));
      ARROW_ASSIGN_OR_RAISE(
          auto filtered,
          Filter(rec_batch, filter_arr,
                 /* null_selection = DROP*/ FilterOptions::Defaults(), ctx_));
      auto out_batch = ExecBatch(*filtered.record_batch());
      ARROW_LOG(DEBUG) << "output seq:" << seq << " " << out_batch.length;
      outputs_[0]->InputReceived(this, seq, std::move(out_batch));
    } else {  // all values are valid for output
      ARROW_LOG(DEBUG) << "output seq:" << seq << " " << batch.length;
      outputs_[0]->InputReceived(this, seq, std::move(batch));
    }

    if (out_counter_.Increment()) {
      finished_.MarkFinished();
    }
    return Status::OK();
  }

  // Attempt to cache a probe batch. If it is not cached, return false.
  // if cached_probe_batches_consumed is true, by the time a thread acquires
  // cached_probe_batches_mutex, it should no longer be cached! instead, it can be
  //  directly consumed!
  bool AttemptToCacheProbeBatch(int seq_num, ExecBatch* batch) {
    ARROW_LOG(DEBUG) << "cache tid:" << get_thread_index_() << " seq:" << seq_num
                     << " len:" << batch->length;
    std::lock_guard<std::mutex> lck(cached_probe_batches_mutex);
    if (cached_probe_batches_consumed) {
      return false;
    }
    cached_probe_batches.emplace_back(seq_num, std::move(*batch));
    return true;
  }

  inline bool IsBuildInput(ExecNode* input) { return input == inputs_[0]; }

  // If all build side batches received? continue streaming using probing
  // else cache the batches in thread-local state
  void InputReceived(ExecNode* input, int seq, ExecBatch batch) override {
    ARROW_LOG(DEBUG) << "input received input:" << (IsBuildInput(input) ? "b" : "p")
                     << " seq:" << seq << " len:" << batch.length;

    ARROW_DCHECK(input == inputs_[0] || input == inputs_[1]);

    if (finished_.is_finished()) {
      return;
    }

    if (IsBuildInput(input)) {  // build input batch is received
      // if a build input is received when build side is completed, something's wrong!
      ARROW_DCHECK(!hash_table_built_);

      ErrorIfNotOk(ConsumeBuildBatch(std::move(batch)));
    } else {  // probe input batch is received
      if (hash_table_built_) {
        // build side done, continue with probing. when hash_table_built_ is set, it is
        // guaranteed that some thread has already called the ConsumeCachedProbeBatches

        // consume this probe batch
        ErrorIfNotOk(ConsumeProbeBatch(seq, std::move(batch)));
      } else {  // build side not completed. Cache this batch!
        if (!AttemptToCacheProbeBatch(seq, &batch)) {
          // if the cache attempt fails, consume the batch
          ErrorIfNotOk(ConsumeProbeBatch(seq, std::move(batch)));
        }
      }
    }
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    ARROW_LOG(DEBUG) << "error received " << error.ToString();
    DCHECK_EQ(input, inputs_[0]);

    outputs_[0]->ErrorReceived(this, std::move(error));
    StopProducing();
  }

  void InputFinished(ExecNode* input, int num_total) override {
    ARROW_LOG(DEBUG) << "input finished input:" << (IsBuildInput(input) ? "b" : "p")
                     << " tot:" << num_total;

    // bail if StopProducing was called
    if (finished_.is_finished()) return;

    ARROW_DCHECK(input == inputs_[0] || input == inputs_[1]);

    // set total for build input
    if (IsBuildInput(input) && build_counter_.SetTotal(num_total)) {
      // only one thread would get inside this block!
      // while incrementing, if the total is reached, call BuildSideCompleted.
      ErrorIfNotOk(BuildSideCompleted());
      return;
    }

    // set total for probe input. If it returns that probe side has completed, nothing to
    // do, because probing inputs will be streamed to the output
    // probe_counter_.SetTotal(num_total);

    // output will be streamed from the probe side. So, they will have the same total.
    if (out_counter_.SetTotal(num_total)) {
      // if out_counter has completed, the future is finished!
      ErrorIfNotOk(ConsumeCachedProbeBatches());
      outputs_[0]->InputFinished(this, num_total);
      finished_.MarkFinished();
    } else {
      outputs_[0]->InputFinished(this, num_total);
    }
  }

  Status StartProducing() override {
    ARROW_LOG(DEBUG) << "start prod";
    finished_ = Future<>::Make();

    local_states_.resize(ThreadIndexer::Capacity());
    return Status::OK();
  }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    ARROW_LOG(DEBUG) << "stop prod from node";

    DCHECK_EQ(output, outputs_[0]);

    if (build_counter_.Cancel()) {
      finished_.MarkFinished();
    } else if (out_counter_.Cancel()) {
      finished_.MarkFinished();
    }

    for (auto&& input : inputs_) {
      input->StopProducing(this);
    }
  }

  // TODO(niranda) couldn't there be multiple outputs for a Node?
  void StopProducing() override {
    ARROW_LOG(DEBUG) << "stop prod ";
    outputs_[0]->StopProducing();
  }

  Future<> finished() override {
    ARROW_LOG(DEBUG) << "finished? " << finished_.is_finished();
    return finished_;
  }

 private:
  struct ThreadLocalState {
    std::unique_ptr<internal::Grouper> grouper;
  };

  ExecContext* ctx_;
  Future<> finished_ = Future<>::MakeFinished();

  ThreadIndexer get_thread_index_;
  const std::vector<int> build_index_field_ids_, probe_index_field_ids_;

  AtomicCounter build_counter_, out_counter_;
  std::vector<ThreadLocalState> local_states_;

  // There's no guarantee on which threads would be coming from the build side. so, out of
  // the thread local states, an appropriate state needs to be chosen to accumulate
  // all built results (ideally, the grouper which has the highest # of elems)
  int32_t build_result_index;

  // need a separate bool to track if the build side complete. Can't use the flag
  // inside the AtomicCounter, because we need to merge the build groupers once we receive
  // all the build batches. So, while merging, we need to prevent probe batches, being
  // consumed.
  bool hash_table_built_;

  std::mutex cached_probe_batches_mutex;
  std::vector<std::pair<int, ExecBatch>> cached_probe_batches{};
  // a flag is required to indicate if the cached probe batches have already been
  // consumed! if cached_probe_batches_consumed is true, by the time a thread aquires
  // cached_probe_batches_mutex, it should no longer be cached! instead, it can be
  // directly consumed!
  bool cached_probe_batches_consumed;
};

Status ValidateJoinInputs(ExecNode* left_input, ExecNode* right_input,
                          const std::vector<std::string>& left_keys,
                          const std::vector<std::string>& right_keys) {
  if (left_keys.size() != right_keys.size()) {
    return Status::Invalid("left and right key sizes do not match");
  }

  const auto& l_schema = left_input->output_schema();
  const auto& r_schema = right_input->output_schema();
  for (size_t i = 0; i < left_keys.size(); i++) {
    auto l_type = l_schema->GetFieldByName(left_keys[i])->type();
    auto r_type = r_schema->GetFieldByName(right_keys[i])->type();

    if (!l_type->Equals(r_type)) {
      return Status::Invalid("build and probe types do not match: " + l_type->ToString() +
                             "!=" + r_type->ToString());
    }
  }

  return Status::OK();
}

Result<std::vector<int>> PopulateKeys(const Schema& schema,
                                      const std::vector<std::string>& keys) {
  std::vector<int> key_field_ids(keys.size());
  // Find input field indices for left key fields
  for (size_t i = 0; i < keys.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto match, FieldRef(keys[i]).FindOne(schema));
    key_field_ids[i] = match[0];
  }

  return key_field_ids;
}

Result<ExecNode*> MakeHashSemiJoinNode(ExecNode* build_input, ExecNode* probe_input,
                                       std::string label,
                                       const std::vector<std::string>& build_keys,
                                       const std::vector<std::string>& probe_keys) {
  RETURN_NOT_OK(ValidateJoinInputs(build_input, probe_input, build_keys, probe_keys));

  auto build_schema = build_input->output_schema();
  auto probe_schema = probe_input->output_schema();

  ARROW_ASSIGN_OR_RAISE(auto build_key_ids, PopulateKeys(*build_schema, build_keys));
  ARROW_ASSIGN_OR_RAISE(auto probe_key_ids, PopulateKeys(*probe_schema, probe_keys));

  //  output schema will be probe schema
  auto ctx = build_input->plan()->exec_context();
  ExecPlan* plan = build_input->plan();

  return plan->EmplaceNode<HashSemiJoinNode>(build_input, probe_input, std::move(label),
                                             ctx, std::move(build_key_ids),
                                             std::move(probe_key_ids));
}

Result<ExecNode*> MakeHashLeftSemiJoinNode(ExecNode* left_input, ExecNode* right_input,
                                           std::string label,
                                           const std::vector<std::string>& left_keys,
                                           const std::vector<std::string>& right_keys) {
  // left join--> build from right and probe from left
  return MakeHashSemiJoinNode(right_input, left_input, std::move(label), right_keys,
                              left_keys);
}

Result<ExecNode*> MakeHashRightSemiJoinNode(ExecNode* left_input, ExecNode* right_input,
                                            std::string label,
                                            const std::vector<std::string>& left_keys,
                                            const std::vector<std::string>& right_keys) {
  // right join--> build from left and probe from right
  return MakeHashSemiJoinNode(left_input, right_input, std::move(label), left_keys,
                              right_keys);
}

static std::string JoinTypeToString[] = {"LEFT_SEMI",   "RIGHT_SEMI", "LEFT_ANTI",
                                         "RIGHT_ANTI",  "INNER",      "LEFT_OUTER",
                                         "RIGHT_OUTER", "FULL_OUTER"};

Result<ExecNode*> MakeHashJoinNode(JoinType join_type, ExecNode* left_input,
                                   ExecNode* right_input, std::string label,
                                   const std::vector<std::string>& left_keys,
                                   const std::vector<std::string>& right_keys) {
  switch (join_type) {
    case LEFT_SEMI:
      // left join--> build from right and probe from left
      return MakeHashSemiJoinNode(right_input, left_input, std::move(label), right_keys,
                                  left_keys);
    case RIGHT_SEMI:
      // right join--> build from left and probe from right
      return MakeHashSemiJoinNode(left_input, right_input, std::move(label), left_keys,
                                  right_keys);
    case LEFT_ANTI:
    case RIGHT_ANTI:
    case INNER:
    case LEFT_OUTER:
    case RIGHT_OUTER:
    case FULL_OUTER:
      return Status::NotImplemented(JoinTypeToString[join_type] +
                                    " joins not implemented!");
    default:
      return Status::Invalid("invalid join type");
  }
}

}  // namespace compute
}  // namespace arrow
