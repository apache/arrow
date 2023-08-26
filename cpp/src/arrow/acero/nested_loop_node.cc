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

#include "arrow/acero/nested_loop_node.h"

#include <unordered_set>
#include <vector>

#include "arrow/acero/accumulation_queue.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/nested_loop_join.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/schema_util.h"
#include "arrow/acero/task_util.h"
#include "arrow/acero/util.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;

namespace acero {

Status NestedLoopJoinSchema::Init(JoinType join_type, const Schema& left_schema,
                                  const Schema& right_schema, const Expression& filter,
                                  const std::string& left_field_name_suffix,
                                  const std::string& right_field_name_suffix) {
  std::vector<FieldRef> left_output;
  if (join_type != JoinType::RIGHT_SEMI && join_type != JoinType::RIGHT_ANTI) {
    const FieldVector& left_fields = left_schema.fields();
    left_output.resize(left_fields.size());
    for (size_t i = 0; i < left_fields.size(); ++i) {
      left_output[i] = FieldRef(static_cast<int>(i));
    }
  }
  // Repeat the same for the right side
  std::vector<FieldRef> right_output;
  if (join_type != JoinType::LEFT_SEMI && join_type != JoinType::LEFT_ANTI) {
    const FieldVector& right_fields = right_schema.fields();
    right_output.resize(right_fields.size());
    for (size_t i = 0; i < right_fields.size(); ++i) {
      right_output[i] = FieldRef(static_cast<int>(i));
    }
  }
  return Init(join_type, left_schema, left_output, right_schema, right_output, filter,
              left_field_name_suffix, right_field_name_suffix);
}

Status NestedLoopJoinSchema::Init(JoinType join_type, const Schema& left_schema,
                                  const std::vector<FieldRef>& left_output,
                                  const Schema& right_schema,
                                  const std::vector<FieldRef>& right_output,
                                  const Expression& filter,
                                  const std::string& left_field_name_suffix,
                                  const std::string& right_field_name_suffix) {
  RETURN_NOT_OK(
      ValidateSchemas(join_type, left_schema, left_output, right_schema, right_output));

  std::vector<NestedLoopJoinProjection> handles;
  std::vector<const std::vector<FieldRef>*> field_refs;

  std::vector<FieldRef> left_filter, right_filter;
  RETURN_NOT_OK(
      CollectFilterColumns(left_filter, right_filter, filter, left_schema, right_schema));

  handles.push_back(NestedLoopJoinProjection::FILTER);
  field_refs.push_back(&left_filter);

  handles.push_back(NestedLoopJoinProjection::OUTPUT);
  field_refs.push_back(&left_output);

  RETURN_NOT_OK(proj_maps[0].Init(NestedLoopJoinProjection::INPUT, left_schema, handles,
                                  field_refs));

  handles.clear();
  field_refs.clear();

  handles.push_back(NestedLoopJoinProjection::FILTER);
  field_refs.push_back(&right_filter);

  handles.push_back(NestedLoopJoinProjection::OUTPUT);
  field_refs.push_back(&right_output);

  RETURN_NOT_OK(proj_maps[1].Init(NestedLoopJoinProjection::INPUT, right_schema, handles,
                                  field_refs));

  return Status::OK();
}

Status NestedLoopJoinSchema::ValidateSchemas(JoinType join_type,
                                             const Schema& left_schema,
                                             const std::vector<FieldRef>& left_output,
                                             const Schema& right_schema,
                                             const std::vector<FieldRef>& right_output) {
  // Check for output fields:
  // 1. Output field refs must match exactly one input field
  // 2. At least one output field
  // 3. Dictionary type is not supported in an output field
  // 4. Left semi/anti join (right semi/anti join) must not output fields from right
  // (left)
  // 5. No name collisions in output fields after adding (potentially empty)
  // suffixes to left and right output
  //
  if (left_output.empty() && right_output.empty()) {
    return Status::Invalid("Join must output at least one field");
  }
  if (join_type == JoinType::LEFT_SEMI || join_type == JoinType::LEFT_ANTI) {
    if (!right_output.empty()) {
      return Status::Invalid(
          join_type == JoinType::LEFT_SEMI ? "Left semi join " : "Left anti-semi join ",
          "may not output fields from right side");
    }
  }
  if (join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI) {
    if (!left_output.empty()) {
      return Status::Invalid(join_type == JoinType::RIGHT_SEMI ? "Right semi join "
                                                               : "Right anti-semi join ",
                             "may not output fields from left side");
    }
  }
  for (size_t i = 0; i < left_output.size() + right_output.size(); ++i) {
    bool left_side = i < left_output.size();
    const FieldRef& field_ref =
        left_side ? left_output[i] : right_output[i - left_output.size()];
    Result<FieldPath> result = field_ref.FindOne(left_side ? left_schema : right_schema);
    if (!result.ok()) {
      return Status::Invalid("No match or multiple matches for output field reference ",
                             field_ref.ToString(), left_side ? " on left " : " on right ",
                             "side of the join");
    }
  }
  return Status::OK();
}

std::shared_ptr<Schema> NestedLoopJoinSchema::MakeOutputSchema(
    const std::string& left_field_name_suffix,
    const std::string& right_field_name_suffix) {
  std::vector<std::shared_ptr<Field>> fields;
  int left_size = proj_maps[0].num_cols(NestedLoopJoinProjection::OUTPUT);
  int right_size = proj_maps[1].num_cols(NestedLoopJoinProjection::OUTPUT);
  fields.resize(left_size + right_size);

  std::unordered_multimap<std::string, int> left_field_map;
  left_field_map.reserve(left_size);
  for (int i = 0; i < left_size; ++i) {
    int side = 0;  // left
    int input_field_id =
        proj_maps[side]
            .map(NestedLoopJoinProjection::OUTPUT, NestedLoopJoinProjection::INPUT)
            .get(i);
    const std::string& input_field_name =
        proj_maps[side].field_name(NestedLoopJoinProjection::INPUT, input_field_id);
    const std::shared_ptr<DataType>& input_data_type =
        proj_maps[side].data_type(NestedLoopJoinProjection::INPUT, input_field_id);
    left_field_map.insert({input_field_name, i});
    // insert left table field
    fields[i] =
        std::make_shared<Field>(input_field_name, input_data_type, true /*nullable*/);
  }

  for (int i = 0; i < right_size; ++i) {
    int side = 1;  // right
    int input_field_id =
        proj_maps[side]
            .map(NestedLoopJoinProjection::OUTPUT, NestedLoopJoinProjection::INPUT)
            .get(i);
    const std::string& input_field_name =
        proj_maps[side].field_name(NestedLoopJoinProjection::INPUT, input_field_id);
    const std::shared_ptr<DataType>& input_data_type =
        proj_maps[side].data_type(NestedLoopJoinProjection::INPUT, input_field_id);
    // search the map and add suffix to the elements which
    // are present both in left and right tables
    auto search_it = left_field_map.equal_range(input_field_name);
    bool match_found = false;
    for (auto search = search_it.first; search != search_it.second; ++search) {
      match_found = true;
      auto left_val = search->first;
      auto left_index = search->second;
      auto left_field = fields[left_index];
      // update left table field with suffix
      fields[left_index] =
          std::make_shared<Field>(input_field_name + left_field_name_suffix,
                                  left_field->type(), true /*nullable*/);
      // insert right table field with suffix
      fields[left_size + i] = std::make_shared<Field>(
          input_field_name + right_field_name_suffix, input_data_type, true /*nullable*/);
    }

    if (!match_found) {
      // insert right table field without suffix
      fields[left_size + i] =
          std::make_shared<Field>(input_field_name, input_data_type, true /*nullable*/);
    }
  }
  return std::make_shared<Schema>(std::move(fields));
}

Status NestedLoopJoinSchema::CollectFilterColumns(std::vector<FieldRef>& left_filter,
                                                  std::vector<FieldRef>& right_filter,
                                                  const Expression& filter,
                                                  const Schema& left_schema,
                                                  const Schema& right_schema) {
  std::vector<FieldRef> nonunique_refs = FieldsInExpression(filter);

  std::unordered_set<FieldPath, FieldPath::Hash> left_seen_paths;
  std::unordered_set<FieldPath, FieldPath::Hash> right_seen_paths;
  for (const FieldRef& ref : nonunique_refs) {
    if (const FieldPath* path = ref.field_path()) {
      std::vector<int> indices = path->indices();
      if (indices[0] >= left_schema.num_fields()) {
        indices[0] -= left_schema.num_fields();
        FieldPath corrected_path(std::move(indices));
        if (right_seen_paths.find(*path) == right_seen_paths.end()) {
          right_filter.push_back(corrected_path);
          right_seen_paths.emplace(std::move(corrected_path));
        }
      } else if (left_seen_paths.find(*path) == left_seen_paths.end()) {
        left_filter.push_back(ref);
        left_seen_paths.emplace(std::move(indices));
      }
    } else {
      ARROW_DCHECK(ref.IsName());
      ARROW_ASSIGN_OR_RAISE(auto left_match, ref.FindOneOrNone(left_schema));
      ARROW_ASSIGN_OR_RAISE(auto right_match, ref.FindOneOrNone(right_schema));
      bool in_left = !left_match.empty();
      bool in_right = !right_match.empty();
      if (in_left && in_right) {
        return Status::Invalid("FieldRef", ref.ToString(),
                               "was found in both left and right schemas");
      } else if (!in_left && !in_right) {
        return Status::Invalid("FieldRef", ref.ToString(),
                               "was not found in either left or right schema");
      }

      ARROW_DCHECK(in_left != in_right);
      auto& target_array = in_left ? left_filter : right_filter;
      auto& target_set = in_left ? left_seen_paths : right_seen_paths;
      auto& target_match = in_left ? left_match : right_match;

      if (target_set.find(target_match) == target_set.end()) {
        target_array.push_back(ref);
        target_set.emplace(std::move(target_match));
      }
    }
  }
  return Status::OK();
}

Result<Expression> NestedLoopJoinSchema::BindFilter(Expression filter,
                                                    const Schema& left_schema,
                                                    const Schema& right_schema,
                                                    ExecContext* exec_context) {
  if (filter.IsBound() || filter == literal(true)) {
    return std::move(filter);
  }
  // Step 1: Construct filter schema
  FieldVector fields;
  auto left_f_to_i =
      proj_maps[0].map(NestedLoopJoinProjection::FILTER, NestedLoopJoinProjection::INPUT);
  auto right_f_to_i =
      proj_maps[1].map(NestedLoopJoinProjection::FILTER, NestedLoopJoinProjection::INPUT);

  auto AppendFieldsInMap = [&fields](const SchemaProjectionMap& map,
                                     const Schema& schema) {
    for (int i = 0; i < map.num_cols; i++) {
      int input_idx = map.get(i);
      fields.push_back(schema.fields()[input_idx]);
    }
  };
  AppendFieldsInMap(left_f_to_i, left_schema);
  AppendFieldsInMap(right_f_to_i, right_schema);
  Schema filter_schema(fields);

  // Step 2: Bind
  ARROW_ASSIGN_OR_RAISE(filter, filter.Bind(filter_schema, exec_context));
  if (filter.type()->id() != Type::BOOL) {
    return Status::TypeError("Filter expression must evaluate to bool, but ",
                             filter.ToString(), " evaluates to ",
                             filter.type()->ToString());
  }
  return std::move(filter);
}

class NestedLoopJoinNode : public ExecNode {
 public:
  NestedLoopJoinNode(ExecPlan* plan, NodeVector inputs,
                     const NestedLoopJoinNodeOptions& join_options,
                     std::shared_ptr<Schema> output_schema,
                     std::unique_ptr<NestedLoopJoinSchema> schema_mgr, Expression filter,
                     std::unique_ptr<NestedLoopJoinImpl> impl)
      : ExecNode(plan, inputs, {"left", "right"},
                 /*output_schema=*/std::move(output_schema)),
        join_type_(join_options.join_type),
        filter_(std::move(filter)),
        schema_mgr_(std::move(schema_mgr)),
        impl_(std::move(impl)) {
    complete_.store(false);
  }

  Status Init() override {
    QueryContext* ctx = plan_->query_context();
    if (ctx->options().use_legacy_batching) {
      return Status::Invalid(
          "The plan was configured to use legacy batching but contained a nestedloop "
          "node "
          "which is incompatible with legacy batching");
    }

    size_t num_threads = GetCpuThreadPoolCapacity();
    RETURN_NOT_OK(impl_->Init(
        ctx, join_type_, num_threads, &(schema_mgr_->proj_maps[0]),
        &(schema_mgr_->proj_maps[1]), filter_,
        [ctx](std::function<Status(size_t, int64_t)> fn,
              std::function<Status(size_t)> on_finished) {
          return ctx->RegisterTaskGroup(std::move(fn), std::move(on_finished));
        },
        [ctx](int task_group_id, int64_t num_tasks) {
          return ctx->StartTaskGroup(task_group_id, num_tasks);
        },
        [this](int64_t, ExecBatch batch) { return this->OutputBatchCallback(batch); },
        [this](int64_t total_num_batches) {
          return this->FinishedCallback(total_num_batches);
        }));

    return Status::OK();
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 2, "NestedLoopJoinNode"));

    std::unique_ptr<NestedLoopJoinSchema> schema_mgr =
        std::make_unique<NestedLoopJoinSchema>();

    const auto& join_options = checked_cast<const NestedLoopJoinNodeOptions&>(options);
    const auto& left_schema = *(inputs[0]->output_schema());
    const auto& right_schema = *(inputs[1]->output_schema());
    // This will also validate input schemas
    if (join_options.output_all) {
      RETURN_NOT_OK(schema_mgr->Init(
          join_options.join_type, left_schema, right_schema, join_options.filter,
          join_options.output_suffix_for_left, join_options.output_suffix_for_right));
    } else {
      RETURN_NOT_OK(schema_mgr->Init(
          join_options.join_type, left_schema, join_options.left_output, right_schema,
          join_options.right_output, join_options.filter,
          join_options.output_suffix_for_left, join_options.output_suffix_for_right));
    }

    ARROW_ASSIGN_OR_RAISE(
        Expression filter,
        schema_mgr->BindFilter(join_options.filter, left_schema, right_schema,
                               plan->query_context()->exec_context()));

    // Generate output schema
    std::shared_ptr<Schema> output_schema = schema_mgr->MakeOutputSchema(
        join_options.output_suffix_for_left, join_options.output_suffix_for_right);

    std::unique_ptr<NestedLoopJoinImpl> impl{new NestedLoopJoinImpl()};
    return plan->EmplaceNode<NestedLoopJoinNode>(
        plan, std::move(inputs), join_options, std::move(output_schema),
        std::move(schema_mgr), std::move(filter), std::move(impl));
  }

  const char* kind_name() const override { return "NestedLoopJoinNode"; }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    size_t thread_index = plan_->query_context()->GetThreadIndex();
    int side = (input == inputs_[0]) ? 0 : 1;

    if (side == 0) {
      ARROW_RETURN_NOT_OK(OnOuterSideBatch(thread_index, std::move(batch)));
    } else {
      ARROW_RETURN_NOT_OK(OnInnerSideBatch(thread_index, std::move(batch)));
    }

    if (batch_count_[side].Increment()) {
      if (side == 0) {
        return OnOuterSideFinished(thread_index);
      } else {
        return OnInnerSideFinished(thread_index);
      }
    }
    return Status::OK();
  }

  Status InputFinished(ExecNode* input, int total_batches) override {
    ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());
    size_t thread_index = plan_->query_context()->GetThreadIndex();
    int side = (input == inputs_[0]) ? 0 : 1;

    if (batch_count_[side].SetTotal(total_batches)) {
      if (side == 0) {
        return OnOuterSideFinished(thread_index);
      } else {
        return OnInnerSideFinished(thread_index);
      }
    }
    return Status::OK();
  }

  Status StartProducing() override {
    START_COMPUTE_SPAN(span_, std::string(kind_name()) + ":" + label(),
                       {{"node.label", label()},
                        {"node.detail", ToString()},
                        {"node.kind", kind_name()}});
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    EVENT(span_, "PauseProducing");
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    EVENT(span_, "ResumeProducing");
  }

  Status StopProducingImpl() override {
    bool expected = false;
    if (complete_.compare_exchange_strong(expected, true)) {
      impl_->Abort([]() {});
    }
    return Status::OK();
  }

 protected:
  std::string ToStringExtra(int indent = 0) const override { return "NestedLoopJoin"; }

 private:
  Status OnInnerSideBatch(size_t thread_index, ExecBatch batch) {
    if (batch.length) {
      inner_accumulator_.InsertBatch(std::move(batch));
    }
    return Status::OK();
  }

  Status OnOuterSideBatch(size_t thread_index, ExecBatch batch) {
    if (batch.length) {
      outer_accumulator_.InsertBatch(std::move(batch));
    }
    return Status::OK();
  }

  Status OnInnerSideFinished(size_t thread_index) { return Status::OK(); }

  Status OnOuterSideFinished(size_t thread_index) {
    return impl_->StartCrossProduct(thread_index, std::move(inner_accumulator_),
                                    std::move(outer_accumulator_));
  }

  Status FinishedCallback(int64_t total_num_batches) {
    bool expected = false;
    if (complete_.compare_exchange_strong(expected, true)) {
      return output_->InputFinished(this, static_cast<int>(total_num_batches));
    }
    return Status::OK();
  }

  Status OutputBatchCallback(ExecBatch batch) {
    return output_->InputReceived(this, std::move(batch));
  }

 protected:
  arrow::util::tracing::Span span_;

 private:
  AtomicCounter batch_count_[2];
  std::atomic<bool> complete_;
  JoinType join_type_;
  Expression filter_;
  std::unique_ptr<NestedLoopJoinSchema> schema_mgr_;
  std::unique_ptr<NestedLoopJoinImpl> impl_;
  util::AccumulationQueue inner_accumulator_;
  util::AccumulationQueue outer_accumulator_;
};

namespace internal {

void RegisterNestedLoopJoinNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("nestedloopjoin", NestedLoopJoinNode::Make));
}

}  // namespace internal

}  // namespace acero
}  // namespace arrow